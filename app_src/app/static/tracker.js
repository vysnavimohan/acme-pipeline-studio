/* Tracker page — Alpine.js controller.

   State-gated UI:
     queued                     → only request details + Initiate button
     in_progress / rerunning    → full view, running stage shows a spinner
     pending_review             → flow PAUSED at await_human (yellow card);
                                  polling stops — nothing happens until the
                                  human clicks approve (launches a gate run)
     blocked_on_pr_approval     → red banner with Re-check
     deployed / failed / rejected → terminal, no animations

   Stage flow at the top renders as continuous chevron arrows. Each agent
   node in the vertical timeline can take a comment and/or be re-run from.
   Every execution (full / gate / from_step) is a Databricks Job run with
   an MLflow run + traces — linked in the "Experiment runs" card.
*/

// The data-engineering tracker flow ends at the STAGING deploy. Promotion to
// prod is handled separately by the platform team in Release Management.
// Base flow (pipeline-only requests). deploy_app is appended dynamically by the
// AGENT_NODES getter when the selected request builds a Databricks App, so a
// pure-pipeline request never shows an app step it will not run.
const BASE_AGENT_NODES = [
  "spec_critic", "source_profiler", "pipeline_generator",
  "standards_validator", "test_generator", "deploy_ingestion", "deploy_dev",
  "branch_and_pr", "pr_reviewer", "await_human",
  "verify_pr_approved", "deploy_staging",
];

// Acme data-product requests (engine = genie_code). The middle of the flow is the approved plan
// (metric view, DQ, Genie Agent, dashboard: only what was asked, see tracker.genieNodes()).
const GENIE_HEAD = ["spec_critic", "source_profiler"];
const GENIE_TAIL = ["branch_and_pr", "pr_reviewer", "await_human", "verify_pr_approved",
                    "deploy_staging", "deploy_prod"];
const GENIE_STAGE_SHORT = {
  spec_critic: "spec check", source_profiler: "profile sources", deploy_ingestion: "pipeline",
  pipeline_generator: "metric view",
  standards_validator: "DQ rules", test_generator: "Genie Agent", deploy_dev: "dashboard",
  branch_and_pr: "PR", pr_reviewer: "AI review", await_human: "approval", verify_pr_approved: "verify",
  deploy_staging: "staging", deploy_prod: "prod",
};
const GENIE_NODE_LABEL = {
  spec_critic: "spec check", source_profiler: "profile sources",
  deploy_ingestion: "pipeline (Genie Code)", pipeline_generator: "metric view (Genie Code)", standards_validator: "DQ rules",
  test_generator: "Genie Agent (Genie Code)", deploy_dev: "dashboard (Genie Code)",
  branch_and_pr: "PR", pr_reviewer: "AI review", await_human: "approval",
  verify_pr_approved: "verify", deploy_staging: "staging", deploy_prod: "prod",
};

const STAGE_SHORT = {
  spec_critic: "spec critic",
  source_profiler: "profiler",
  pipeline_generator: "generator",
  standards_validator: "standards",
  test_generator: "tests",
  deploy_ingestion: "ingest (LFC)",
  deploy_dev: "deploy dev",
  branch_and_pr: "branch + PR",
  pr_reviewer: "reviewer",
  await_human: "approval",
  verify_pr_approved: "verify PR",
  deploy_staging: "deploy stg",
  deploy_app: "deploy app",
};

function tracker() {
  return {
    // Flow nodes for the chevron stepper. Appends deploy_app for app-building
    // requests so the App deploy shows as its own step in the flow.
    get AGENT_NODES() {
      if (this.isGenie) return this.genieNodes();
      return this.hasApp ? [...BASE_AGENT_NODES, "deploy_app"] : BASE_AGENT_NODES;
    },
    requests: [],
    selected: null,
    events: [],
    polling: false,
    pollHandle: null,
    cyPipeline: null,
    lastEventSeq: 0,

    // per-step UI state
    expanded: {},
    commentDrafts: {},
    commentSaved: {},
    rerunBusy: {},

    busyInit: false,
    initiateResult: "",
    reviewResult: "",

    similar: [],
    similarBusy: false,

    specView: {},
    assetUrls: {},
    mlflowRuns: [],
    // True when the selected request builds a Databricks App (adds the
    // deploy_app node to the flow). Derived in _refreshDerived.
    hasApp: false,
    // "How this was grilled" — intake Q&A transcript captured at submit.
    grillTranscript: [],

    // Acme data-product (engine = genie_code) state, derived in _refreshDerived.
    isGenie: false,
    genieOutputs: [],     // need.outputs: "dashboard" and/or "genie_agent"
    genieHasMv: false,    // the plan builds a new metric view
    plan: [],
    genieRows: [],
    benchRuns: [],
    prodRelease: null,
    stageAsset: null,
    dqRules: [],
    dqCounts: {},
    aiReview: null,
    rejectNote: "",
    // pipeline runs per env (pipeline_run events): {dev: {...}, stage: {...}, prod: {...}}
    pipeRuns: {},
    isExample: false,   // a seeded example (labelled), not a live run

    // In-app PR mock (no external Git): the generated "PR" + AI review rounds.
    mockPr: null,
    prReviews: [],
    prModalOpen: false,

    // KPI strip (computed from the request list)
    kpis: [],
    // assignee picker (supervisor assigns an owner when initiating)
    team: [],
    assigneeChoice: "Vaishnavi Mohan",   // preselected so the plan approval is one click (the lead can change it)
    // "spinning up the job" indicator — true between a manual action
    // (initiate / approve / re-check) and the first new event arriving.
    launching: false,

    async init() {
      await this.loadTeam();
      await this.loadList();
      const params = new URLSearchParams(location.search);
      const qsId = params.get("request_id") || params.get("request");
      const initialId = qsId || (this.requests[0] && this.requests[0].request_id);
      if (initialId) await this.select(initialId);
      this.startPolling();
    },

    async loadTeam() {
      try {
        const r = await fetch("/api/requests/meta/team");
        if (r.ok) this.team = (await r.json()).members || [];
      } catch (e) { /* non-fatal */ }
    },

    _computeKpis() {
      const c = (fn) => this.requests.filter(fn).length;
      const isReleased = (r) => (r.release_status || "") === "released";
      this.kpis = [
        {key: "queued",   label: "Queued",     count: c(r => r.final_status === "queued"),      color: "text-slate-700"},
        {key: "inprog",   label: "In progress",count: c(r => r.final_status === "in_progress"), color: "text-amber-600"},
        {key: "review",   label: "Awaiting approval", count: c(r => r.final_status === "pending_review"), color: "text-blue-600"},
        // Blocked = the build stopped at a gate and needs a decision (blocked_on_pr_approval). It is
        // counted on its own, not under Failed. Deployed = the build finished (deployed or done); a
        // finished request that was also released to prod is counted in both cards.
        {key: "blocked",  label: "Blocked",    count: c(r => r.final_status === "blocked_on_pr_approval"), color: "text-rose-500"},
        {key: "staged",   label: "Deployed",   count: c(r => ["deployed", "done"].includes(r.final_status)), color: "text-teal-700"},
        {key: "released", label: "Released to prod", count: c(isReleased), color: "text-emerald-600"},
        {key: "failed",   label: "Failed",     count: c(r => ["failed", "rejected"].includes(r.final_status)), color: "text-rose-600"},
      ];
    },

    displayStatus(r) {
      const s = r.final_status || "pending";
      if (s === "pending_review") return "awaiting approval";
      if (s === "deployed") {
        return (r.release_status === "released") ? "released" : "deployed";
      }
      return s.replace(/_/g, " ");
    },

    // Release lifecycle label for the selected request (deployed → staging/prod).
    releaseLabel() {
      if (!this.selected || this.selected.final_status !== "deployed") return "";
      const rs = this.selected.release_status || "staged";
      if (rs === "released") return "released to prod";
      if (rs === "releasing") return "promoting to prod…";
      return "deployed";
    },
    releaseKey() {
      if (!this.selected) return "";
      const rs = this.selected.release_status || "staged";
      return (this.selected.final_status === "deployed") ? rs : "";
    },

    statusKey() {
      if (!this.selected) return "";
      return (this.selected.final_status || "pending").replace(/[ -]/g, "_");
    },

    statusLabel() {
      const map = {
        queued: "queued",
        in_progress: "in progress",
        deployed: "deployed",
        failed: "failed",
        rejected: "rejected",
        blocked_on_pr_approval: this.isGenie ? "blocked" : "blocked on PR approval",
        pending_review: "awaiting approval",
        done: "done",
      };
      const key = this.statusKey();
      return map[key] || key;
    },

    // Genie flow nodes: head, then the approved plan's build steps in order, then the release tail.
    genieNodes() {
      const mid = [];
      for (const st of (this.plan || [])) {
        if (st.node && !mid.includes(st.node)) mid.push(st.node);
      }
      if (!mid.includes("standards_validator")) mid.unshift("standards_validator");
      return [...GENIE_HEAD, ...mid, ...GENIE_TAIL];
    },
    wantsAgent() { return this.genieOutputs.includes("genie_agent"); },
    wantsDash() { return this.genieOutputs.includes("dashboard"); },
    // "metric view + Genie Agent + dashboard" for what this request builds
    outputsLabel() {
      const parts = [];
      if (this.genieHasMv) parts.push("metric view");
      if (this.wantsAgent()) parts.push("Genie Agent");
      if (this.wantsDash()) parts.push("dashboard");
      return parts.join(" + ") || "nothing";
    },
    // Labels of the assets that exist in dev (for the approval card)
    devBuiltLabel() {
      const names = this.genieRows.filter(r => r.env === "dev" && !r.table).map(r => r.label);
      return names.length ? names.join(", ") : "nothing";
    },

    isQueued() {
      if (!this.selected) return true;
      return (this.selected.final_status || "pending") === "queued";
    },

    startPolling() {
      this.stopPolling();
      const tick = async () => {
        try {
          this.polling = true;
          await this.loadList();
          if (this.selected) await this.loadOne();
        } finally {
          this.polling = false;
        }
        // pending_review is a PAUSED state — the backend does nothing until
        // the human clicks, so polling would be wasted requests.
        const stoppedStates = ["deployed", "failed", "rejected", "queued",
                               "blocked_on_pr_approval", "pending_review", "done"];
        const releasing = this.selected &&
          (this.selected.release_status || "") === "releasing";
        const stopped = this.selected && !releasing &&
          stoppedStates.includes(this.selected.final_status || "");
        if (!stopped) {
          this.pollHandle = setTimeout(tick, 3000);
        } else {
          this.pollHandle = null;
        }
      };
      this.pollHandle = setTimeout(tick, 3000);
    },

    stopPolling() {
      if (this.pollHandle) { clearTimeout(this.pollHandle); this.pollHandle = null; }
      this.polling = false;
    },

    async loadList() {
      const r = await fetch("/api/requests");
      if (r.ok) {
        const data = await r.json();
        this.requests = data.requests || [];
        this._computeKpis();
      }
    },

    async select(id) {
      this.lastEventSeq = 0;
      this.events = [];
      this.similar = [];
      const r = await fetch("/api/requests/" + id);
      if (!r.ok) return;
      const data = await r.json();
      this.selected = data.request;
      this.plan = data.plan || [];
      this.events = data.events || [];
      this.lastEventSeq = this.events.reduce((m, e) => Math.max(m, e.seq || 0), 0);
      this._refreshDerived();
      this.renderLineage();
      this.startPolling();
      // Similar-request check only matters at the queued (pre-initiate) step.
      if ((this.selected.final_status || "") === "queued") this.loadSimilar(id);
    },

    async loadSimilar(id) {
      this.similarBusy = true;
      try {
        const r = await fetch("/api/requests/" + id + "/similar");
        if (!this.selected || this.selected.request_id !== id) return;
        if (r.ok) {
          const data = await r.json();
          // surface only meaningful matches (skip 'low' noise)
          this.similar = (data.candidates || []).filter(
            c => c.similarity && c.similarity !== "low");
        }
      } catch (e) { /* non-fatal */ } finally {
        this.similarBusy = false;
      }
    },

    async loadOne() {
      if (!this.selected) return;
      // Capture the id at call time. If the user clicks a different request
      // while we're awaiting a response, drop the response on the floor so
      // we don't overwrite their new selection with stale data.
      const expectedId = this.selected.request_id;
      const r1 = await fetch("/api/requests/" + expectedId);
      if (!this.selected || this.selected.request_id !== expectedId) return;   // selection changed mid-flight
      if (r1.ok) {
        const data = await r1.json();
        if (!this.selected || this.selected.request_id !== expectedId) return;
        this.selected = data.request;
        this.plan = data.plan || this.plan;
      }
      const r2 = await fetch("/api/requests/" + expectedId + "/events?since_seq=" + this.lastEventSeq);
      if (!this.selected || this.selected.request_id !== expectedId) return;
      if (r2.ok) {
        const data = await r2.json();
        if (!this.selected || this.selected.request_id !== expectedId) return;
        const newEvents = data.events || [];
        if (newEvents.length) {
          this.events = this.events.concat(newEvents);
          this.lastEventSeq = this.events.reduce((m, e) => Math.max(m, e.seq || 0), 0);
          // The next stage has started emitting — stop the "spinning up" hint.
          this.launching = false;
        }
      }
      this._refreshDerived();
    },

    _details(ev) {
      let d = ev.details_json !== undefined ? ev.details_json : ev.details;
      if (typeof d === "string") {
        try { d = JSON.parse(d); } catch (e) { d = {}; }
      }
      return d || {};
    },

    _refreshDerived() {
      // Decode spec
      let spec = {};
      try { spec = JSON.parse(this.selected.spec_json || "{}"); } catch (e) {}
      // Does this request build an App? (spec flag, or any app step emitted).
      this.hasApp = !!(spec.app && spec.app.enabled) ||
        this.events.some(ev => ["app_generator", "app_standards_validator", "deploy_app"].includes(ev.step));
      const flat = {
        source_system: spec.source?.system,
        source_tables: spec.source?.tables || [],
        source_incremental_column: spec.source?.incremental_column,
        target_catalog: spec.target?.catalog,
        target_schema: spec.target?.target_schema,
        target_table: spec.target?.table,
        target_write_mode: spec.target?.write_mode,
        transform_description: spec.transform?.description,
        validation_not_null: spec.validation?.not_null || [],
        validation_unique: spec.validation?.unique || [],
        sla_mode: spec.sla?.mode,
        sla_freshness_minutes: spec.sla?.freshness_minutes,
        sla_schedule_cron: spec.sla?.schedule_cron,
        required_approvers: spec.required_approvers || [],
      };
      this.specView = flat;

      // Asset URLs — derive from the deploy_result details on the latest
      // deploy_dev / deploy_staging / deploy_prod success event.
      // Derive the workspace host from any absolute URL the events already carry
      // (pipelines emit a full pipeline_url) so links never point at a stale,
      // hardcoded workspace; fall back to a constant only if none is present.
      let host = "https://fevm-pipeline-studio.cloud.databricks.com";
      for (const ev of this.events) {
        const d = this._details(ev);
        const drp = d.deploy_result || {};
        const probe = drp.pipeline_url || ((drp.results || [])[0] || {}).pipeline_url ||
          (d.ingestion_result || {}).ingestion_pipeline_url || d.run_url || d.pipeline_url;
        if (probe) { try { host = new URL(probe).origin; break; } catch (e) {} }
      }
      const ENV_BY_STEP = {deploy_dev: "dev", deploy_staging: "stage", deploy_prod: "prod"};
      const exploreUrl = (fq) => {
        const p = String(fq).replace(/`/g, "").split(".");
        return p.length === 3 ? `${host}/explore/data/${p[0]}/${p[1]}/${p[2]}` : null;
      };
      const a = {};
      for (const ev of this.events) {
        const prefix = ENV_BY_STEP[ev.step];
        if (!prefix) continue;
        if (ev.status !== "succeeded") continue;
        const d = this._details(ev);
        // Older / hand-seeded events store deploy details flat rather than
        // nested under deploy_result — accept both.
        const dr = d.deploy_result || d;
        // Multi-dataset builds emit results[] (one SDP pipeline + silver PER feed);
        // single builds emit the flat pipeline_id/silver_table. Handle both, and
        // show a "+N" hint when a request spans several feeds.
        const results = Array.isArray(dr.results) ? dr.results.filter(Boolean) : [];
        const more = (n) => (n > 1 ? ` +${n - 1}` : "");
        if (results.length) {
          const r0 = results[0];
          if (r0.pipeline_id || r0.pipeline_url) {
            // One consolidated request pipeline owns all the silvers (+gold), so
            // only show a "+N" hint if there are genuinely several distinct pipelines.
            const pipeCount = new Set(results.map(r => r.pipeline_id).filter(Boolean)).size || 1;
            a[`${prefix}_pipeline`] = r0.pipeline_url || `${host}/pipelines/${r0.pipeline_id}`;
            a[`${prefix}_pipeline_label`] = (r0.pipeline_name || String(r0.pipeline_id).slice(0, 8)) + more(pipeCount);
          }
          const st = r0.silver_table || (dr.silver_tables || [])[0];
          if (st) {
            a[`${prefix}_table`] = exploreUrl(st);
            a[`${prefix}_table_label`] = String(st).replace(/`/g, "").split(".").pop() + more(results.length);
          }
        } else {
          if (dr.pipeline_id) {
            a[`${prefix}_pipeline`] = dr.pipeline_url || `${host}/pipelines/${dr.pipeline_id}`;
            a[`${prefix}_pipeline_label`] = dr.pipeline_name || dr.pipeline_id.slice(0, 8);
          }
          if (dr.silver_table) {
            a[`${prefix}_table`] = exploreUrl(dr.silver_table);
            a[`${prefix}_table_label`] = String(dr.silver_table).replace(/`/g, "").split(".").pop();
          }
        }
        // Scheduled Job is created on staging + prod deploys.
        if ((prefix === "stage" || prefix === "prod") && dr.job_id) {
          a.job = dr.job_url || `${host}/jobs/${dr.job_id}`;
          a.job_label = dr.job_schedule_cron ? `cron \`${dr.job_schedule_cron}\`` : (dr.job_name || dr.job_id);
        }
      }
      // Lakeflow ingestion pipeline (dev) — emitted by deploy_ingestion. Multi
      // builds emit ingestion_result.datasets[] (one shared consolidated ingest
      // pipeline); single builds emit ingestion_pipeline_id/url directly.
      for (const ev of this.events) {
        const d = this._details(ev);
        const ing = d.ingestion_result;
        if (!ing) continue;
        if (ing.ingestion_pipeline_id || ing.ingestion_pipeline_url) {
          a.ingest = ing.ingestion_pipeline_url || `${host}/pipelines/${ing.ingestion_pipeline_id}`;
          a.ingest_label = ing.ingestion_pipeline_name || "lakeflow connect";
        } else if (ing.ingestion_pipeline_name || (ing.datasets || []).length) {
          const ds = ing.datasets || [];
          const nm = ing.ingestion_pipeline_name || (ds[0] || {}).ingestion_pipeline_name || "file ingest";
          a.ingest = `${host}/pipelines`;
          a.ingest_label = nm + (ds.length > 1 ? ` · ${ds.length} feeds` : "");
        }
      }
      // Governed metric views + linked Genie spaces (from the spec). These are
      // the downstream governed consumers — surface the first of each as an asset
      // shortcut (Explore for the metric view, the Genie room for the space) so
      // the Assets panel shows them alongside the pipeline/tables, matching the
      // lineage graph's metric + Genie nodes.
      const mvs = spec.metric_views || [];
      if (mvs.length) {
        const mv0 = mvs[0].name || mvs[0].table;
        if (mv0) {
          a.metric_view = exploreUrl(mv0) || `${host}/explore/data`;
          a.metric_view_label = String(mv0).replace(/`/g, "").split(".").pop()
                                + (mvs.length > 1 ? ` +${mvs.length - 1}` : "");
        }
      }
      const gspaces = spec.genie_spaces || [];
      if (gspaces.length) {
        const g0 = gspaces[0];
        if (g0.id) {
          a.genie = `${host}/genie/rooms/${g0.id}`;
          a.genie_label = (g0.title || g0.name || "Genie")
                          + (gspaces.length > 1 ? ` +${gspaces.length - 1}` : "");
        }
      }
      this.assetUrls = a;

      // MLflow runs — every agent execution emits a kind="mlflow_run" event
      // (and the job launch emits kind="job_run"). Pair them by mode so each
      // row links to the MLflow run, its traces, AND the job run.
      const runs = [];
      const pendingJobByMode = {};
      for (const ev of this.events) {
        const d = this._details(ev);
        if (d.kind === "job_run") {
          pendingJobByMode[d.mode || "full"] = d.run_url;
        } else if (d.kind === "mlflow_run") {
          const mode = d.mode || "full";
          runs.push({
            mode,
            run_id: d.run_id,
            run_url: d.run_url,
            experiment_url: d.experiment_url,
            traces_url: d.traces_url,
            job_run_url: pendingJobByMode[mode] || null,
            when: (ev.timestamp || "").toString().slice(0, 16).replace("T", " "),
          });
          delete pendingJobByMode[mode];
        }
      }
      this.mlflowRuns = runs.reverse();   // newest first

      // In-app PR mock: latest mock_pr event + all review-round comments.
      let mockPr = null;
      const reviews = [];
      for (const ev of this.events) {
        const d = this._details(ev);
        if (d.kind === "mock_pr") {
          mockPr = {
            number: d.pr_number, title: d.pr_title, body: d.pr_body,
            branch: d.branch, files: d.files || {}, file_count: d.file_count,
            artifacts: d.artifacts || null,
          };
        } else if (d.kind === "pr_review_comment") {
          reviews.push({round: d.round, verdict: d.verdict, comment_md: d.comment_md,
                        checklist: d.checklist || []});
        }
      }
      this.mockPr = mockPr;
      this.prReviews = reviews;

      // "How this was grilled" — the intake conversation captured at submit
      // (kind="intake_transcript"). Render it as user → agent Q&A so the
      // grilling is visible on the tracker.
      let grill = [];
      for (const ev of this.events) {
        const d = this._details(ev);
        if (d.kind === "intake_transcript" && Array.isArray(d.history)) {
          grill = d.history
            .filter(t => t && (t.content || "").trim() &&
                         (t.role === "user" || t.role === "assistant"))
            .map(t => ({role: t.role, content: t.content}));
        }
      }
      this.grillTranscript = grill;

      // ---- Acme data-product (engine = genie_code) state ----------------------
      this.isGenie = !!(spec.data_product && spec.data_product.engine === "genie_code");
      const dpv = spec.data_product || {};
      this.genieOutputs = Array.isArray((dpv.need || {}).outputs) ? dpv.need.outputs : [];
      this.genieHasMv = !!((dpv.metric_view || {}).name);
      const LABEL = {pipeline: "Pipeline", metric_view: "Metric view", genie_agent: "Genie Agent", dashboard: "Dashboard"};
      const rowsByKey = {};
      const benchRuns = [];
      let prodRel = null, stageEv = null, review = null, dqEv = null;
      const pipe = {};
      let example = false;
      for (const ev of this.events) {
        const d = this._details(ev);
        if (d.kind === "seed_example") example = true;
        if (d.kind === "pipeline_run" && d.env) {
          pipe[d.env] = d;
        } else if (d.kind === "genie_code_run" && d.asset) {
          const key = d.asset + ":" + (d.env || "dev");
          rowsByKey[key] = Object.assign({}, rowsByKey[key] || {}, d);
        } else if (d.kind === "benchmark_results") {
          benchRuns.push(d);
        } else if (d.kind === "prod_release") {
          prodRel = d;
        } else if (d.kind === "stage_asset" && d.stage_asset) {
          stageEv = d;
        } else if (d.kind === "dq_check") {
          dqEv = d;
        } else if (d.kind === "pr_review_comment") {
          review = d;
        }
      }
      const rows = [];
      for (const key of Object.keys(rowsByKey)) {
        const r = rowsByKey[key];
        let state = "in_progress", statusLabel = "running";
        if (r.state === "TERMINATED" && r.verified) { state = "succeeded"; statusLabel = r.fallback ? "created by app" : "verified"; }
        else if (r.state === "TERMINATED" && !r.verified) { state = "failed"; statusLabel = "not verified"; }
        else if (r.state === "FALLBACK") { statusLabel = "fallback: creating"; }
        rows.push({key, label: LABEL[r.asset] || r.asset, env: r.env || "dev", state, statusLabel,
                   asset_url: r.asset_url || "", conversation_url: r.conversation_url || "",
                   job_run_url: r.job_run_url || "", fallback: !!r.fallback, table: false});
      }
      // Pipeline rows: the stage and prod pipelines (the dev one is a genie_code_run row above), and the
      // tables each pipeline run created with their row counts.
      for (const env of ["stage", "prod"]) {
        const pr = pipe[env];
        if (!pr) continue;
        rows.push({key: env + ":pipeline", label: "Pipeline (" + (env === "stage" ? "staging" : "prod") + ")",
                   env, state: "succeeded", statusLabel: pr.update_state === "COMPLETED" ? "run complete" : "run",
                   asset_url: pr.pipeline_url || "", conversation_url: "", job_run_url: "",
                   fallback: !!pr.fallback, table: false});
      }
      for (const env of ["dev", "stage", "prod"]) {
        const pr = pipe[env];
        if (!pr) continue;
        for (const t of (pr.tables || [])) {
          rows.push({key: env + ":table:" + t.name, label: "Table " + t.name, env, state: "succeeded",
                     statusLabel: (t.rows == null ? "table" : Number(t.rows).toLocaleString() + " rows"),
                     asset_url: t.url || "", conversation_url: "", job_run_url: "", fallback: false, table: true});
        }
      }
      if (stageEv) rows.push({key: "stage:metric_view", label: "Metric view (staging)", env: "stage",
                              state: "succeeded", statusLabel: "staged", asset_url: stageEv.asset_url || "",
                              conversation_url: "", job_run_url: "", fallback: false, table: false});
      if (prodRel && prodRel.assets) {
        for (const k of ["metric_view", "genie_agent", "dashboard"]) {
          const a = prodRel.assets[k];
          if (!a) continue;
          rows.push({key: "prod:" + k, label: LABEL[k] + " (prod)", env: "prod", state: "succeeded",
                     statusLabel: "released", asset_url: a.published_url || a.url || "",
                     conversation_url: "", job_run_url: "", fallback: false, table: false});
        }
      }
      this.genieRows = rows;
      this.pipeRuns = pipe;
      this.isExample = example;
      this.benchRuns = benchRuns.slice().reverse();
      this.prodRelease = prodRel;
      this.stageAsset = stageEv;
      this.dqRules = dqEv ? (dqEv.rules || []) : [];
      this.dqCounts = dqEv ? (dqEv.counts || {}) : {};
      this.aiReview = review ? {verdict: review.verdict, comment_md: review.comment_md,
                                checklist: review.checklist || []} : null;
    },

    // Benchmark table to show on "Use it": prod run if present, else the dev run from staging.
    benchTable() {
      return this.benchRuns.find(b => b.env === "prod") || this.benchRuns.find(b => b.env === "dev") || null;
    },
    benchLabel(row) {
      if (row.pass === null || row.pass === undefined) return "manual (no numbers)";
      return row.pass ? "pass" : "fail";
    },
    benchClass(row) {
      if (row.pass === null || row.pass === undefined) return "pill-pending";
      return row.pass ? "pill-succeeded" : "pill-failed";
    },
    aiVerdictLabel() {
      if (!this.aiReview) return "pending";
      return this.aiReview.verdict === "request_changes" ? "request changes" : "approve";
    },
    aiFindingCount() {
      if (!this.aiReview) return 0;
      return (this.aiReview.checklist || []).filter(c => !c.pass).length;
    },
    prChecklist() {
      return (this.aiReview && this.aiReview.checklist) || [];
    },
    get prArtifacts() {
      return (this.mockPr && this.mockPr.artifacts) || null;
    },

    isMockPr() {
      return !!(this.selected && (this.selected.pr_url || "").startsWith("app://"));
    },
    openPrModal() { this.prModalOpen = true; },
    closePrModal() { this.prModalOpen = false; },
    // very small markdown → HTML for the review comments (headings, bold, lists, code)
    mdToHtml(md) {
      if (!md) return "";
      let h = md.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
      h = h.replace(/^### (.*)$/gm, "<h4>$1</h4>")
           .replace(/^## (.*)$/gm, "<h3>$1</h3>")
           .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
           .replace(/`([^`]+)`/g, "<code>$1</code>")
           .replace(/^- (.*)$/gm, "<li>$1</li>")
           .replace(/\n{2,}/g, "<br><br>").replace(/\n/g, "<br>");
      return h.replace(/(<li>.*?<\/li>)(<br>)?/g, "$1");
    },

    prettyName(node) {
      if (this.isGenie) return GENIE_NODE_LABEL[node] || node.replace(/_/g, " ");
      return node.replace(/_/g, " ");
    },

    stageShort(node) {
      if (this.isGenie) return GENIE_STAGE_SHORT[node] || node.replace(/_/g, " ");
      return STAGE_SHORT[node] || node.replace(/_/g, " ");
    },

    // Link for a timeline event: job run, Genie Code conversation, or the built asset.
    evLink(ev) {
      const d = this._details(ev);
      return d.job_run_url || d.conversation_url || d.asset_url || "";
    },

    evLinkLabel(ev) {
      const d = this._details(ev);
      if (d.job_run_url) return "job run ↗";
      if (d.conversation_url) return "Genie Code ↗";
      if (d.asset_url) return "open asset ↗";
      return "";
    },

    stepEvents(node) {
      // Collect events for this node, including human_comment events
      const out = [];
      for (const ev of this.events) {
        if (ev.step !== node) continue;
        const d = this._details(ev);
        let kind = ev.status;
        if (d.kind === "human_comment") kind = "human_comment";
        out.push({...ev, _kind: kind});
      }
      return out;
    },

    displayedEvents(node) {
      const all = this.stepEvents(node);
      if (this.expanded[node] || all.length <= 2) return all;
      return all.slice(-2);  // collapsed = last 2
    },

    toggleExpand(node) {
      this.expanded[node] = !this.expanded[node];
      this.expanded = {...this.expanded};
    },

    // Latest raw status seen for a node (or 'pending' if no events yet)
    stepStatus(node) {
      let last = "pending";
      for (const ev of this.events) {
        if (ev.step !== node) continue;
        if (this._details(ev).kind === "human_comment") continue;   // comments don't change step status
        last = ev.status;
      }
      return last;
    },

    // Render kind for the chevron stage + timeline dot:
    //   succeeded | failed | running | awaiting | pending | skipped
    stageKind(node) {
      const s = this.stepStatus(node);
      const fs = this.selected ? (this.selected.final_status || "") : "";
      if (s === "in_progress") {
        // await_human "in progress" means the flow is PAUSED on a human.
        if (node === "await_human" && fs === "pending_review") return "awaiting";
        // a terminal/paused request can't have a genuinely running step —
        // show interrupted steps as pending rather than spinning forever
        if (["deployed", "failed", "rejected", "pending_review",
             "blocked_on_pr_approval"].includes(fs)) {
          return node === "await_human" ? "awaiting" : "pending";
        }
        return "running";
      }
      return s;   // succeeded | failed | skipped | pending
    },

    stepCardClass(node) {
      const k = this.stageKind(node);
      if (k === "running" || k === "awaiting") return "active";
      if (k === "failed") return "failed";
      return "";
    },

    renderLineage() {
      const container = document.getElementById("cy-pipeline");
      if (!container || !this.selected) return;
      let spec = null;
      try { spec = JSON.parse(this.selected.spec_json || "{}"); } catch (e) {}
      if (!spec) {
        container.innerHTML = '<div class="text-sm text-slate-500 p-4">No spec available.</div>';
        return;
      }
      if (spec.data_product && spec.data_product.engine === "genie_code") {
        this._renderGenieLineage(container, spec);
        return;
      }
      const source = spec.source || {};
      const target = spec.target || {};
      const system = source.system || "?";
      const silverSchema = target.target_schema || "silver";

      // Per-dataset list. File (Auto Loader) sources carry source.files[] — each
      // dataset has its OWN silver_table, so it's 1 bronze → 1 silver PER feed.
      // Legacy table/CDC sources carry source.tables[] collapsing into the single
      // target.table. Either way we build one src→bronze→silver chain per dataset.
      const files = source.files || [];
      let datasets;
      if (files.length) {
        datasets = files.map(f => ({
          name: f.dataset || f.silver_table || "dataset",
          silver: f.silver_table || target.table || "silver",
        }));
      } else {
        const tables = source.tables || [];
        datasets = (tables.length ? tables : [target.table || "?"]).map(t => ({
          name: t, silver: target.table || "?",
        }));
      }

      const nodes = [];
      const edges = [];
      const silverNodeByName = {};   // silver table name → node id (so gold edges connect)

      datasets.forEach((d, i) => {
        const parts = String(d.name).split(".");
        const leaf = parts[parts.length - 1];
        const srcId = `src${i}`, bId = `b${i}`, sId = `s${i}`;
        nodes.push({data: {id: srcId, label: d.name, kind: "src"}});
        nodes.push({data: {id: bId, label: `bronze.${leaf}`, kind: "bronze"}});
        edges.push({data: {id: `e_sb${i}`, source: srcId, target: bId}});
        nodes.push({data: {id: sId, label: `${silverSchema}.${d.silver}`, kind: "silver"}});
        edges.push({data: {id: `e_bs${i}`, source: bId, target: sId}});
        silverNodeByName[d.silver] = sId;
        silverNodeByName[String(d.silver).split(".").pop()] = sId;
      });

      // Gold facts / dimensions — each fans IN from its source silver table(s),
      // so cross-feed joins (e.g. fact_customer_engagement over 3 silvers) show
      // multiple edges converging on one gold node.
      const goldNodeByName = {};   // gold name → node id (so metric views can link)
      (spec.gold_facts || []).forEach((g, gi) => {
        const gName = g.name || g.table || `gold_${gi}`;
        const isDim = /^dim/i.test(gName) || g.kind === "dimension";
        const gId = `g${gi}`;
        nodes.push({data: {id: gId, label: `gold.${gName}`, kind: isDim ? "dim" : "gold"}});
        goldNodeByName[gName] = gId;
        goldNodeByName[String(gName).split(".").pop()] = gId;
        let linked = 0;
        (g.sources || []).forEach((s) => {
          const sid = silverNodeByName[s] || silverNodeByName[String(s).split(".").pop()];
          if (sid) { edges.push({data: {id: `e_sg${gi}_${linked}`, source: sid, target: gId}}); linked++; }
        });
        if (!linked && datasets.length) {
          edges.push({data: {id: `e_sg${gi}_0`, source: "s0", target: gId}});
        }
      });

      // Governed UC METRIC VIEWS (sem_*) — a metric view sits downstream of the
      // Silver/Gold it standardizes; every consumer (dashboard, Genie, briefing)
      // then shares ONE definition. spec.metric_views[] = {name, sources:[silver
      // /gold table name(s)]}. Resolve each source to its silver or gold node so
      // the lineage visibly shows the metric-view layer, not just tables.
      const metricNodeByName = {};   // metric-view name → node id (so Genie links)
      const _resolveUpstream = (s) => {
        const leaf = String(s).split(".").pop();
        return silverNodeByName[s] || silverNodeByName[leaf]
            || goldNodeByName[s] || goldNodeByName[leaf] || null;
      };
      (spec.metric_views || []).forEach((mv, mi) => {
        const mName = mv.name || mv.table || `metric_${mi}`;
        const leaf = String(mName).split(".").pop();
        const mId = `mv${mi}`;
        nodes.push({data: {id: mId, label: `metric · ${leaf}`, kind: "metric"}});
        metricNodeByName[mName] = mId;
        metricNodeByName[leaf] = mId;
        let linked = 0;
        (mv.sources || []).forEach((s) => {
          const up = _resolveUpstream(s);
          if (up) { edges.push({data: {id: `e_mv${mi}_${linked}`, source: up, target: mId}}); linked++; }
        });
        if (!linked) {  // default: hang off the first silver so the node still connects
          const anchor = Object.values(silverNodeByName)[0] || "s0";
          edges.push({data: {id: `e_mv${mi}_0`, source: anchor, target: mId}});
        }
      });

      // GENIE SPACES — the conversational consumers, downstream of the metric
      // views / Gold they read. spec.genie_spaces[] = {title, id, sources:
      // [metric-view or gold/silver name(s)]}. Rendered as their own node kind so
      // the lineage shows the Genie layer sharing the governed metric definitions.
      (spec.genie_spaces || []).forEach((gs, si) => {
        const gsId = `genie${si}`;
        const title = gs.title || gs.name || `Genie ${si + 1}`;
        nodes.push({data: {id: gsId, label: `🧞 ${title}`, kind: "genie",
                           spaceId: gs.id || ""}});
        let linked = 0;
        (gs.sources || []).forEach((s) => {
          const leaf = String(s).split(".").pop();
          const up = metricNodeByName[s] || metricNodeByName[leaf] || _resolveUpstream(s);
          if (up) { edges.push({data: {id: `e_gs${si}_${linked}`, source: up, target: gsId}}); linked++; }
        });
        if (!linked) {  // hang off the first metric view (or first silver) so it connects
          const anchor = Object.values(metricNodeByName)[0]
                       || Object.values(silverNodeByName)[0] || "s0";
          edges.push({data: {id: `e_gs${si}_0`, source: anchor, target: gsId}});
        }
      });

      if (this.cyPipeline) { try { this.cyPipeline.destroy(); } catch(e) {} }
      // Databricks light palette: white/aqua nodes, teal + yellow accents
      const kindBorder = {src: "#9AA8AF", bronze: "#CD7F32", silver: "#8A9BA8", gold: "#C99700", dim: "#7C3AED", metric: "#137A45", genie: "#2272B4"};
      const kindFill = {src: "#FFFFFF", bronze: "#F5E6D3", silver: "#EDF1F4", gold: "#FFE7A3", dim: "#EDE7FB", metric: "#E2F6EC", genie: "#EEF4FF"};
      const kindText = {src: "#46555D", bronze: "#5C4A00", silver: "#00525B", gold: "#6B4E00", dim: "#4B2E91", metric: "#0B6B3A", genie: "#1A4F86"};
      this.cyPipeline = cytoscape({
        container,
        elements: [...nodes, ...edges],
        style: [
          {selector: "node", style: {
            "shape": "round-rectangle",
            "background-color": (e) => kindFill[e.data("kind")] || "#FFFFFF",
            "border-width": 1.5,
            "border-color": (e) => kindBorder[e.data("kind")] || "#9AA8AF",
            "label": "data(label)",
            "color": (e) => kindText[e.data("kind")] || "#46555D",
            "font-size": "10px",
            "text-valign": "center",
            "text-halign": "center",
            "padding": "8px",
            "width": "label", "height": "label",
          }},
          {selector: "edge", style: {
            "width": 1.6,
            "line-color": "#1B3139",
            "curve-style": "bezier",
            "target-arrow-shape": "triangle",
            "target-arrow-color": "#1B3139",
          }},
        ],
        layout: {name: "dagre", rankDir: "LR", nodeSep: 30, edgeSep: 20, rankSep: 60},
        userZoomingEnabled: false, userPanningEnabled: false,
      });
    },

    // Genie Code requests: sources -> (new metric view) -> Genie Agent / dashboard, only what is built.
    _renderGenieLineage(container, spec) {
      const dp = spec.data_product || {};
      const design = dp.design || {};
      const outs = (dp.need || {}).outputs || [];
      const norm = (x) => String(x || "").replace(/`/g, "").trim().toLowerCase();
      const mv = dp.metric_view || null;
      const mvDev = mv ? norm(mv.name) : "";
      const nodes = [], edges = [];
      const idOf = {};   // fully-qualified metric view name -> node id
      // RAW data: the pipeline that builds the gold table (raw feed -> bronze -> silver -> gold)
      const pipe = dp.pipeline || null;
      let goldId = null;
      if (pipe) {
        const pt = pipe.tables || {};
        const goldName = (pt.gold || [])[0] || "";
        const feed = String((pipe.source || {}).path || "feed").split("/").filter(Boolean).pop();
        nodes.push({data: {id: "raw", label: "raw · " + feed, kind: "src"}});
        nodes.push({data: {id: "bz", label: "bronze · " + (pt.bronze || ""), kind: "pipe"}});
        nodes.push({data: {id: "sv", label: "silver · " + (pt.silver || ""), kind: "pipe"}});
        nodes.push({data: {id: "gd", label: "gold · " + goldName, kind: "pipe"}});
        edges.push({data: {id: "e_raw", source: "raw", target: "bz"}});
        edges.push({data: {id: "e_bz", source: "bz", target: "sv"}});
        edges.push({data: {id: "e_sv", source: "sv", target: "gd"}});
        goldId = "gd";
      }
      if (mv) {
        // gold tables the new metric view reads: the FROM/JOIN/source names in its SQL template
        const tables = new Set();
        const re = /\b(?:FROM|JOIN|source:)\s*(vmohan_demo_catalog\.[a-z0-9_]+\.[a-z0-9_]+)/gi;
        let m;
        while ((m = re.exec(mv.sql_template || "")) !== null) tables.add(norm(m[1]));
        [...tables].forEach((t, i) => {
          nodes.push({data: {id: "src" + i, label: t.split(".").slice(1).join("."), kind: "src"}});
          edges.push({data: {id: "e_src" + i, source: "src" + i, target: "mv"}});
        });
        nodes.push({data: {id: "mv", label: "metric · " + mvDev.split(".").pop(), kind: "metric"}});
        idOf[mvDev] = "mv";
        if (goldId) edges.push({data: {id: "e_gold_mv", source: goldId, target: "mv"}});
      }
      // metric views the request reuses (nothing new is built for them)
      (dp.data_sources || []).map(norm).filter(s => s && s !== mvDev).forEach((s, i) => {
        const id = "rm" + i;
        nodes.push({data: {id, label: "metric · " + s.split(".").pop(), kind: "metric"}});
        idOf[s] = id;
      });
      const ga = design.genie_agent;
      if (ga && outs.includes("genie_agent")) {
        nodes.push({data: {id: "ga", label: "🧞 " + ga.title, kind: "genie"}});
        (ga.data_sources || []).map(norm).forEach((s, k) => {
          if (idOf[s]) edges.push({data: {id: "e_ga" + k, source: idOf[s], target: "ga"}});
        });
      }
      const db = design.dashboard;
      if (db && outs.includes("dashboard")) {
        nodes.push({data: {id: "db", label: "📊 " + db.title, kind: "gold"}});
        const reads = new Set();
        for (const p of (db.pages || [])) for (const w of (p.widgets || [])) {
          (String(w.sql || "").match(/vmohan_demo_catalog\.[`\w]+\.[`\w]+/gi) || []).forEach(t => reads.add(norm(t)));
        }
        [...reads].forEach((s, k) => {
          if (idOf[s]) edges.push({data: {id: "e_db" + k, source: idOf[s], target: "db"}});
        });
      }
      if (this.cyPipeline) { try { this.cyPipeline.destroy(); } catch(e) {} }
      const kindBorder = {src: "#9AA8AF", pipe: "#6B5BD6", metric: "#137A45", genie: "#2272B4", gold: "#C99700"};
      const kindFill = {src: "#FFFFFF", pipe: "#F1EEFF", metric: "#E2F6EC", genie: "#EEF4FF", gold: "#FFE7A3"};
      const kindText = {src: "#46555D", pipe: "#3F2F9A", metric: "#0B6B3A", genie: "#1A4F86", gold: "#6B4E00"};
      this.cyPipeline = cytoscape({
        container,
        elements: [...nodes, ...edges],
        style: [
          {selector: "node", style: {
            "shape": "round-rectangle", "background-color": (e) => kindFill[e.data("kind")] || "#FFFFFF",
            "border-width": 1.5, "border-color": (e) => kindBorder[e.data("kind")] || "#9AA8AF",
            "label": "data(label)", "color": (e) => kindText[e.data("kind")] || "#46555D",
            "font-size": "10px", "text-valign": "center", "text-halign": "center", "padding": "8px",
            "width": "label", "height": "label",
          }},
          {selector: "edge", style: {
            "width": 1.6, "line-color": "#1B3139", "curve-style": "bezier",
            "target-arrow-shape": "triangle", "target-arrow-color": "#1B3139",
          }},
        ],
        layout: {name: "dagre", rankDir: "LR", nodeSep: 24, edgeSep: 16, rankSep: 60},
        userZoomingEnabled: false, userPanningEnabled: false,
      });
    },

    // The last event that is not a human comment (what a blocked or stopped request says last)
    lastEventMessage() {
      let msg = "";
      for (const ev of this.events) {
        if (this._details(ev).kind === "human_comment") continue;
        msg = ev.message || msg;
      }
      return msg;
    },

    latestApprovalGateMessage() {
      let msg = "";
      for (const ev of this.events) {
        if (ev.step === "verify_pr_approved") msg = ev.message || msg;
      }
      return msg;
    },

    async initiate() {
      if (!this.selected) return;
      if (!this.assigneeChoice) { this.initiateResult = "Select an assignee first."; return; }
      this.busyInit = true; this.launching = true;
      this.initiateResult = "Launching job run…";
      try {
        const r = await fetch("/api/requests/" + this.selected.request_id + "/initiate", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({assignee: this.assigneeChoice}),
        });
        const data = await r.json();
        if (r.ok) {
          const how = data.launch && data.launch.runner === "job"
            ? `Job run ${data.launch.run_id} launched.`
            : (data.launch && data.launch.runner === "genie_engine"
                ? "Build dispatched to Genie Code." : "Agent started.");
          this.initiateResult = how;
        } else {
          this.initiateResult = "Failed: " + (data.detail || r.status);
          this.launching = false;
        }
        this.loadOne(); this.startPolling();
      } catch (e) {
        this.initiateResult = "Error: " + e.message;
        this.launching = false;
      } finally {
        this.busyInit = false;
      }
    },

    async approve() {
      if (!this.selected) return;
      this.launching = true;
      this.reviewResult = "Launching gate run…";
      const r = await fetch("/api/requests/" + this.selected.request_id + "/approve", {method: "POST"});
      const data = await r.json().catch(() => ({}));
      this.reviewResult = r.ok
        ? (this.isGenie
            ? "Approved — verifying, then staging and promoting to prod."
            : "Approved — gate run launched (verify PR → deploy to staging).")
        : "Failed: " + (data.detail || r.status);
      if (!r.ok) this.launching = false;
      this.loadOne(); this.startPolling();
    },

    async reject() {
      if (!this.selected) return;
      this.reviewResult = "Submitting…";
      const r = await fetch("/api/requests/" + this.selected.request_id + "/reject", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({note: (this.rejectNote || "").trim()}),
      });
      this.reviewResult = r.ok ? "Marked as rejected." : "Failed to reject.";
      this.loadOne();
    },

    async retryProdDeploy() {
      if (!this.selected) return;
      this.launching = true;
      this.reviewResult = "Launching gate run…";
      const r = await fetch("/api/requests/" + this.selected.request_id + "/retry-prod-deploy", {method: "POST"});
      const data = await r.json().catch(() => ({}));
      this.reviewResult = r.ok ? "Gate run launched." : "Failed: " + (data.detail || r.status);
      if (!r.ok) this.launching = false;
      this.loadOne(); this.startPolling();
    },

    async saveComment(node) {
      if (!this.selected) return;
      const comment = (this.commentDrafts[node] || "").trim();
      if (!comment) return;
      const r = await fetch("/api/requests/" + this.selected.request_id + "/comment-on-step", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({step: node, comment}),
      });
      if (r.ok) {
        this.commentSaved[node] = "saved ✓";
        this.commentSaved = {...this.commentSaved};
        setTimeout(() => { this.commentSaved[node] = ""; this.commentSaved = {...this.commentSaved}; }, 2500);
        this.loadOne();
      } else {
        this.commentSaved[node] = "save failed";
        this.commentSaved = {...this.commentSaved};
      }
    },

    async rerunFromStep(node) {
      if (!this.selected) return;
      // If there's a pending comment, save it first so the rerun picks it up
      if ((this.commentDrafts[node] || "").trim()) {
        await this.saveComment(node);
        this.commentDrafts[node] = "";
        this.commentDrafts = {...this.commentDrafts};
      }
      this.rerunBusy[node] = true;
      this.rerunBusy = {...this.rerunBusy};
      try {
        const r = await fetch("/api/requests/" + this.selected.request_id + "/rerun-from-step", {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({step: node}),
        });
        if (r.ok) {
          this.commentSaved[node] = "re-running…";
          this.commentSaved = {...this.commentSaved};
          this.loadOne(); this.startPolling();
        } else {
          const data = await r.json().catch(() => ({}));
          this.commentSaved[node] = "rerun failed: " + (data.detail || r.status);
          this.commentSaved = {...this.commentSaved};
        }
      } finally {
        this.rerunBusy[node] = false;
        this.rerunBusy = {...this.rerunBusy};
      }
    },
  };
}
