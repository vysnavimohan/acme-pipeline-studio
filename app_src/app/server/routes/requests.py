"""Request lifecycle endpoints.

POST   /api/requests                   — submit a new spec (queued)
GET    /api/requests                   — list requests
GET    /api/requests/{id}              — request detail + events
POST   /api/requests/{id}/initiate     — launch the agent (Databricks Job run)
POST   /api/requests/{id}/approve      — human approved → launch gate run
POST   /api/requests/{id}/reject       — human rejected → terminal
POST   /api/requests/{id}/retry-prod-deploy — re-run the approval gate
POST   /api/requests/{id}/comment-on-step   — operator note for an agent
POST   /api/requests/{id}/rerun-from-step   — partial re-run (job run)

Execution model: every launch goes through agent.runner — a Databricks Job
run per execution (thread fallback only for local dev/tests). The graph
pauses at await_human (final_status=pending_review); /approve resumes it
as a fresh gate-mode run. Nothing in this process holds agent state.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, field_validator

from agent import event_store, genie_engine, runner
from agent.state import (
    AgentState, PipelineSpec, SourceSpec, TargetSpec, TransformSpec,
    ValidationSpec, SLASpec, GovernanceSpec, GoldFactSpec, StepEvent, StepName, StepStatus,
    AppSpec, AppKPI, AppChart,
)

router = APIRouter()
LOG = logging.getLogger("ops.requests")

# Serialises the queued->in_progress and pending_review->in_progress transitions so a double click
# (or two tabs) cannot start a second build or a second promotion for the same request.
_ACTION_LOCK = threading.Lock()


class SubmitSpec(BaseModel):
    """Wire-format spec body. Aligned with PipelineSpec but each section
    flat-ish so the form can post FormData or JSON."""
    intent: str
    source_system: str
    source_tables: list[str]
    source_incremental_column: Optional[str] = None
    source_primary_key: list[str] = Field(default_factory=list)

    # Source kind: "cdc" (Postgres/Lakeflow Connect, secondary) or "file"
    # (Auto Loader from a landing-zone Volume — the primary Databricks path).
    source_kind: str = "cdc"
    # File-source fields (used when source_kind == "file"); typically prefilled
    # from a landing-zone dataset profile.
    file_dataset: Optional[str] = None
    file_landing_path: Optional[str] = None
    file_format: str = "text"
    file_delimiter: str = ""
    file_has_header: bool = True
    file_columns: list[dict[str, Any]] = Field(default_factory=list)
    # MULTI-dataset file requests: a list of dataset configs, each shaped like
    # the single file_* block PLUS a `silver_table`. When non-empty, ONE request
    # ingests every dataset → its own Bronze + Silver, and Gold joins across them.
    file_datasets: list[dict[str, Any]] = Field(default_factory=list)

    target_catalog: str
    target_schema: str = "silver"
    target_table: str
    target_write_mode: str = "merge"

    transform_description: str = ""
    transform_join_with: Optional[dict[str, Any]] = None
    transform_type_alignment: str = "strict_to_source"
    transform_business_rules: list[str] = Field(default_factory=list)

    validation_not_null: list[str] = Field(default_factory=list)
    validation_unique: list[str] = Field(default_factory=list)
    # Accept None / int / float — the chat intake agent occasionally emits
    # null here when the LLM means "use the default". Coerce in the
    # validator below.
    validation_row_count_tolerance_pct: Optional[float] = 5.0

    sla_mode: str = "batch"            # "batch" | "streaming"
    sla_freshness_minutes: Optional[int] = 60
    sla_schedule_cron: Optional[str] = None

    required_approvers: list[str] = Field(default_factory=list)
    # P3 governance metadata (the standards validator enforces these)
    governance_data_owner: str = ""
    governance_business_owner: str = ""
    governance_domain: str = ""
    governance_classification: str = ""
    # Optional Gold layer (the 'curate' step) — each: {name, intent, grain, sources?}
    gold_facts: list[dict[str, Any]] = Field(default_factory=list)
    # Optional Databricks App (the 'consume' step) built on the Gold layer.
    app_enabled: bool = False
    app_name: str = ""
    app_title: str = ""
    app_subtitle: str = ""
    app_audience: str = ""
    app_gold_tables: list[str] = Field(default_factory=list)
    app_tenant_column: str = ""
    app_kpis: list[dict[str, Any]] = Field(default_factory=list)
    app_charts: list[dict[str, Any]] = Field(default_factory=list)
    app_include_genie: bool = True
    app_include_writeback: bool = False
    app_brand_name: str = ""
    requested_by: Optional[str] = None
    # Set by the UI when the spec was confirmed through the intake chat
    intake_confirmed: bool = False
    # The intake chat history — list of [{role, content}] tuples from the grilling conversation
    intake_history: list[dict[str, str]] = Field(default_factory=list)
    # Acme Corp data-product journey (reuse decisions, metric view, DQ review, Genie Agent +
    # dashboard design, engine). Opaque to the legacy pipeline agents.
    data_product: dict[str, Any] = Field(default_factory=dict)

    # Pydantic v2 validators: coerce None → default for fields the LLM
    # sometimes blanks out
    @field_validator("validation_row_count_tolerance_pct", mode="before")
    @classmethod
    def _default_tolerance(cls, v: Any) -> float:
        if v is None or v == "":
            return 5.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return 5.0

    @field_validator("sla_freshness_minutes", mode="before")
    @classmethod
    def _default_freshness(cls, v: Any) -> int:
        if v is None or v == "":
            return 60
        try:
            return int(v)
        except (TypeError, ValueError):
            return 60

    @field_validator("source_tables", "source_primary_key", "validation_not_null",
                     "validation_unique", "transform_business_rules",
                     "required_approvers", mode="before")
    @classmethod
    def _default_list(cls, v: Any) -> list:
        if v is None:
            return []
        return v

    def to_pipeline_spec(self) -> PipelineSpec:
        from agent.state import FileColumn, FileSource

        def _mk_file(dataset, landing_path, file_format, delimiter, has_header,
                     columns, silver_table=None) -> "FileSource":
            return FileSource(
                dataset=dataset,
                landing_path=landing_path or "",
                file_format=file_format or "text",
                delimiter=delimiter or "",
                has_header=bool(has_header),
                silver_table=silver_table or None,
                columns=[FileColumn(name=c.get("name", ""),
                                    type=c.get("type", c.get("inferred_type", "string")),
                                    is_json=bool(c.get("is_json")),
                                    source_name=c.get("source_name") or c.get("name", ""))
                         for c in (columns or []) if c.get("name")],
            )

        file_source = None
        files: list = []
        is_file = (self.source_kind or "cdc") == "file"
        if is_file and self.file_datasets:
            # MULTI-dataset: build one FileSource per dataset.
            for d in self.file_datasets:
                if not d.get("dataset"):
                    continue
                files.append(_mk_file(
                    d.get("dataset"), d.get("landing_path"), d.get("file_format", "text"),
                    d.get("delimiter", ""), d.get("has_header", True), d.get("columns") or [],
                    d.get("silver_table")))
            if files:
                file_source = files[0]  # representative (single-dataset code paths)
        elif is_file and self.file_dataset:
            file_source = _mk_file(self.file_dataset, self.file_landing_path, self.file_format,
                                   self.file_delimiter, self.file_has_header, self.file_columns)
        # source.tables: dataset name(s) for file; the posted list for CDC.
        if files:
            src_tables = [f.dataset for f in files]
            primary_silver = files[0].silver_table or files[0].dataset
        else:
            src_tables = self.source_tables
            primary_silver = self.target_table
        return PipelineSpec(
            intent=self.intent,
            source=SourceSpec(
                kind=self.source_kind or "cdc",  # type: ignore[arg-type]
                system=self.source_system,  # type: ignore[arg-type]
                tables=src_tables,
                incremental_column=self.source_incremental_column,
                primary_key=self.source_primary_key,
                file=file_source,
                files=files,
            ),
            target=TargetSpec(
                catalog=self.target_catalog,
                target_schema=self.target_schema,
                table=primary_silver,
                write_mode=self.target_write_mode,  # type: ignore[arg-type]
            ),
            transform=TransformSpec(
                description=self.transform_description,
                join_with=self.transform_join_with,
                type_alignment=self.transform_type_alignment,  # type: ignore[arg-type]
                business_rules=self.transform_business_rules,
            ),
            validation=ValidationSpec(
                not_null=self.validation_not_null,
                unique=self.validation_unique,
                row_count_tolerance_pct=self.validation_row_count_tolerance_pct,
            ),
            sla=SLASpec(
                mode=self.sla_mode,  # type: ignore[arg-type]
                freshness_minutes=self.sla_freshness_minutes,
                schedule_cron=self.sla_schedule_cron,
            ),
            governance=GovernanceSpec(
                data_owner=self.governance_data_owner,
                business_owner=self.governance_business_owner,
                domain=self.governance_domain,
                classification=self.governance_classification,
            ),
            gold_facts=[GoldFactSpec(name=g.get("name", ""), intent=g.get("intent", ""),
                                     grain=g.get("grain", ""), sources=g.get("sources") or [])
                        for g in (self.gold_facts or []) if g.get("name")],
            app=AppSpec(
                enabled=bool(self.app_enabled),
                name=self.app_name,
                title=self.app_title,
                subtitle=self.app_subtitle,
                audience=self.app_audience,
                gold_tables=self.app_gold_tables,
                tenant_column=self.app_tenant_column,
                kpis=[AppKPI(**k) for k in (self.app_kpis or []) if k.get("label") and k.get("column")],
                charts=[AppChart(**c) for c in (self.app_charts or []) if c.get("title")],
                include_genie=bool(self.app_include_genie),
                include_writeback=bool(self.app_include_writeback),
                brand_name=self.app_brand_name,
            ),
            required_approvers=self.required_approvers,
            requested_by=self.requested_by,
            intake_confirmed=self.intake_confirmed,
            data_product=self.data_product or {},
        )


def _reconcile_dead_job(request_id: str, req: dict) -> dict:
    """If a request says `in_progress` but its Job run is dead, say so.

    The agent writes its own failure events, which covers every failure INSIDE
    the graph. It cannot cover a run that dies before Python starts — a bad
    serverless environment, a revoked permission, workspace storage it cannot
    read. Then the launch event is the last word and the tracker shows a
    spinner forever: the flow looks alive, nothing is polling, and the operator
    has no way to tell from the UI that it is over. That is exactly what
    happened on a workspace whose serverless compute could not decrypt
    /Workspace, and the 12-stage pipeline sat at "in progress" indefinitely.

    So: ask the Jobs API about the run id we recorded at launch. Only ever
    promotes in_progress → failed, never the reverse, so a live run and any
    terminal state the agent set itself are both left alone.
    """
    if req.get("final_status") != "in_progress":
        return req
    # The launch event carries the run id (details kind="job_run"). Note the two
    # shapes: the DBSQL store serialises details to a `details_json` string,
    # while the local JSON store keeps a `details` dict. Reading only one of them
    # makes this silently do nothing in the other mode.
    run_id = job_id = None
    for e in reversed(event_store.fetch_events(request_id)):
        details = e.get("details")
        if not isinstance(details, dict):
            try:
                details = json.loads(e.get("details_json") or "{}")
            except Exception:  # noqa: BLE001 — a malformed row must not break a read
                continue
        if details.get("kind") == "job_run":
            run_id, job_id = details.get("run_id"), details.get("job_id")
            break
    if not run_id:
        return req  # thread runner (local dev) — nothing to ask about

    try:
        from agent.event_store import _get_ws_client
        run = _get_ws_client().jobs.get_run(run_id=int(run_id))
        state = run.state
        life = getattr(getattr(state, "life_cycle_state", None), "value", None)
        result = getattr(getattr(state, "result_state", None), "value", None)
        if life not in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            return req                      # still running — leave it alone
        if result == "SUCCESS":
            # Terminal-success but the request never reached a terminal status:
            # the graph paused (await_human) or the agent already recorded the
            # outcome. Not ours to relabel.
            return req
        why = (getattr(state, "state_message", "") or "").strip()
    except Exception as e:  # noqa: BLE001 — diagnosis must not break the tracker
        LOG.warning("could not reconcile job state for %s: %s", request_id, e)
        return req

    msg = ("The agent's Databricks Job run ended without reporting a result "
           f"({life}{'/' + result if result else ''}), so no step could record "
           "the failure. This means the run died before the agent started — "
           "check the Job run output.")
    if why:
        msg += f" Databricks said: {why[:300]}"
    event_store.append_event(request_id, StepEvent(
        step=StepName.INITIATED, status=StepStatus.FAILED, message=msg,
        details={"kind": "job_died", "job_id": job_id, "run_id": run_id,
                 "life_cycle_state": life, "result_state": result},
    ))
    event_store.update_request(request_id, final_status="failed")
    return event_store.fetch_request(request_id) or req


def _refuse_example(request_id: str) -> None:
    """Seeded examples (snapshots of real runs, first event kind seed_example) are view only: no action on them."""
    for e in event_store.fetch_events(request_id):
        d = e.get("details") if isinstance(e.get("details"), dict) else None
        if d is None:
            try:
                d = json.loads(e.get("details_json") or "{}")
            except Exception:  # noqa: BLE001
                d = {}
        if isinstance(d, dict) and d.get("kind") == "seed_example":
            raise HTTPException(status_code=409, detail="This is an example seeded on reset (a snapshot of a real run). It is view only.")


def _require_request(request_id: str) -> dict:
    req = event_store.fetch_request(request_id)
    if not req:
        raise HTTPException(status_code=404, detail="request not found")
    return _reconcile_dead_job(request_id, req)


@router.post("")
def submit_request(body: SubmitSpec):
    """Create a request in the **queued** state. Does NOT start the agent.

    The operator clicks **Initiate** on the tracker (POST /initiate) when
    they're ready for the agent to fire. This gives the team control over
    when LLM credits + compute get spent, and lets multiple requests be
    queued up for batch review.
    """
    dp = body.data_product or {}
    if dp.get("engine") == "genie_code" and dp.get("version") not in (2, 3):
        raise HTTPException(status_code=400,
                            detail="This data product uses schema v1, which is retired. Submit it again from the intake.")
    spec = body.to_pipeline_spec()
    request_id = str(uuid.uuid4())
    state = AgentState(request_id=request_id, spec=spec)
    # One write on the request path (row inserted as 'queued'); the tracker events follow in a background
    # thread. Submit used to make 4 sequential warehouse writes and took 15-45 s.
    event_store.submit_request(state, final_status="queued")
    history = list(body.intake_history or [])

    def _write_events() -> None:
        try:
            event_store.append_event(
                request_id,
                StepEvent(step=StepName.QUEUED, status=StepStatus.PENDING,
                          message="Request queued. Click Initiate on the tracker to start the agent."),
            )
            if history:
                event_store.append_event(
                    request_id,
                    StepEvent(step=StepName.QUEUED, status=StepStatus.SUCCEEDED,
                              message=f"Intake conversation captured ({len(history)} turns)",
                              details={"kind": "intake_transcript", "history": history}),
                )
        except Exception as e:  # noqa: BLE001
            print(f"[ops] submit events for {request_id[:8]} failed: {e}", flush=True)

    import threading
    threading.Thread(target=_write_events, daemon=True, name=f"submit-events-{request_id[:8]}").start()
    return {"request_id": request_id, "status": "queued"}


class InitiateBody(BaseModel):
    # The lead/supervisor assigns an owner when initiating. Optional on the
    # server (the UI enforces selection) so older callers/tests still work.
    assignee: Optional[str] = None


@router.post("/{request_id}/initiate")
def initiate(request_id: str, body: Optional[InitiateBody] = None):
    """Launch the agent for a queued request as a Databricks Job run.
    A supervisor assigns an owner (assignee) when initiating. Idempotent —
    running again on a non-queued request returns its current status."""
    _refuse_example(request_id)
    with _ACTION_LOCK:
        req = _require_request(request_id)
        current = req.get("final_status") or "queued"
        assignee = (body.assignee if body else None)
        if current != "queued":
            if assignee and not req.get("assignee"):
                event_store.update_request(request_id, assignee=assignee)
            return {"status": current, "note": "already initiated"}
        if genie_engine.is_genie(req):
            why = genie_engine.refusal_for(req)
            if why:
                raise HTTPException(status_code=400, detail=why)

        updates = {"final_status": "in_progress"}
        if assignee:
            updates["assignee"] = assignee
        event_store.update_request(request_id, **updates)

    if genie_engine.is_genie(req):
        # Acme data-product request: the plan is approved, dispatch the Genie Code build.
        who = f" Assigned to {assignee}." if assignee else ""
        _, dp = genie_engine._spec_dp(req)
        built = ", ".join(s["title"] for s in genie_engine.plan_for(dp) if s.get("engine") == "genie_code")
        event_store.append_event(
            request_id,
            StepEvent(step=StepName.INITIATED, status=StepStatus.SUCCEEDED,
                      message=f"Plan approved.{who} Dispatching the build to Genie Code: {built}.",
                      details={"kind": "genie_build", "engine": "genie_code"}),
        )
        genie_engine.start_build(request_id)
        return {"status": "in_progress", "launch": {"runner": "genie_engine", "engine": "genie_code"}}
    who = f" Assigned to {assignee}." if assignee else ""
    # Route Gold-fact requests (source.kind == 'gold') to the gold builder mode.
    mode = "full"
    try:
        import json as _json
        sj = req.get("spec_json")
        if isinstance(sj, str):
            sj = _json.loads(sj)
        if ((sj or {}).get("source") or {}).get("kind") == "gold":
            mode = "gold"
    except Exception:
        mode = "full"
    what = "the Gold fact" if mode == "gold" else "in dev"
    event_store.append_event(
        request_id,
        StepEvent(step=StepName.INITIATED, status=StepStatus.SUCCEEDED,
                  message=f"Operator initiated the agent.{who} Building {what} now."),
    )
    launch = runner.launch(mode, request_id)
    return {"status": "in_progress", "launch": launch}


# Sample team for the assignee picker (supervisor assigns an owner at
# initiate time). Vaishnavi (lead) + a few sample engineers for the demo.
TEAM_MEMBERS = [
    "Vaishnavi Mohan",
    "Priya Nair",
    "Marcus Chen",
    "Sophie Williams",
    "Daniel Kim",
]


@router.get("/meta/team")
def team():
    return {"members": TEAM_MEMBERS}


@router.get("")
def list_all(limit: int = 50):
    # Reconcile here too, not just on the detail route: the tracker's summary
    # counters and sidebar read this, so without it a request whose Job died
    # keeps inflating "IN PROGRESS" until someone happens to open it.
    reqs = event_store.list_requests(limit=limit)
    return {"requests": [
        _reconcile_dead_job(r["request_id"], r) if r.get("final_status") == "in_progress"
        and r.get("request_id") else r
        for r in reqs
    ]}


@router.get("/{request_id}")
def get_one(request_id: str):
    req = _require_request(request_id)
    events = event_store.fetch_events(request_id)
    out: dict[str, Any] = {"request": req, "events": events}
    if genie_engine.is_genie(req):
        dp = genie_engine._spec_dp(req)[1]
        out["engine"] = "genie_code"
        out["plan"] = genie_engine.plan_for(dp)
    return out


@router.get("/{request_id}/events")
def get_events(request_id: str, since_seq: int = 0):
    events = event_store.fetch_events(request_id)
    if since_seq:
        events = [e for e in events if (e.get("seq") or 0) > since_seq]
    return {"events": events}


@router.get("/{request_id}/similar")
def similar(request_id: str):
    """Prior already-built requests similar to this one — shown at the
    queued step so the operator deciding whether to initiate can spot
    duplicates, table reuse, and target-name collisions."""
    _require_request(request_id)
    from agent.similarity import find_similar
    try:
        return find_similar(request_id)
    except Exception as e:
        return {"target": None, "candidates": [], "error": str(e)[:200]}


@router.post("/{request_id}/approve")
def approve(request_id: str):
    """Human clicked 'PR approved — deploy to prod'. Record the decision
    and resume the flow as a NEW gate-mode job run (verify_pr_approved →
    deploy_prod). The original run already ended at await_human."""
    _refuse_example(request_id)
    with _ACTION_LOCK:
        req = _require_request(request_id)
        if genie_engine.is_genie(req):
            if req.get("final_status") != "pending_review":
                return {"ok": True, "status": req.get("final_status"), "note": "already decided"}
            event_store.update_request(request_id, human_decision="approved", final_status="in_progress")
            event_store.append_event(
                request_id,
                StepEvent(step=StepName.AWAIT_HUMAN, status=StepStatus.SUCCEEDED,
                          message="Approved in the tracker. Verifying, then staging and promoting to prod.",
                          details={"kind": "human_decision", "decision": "approved"}),
            )
            genie_engine.start_promote(request_id)
            return {"ok": True, "status": "in_progress", "launch": {"runner": "genie_engine"}}

    if not req.get("pr_number"):
        raise HTTPException(status_code=400, detail="request has no PR yet")

    event_store.update_request(request_id, human_decision="approved",
                               final_status="in_progress")
    event_store.append_event(
        request_id,
        StepEvent(step=StepName.AWAIT_HUMAN, status=StepStatus.SUCCEEDED,
                  message="Approved in the tracker — resuming flow: verifying the "
                          "PR approval, then deploying to staging."),
    )
    launch = runner.launch("gate", request_id)
    return {"ok": True, "status": "in_progress", "launch": launch}


class RejectBody(BaseModel):
    note: Optional[str] = None


@router.post("/{request_id}/reject")
def reject(request_id: str, body: Optional[RejectBody] = None):
    """Human rejected — terminal. No further runs are launched. The optional note is the reviewer's reason."""
    _refuse_example(request_id)
    with _ACTION_LOCK:
        req = _require_request(request_id)
        if req.get("final_status") in ("done", "rejected"):
            return {"ok": True, "status": req.get("final_status"), "note": "already decided"}
        note = ((body.note if body else None) or "").strip()
        event_store.update_request(request_id, human_decision="rejected",
                                   final_status="rejected")
    msg = "Human rejected the request in the tracker. Flow ends here."
    if note:
        msg += f" Reason: {note[:500]}"
    event_store.append_event(
        request_id,
        StepEvent(step=StepName.AWAIT_HUMAN, status=StepStatus.SKIPPED, message=msg,
                  details={"kind": "human_decision", "decision": "rejected", "note": note}),
    )
    return {"ok": True, "status": "rejected"}


@router.get("/{request_id}/pr-status")
def pr_status(request_id: str):
    """Live check of the PR's review state on GitHub. Used by the tracker
    to show a friendly approval indicator while in pending_review."""
    req = _require_request(request_id)
    pr_number = req.get("pr_number")
    if not pr_number:
        return {"available": False, "reason": "PR not opened yet."}
    # An `app://` PR lives in the app — there is nothing on GitHub to look up,
    # and asking would 404 (or 403 behind the enterprise IP allow list) and show
    # the operator an error for a PR that is working exactly as intended.
    if str(req.get("pr_url") or "").startswith("app://"):
        return {"available": False, "in_app": True,
                "reason": "This PR is rendered in-app — approve it in the tracker."}
    import os

    from tools import github_pr
    repo = github_pr.pipelines_repo()
    try:
        review = github_pr.get_pr_review_state(repo, int(pr_number))
        return {"available": True, "pr_number": pr_number, "review": review}
    except Exception as e:
        return {"available": False, "reason": str(e)}


@router.post("/{request_id}/retry-prod-deploy")
def retry_prod_deploy(request_id: str):
    """Re-run only the PR-approval gate + prod deploy for a request that
    was previously blocked on PR approval. The operator should have
    approved the PR on GitHub before clicking this."""
    _refuse_example(request_id)
    with _ACTION_LOCK:
        req = _require_request(request_id)
        if genie_engine.is_genie(req):
            # Resume a promotion that failed after the human approved it (idempotent: prod ids are reused).
            if req.get("final_status") != "failed" or req.get("human_decision") != "approved":
                raise HTTPException(status_code=400, detail="Nothing to resume: the promotion has not failed after approval.")
            event_store.update_request(request_id, final_status="in_progress")
            genie_engine.start_promote(request_id)
            return {"status": "in_progress", "launch": {"runner": "genie_engine", "resumed": True}}
    if not req.get("pr_number"):
        raise HTTPException(status_code=400, detail="request has no PR yet")
    # The user explicitly clicking retry IS the approval action.
    event_store.update_request(
        request_id,
        human_decision="approved",
        final_status="in_progress",
    )
    event_store.append_event(
        request_id,
        StepEvent(step=StepName.VERIFY_PR_APPROVED, status=StepStatus.IN_PROGRESS,
                  message="Operator clicked 'Re-check PR approval & deploy to prod'."),
    )
    launch = runner.launch("gate", request_id)
    return {"status": "in_progress", "launch": launch}


# =====================================================================
# Per-step human feedback loop
# =====================================================================

class StepCommentBody(BaseModel):
    step: str           # StepName.value (e.g. "spec_critic")
    comment: str        # operator's note — will be folded into the agent's prompt


@router.post("/{request_id}/comment-on-step")
def comment_on_step(request_id: str, body: StepCommentBody):
    """Attach an operator comment to a specific step. Persisted as a
    `human_comment` event so it's rehydrated on /rerun-from-step. The
    target node will fold it into its prompt next time it runs."""
    _refuse_example(request_id)
    _require_request(request_id)
    try:
        step = StepName(body.step)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"unknown step: {body.step!r}")
    snippet = (body.comment or "").strip()[:90]
    event_store.append_event(
        request_id,
        StepEvent(
            step=step, status=StepStatus.IN_PROGRESS,
            message=f"Operator comment: {snippet}",
            details={"kind": "human_comment", "comment": body.comment},
        ),
    )
    return {"ok": True, "step": step.value, "comment_length": len(body.comment or "")}


class RerunFromStepBody(BaseModel):
    step: str   # StepName.value — must be in RERUNNABLE_STEPS


@router.post("/{request_id}/rerun-from-step")
def rerun_from_step(request_id: str, body: RerunFromStepBody):
    """Re-execute the agent graph from the chosen step onward as a fresh
    job run. State is rehydrated inside the run from the event log; any
    per-step operator comments are folded into each node's prompt."""
    _refuse_example(request_id)
    try:
        step = StepName(body.step)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"unknown step: {body.step!r}")

    from agent.state import RERUNNABLE_STEPS  # langgraph-free import (App has no langgraph)
    if step not in RERUNNABLE_STEPS:
        raise HTTPException(status_code=400, detail=f"step {step.value!r} not re-runnable")

    if genie_engine.is_genie(_require_request(request_id)):
        raise HTTPException(status_code=400, detail="Genie Code requests cannot be re-run from a step.")
    event_store.update_request(request_id, final_status="in_progress")
    event_store.append_event(
        request_id,
        StepEvent(
            step=step, status=StepStatus.IN_PROGRESS,
            message=f"Operator clicked Re-run from `{step.value}` (incorporating any prior comments).",
        ),
    )
    launch = runner.launch("from_step", request_id, start_step=step.value)
    return {"status": "in_progress", "rerun_from": step.value, "launch": launch}
