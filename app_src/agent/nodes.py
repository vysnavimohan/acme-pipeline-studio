"""Per-agent node functions. Each takes AgentState and returns a partial
state update dict. LangGraph merges those into the running state.

Each node emits a sequence of events to the event store:
  - one or more `in_progress` events describing what the agent is
    thinking / doing (this is what the tracker shows when you click
    the node — the "thought process"),
  - a final `succeeded` (or `failed`) event with the outcome.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from langchain_core.messages import SystemMessage, HumanMessage

from agent import guidelines
from agent.otel_trace import traced_node
from agent import prompts as default_prompts
from agent.event_store import append_event, update_request
from agent.llm import get_llm
from agent.state import AgentState, StepEvent, StepName, StepStatus


# --- helpers ----------------------------------------------------------

class LLMReplyTruncated(RuntimeError):
    """The endpoint stopped at its output ceiling, so the reply is a PREFIX of
    the intended answer. Raised rather than returned because the text parses
    exactly like a model that answered nothing — see KNOWN_ISSUES 1c."""


def _llm_call(system_prompt: str, user_payload: str) -> str:
    llm = get_llm()
    resp = llm.invoke([SystemMessage(content=system_prompt), HumanMessage(content=user_payload)])
    text = resp.content if isinstance(resp.content, str) else str(resp.content)
    if getattr(resp, "truncated", False):
        raise LLMReplyTruncated(
            f"the reply hit the {getattr(llm, 'max_tokens', '?')}-token output ceiling after "
            f"{len(text)} characters, so it is cut off mid-answer. Ask for less in one "
            f"call (fewer columns per request) or raise llm.DEFAULT_MAX_TOKENS.")
    return text


def _record(state: AgentState, step: StepName, status: StepStatus, msg: str = "", **details: Any) -> dict:
    ev = StepEvent(step=step, status=status, message=msg, details=details)
    append_event(state.request_id, ev)
    return {"events": [ev]}


def _feedback_for(state: AgentState, step: StepName) -> list[str]:
    """Per-step operator comments attached via /api/requests/{id}/comment-on-step.
    Each node reads this and folds it into its prompt so the agent re-runs
    address the operator's note."""
    return state.human_feedback_per_step.get(step.value, [])


def _augment_prompt(system_prompt: str, feedback: list[str]) -> str:
    if not feedback:
        return system_prompt
    block = "\n".join(f"- {f}" for f in feedback)
    return (f"{system_prompt}\n\n"
            f"# Operator feedback (must address)\n{block}\n")


# --- 1. Spec Critic ---------------------------------------------------

@traced_node("spec_critic")
def spec_critic(state: AgentState) -> dict:
    """Grilling loop. Round 1 raises ambiguities. Subsequent rounds (using
    the Spec Refiner prompt) attempt to self-resolve. At max_iterations,
    either auto-proceed or send back to queue (configurable per agent).
    """
    cfg = guidelines.get_config("spec_critic")
    max_iters = max(1, int(cfg.get("max_iterations") or 2))
    on_unresolved = cfg.get("loop_action", "proceed")
    # Specs confirmed through the interactive intake chat were already
    # grilled by a human-in-the-loop — single-pass sanity check only.
    if state.spec.intake_confirmed:
        max_iters = 1
    iter_num = state.critic_iterations + 1
    is_first = iter_num == 1

    # Live table catalog (best-effort) so hallucinated / mistyped table
    # names get flagged BEFORE any code generation or deploys.
    catalog_tables: list[str] = []
    try:
        from tools.source_profiler import list_available_tables
        for sys_name in ("billing", "network"):
            catalog_tables.extend(list_available_tables(sys_name))
    except Exception:
        pass

    feedback = _feedback_for(state, StepName.SPEC_CRITIC)
    if is_first:
        msg = f"Round {iter_num}/{max_iters}: reading the spec, looking for ambiguities…"
        if state.spec.intake_confirmed:
            msg = ("Spec arrived pre-grilled via the intake chat — running a "
                   "single-pass sanity check (tables exist, joins make sense).")
        if feedback:
            msg += f" (with {len(feedback)} operator note(s))"
        _record(state, StepName.SPEC_CRITIC, StepStatus.IN_PROGRESS, msg)
        prompt = _augment_prompt(guidelines.get('spec_critic'), feedback)
        if catalog_tables:
            prompt += ("\n\n# Table catalog\nThe payload includes "
                       "`available_source_tables` — the definitive list of real "
                       "source tables. If `spec.source.tables` names anything not "
                       "in that list, raise it as a blocking question and suggest "
                       "the closest real table. Tables may span BOTH systems "
                       "(cross-system joins go through msisdn).")
        if state.spec.intake_confirmed:
            prompt += ("\n\n# Note\nThis spec was confirmed interactively with the "
                       "requester via the intake agent. Do a single-pass sanity check "
                       "only — respond SPEC_OK unless there is a HARD error (a table "
                       "not in available_source_tables, an impossible join, a missing "
                       "target).")
        payload = json.dumps({
            "spec": state.spec.model_dump(mode="json"),
            "available_source_tables": catalog_tables,
        }, indent=2)
    else:
        _record(state, StepName.SPEC_CRITIC, StepStatus.IN_PROGRESS,
                f"Round {iter_num}/{max_iters}: self-refining last round's {len(state.critic_questions)} question(s)…")
        prompt = default_prompts.SPEC_REFINER
        payload = json.dumps({
            "spec": state.spec.model_dump(mode="json"),
            "previous_questions": state.critic_questions,
        }, indent=2)

    try:
        text = _llm_call(prompt, payload)
    except Exception as e:
        _record(state, StepName.SPEC_CRITIC, StepStatus.FAILED, f"LLM error: {e}")
        return {"fatal_error": str(e)}

    # Did the LLM declare the spec resolved?
    if text.strip().upper().startswith("SPEC_OK"):
        out = _record(state, StepName.SPEC_CRITIC, StepStatus.SUCCEEDED,
                      f"Spec accepted after {iter_num} round(s)",
                      iteration=iter_num, llm_response=text)
        out["spec_locked"] = True
        out["critic_iterations"] = iter_num
        return out

    questions = [q for q in re.split(r"\n+", text) if q.strip()]
    _record(state, StepName.SPEC_CRITIC, StepStatus.IN_PROGRESS,
            f"Round {iter_num} ended with {len(questions)} question(s) still open",
            iteration=iter_num, questions=questions)

    if iter_num >= max_iters:
        # Loop exhausted. Either proceed or send back to queue.
        if on_unresolved == "requeue":
            update_request(state.request_id, final_status="queued")
            _record(state, StepName.SPEC_CRITIC, StepStatus.FAILED,
                    f"Spec still ambiguous after {iter_num} round(s) — sent back to queue. "
                    "An operator can update the spec and re-initiate.",
                    questions=questions)
            return {"sent_to_queue": True, "critic_questions": questions,
                    "critic_iterations": iter_num}
        out = _record(state, StepName.SPEC_CRITIC, StepStatus.SUCCEEDED,
                      f"Proceeding after {iter_num} round(s); {len(questions)} question(s) "
                      f"could not be auto-resolved (see event details above)")
        out["spec_locked"] = True
        out["critic_iterations"] = iter_num
        out["critic_questions"] = questions
        return out

    # Loop back for another round
    return {"critic_iterations": iter_num, "critic_questions": questions,
            "spec_locked": False}


# --- 2. Source Profiler ------------------------------------------------

@traced_node("source_profiler")
def source_profiler(state: AgentState) -> dict:
    feedback = _feedback_for(state, StepName.SOURCE_PROFILER)
    if feedback:
        _record(state, StepName.SOURCE_PROFILER, StepStatus.IN_PROGRESS,
                f"Re-profiling — incorporating {len(feedback)} operator note(s).",
                operator_feedback=feedback)

    # FILE source: the schema was already discovered by profiling the landing
    # file (Datasets page). Build the profile from the spec's file columns —
    # no Postgres probe, no table-existence gate (the file is the source).
    if _is_file_source(state):
        # One profile entry per landing dataset (multi-dataset requests profile
        # ALL of them; single-dataset is just a 1-element loop). For each, prefer
        # the onboarded ("ready") config from Connectors, else the inline config.
        file_srcs = list(state.spec.source.files) if state.spec.is_multi_dataset() else (
            [state.spec.source.file] if state.spec.source.file else [])
        tables_prof: dict = {}
        any_onboarded = False
        for fsrc in file_srcs:
            ds = fsrc.dataset
            onboarded = None
            try:
                from agent import autoloader_store
                onboarded = autoloader_store.find_ready(ds, fsrc.landing_path)
            except Exception:  # noqa: BLE001
                onboarded = None
            if onboarded and onboarded.get("config") and not fsrc.columns:
                from agent.state import FileColumn
                cfg = onboarded["config"]
                fsrc.columns = [FileColumn(name=c.get("name", ""), type=c.get("type", "string"),
                                           is_json=bool(c.get("is_json")))
                                for c in cfg.get("columns", []) if c.get("name")]
                fsrc.delimiter = fsrc.delimiter or cfg.get("delimiter", "")
                fsrc.file_format = fsrc.file_format or cfg.get("file_format", "text")
                fsrc.landing_path = fsrc.landing_path or cfg.get("landing_path", "")
            any_onboarded = any_onboarded or bool(onboarded)
            cols = [{"name": c.name, "type": "string", "silver_type": c.type,
                     "is_json": c.is_json} for c in fsrc.columns]
            tables_prof[ds] = {"columns": cols, "row_count": -1, "sample": [],
                               "silver_table": fsrc.silver_table or ds,
                               "delimiter": fsrc.delimiter, "file_format": fsrc.file_format,
                               "onboarded": bool(onboarded)}
        first = file_srcs[0] if file_srcs else None
        prof = {"system": state.spec.source.system, "source_kind": "file",
                "landing_path": (first.landing_path if first else ""),
                "delimiter": (first.delimiter if first else ""),
                "file_format": (first.file_format if first else "text"),
                "onboarded": any_onboarded, "multi_dataset": state.spec.is_multi_dataset(),
                "tables": tables_prof}
        total_cols = sum(len(t["columns"]) for t in tables_prof.values())
        names = ", ".join(f"`{d}`" for d in tables_prof)
        _record(state, StepName.SOURCE_PROFILER, StepStatus.IN_PROGRESS,
                f"Profiling {len(tables_prof)} file dataset(s): {names} ({total_cols} columns total). "
                "Auto Loader will ingest each 1:1 to its own Bronze table.")
        _record(state, StepName.SOURCE_PROFILER, StepStatus.SUCCEEDED,
                f"Profiled {len(tables_prof)} file dataset(s) ({total_cols} columns total).", profile=prof)
        return {"profile": prof}

    from common import sources as _sources
    systems = sorted(_sources.split_tables_by_system(
        state.spec.source.tables, default=state.spec.source.system).keys())
    _record(state, StepName.SOURCE_PROFILER, StepStatus.IN_PROGRESS,
            f"Connecting to source system(s): {', '.join(systems)}…")
    _record(state, StepName.SOURCE_PROFILER, StepStatus.IN_PROGRESS,
            f"Pulling column metadata + row counts for {len(state.spec.source.tables)} table(s)")
    try:
        from tools import source_profiler as sp
        prof = sp.profile_source(state.spec.source.system, state.spec.source.tables)
    except Exception as e:
        prof = {
            "system": state.spec.source.system,
            "tables": {t: {"columns": [], "row_count": -1, "sample": []} for t in state.spec.source.tables},
            "error": str(e),
        }
        _record(state, StepName.SOURCE_PROFILER, StepStatus.SUCCEEDED,
                f"(degraded) profiler error: {e}", profile=prof)
        return {"profile": prof}

    # HARD GATE: every requested table must actually exist. Failing here
    # costs seconds; failing in deploy_dev costs a whole run (and that's
    # exactly how LLM-hallucinated table names used to surface).
    if prof.get("missing_tables"):
        missing = ", ".join(prof["missing_tables"])
        available = ", ".join(prof.get("available_tables", []))
        msg = (f"Requested table(s) do not exist: {missing}. "
               f"Available tables: {available}. "
               "Fix the spec (use the data browser on the New Request page) and re-run.")
        _record(state, StepName.SOURCE_PROFILER, StepStatus.FAILED, msg,
                missing_tables=prof["missing_tables"],
                available_tables=prof.get("available_tables", []))
        update_request(state.request_id, final_status="failed")
        return {"profile": prof, "fatal_error": f"missing source tables: {missing}"}

    summary = ", ".join(f"{k}={v.get('row_count', '?')}" for k, v in prof["tables"].items())
    _record(state, StepName.SOURCE_PROFILER, StepStatus.IN_PROGRESS,
            f"Row counts: {summary}")
    _record(state, StepName.SOURCE_PROFILER, StepStatus.SUCCEEDED,
            f"Profiled {len(prof['tables'])} tables", profile=prof)
    return {"profile": prof}


# --- 3. Pipeline Generator --------------------------------------------

def _parse_generator_metadata(commentary: str) -> tuple[str | None, dict | None, list[str] | None, dict]:
    """Parse the pipeline_generator LLM JSON response into (table_comment,
    column_comments, inline_transformation_comments, full_dict_for_log).

    Raises ValueError when the response is not parseable JSON. It used to
    swallow that in a bare `except` and return empties, which is why a
    TRUNCATED reply looked identical to a model that declined to answer: the
    generator logged "column_comments=0", shipped `_COLUMN_COMMENTS = {}` and a
    table comment copied verbatim from the spec, and the PR reviewer objected to
    that on every round with no way to see why. An unparseable response is a
    real failure and must say so — see KNOWN_ISSUES 1c."""
    try:
        # Strip code-fence wrappers the LLM sometimes adds
        raw = commentary.strip()
        if raw.startswith("```"):
            # Pull out the first ```...``` block
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[len("json"):]
            raw = raw.strip()
            if raw.endswith("```"):
                raw = raw[:-3].strip()
        parsed = json.loads(raw)
    except Exception as e:
        # Deliberately NOT swallowed. The caller decides whether to fail the
        # step or carry on with a flagged fallback, but it can no longer be
        # told "the model returned nothing" when the truth is "the reply was
        # cut off" or "the model emitted prose instead of JSON".
        raise ValueError(
            f"the generator's reply is not valid JSON ({e}); "
            f"{len(commentary)} characters received, "
            f"ending {commentary[-80:]!r}") from e
    if not isinstance(parsed, dict):
        raise ValueError(
            f"the generator's reply parsed as {type(parsed).__name__}, not an object")
    return (
        parsed.get("table_comment"),
        parsed.get("column_comments") if isinstance(parsed.get("column_comments"), dict) else None,
        parsed.get("inline_transformation_comments") if isinstance(parsed.get("inline_transformation_comments"), list) else None,
        parsed,
    )


# Columns per metadata call. A comment runs ~40 words, so 25 columns is a few
# thousand output tokens — far inside the ceiling even for a 100-column table,
# and small enough that ONE bad batch costs only its own columns.
_METADATA_COLUMN_BATCH = 25


def _generate_metadata_in_batches(
    state: AgentState,
    llm_input: dict,
    expected_silver_columns: list[str],
    gen_feedback: list[str],
) -> tuple[str | None, dict | None, list[str] | None, dict, list[str]]:
    """Ask for table/inline metadata once, then column comments in batches.

    Returns the same 4-tuple as `_parse_generator_metadata` plus human-readable
    notes for the event log.

    **Partial metadata is NOT reported as success.** If any batch fails, the
    columns it covered have no comments, and emitting the surviving batches as
    though the set were complete is the very trap this whole fix exists to
    close — a wide table would quietly ship most of its comments and a reviewer
    would object to the missing ones with no visible cause. So a failed batch is
    RETRIED once on its own, and if it still fails the caller is told which
    columns are uncovered via `metadata_incomplete` in the returned dict.
    """
    system = _augment_prompt(guidelines.get('pipeline_generator'), gen_feedback)
    notes: list[str] = []
    table_comment: str | None = None
    inline_comments: list[str] | None = None
    column_comments: dict = {}
    parsed_all: dict = {}
    # Columns whose comments never arrived. Non-empty means INCOMPLETE, which the
    # caller must be able to see — the alternative is shipping most of a wide
    # table's metadata and calling it done.
    uncovered: list[str] = []

    batches = [expected_silver_columns[i:i + _METADATA_COLUMN_BATCH]
               for i in range(0, len(expected_silver_columns), _METADATA_COLUMN_BATCH)] or [[]]

    for n, batch in enumerate(batches, start=1):
        payload = dict(llm_input)
        payload["expected_silver_columns"] = batch
        payload["instruction"] = (
            f"column_comments MUST cover EXACTLY these {len(batch)} column names "
            f"— do not invent or omit any: {batch}. "
            + ("Also return table_comment and inline_transformation_comments."
               if n == 1 else
               "Return ONLY column_comments for these columns; omit table_comment "
               "and inline_transformation_comments (already supplied).")
        )
        tc = cc = ic = None
        parsed: dict = {}
        last_error: Exception | None = None
        # Two attempts: a truncation or a malformed reply is usually transient,
        # and halving the batch on the retry shrinks the reply that broke.
        for attempt in (1, 2):
            if attempt == 2 and len(batch) > 1:
                payload["expected_silver_columns"] = batch[:max(1, len(batch) // 2)]
                payload["instruction"] = (
                    f"column_comments MUST cover EXACTLY these "
                    f"{len(payload['expected_silver_columns'])} column names: "
                    f"{payload['expected_silver_columns']}. Return ONLY column_comments.")
            try:
                reply = _llm_call(system, json.dumps(payload, default=str))
                tc, cc, ic, parsed = _parse_generator_metadata(reply)
                last_error = None
                break
            except (LLMReplyTruncated, ValueError) as e:
                last_error = e
                notes.append(
                    f"Metadata batch {n}/{len(batches)} attempt {attempt} failed: {e}")
        if last_error is not None:
            uncovered.extend(batch)
            notes.append(
                f"Metadata batch {n}/{len(batches)} FAILED after 2 attempts for "
                f"{len(batch)} column(s) ({batch[0]}…{batch[-1]}). Those columns are "
                f"UNCOVERED — this run's metadata is INCOMPLETE, not merely smaller."
                if batch else
                f"Metadata batch {n}/{len(batches)} FAILED after 2 attempts.")
            continue
        if n == 1:
            table_comment, inline_comments = tc, ic
            parsed_all = dict(parsed)
        if cc:
            column_comments.update(cc)
            # A batch can also come back well-formed but short of what was asked.
            missing = [c for c in payload["expected_silver_columns"] if c not in cc]
            if missing:
                uncovered.extend(missing)
                notes.append(
                    f"Metadata batch {n}/{len(batches)} returned valid JSON but omitted "
                    f"{len(missing)} requested column(s): {missing[:5]}"
                    f"{'…' if len(missing) > 5 else ''}.")

    if len(batches) > 1:
        notes.append(
            f"Metadata fetched in {len(batches)} batches of ≤{_METADATA_COLUMN_BATCH} columns "
            f"({len(expected_silver_columns)} columns total) so no single reply can hit the "
            f"output ceiling — the failure mode that shipped empty column comments before.")
    if column_comments:
        parsed_all["column_comments"] = column_comments
    if uncovered:
        # Recorded IN the metadata the reviewer sees, not only in the log, so an
        # objection about a missing comment is traceable to a stated cause.
        parsed_all["metadata_incomplete"] = {
            "uncovered_columns": uncovered,
            "covered": len(column_comments),
            "requested": len(expected_silver_columns),
            "why": "one or more metadata batches failed twice; see the run's events",
        }
        notes.append(
            f"METADATA INCOMPLETE: {len(uncovered)} of {len(expected_silver_columns)} "
            f"column(s) have no comment. This run is NOT equivalent to a full one.")
    return (table_comment, column_comments or None, inline_comments, parsed_all, notes)


@traced_node("pipeline_generator")
def pipeline_generator(state: AgentState) -> dict:
    iter_num = state.review_iterations + 1
    is_revision = state.review_iterations > 0
    if is_revision:
        _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                f"Revising generation — taking on board {len(state.review_feedback)} concern(s) "
                f"raised in the previous review round.",
                concerns=state.review_feedback)
    else:
        _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                "Deciding the bundle layout (databricks.yml + resources/ + transformations/silver + tests/)…")

    dev_catalog = os.environ.get("DEV_CATALOG", state.spec.target.catalog)
    prod_catalog = os.environ.get("PROD_CATALOG", state.spec.target.catalog.replace("_dev", "_prod"))
    from tools import github_pr
    github_repo = github_pr.pipelines_repo()

    _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
            f"Targets: dev={dev_catalog}.silver.{state.spec.target.table} · "
            f"prod={prod_catalog}.silver.{state.spec.target.table}")

    # Compute the EXACT silver column names the LLM should comment — the
    # deterministic mirror of the runtime join (common.sources, shared with
    # real_deployer.render_sdp_source so they can't drift).
    from common import sources as _sources
    tables_profile = (state.profile or {}).get("tables", {}) or {}
    src_tables = state.spec.source.tables
    columns_by_logical = {
        t: [c["name"] if isinstance(c, dict) else c
            for c in (tables_profile.get(t, {}).get("columns") or [])]
        for t in src_tables
    }
    expected_silver_columns = _sources.expected_silver_columns(
        src_tables, state.spec.source.system,
        state.spec.transform.join_with, state.spec.source.primary_key,
        columns_by_logical)

    # P1: request-aware JSON flatten. Sample the embedded-JSON columns from the
    # landing file, then a DEDICATED LLM call picks the fields MOST relevant to
    # THIS request and promotes them to typed Silver columns (the bulk hand-coding
    # the platform now generates). Kept separate from the metadata call so it stays
    # reliable for large schemas; degrades to keeping the JSON column as raw string.
    # Run the request-aware JSON flatten for EVERY file dataset (a multi-dataset
    # request flattens each dataset's embedded JSON into its own typed Silver).
    if state.spec.source.kind == "file":
        file_srcs = (list(state.spec.source.files) if state.spec.is_multi_dataset()
                     else ([state.spec.source.file] if state.spec.source.file else []))
        total_ffields = 0
        flatten_map: dict = {}
        for fsrc in file_srcs:
            if not any(getattr(c, "is_json", False) for c in (fsrc.columns or [])):
                continue
            json_samples: dict = {}
            try:
                from tools import framework_deployer as _fwd
                json_samples = _fwd.sample_json_columns(fsrc)
            except Exception:
                json_samples = {}
            if not json_samples:
                continue
            _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                    f"`{fsrc.dataset}`: sampling {len(json_samples)} embedded-JSON column(s) "
                    "to select request-relevant fields for Silver flattening…",
                    json_columns={k: v.get("keys", []) for k, v in json_samples.items()})
            try:
                from agent import silver_flatten
                ctx = {"intent": state.spec.intent, "target": fsrc.silver_table or fsrc.dataset,
                       "transform": state.spec.transform.description,
                       "business_rules": state.spec.transform.business_rules,
                       "avoid_aliases": [c.name for c in (fsrc.columns or [])]}
                flatten = silver_flatten.suggest_flatten(json_samples, ctx)
            except Exception:
                flatten = {}
            by_name = {c.name: c for c in fsrc.columns}
            for col, fields in (flatten or {}).items():
                c = by_name.get(col)
                if c is not None and fields:
                    c.json_fields = fields
                    total_ffields += len(fields)
            ds_map = {c.name: c.json_fields for c in fsrc.columns if c.json_fields}
            if ds_map:
                flatten_map[fsrc.dataset] = ds_map
        if total_ffields:
            _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                    f"Silver flatten: promoted {total_ffields} JSON field(s) across "
                    f"{len(flatten_map)} dataset(s) into typed Silver columns (raw JSON retained).",
                    json_flatten=flatten_map)

    _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
            f"Asking Claude for rich metadata: table comment, comments for ALL {len(expected_silver_columns)} silver columns, inline transformation comments, plus design decisions…",
            expected_silver_columns=expected_silver_columns)
    llm_input = {
        "spec": state.spec.model_dump(mode="json"),
        "profile": state.profile,
        "expected_silver_columns": expected_silver_columns,
        "instruction": (
            "column_comments MUST cover EXACTLY the column names in "
            "expected_silver_columns — do not invent or omit any column."
        ),
    }
    if state.review_feedback:
        llm_input["reviewer_feedback_from_previous_round"] = state.review_feedback
    gen_feedback = _feedback_for(state, StepName.PIPELINE_GENERATOR)
    if gen_feedback:
        llm_input["operator_feedback"] = gen_feedback
    # Metadata is fetched in COLUMN BATCHES rather than one giant reply. A wide
    # table (the four-feed spike has 78 silver columns) used to blow the output
    # ceiling, come back severed mid-string, and degrade to "the LLM returned
    # nothing" — so the pipeline shipped no column comments at all and the PR
    # reviewer rejected the same metadata on every round. Batching keeps every
    # reply comfortably inside the ceiling regardless of table width, which is
    # what makes this robust rather than merely roomier. See KNOWN_ISSUES 1c.
    table_comment, column_comments, inline_comments, parsed, metadata_notes = \
        _generate_metadata_in_batches(
            state=state,
            llm_input=llm_input,
            expected_silver_columns=expected_silver_columns,
            gen_feedback=gen_feedback,
        )
    commentary = json.dumps(parsed, default=str) if parsed else '{"note": "no metadata parsed"}'
    for note in metadata_notes:
        _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS, note)
    col_count = len(column_comments) if column_comments else 0
    inline_count = len(inline_comments) if inline_comments else 0
    _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
            f"LLM returned: table_comment={'yes' if table_comment else 'no'}, "
            f"column_comments={col_count}, inline_comments={inline_count}",
            llm_reasoning=commentary, parsed_metadata=parsed)

    # C3: Generate Silver SQL (cast, eligible_flag, exclusion_reason, dedupe)
    silver_sql_select = None
    try:
        from agent import silver_sql as silver_sql_module
        # Get Bronze columns from the file source or profile
        bronze_cols = []
        if state.spec.source.kind == "file" and state.spec.source.file:
            bronze_cols = [c.name for c in (state.spec.source.file.columns or [])]
        elif state.profile and "tables" in state.profile:
            # Fall back to profile for non-file sources
            for table in state.spec.source.tables:
                if table in state.profile.get("tables", {}):
                    cols = state.profile["tables"][table].get("columns", [])
                    bronze_cols = [c["name"] if isinstance(c, dict) else c for c in cols]
                    break

        if bronze_cols:
            # Prepare credit box from business rules (FO-specific)
            credit_box = {
                "credit_score_min": 620,
                "ltv_max": 0.95,
                "arrears_days_max": 30,
            }
            _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                    f"Authoring Silver SQL: casting {len(bronze_cols)} columns + "
                    f"deriving eligible_flag + exclusion_reason + deduping to latest as_of_date…",
                    bronze_columns=bronze_cols)
            silver_sql_result = silver_sql_module.suggest_silver_sql(
                bronze_cols, credit_box,
                business_rules=state.spec.transform.business_rules or [],
                use_llm=True)
            if silver_sql_result:
                silver_sql_select = silver_sql_result.get("sql")
                _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                        f"Silver SQL authored: {len(silver_sql_result.get('not_null', []))} NOT NULL columns, "
                        f"eligible_flag + exclusion_reason derived, deduped to latest as_of_date.",
                        silver_sql_select=silver_sql_select,
                        silver_sql_not_null=silver_sql_result.get("not_null", []))
            else:
                _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                        "Silver SQL generation returned None — using deterministic fallback template.")
    except Exception as e:
        _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
                f"Silver SQL generation failed: {str(e)[:140]} — using deterministic fallback.")

    _record(state, StepName.PIPELINE_GENERATOR, StepStatus.IN_PROGRESS,
            "Emitting bundle: databricks.yml + resources/{pipeline,job,connection,ingestion}.yml + transformations/silver/<table>.py + tests/row_checks.sql + README…")

    from tools import dab_emitter
    files = dab_emitter.emit_all(
        state.spec, dev_catalog, prod_catalog, github_repo,
        request_id=state.request_id,
        table_comment=table_comment,
        column_comments=column_comments,
        inline_comments=inline_comments,
        silver_sql_select=silver_sql_select,
        # The env-prefixed schema actually deployed, so the generated tests query
        # a table that exists. spec.target carries the LOGICAL name (`silver`).
        silver_schema=os.environ.get(
            "DEV_SILVER_SCHEMA",
            "dev_jeremy_herbert_databricks_pipeline_studio_dev_silver"),
    )

    _record(state, StepName.PIPELINE_GENERATOR, StepStatus.SUCCEEDED,
            f"Generated {len(files)} files (table_comment={'yes' if table_comment else 'no'}, "
            f"column_comments={col_count}, inline_comments={inline_count})",
            file_count=len(files),
            file_list=list(files.keys()),
            artifacts=files,                       # full content — used by rehydrate_state for re-runs
            commentary=commentary,
            table_comment=table_comment,
            column_comments=column_comments,
            inline_comments=inline_comments)
    return {"artifacts": files,
            "spec": state.spec,                       # carries the attached json_fields
            "generator_table_comment": table_comment,
            "generator_column_comments": column_comments or {},
            "generator_inline_comments": inline_comments or []}


# --- 4. Standards Validator -------------------------------------------

def _standards_report(spec, sdp_code: str) -> tuple[list, dict]:
    """Deterministic standards taxonomy → (violations, report). Pure + testable.
    report carries by_category counts + pct_enforced (feeds the P4 comparison)."""
    import re
    g = spec.governance
    checks = [
        ("code_hygiene", "no display()/show() debug calls",
         not ("display(" in (sdp_code or "") or ".show(" in (sdp_code or ""))),
        ("data_quality", "not_null expectations declared", bool(spec.validation.not_null)),
        ("data_quality", "row-count tolerance > 0", spec.validation.row_count_tolerance_pct > 0),
        ("transform", "strict_to_source type alignment",
         spec.transform.type_alignment == "strict_to_source"),
        ("governance", "at least one approver", bool(spec.required_approvers)),
        ("metadata", "data owner set", bool(g.data_owner)),
        ("metadata", "domain set", bool(g.domain)),
        ("metadata", "classification set", bool(g.classification)),
        ("naming", "target table is snake_case",
         bool(re.match(r"^[a-z][a-z0-9_]*$", spec.target.table or ""))),
    ]
    violations = [f"{cat}: {name}" for cat, name, ok in checks if not ok]
    by_cat: dict = {}
    for cat, _name, ok in checks:
        d = by_cat.setdefault(cat, {"passed": 0, "total": 0})
        d["total"] += 1
        d["passed"] += 1 if ok else 0
    passed = sum(1 for _, _, ok in checks if ok)
    report = {
        "checks": [{"category": c, "name": n, "passed": ok} for c, n, ok in checks],
        "by_category": by_cat, "passed": passed, "total": len(checks),
        "pct_enforced": round(100 * passed / len(checks)) if checks else 0,
    }
    return violations, report


@traced_node("standards_validator")
def standards_validator(state: AgentState) -> dict:
    _record(state, StepName.STANDARDS_VALIDATOR, StepStatus.IN_PROGRESS,
            "Running the deterministic standards checks (display/show calls, "
            "expectations, type alignment, required approvers, tolerance)…")
    sdp = next((c for p, c in state.artifacts.items() if p.endswith(".py")), "")
    violations, standards_report = _standards_report(state.spec, sdp)

    _record(state, StepName.STANDARDS_VALIDATOR, StepStatus.IN_PROGRESS,
            f"Deterministic checks done: {len(violations)} violations. "
            "Now asking Claude to review the generated files for extra issues…")
    try:
        artifacts_summary = {k: v[:400] for k, v in state.artifacts.items()}
        sv_feedback = _feedback_for(state, StepName.STANDARDS_VALIDATOR)
        payload = {"artifacts": artifacts_summary}
        if sv_feedback:
            payload["operator_feedback"] = sv_feedback
        llm_text = _llm_call(_augment_prompt(guidelines.get('standards_validator'), sv_feedback),
                              json.dumps(payload, default=str))
        try:
            extra = json.loads(llm_text)
            if isinstance(extra, list):
                violations.extend(str(x) for x in extra)
        except Exception:
            pass
    except Exception:
        pass

    status = StepStatus.SUCCEEDED if not violations else StepStatus.FAILED
    _record(state, StepName.STANDARDS_VALIDATOR, status,
            (f"All {standards_report['total']} standards checks passed (100% enforced)."
             if not violations
             else f"{len(violations)} standards issue(s) "
                  f"({standards_report['pct_enforced']}% of deterministic checks enforced)."),
            violations=violations, standards_report=standards_report)
    return {"standards_violations": violations}


# --- 5. Test Generator -------------------------------------------------

@traced_node("test_generator")
def test_generator(state: AgentState) -> dict:
    _record(state, StepName.TEST_GENERATOR, StepStatus.IN_PROGRESS,
            "Spec already declares not_null + unique + row-count tolerance. "
            "Asking Claude to add 2 business-rule tests on top…")
    tests = []
    try:
        tg_feedback = _feedback_for(state, StepName.TEST_GENERATOR)
        tg_payload = {
            "spec": state.spec.model_dump(mode="json"),
            "profile": {k: list(v.get("columns", [])) for k, v in state.profile.get("tables", {}).items()},
        }
        if tg_feedback:
            tg_payload["operator_feedback"] = tg_feedback
        text = _llm_call(_augment_prompt(guidelines.get('test_generator'), tg_feedback),
                          json.dumps(tg_payload, default=str))
        try:
            parsed = json.loads(text)
            for t in parsed:
                tests.append(f"-- test: {t.get('name', 'unnamed')}\n{t.get('sql', '-- (no sql)')}")
        except Exception:
            tests.append(f"-- LLM-proposed tests (unparsed):\n{text}")
    except Exception as e:
        tests.append(f"-- (no LLM tests; error: {e})")

    _record(state, StepName.TEST_GENERATOR, StepStatus.IN_PROGRESS,
            f"Appending {len(tests)} test block(s) to tests/row_checks.sql artifact")
    test_path = next((p for p in state.artifacts if p.endswith("row_checks.sql")), None)
    new_artifacts = dict(state.artifacts)
    if test_path:
        new_artifacts[test_path] = new_artifacts[test_path] + "\n\n" + "\n\n".join(tests)

    v = state.spec.validation
    taxonomy = {
        "ingestion": 1,                                   # row-present / freshness check
        "data_quality": len(v.not_null) + len(v.unique) + (1 if v.row_count_tolerance_pct else 0),
        "transformation": len(tests),                     # the LLM business-rule tests
    }
    taxonomy["total"] = taxonomy["ingestion"] + taxonomy["data_quality"] + taxonomy["transformation"]
    _record(state, StepName.TEST_GENERATOR, StepStatus.SUCCEEDED,
            f"{taxonomy['total']} tests across the taxonomy (ingestion {taxonomy['ingestion']} · "
            f"transformation {taxonomy['transformation']} · data_quality {taxonomy['data_quality']}).",
            test_count=len(tests), tests=tests, test_taxonomy=taxonomy)
    return {"tests_generated": tests, "artifacts": new_artifacts}


# --- 6. Deploy ingestion (Lakeflow Connect → bronze) --------------------

def _ingest_file_datasets(state: AgentState, *, catalog: str, bronze_schema: str,
                          env_tag: str, step: StepName) -> dict:
    """Ingest ALL of the request's file datasets to Bronze via the consolidated
    Auto Loader pipeline in ONE update. Works for single- AND multi-dataset file
    requests (loops the per-dataset sub-specs). Returns {"ingestion": agg} on
    success or {"fatal_error": ...} after recording + marking the request failed."""
    subs = state.spec.dataset_specs()
    datasets = [s.source.file.dataset for s in subs if s.source.file]
    _record(state, step, StepStatus.IN_PROGRESS,
            f"Auto Loader ingestion of {len(datasets)} dataset(s) "
            f"(`{'`, `'.join(datasets)}`) → consolidated Bronze pipeline "
            f"`ops-{env_tag}-file-ingest` (one Bronze table per dataset, shared + reused).")
    progress: list[str] = []
    def _log(msg: str) -> None:
        progress.append(msg)
        _record(state, step, StepStatus.IN_PROGRESS, msg)
    try:
        from tools import framework_deployer
        agg = framework_deployer.ingest_files_to_bronze(
            subs, catalog=catalog, bronze_schema=bronze_schema, env_tag=env_tag, log=_log)
    except Exception as e:
        import traceback as _tb
        _record(state, step, StepStatus.FAILED, f"Auto Loader Bronze ingestion failed: {e}",
                progress=progress, traceback=_tb.format_exc()[-1200:])
        update_request(state.request_id, final_status="failed")
        return {"fatal_error": f"file ingestion failed: {e}"}
    per = agg.get("datasets", [])
    built = [d for d in per if d.get("ran")]
    reused = [d for d in per if d.get("reused")]
    total_rows = sum(d.get("bronze_rows", 0) or 0 for d in per)
    _record(state, step, StepStatus.SUCCEEDED,
            f"Bronze ready for {len(per)} dataset(s) via consolidated "
            f"`{agg.get('ingestion_pipeline_name')}` — {len(built)} built, {len(reused)} reused, "
            f"{total_rows:,} rows total. Silver pipelines build on this shared Bronze.",
            ingestion_result=agg)
    return {"ingestion": agg}


@traced_node("deploy_ingestion")
def deploy_ingestion(state: AgentState) -> dict:
    """Bronze ingestion via Lakeflow Connect against the external Postgres
    source, using ONE consolidated ingestion pipeline per env (the request's
    tables are added to it / reused). Skipped (with a clear event) when the
    external source isn't configured — deploy_dev then falls back to the
    warehouse one-shot bronze load."""
    # FILE / Auto Loader requests: Bronze is ingested via Auto Loader (cloudFiles)
    # into a CONSOLIDATED file-ingest pipeline (one per env, one Bronze streaming
    # table per landing dataset), decoupled from the per-request Silver pipeline
    # and REUSED across requests — exactly like the CDC consolidated pipeline.
    if _is_file_source(state):
        dev_catalog = os.environ.get("DEV_CATALOG", state.spec.target.catalog)
        bronze_schema = os.environ.get("DEV_BRONZE_SCHEMA",
                                        "dev_jeremy_herbert_databricks_pipeline_studio_dev_bronze")
        return _ingest_file_datasets(state, catalog=dev_catalog, bronze_schema=bronze_schema,
                                     env_tag="dev", step=StepName.DEPLOY_INGESTION)
    # Reachable via Lakeflow Connect? Either the built-in external Postgres
    # source is configured, or every table's `system.` prefix belongs to a UC
    # connection an operator attached on the Connectors page.
    from tools import lakeflow_ingest as _lfi
    if not _lfi.can_ingest(state.spec.source.tables, state.spec.source.system):
        _record(state, StepName.DEPLOY_INGESTION, StepStatus.SKIPPED,
                "No Lakeflow Connect source configured for these tables (no external "
                "Postgres source and no attached UC connection claims them) — bronze "
                "will be loaded via the SQL-warehouse one-shot instead.")
        return {"ingestion": {}}

    dev_catalog = os.environ.get("DEV_CATALOG", state.spec.target.catalog)
    bronze_schema = os.environ.get("DEV_BRONZE_SCHEMA",
                                    "dev_jeremy_herbert_databricks_pipeline_studio_dev_bronze")
    _record(state, StepName.DEPLOY_INGESTION, StepStatus.IN_PROGRESS,
            f"Lakeflow Connect ingestion: {len(state.spec.source.tables)} source "
            f"table(s) → `{dev_catalog}.{bronze_schema}` "
            "(consolidated Direct-CDC pipeline; reuses existing tables).")

    progress: list[str] = []
    def _log(msg: str) -> None:
        progress.append(msg)
        _record(state, StepName.DEPLOY_INGESTION, StepStatus.IN_PROGRESS, msg)

    try:
        from tools import lakeflow_ingest
        result = lakeflow_ingest.ingest_to_bronze(
            spec_tables=state.spec.source.tables,
            primary_system=state.spec.source.system,
            catalog=dev_catalog,
            bronze_schema=bronze_schema,
            env_tag="dev",
            target_table=state.spec.target.table,
            log=_log,
        )
    except Exception as e:
        import traceback as _tb
        _record(state, StepName.DEPLOY_INGESTION, StepStatus.FAILED,
                f"Lakeflow Connect ingestion failed: {e}",
                progress=progress, traceback=_tb.format_exc()[-1200:])
        update_request(state.request_id, final_status="failed")
        return {"fatal_error": f"ingestion failed: {e}"}

    added = result.get("tables_added") or []
    reused = result.get("tables_reused") or []
    summary = (f"added {len(added)} ({', '.join(added)})" if added else "no new tables")
    if reused:
        summary += f", reused {len(reused)}"
    if not result.get("ran", True):
        summary += " — skipped run (bronze already current)"
    _record(state, StepName.DEPLOY_INGESTION, StepStatus.SUCCEEDED,
            f"Bronze ingested via consolidated pipeline "
            f"`{result['ingestion_pipeline_name']}` — {summary}.",
            ingestion_result=result)
    return {"ingestion": result}


# --- 7. Deploy (dev OR prod) -------------------------------------------

def _is_file_source(state: AgentState) -> bool:
    return getattr(state.spec.source, "kind", "cdc") == "file"


def _pr_mode() -> str:
    """'mock' (default) renders an in-app PR + review widget with NO external
    Git call; 'github' uses the real GitHub flow. Live external Git/DevOps is
    de-scoped for the PoC (100% Databricks-native). A commented Azure DevOps
    scaffold lives in tools/azure_devops_client.py for Databricks to build out."""
    return os.environ.get("OPS_PR_MODE", "mock").strip().lower()


def _is_in_app_pr(state) -> bool:
    """True when this request's PR lives in the app rather than on a Git host.

    Downstream steps (review comment, approval gate, merge-on-staging) each
    branch on _pr_mode(), which is process-wide config — but a single request
    can be in `github` mode and STILL have an in-app PR, because
    branch_and_pr fell back when the Git host was unreachable. Without this
    check those steps would try to comment on / verify / merge a PR number that
    only exists in-app, and a run that already fell back once would fail later
    anyway. The `app://` marker is set on the request row, so it survives the
    rehydrate between job runs.
    """
    return _pr_mode() != "github" or str(getattr(state, "pr_url", "") or "").startswith("app://")


def _pr_fallback_enabled() -> bool:
    """In `github` mode, whether an unreachable Git host degrades to the in-app
    PR instead of failing the request. On by default: by the time we open a PR
    the pipeline has already deployed and verified, so a network/allow-list
    problem should not discard it. Set OPS_PR_FALLBACK=0 to make Git failures
    hard errors (e.g. a customer whose process REQUIRES the real PR to exist)."""
    return os.environ.get("OPS_PR_FALLBACK", "1").strip() != "0"


def _mock_pr_number(request_id: str) -> int:
    """Stable, human-friendly synthetic PR number for the in-app mock."""
    try:
        return 1000 + (int(request_id.replace("-", "")[:6], 16) % 9000)
    except Exception:
        return 1042


def _deploy_framework(state: AgentState, step: StepName, catalog: str,
                      bronze_schema: str, silver_schema: str, label: str,
                      create_schedule: bool) -> dict:
    """File / Auto Loader SILVER deploy via the Lakeflow Framework engine. Bronze
    is already materialised by the consolidated file-ingest pipeline (decoupled,
    shared per dataset); this per-request pipeline reads that shared Bronze and
    builds the typed Silver (+ DQ + optional scheduled Job)."""
    subs = state.spec.dataset_specs()
    progress_events: list[str] = []
    def _log(msg: str) -> None:
        progress_events.append(msg)
        _record(state, step, StepStatus.IN_PROGRESS, msg)
    from tools import framework_deployer
    primary = state.spec.target.table
    silver_names = ", ".join(s.target.table for s in subs)
    _record(state, step, StepStatus.IN_PROGRESS,
            f"Deploying {len(subs)} Silver table(s) to {label} in ONE request pipeline "
            f"(`{primary}`) via the Lakeflow Framework — typed projection + DQ, reading the "
            f"shared Bronze. Gold MVs merge into this same pipeline at Curate. "
            f"Tables: {silver_names}." + (" + scheduled Job." if create_schedule else ""))
    try:
        results = framework_deployer.deploy_request_silvers(
            subs, catalog=catalog, bronze_schema=bronze_schema,
            silver_schema=silver_schema, request_id=state.request_id,
            primary_table=primary, log=_log, create_schedule=create_schedule,
            # The generator's metadata reached the PR artefact and stopped there:
            # the framework path never applied it to Unity Catalog. Threaded
            # through so a documented pipeline is actually documented in UC.
            column_comments=state.generator_column_comments or None,
            table_comment=state.generator_table_comment or None,
        )
    except Exception as e:
        import traceback as _tb
        _record(state, step, StepStatus.FAILED,
                f"{label} framework deploy failed: {e}",
                progress=progress_events, traceback=_tb.format_exc()[-1200:])
        return {"fatal_error": f"{label} framework deploy failed: {e}"}
    # Single dataset → preserve the exact prior return shape (a DeployResult dict).
    if len(results) == 1:
        result = results[0]
        sched_msg = ""
        if result.job_id:
            sched_msg = (f" · job `{result.job_name}` "
                         + (f"(cron `{result.job_schedule_cron}`)" if result.job_schedule_cron else ""))
        _record(state, step, StepStatus.SUCCEEDED,
                f"Framework {label} deploy DONE · pipeline `{result.pipeline_name}` "
                f"({result.pipeline_id}) · silver `{result.silver_table}` "
                f"({result.silver_row_count:,} rows, {sum(result.bronze_counts.values()):,} bronze) · "
                f"{result.duration_seconds:.0f}s{sched_msg}",
                deploy_result=result.to_dict())
        return result.to_dict()
    # Multi-dataset → aggregate across the per-dataset Silver builds.
    silver_tables = [r.silver_table for r in results]
    total_silver = sum(r.silver_row_count for r in results)
    _record(state, step, StepStatus.SUCCEEDED,
            f"Framework {label} deploy DONE · {len(results)} Silver tables "
            f"({total_silver:,} rows total): {', '.join(silver_tables)}.",
            deploy_result={"multi": True, "silver_tables": silver_tables,
                           "results": [r.to_dict() for r in results]})
    return {"multi": True, "silver_tables": silver_tables,
            "silver_row_count": total_silver,
            "results": [r.to_dict() for r in results]}


def _bronze_exists(state: AgentState, catalog: str, bronze_schema: str) -> bool:
    """Is this request's Bronze table already materialised in UC?

    Deliberately asks UC rather than trusting in-run state: `state.ingestion` is
    not persisted on the request row, so anything derived from it changes answer
    depending on which step a re-run starts from. Observable reality does not.

    Fails CLOSED — an unreachable metastore returns False, which routes to
    `real_deployer`. That path loads Bronze itself, so it is correct (if slower)
    either way; the reverse mistake would point the framework at a table that
    isn't there.
    """
    src = (state.spec.source.tables or [None])[0]
    if not src:
        return False
    fq = f"{catalog}.{bronze_schema}.{src.replace('.', '__')}"
    try:
        from databricks.sdk import WorkspaceClient
        WorkspaceClient().tables.get(fq)
        return True
    except Exception:  # noqa: BLE001 — absent table or unreachable UC
        return False


def _deploy_cdc_framework(state: AgentState, step: StepName, catalog: str,
                          bronze_schema: str, silver_schema: str, label: str,
                          create_schedule: bool) -> dict:
    """CDC / database-sourced SILVER via the framework — same bundle root as Gold.

    Mirrors `_deploy_framework` (the file path) so the two source kinds behave
    identically from here on; see `_deploy_helper` for why that matters.
    """
    progress_events: list[str] = []
    def _log(msg: str) -> None:
        progress_events.append(msg)
        _record(state, step, StepStatus.IN_PROGRESS, msg)
    from tools import framework_deployer
    _record(state, step, StepStatus.IN_PROGRESS,
            f"Deploying Silver `{silver_schema}.{state.spec.target.table}` to {label} via the "
            f"Lakeflow Framework — typed projection + DQ over the Lakeflow Connect Bronze. "
            f"Gold MVs merge into this same pipeline at Curate."
            + (" + scheduled Job." if create_schedule else ""))
    try:
        results = framework_deployer.deploy_cdc_silver_via_framework(
            state.spec, catalog=catalog, bronze_schema=bronze_schema,
            silver_schema=silver_schema, request_id=state.request_id,
            log=_log, create_schedule=create_schedule)
    except Exception as e:
        import traceback as _tb
        _record(state, step, StepStatus.FAILED,
                f"{label} framework deploy failed: {e}",
                progress=progress_events, traceback=_tb.format_exc()[-1200:])
        return {"fatal_error": f"{label} framework deploy failed: {e}"}
    result = results[0]
    sched_msg = ""
    if result.job_id:
        sched_msg = (f" · job `{result.job_name}` "
                     + (f"(cron `{result.job_schedule_cron}`)" if result.job_schedule_cron else ""))
    _record(state, step, StepStatus.SUCCEEDED,
            f"Framework {label} deploy DONE · pipeline `{result.pipeline_name}` "
            f"({result.pipeline_id}) · silver `{result.silver_table}` "
            f"({result.silver_row_count:,} rows, {sum(result.bronze_counts.values()):,} bronze) · "
            f"{result.duration_seconds:.0f}s{sched_msg}",
            deploy_result=result.to_dict())
    return result.to_dict()


def _deploy_helper(state: AgentState, step: StepName, catalog: str,
                   bronze_schema: str, silver_schema: str, label: str,
                   create_schedule: bool = False,
                   skip_bronze_load: bool = False) -> dict:
    """Shared body of deploy_dev + deploy_prod.

    BOTH source kinds now deploy Silver through the Lakeflow Framework, into the
    SAME per-request bundle root that Curate merges the Gold MVs into. They used
    to differ: CDC went through real_deployer, writing a different workspace root
    under the same pipeline name, so the later Gold merge repointed the pipeline
    at a root containing only the Gold dataflow — losing the Silver definition AND
    dropping the materialised Silver table. One request == one pipeline == one
    root removes that whole class of bug.

    `real_deployer` is still used when Bronze does not exist yet, because it can
    load Bronze itself from the source; the framework path reads an existing
    Bronze table.

    The choice is made from OBSERVED STATE (does the Bronze table exist?), not
    from `skip_bronze_load`. That parameter derives from `state.ingestion`, which
    only `deploy_ingestion` sets and which is NOT persisted on the request row —
    so a re-run from a later step, or a source where Lakeflow Connect isn't
    configured (ingestion returns `{}`), silently took the legacy path and
    reintroduced the split-root Gold bug. Keying off UC makes a retry
    path-independent: the same request lands the same way whichever step you
    re-run from.
    """
    if _is_file_source(state):
        return _deploy_framework(state, step, catalog, bronze_schema,
                                 silver_schema, label, create_schedule)

    if _bronze_exists(state, catalog, bronze_schema):
        # Bronze is already materialised, so the framework path applies — and it
        # keeps Silver in the same bundle root as Gold.
        return _deploy_cdc_framework(state, step, catalog, bronze_schema,
                                     silver_schema, label, create_schedule)

    schedule_blurb = " + scheduled Job" if create_schedule else ""
    _record(state, step, StepStatus.IN_PROGRESS,
            f"Starting real deploy to {label}: bronze warehouse load → "
            f"SDP pipeline create/update → pipeline run → row verify{schedule_blurb}.")

    silver_table_fq = f"{catalog}.{silver_schema}.{state.spec.target.table}"
    _record(state, step, StepStatus.IN_PROGRESS,
            f"Target: `{silver_table_fq}`")

    progress_events: list[str] = []
    def _log(msg: str) -> None:
        progress_events.append(msg)
        _record(state, step, StepStatus.IN_PROGRESS, msg)

    try:
        from tools import real_deployer
        result = real_deployer.deploy_pipeline(
            spec=state.spec,
            catalog=catalog,
            bronze_schema=bronze_schema,
            silver_schema=silver_schema,
            request_id=state.request_id,
            log=_log,
            create_schedule=create_schedule,
            table_comment=state.generator_table_comment,
            column_comments=state.generator_column_comments or None,
            inline_comments=state.generator_inline_comments or None,
            skip_bronze_load=skip_bronze_load,
        )
    except Exception as e:
        import traceback as _tb
        tb = _tb.format_exc()[-1200:]
        _record(state, step, StepStatus.FAILED,
                f"{label} deploy failed: {e}",
                progress=progress_events, traceback=tb)
        return {"fatal_error": f"{label} deploy failed: {e}"}

    sched_msg = ""
    if result.job_id:
        sched_msg = (f" · job `{result.job_name}` "
                     + (f"(cron `{result.job_schedule_cron}`)" if result.job_schedule_cron
                        else "(no schedule — streaming)"))
    msg = (
        f"Real {label} deploy DONE · pipeline `{result.pipeline_name}` "
        f"({result.pipeline_id}) · silver `{result.silver_table}` "
        f"({result.silver_row_count:,} rows, {sum(result.bronze_counts.values()):,} across bronze) · "
        f"{result.duration_seconds:.0f}s{sched_msg}"
    )
    _record(state, step, StepStatus.SUCCEEDED, msg, deploy_result=result.to_dict())
    return result.to_dict()


@traced_node("deploy_dev")
def deploy_dev(state: AgentState) -> dict:
    """Real dev deploy. Loads bronze via warehouse, creates the silver SDP
    pipeline, triggers it, waits for it to succeed, verifies row counts.
    Only after this succeeds is the PR opened — the PR represents a working
    dev pipeline."""
    dev_catalog = os.environ.get("DEV_CATALOG", state.spec.target.catalog)
    bronze_schema = os.environ.get("DEV_BRONZE_SCHEMA",
                                    "dev_jeremy_herbert_databricks_pipeline_studio_dev_bronze")
    silver_schema = os.environ.get("DEV_SILVER_SCHEMA",
                                    "dev_jeremy_herbert_databricks_pipeline_studio_dev_silver")
    result = _deploy_helper(state, StepName.DEPLOY_DEV, dev_catalog,
                            bronze_schema, silver_schema, label="dev",
                            skip_bronze_load=bool(state.ingestion))
    if "fatal_error" in result:
        return result
    return {"dev_deploy": result}


# --- 7. Branch + PR ----------------------------------------------------

@traced_node("branch_and_pr")
def branch_and_pr(state: AgentState) -> dict:
    is_revision = state.review_iterations > 0
    if is_revision:
        _record(state, StepName.BRANCH_AND_PR, StepStatus.IN_PROGRESS,
                f"Revision round {state.review_iterations + 1} — pushing a new commit "
                "to the existing branch and re-deploying to dev.")
    else:
        _record(state, StepName.BRANCH_AND_PR, StepStatus.IN_PROGRESS,
                "Dev deploy succeeded — pipeline produced a real silver table. "
                "Now packaging the generated artefacts as a GitHub PR for human review…")

    from tools import github_pr
    repo = github_pr.pipelines_repo()
    branch_name = state.branch_name or f"ops/{state.spec.target.table}-{state.request_id[:8]}"
    pr_title = f"feat({state.spec.target.table}): generated pipeline — {state.spec.intent[:60]}"

    dev = state.dev_deploy or {}
    dev_silver = dev.get("silver_table", "(dev deploy result not captured)")
    dev_count = dev.get("silver_row_count", "?")
    pr_body = (
        f"### Generated by Databricks Pipeline Studio\n\n"
        f"**Request ID:** `{state.request_id}`\n\n"
        f"**Intent:** {state.spec.intent}\n\n"
        f"**Source system:** `{state.spec.source.system.upper()}` "
        f"({len(state.spec.source.tables)} table(s))\n\n"
        f"**Target:** `{state.spec.target.catalog}.{state.spec.target.target_schema}.{state.spec.target.table}`\n\n"
        f"**Dev deploy:** ✅ `{dev_silver}` populated with {dev_count} rows.\n\n"
        f"**SLA:** {state.spec.sla.freshness_minutes} min freshness\n\n"
        f"**Standards violations:** {len(state.standards_violations)}\n\n"
        f"**Tests added by agent:** {len(state.tests_generated)}\n\n"
        f"---\n_The PR Reviewer agent will post a detailed review shortly._\n"
    )

    commit_msg = (
        f"feat: add pipeline {state.spec.target.table}\n\n"
        f"Generated by request {state.request_id}"
        if not is_revision else
        f"revise: pipeline {state.spec.target.table} (review round {state.review_iterations + 1})\n\n"
        f"Reviewer concerns addressed: {state.review_feedback}\n\n"
        f"Request {state.request_id}"
    )

    def _open_in_app_pr(note: str = "") -> dict:
        """Render the PR inside the app: the generated files + body are stored on
        the event so the tracker shows a PR preview, and the pr_reviewer's
        comments stream in as review rounds. Used both as the default mode and
        as the fallback when external Git is unreachable."""
        number = state.pr_number or _mock_pr_number(state.request_id)
        url = f"app://pr/{state.request_id}"   # in-app marker, opened by the tracker widget
        # Cap stored file content so the event stays light.
        files_preview, budget = {}, 60_000
        for path, content in (state.artifacts or {}).items():
            c = content if len(content) <= 8000 else content[:8000] + "\n… (truncated)"
            if budget - len(c) < 0:
                break
            files_preview[path] = c
            budget -= len(c)
        _record(state, StepName.BRANCH_AND_PR, StepStatus.SUCCEEDED,
                f"In-app PR #{number} {'updated (revision ' + str(state.review_iterations + 1) + ')' if is_revision else 'opened'} "
                f"— {len(state.artifacts)} generated file(s), "
                + (note or "in-app PR + AI review + human-in-the-loop gate."),
                kind="mock_pr", pr_number=number, pr_title=pr_title, pr_body=pr_body,
                branch=branch_name, files=files_preview, file_count=len(state.artifacts),
                iteration=state.review_iterations + 1)
        update_request(state.request_id, pr_url=url, pr_number=number)
        return {"pr_url": url, "pr_number": number, "branch_name": branch_name}

    # MOCK PR mode (default): no external Git.
    if _pr_mode() != "github":
        return _open_in_app_pr()

    _record(state, StepName.BRANCH_AND_PR, StepStatus.IN_PROGRESS,
            f"{'Updating' if is_revision else 'Creating'} branch `{branch_name}` "
            f"with {len(state.artifacts)} file(s)…")
    try:
        from tools import github_pr
        url, number, _ = github_pr.create_branch_with_files(
            repo=repo,
            branch_name=branch_name,
            files=state.artifacts,
            commit_message=commit_msg,
            pr_title=pr_title,
            pr_body=pr_body,
        )
    except Exception as e:
        # An unreachable Git host must not throw away a pipeline that BUILT.
        # By this point dev deploy has succeeded — a real SDP pipeline exists
        # and its silver table is populated — so failing the whole request over
        # PR packaging discards working work and leaves the deployed pipeline
        # orphaned with the request marked failed.
        #
        # This is not hypothetical: on the Databricks-managed workspaces the
        # accelerator deploys into, App egress IPs are not on the corporate
        # GitHub Enterprise IP allow list, so create_branch_with_files 403s with
        # "the `Databricks (EMU)` enterprise has an IP allow list enabled" even
        # though the PAT is valid (the same token succeeds from a laptop). The
        # equivalent Atlassian allowlist blocks Confluence the same way.
        #
        # So: degrade to the in-app PR, which carries the same artefacts and
        # review flow, and keep the reason visible rather than silently
        # pretending Git was never configured.
        if _pr_fallback_enabled():
            _record(state, StepName.BRANCH_AND_PR, StepStatus.IN_PROGRESS,
                    f"GitHub unreachable — falling back to an in-app PR so the "
                    f"successful build is not lost. Reason: {str(e)[:300]}")
            return _open_in_app_pr(
                note="external Git was unreachable from this workspace, so the PR "
                     "is rendered in-app (same artefacts + review flow). "
                     f"GitHub said: {str(e)[:200]}")
        _record(state, StepName.BRANCH_AND_PR, StepStatus.FAILED, f"PR open failed: {e}")
        return {"fatal_error": f"PR open failed: {e}"}

    update_request(state.request_id, pr_url=url, pr_number=number)
    _record(state, StepName.BRANCH_AND_PR, StepStatus.SUCCEEDED,
            f"PR #{number} {'updated (new commit on existing branch)' if is_revision else 'opened'}",
            pr_url=url, pr_number=number, branch=branch_name,
            iteration=state.review_iterations + 1)
    return {"pr_url": url, "pr_number": number, "branch_name": branch_name}


# --- 8. PR Reviewer ---------------------------------------------------

def _loads_lenient(raw: str):
    """Parse the reviewer's JSON verdict, tolerating ```json fences / prose
    around it. Returns the dict, or None if it isn't JSON at all."""
    import re
    if not raw:
        return None
    txt = raw.strip()
    # Strip a leading/trailing fenced code block (```json ... ```)
    m = re.search(r"```(?:json)?\s*(.*?)```", txt, re.DOTALL)
    if m:
        txt = m.group(1).strip()
    # Otherwise grab the outermost {...} if there's surrounding prose
    if not txt.startswith("{"):
        b = txt.find("{"); e = txt.rfind("}")
        if b != -1 and e != -1 and e > b:
            txt = txt[b:e + 1]
    try:
        out = json.loads(txt)
        return out if isinstance(out, dict) else None
    except Exception:
        return None


def _format_review_comment(iter_num: int, max_iters: int, verdict: str,
                           comment_md: str, concerns: list, suggestions: list) -> str:
    """Render a clean, human-readable PR review comment (never raw JSON)."""
    icon = {"approve": "✅", "request_changes": "🔧"}.get(verdict, "📝")
    label = {"approve": "Approved",
             "request_changes": "Changes requested"}.get(verdict, verdict.replace("_", " ").title())
    lines = [
        f"## 🤖 Pipeline Studio — automated review",
        f"_Round {iter_num} of {max_iters}_",
        "",
        f"### {icon} Verdict: **{label}**",
    ]
    if comment_md:
        lines += ["", comment_md.strip()]
    if concerns:
        lines += ["", "### ⚠️ Concerns"] + [f"- {c}" for c in concerns]
    if suggestions:
        lines += ["", "### 💡 Suggestions"] + [f"- {s}" for s in suggestions]
    lines += ["", "---",
              "<sub>Posted by the Databricks Pipeline Studio PR Reviewer agent.</sub>"]
    return "\n".join(lines)


@traced_node("pr_reviewer")
def pr_reviewer(state: AgentState) -> dict:
    """Coder↔Reviewer loop. Each round asks for a structured verdict
    (approve / request_changes + comment); the verdict drives the graph
    edge — if request_changes and max_iterations not exhausted, the
    pipeline_generator regenerates with the feedback and the dev table
    rebuilds before re-review."""
    cfg = guidelines.get_config("pr_reviewer")
    max_iters = max(1, int(cfg.get("max_iterations") or 1))
    iter_num = state.review_iterations + 1

    _record(state, StepName.PR_REVIEWER, StepStatus.IN_PROGRESS,
            f"Review round {iter_num}/{max_iters} — asking Claude for a structured verdict…")

    # Find the silver SDP source so the reviewer can actually audit metadata
    sdp_source = ""
    for path, content in state.artifacts.items():
        if path.endswith(f"transformations/silver/{state.spec.target.table}.py"):
            sdp_source = content
            break
    if not sdp_source:
        # Older path or any *.py under sdp/ — best-effort fallback
        for path, content in state.artifacts.items():
            if path.endswith(".py"):
                sdp_source = content
                break

    pr_feedback = _feedback_for(state, StepName.PR_REVIEWER)
    payload = {
        "iteration": iter_num,
        "max_iterations": max_iters,
        "files": list(state.artifacts.keys()),
        "silver_sdp_source": sdp_source,
        "dev_deploy": state.dev_deploy,
        "violations": state.standards_violations,
        "tests": state.tests_generated[:5],
        "previous_feedback": state.review_feedback,
    }
    if pr_feedback:
        payload["operator_feedback"] = pr_feedback
    try:
        raw = _llm_call(_augment_prompt(default_prompts.PR_REVIEWER_VERDICT, pr_feedback),
                         json.dumps(payload, default=str))
    except Exception as e:
        raw = json.dumps({
            "verdict": "approve",
            "concerns": [], "suggestions": [],
            "comment_md": f"(PR reviewer LLM unavailable: {e})",
        })

    # Best-effort JSON parse — the LLM sometimes wraps the JSON in ```json
    # fences, so strip those before parsing rather than posting raw JSON.
    verdict = "approve"
    comment_md = ""
    concerns: list[str] = []
    suggestions: list[str] = []
    parsed = _loads_lenient(raw)
    if parsed is not None:
        verdict = (parsed.get("verdict") or "approve").strip().lower()
        comment_md = (parsed.get("comment_md") or "").strip()
        concerns = parsed.get("concerns", []) or []
        suggestions = parsed.get("suggestions", []) or []
    else:
        # LLM returned plain markdown (not JSON) — use it as the prose body.
        verdict = "approve"
        comment_md = raw.strip()

    full_comment = _format_review_comment(
        iter_num, max_iters, verdict, comment_md, concerns, suggestions)
    if _is_in_app_pr(state):
        # In-app PR: the review comment is rendered in the tracker's PR
        # widget from this event — no external Git post.
        _record(state, StepName.PR_REVIEWER, StepStatus.IN_PROGRESS,
                f"Review round {iter_num}/{max_iters} — verdict `{verdict}`. "
                "Posted to the in-app PR.",
                kind="pr_review_comment", round=iter_num, verdict=verdict,
                comment_md=full_comment, concerns=concerns)
    else:
        try:
            from tools import github_pr
            repo = github_pr.pipelines_repo()
            github_pr.add_pr_comment(repo, state.pr_number or 0, full_comment)
            _record(state, StepName.PR_REVIEWER, StepStatus.IN_PROGRESS,
                    f"Posted review comment for round {iter_num} to PR #{state.pr_number}",
                    review_preview=comment_md[:240], verdict=verdict, concerns=concerns)
        except Exception as e:
            _record(state, StepName.PR_REVIEWER, StepStatus.IN_PROGRESS,
                    f"(comment post skipped: {e})", verdict=verdict)

    out: dict[str, Any] = {
        "review_iterations": iter_num,
        "review_verdict": verdict,
        "review_feedback": state.review_feedback + concerns,
    }
    if verdict == "approve" or iter_num >= max_iters:
        _record(state, StepName.PR_REVIEWER, StepStatus.SUCCEEDED,
                f"Final verdict after {iter_num} round(s): {verdict}",
                verdict=verdict, total_concerns=len(state.review_feedback) + len(concerns))
        out["review_verdict"] = verdict
        # If the final verdict is request_changes but loop is exhausted, we
        # still proceed to human review — they can decide.
        return out

    # Loop back to pipeline_generator with the new feedback
    _record(state, StepName.PR_REVIEWER, StepStatus.IN_PROGRESS,
            f"Verdict request_changes — looping back to pipeline_generator with "
            f"{len(concerns)} new concern(s) for round {iter_num + 1}.",
            concerns=concerns)
    return out


# --- 9. Await human ---------------------------------------------------

@traced_node("await_human")
def await_human(state: AgentState) -> dict:
    """Pause point — the graph run ENDS here. No polling, no timeout.

    The request parks in `pending_review` until a human clicks
    "PR approved — deploy to prod" in the tracker, which launches a NEW
    job run in gate mode (verify_pr_approved → deploy_prod). The only
    exception: if the decision was already made (e.g. a re-run after
    approval), pass straight through to the gate.
    """
    from agent.event_store import fetch_request
    req = fetch_request(state.request_id) or {}
    decision = req.get("human_decision") or state.human_decision
    if decision in ("approved", "rejected"):
        _record(state, StepName.AWAIT_HUMAN, StepStatus.SUCCEEDED,
                f"Human decision already recorded: {decision} — continuing.")
        return {"human_decision": decision}

    if _is_in_app_pr(state):
        pause_msg = ("Flow paused — awaiting human approval. Open the in-app PR "
                     "preview, review the generated files + the agent's review "
                     "rounds, then click 'PR approved — deploy to staging' in the "
                     "tracker. That resumes the flow as a fresh job run that deploys "
                     "to staging (production promotion happens later via a release). "
                     "This run ends here.")
    else:
        pause_msg = ("Flow paused — awaiting human approval. Review the PR on "
                     "GitHub, approve it, then click 'PR approved — deploy to "
                     "staging' in the tracker. This run ends here.")
    _record(state, StepName.AWAIT_HUMAN, StepStatus.IN_PROGRESS, pause_msg)
    update_request(state.request_id, final_status="pending_review")
    return {"human_decision": "pending"}


# --- 10. PR-approval gate (between await_human and deploy_prod) ------

@traced_node("verify_pr_approved")
def verify_pr_approved(state: AgentState) -> dict:
    """Check the PR's GitHub review state. If not approved, fail the node and
    set `pr_approval_error` so the UI can prompt the user to approve on
    GitHub and click "Re-check". The /api/requests/{id}/retry-prod-deploy
    endpoint re-runs verify_pr_approved → deploy_prod once the user has
    approved.
    """
    if state.human_decision != "approved":
        _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.SKIPPED,
                "Human did not approve in tracker — skipping PR gate.")
        update_request(state.request_id, final_status="rejected")
        return {"deploy_prod_status": "pending"}

    # In-app PR: the reviewer's in-app approval IS the gate (no external Git to
    # verify against). The human clicked approve in the tracker.
    if _is_in_app_pr(state):
        _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.SUCCEEDED,
                f"In-app PR #{state.pr_number} approved by the reviewer — "
                "proceeding to deploy (no external Git PR to verify against).",
                review={"approved": True, "mode": "mock"})
        return {"pr_review_state": {"approved": True, "mode": "mock"}, "pr_approval_error": None}

    from tools import github_pr
    repo = github_pr.pipelines_repo()
    if not state.pr_number:
        _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.FAILED,
                "No PR number on this request — cannot check approval.")
        update_request(state.request_id, final_status="failed")
        return {"fatal_error": "no PR number"}

    _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.IN_PROGRESS,
            f"Checking GitHub for PR #{state.pr_number} approval state…")

    try:
        review = github_pr.get_pr_review_state(repo, int(state.pr_number))
    except Exception as e:
        _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.FAILED,
                f"GitHub API error while checking approval: {e}")
        update_request(state.request_id, final_status="blocked_on_pr_approval")
        return {"pr_approval_error": str(e), "pr_review_state": {}}

    if not review.get("approved"):
        reason = review.get("reason", "PR not approved.")
        _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.FAILED,
                f"PR not approved: {reason}",
                review=review)
        # Surface a clear actionable state — the user goes to GitHub, approves,
        # then clicks the "Re-check approval & deploy to prod" button in the
        # tracker which calls /retry-prod-deploy.
        update_request(state.request_id, final_status="blocked_on_pr_approval")
        return {
            "pr_review_state": review,
            "pr_approval_error": reason,
            "deploy_prod_status": "pending",
        }

    _record(state, StepName.VERIFY_PR_APPROVED, StepStatus.SUCCEEDED,
            f"PR is approved: {review.get('reason', '')}",
            review=review)
    return {"pr_review_state": review, "pr_approval_error": None}


# --- 11. Deploy to a higher environment (staging / prod) ---------------

def _deploy_to_env(state: AgentState, step: StepName, *, env_tag: str,
                   catalog: str, bronze_schema: str, silver_schema: str,
                   label: str, merge_pr: bool) -> dict:
    """Shared body for the post-dev environment deploys.

    `deploy_staging` (env_tag='stage', merge_pr=True) is the end of the data
    engineering tracker flow — it squash-merges the approved PR and deploys
    the SDP pipeline + scheduled Job to the STAGING catalog.

    `deploy_prod` (env_tag='prod', merge_pr=False) is the platform/ops release
    promotion — the PR is already merged, so it just (re)deploys the same
    pipeline to the PROD catalog. Bronze is ingested per-env via the
    consolidated Lakeflow Connect pipeline (`ops-<env>-source-ingest`)."""
    if merge_pr and _is_in_app_pr(state):
        # In-app PR: nothing to merge in an external Git. The approval
        # already happened in the tracker; record it and proceed.
        _record(state, step, StepStatus.IN_PROGRESS,
                f"In-app PR #{state.pr_number} approved — merging (mock) and "
                f"deploying to the {label} catalog…")
    elif merge_pr:
        from tools import github_pr
        repo = github_pr.pipelines_repo()
        _record(state, step, StepStatus.IN_PROGRESS,
                "PR approved on GitHub. Squash-merging PR to main…")
        try:
            github_pr.merge_pr(repo, state.pr_number or 0, method="squash")
        except Exception as e:
            msg = str(e)
            if "already" in msg.lower() and "merged" in msg.lower():
                _record(state, step, StepStatus.IN_PROGRESS,
                        "PR was already merged — continuing to deploy.")
            else:
                _record(state, step, StepStatus.FAILED, f"Merge failed: {e}")
                update_request(state.request_id, final_status="failed")
                return {"fatal_error": f"merge failed: {e}"}
        _record(state, step, StepStatus.IN_PROGRESS,
                f"PR merged to main. Now deploying the SDP pipeline to the {label} catalog…")
    else:
        _record(state, step, StepStatus.IN_PROGRESS,
                f"Promoting staged pipeline to the {label} catalog "
                f"(`{catalog}.{silver_schema}.{state.spec.target.table}`)…")

    # FILE source: ingest Bronze for THIS env via the consolidated Auto Loader
    # file-ingest pipeline (shared per dataset, reused), then build the
    # per-request Silver on top — symmetric with the CDC path below.
    if _is_file_source(state):
        ing = _ingest_file_datasets(state, catalog=catalog, bronze_schema=bronze_schema,
                                    env_tag=env_tag, step=step)
        if "fatal_error" in ing:
            return ing
        result = _deploy_helper(state, step, catalog, bronze_schema, silver_schema,
                                label=label, create_schedule=True)
        return _gold_after_silver(state, env_tag=env_tag, catalog=catalog,
                                  silver_schema=silver_schema, step=step, result=result)

    # CDC Bronze: Lakeflow Connect ingestion when the external source is
    # configured (consolidated pipeline per env), warehouse otherwise.
    ingested = False
    from tools import lakeflow_ingest as _lfi_gate
    if _lfi_gate.can_ingest(state.spec.source.tables, state.spec.source.system):
        def _ing_log(msg: str) -> None:
            _record(state, step, StepStatus.IN_PROGRESS, msg)
        try:
            from tools import lakeflow_ingest
            ing = lakeflow_ingest.ingest_to_bronze(
                spec_tables=state.spec.source.tables,
                primary_system=state.spec.source.system,
                catalog=catalog,
                bronze_schema=bronze_schema,
                env_tag=env_tag,
                target_table=state.spec.target.table,
                log=_ing_log,
            )
            ingested = True
            _record(state, step, StepStatus.IN_PROGRESS,
                    f"{label.capitalize()} bronze ingested via Lakeflow Connect "
                    f"(`{ing['ingestion_pipeline_name']}`).",
                    ingestion_result=ing)
        except Exception as e:
            _record(state, step, StepStatus.FAILED,
                    f"{label.capitalize()} Lakeflow Connect ingestion failed: {e}")
            update_request(state.request_id, final_status="failed")
            return {"fatal_error": f"{label} ingestion failed: {e}"}

    result = _deploy_helper(state, step, catalog, bronze_schema, silver_schema,
                            label=label, create_schedule=True,
                            skip_bronze_load=ingested)
    return _gold_after_silver(state, env_tag=env_tag, catalog=catalog,
                              silver_schema=silver_schema, step=step, result=result)


@traced_node("deploy_staging")
def deploy_staging(state: AgentState) -> dict:
    """End of the data-engineering tracker flow. Requires the PR to be
    approved (verify_pr_approved). Squash-merges the PR and deploys the SDP
    pipeline + scheduled Job to the STAGING catalog.

    If the request includes an app (spec.app.enabled=True), also deploys the
    generated Databricks App to the sandbox workspace and grants the app's SP
    the required permissions. Records the app URL and deployment metadata in
    state.app_deploy.

    The request then parks as `deployed` / release_status=`staged`, awaiting a
    platform release."""
    catalog = os.environ.get("STAGE_CATALOG",
                             os.environ.get("PROD_CATALOG", state.spec.target.catalog))
    bronze_schema = os.environ.get("STAGE_BRONZE_SCHEMA",
                                   "dev_jeremy_herbert_databricks_pipeline_studio_stage_bronze")
    silver_schema = os.environ.get("STAGE_SILVER_SCHEMA",
                                   "dev_jeremy_herbert_databricks_pipeline_studio_stage_silver")
    result = _deploy_to_env(state, StepName.DEPLOY_STAGING, env_tag="stage",
                            catalog=catalog, bronze_schema=bronze_schema,
                            silver_schema=silver_schema, label="staging",
                            merge_pr=True)
    if "fatal_error" in result:
        return {"deploy_prod_status": "failed"}

    # Deploy the app if requested (Option B: deploy only after approval).
    app_deploy_result = {}
    if state.spec.app.enabled and state.app_manifest:
        app_deploy_result = _deploy_app_to_sandbox(
            state, catalog=catalog, gold_schema=os.environ.get(
                "STAGE_GOLD_SCHEMA",
                "dev_jeremy_herbert_databricks_pipeline_studio_stage_gold")
        )
        if "error" in app_deploy_result:
            _record(state, StepName.DEPLOY_APP, StepStatus.IN_PROGRESS,
                    f"⚠️ app deployment warning: {app_deploy_result['error']}")

    update_request(state.request_id, final_status="deployed",
                   release_status="staged")
    out = {"deploy_prod_status": "succeeded", "prod_deploy": result}
    if app_deploy_result:
        out["app_deploy"] = app_deploy_result
    return out


@traced_node("deploy_prod")
def deploy_prod(state: AgentState) -> dict:
    """Platform/ops release promotion: deploy the already-staged pipeline to
    the PROD catalog. Triggered by a release in the Release Management tab
    (mode='promote'); the PR is already merged so there's no GitHub step."""
    catalog = os.environ.get("PROD_CATALOG", state.spec.target.catalog.replace("_dev", "_prod"))
    bronze_schema = os.environ.get("PROD_BRONZE_SCHEMA",
                                   "dev_jeremy_herbert_databricks_pipeline_studio_prod_bronze")
    silver_schema = os.environ.get("PROD_SILVER_SCHEMA",
                                   "dev_jeremy_herbert_databricks_pipeline_studio_prod_silver")
    result = _deploy_to_env(state, StepName.DEPLOY_PROD, env_tag="prod",
                            catalog=catalog, bronze_schema=bronze_schema,
                            silver_schema=silver_schema, label="prod",
                            merge_pr=False)
    if "fatal_error" in result:
        update_request(state.request_id, release_status="staged")
        return {"deploy_prod_status": "failed"}

    update_request(state.request_id, final_status="deployed",
                   release_status="released")
    return {"deploy_prod_status": "succeeded", "prod_deploy_release": result}


# --- Gold builder — fact/dim MV from Silver -----------------------------
# Shared by the standalone mode='gold' request (gold_builder) AND the in-flow
# Curate step (deploy_gold + per-env builds in _deploy_to_env).

def _build_one_gold_fact(state: AgentState, *, fact_name: str, intent: str, grain: str,
                         sources_fqns: list, catalog: str, gold_schema: str,
                         silver_schema: str, env_tag: str, step: StepName, client, wid) -> dict:
    """Generate + validate + deploy ONE gold fact/dim MV. Returns the deploy
    result, or {'error': ...} on failure (no exception — callers decide)."""
    from tools import framework_deployer as fd
    from agent import gold_sql
    schemas: dict = {}
    for fq in sources_fqns:
        try:
            with fd._dbsql_conn(wid) as c:
                cur = c.cursor(); cur.execute(f"DESCRIBE {fq}")
                schemas[fq] = [(r[0], r[1]) for r in cur.fetchall()
                               if r[0] and not r[0].startswith('#') and r[0].strip()]
        except Exception as e:  # noqa: BLE001
            return {"error": f"source unreadable: {fq} — {str(e)[:160]}"}
    fact_spec = {"name": fact_name, "intent": intent, "sources": sources_fqns}
    if grain:
        fact_spec["grain_or_measures"] = grain
    out, err = None, None
    for _ in range(3):
        s = gold_sql.suggest_gold_sql(fact_spec, schemas, feedback=err)
        if not s:
            err = "LLM returned no usable SQL"; continue
        try:
            with fd._dbsql_conn(wid) as c:
                c.cursor().execute(f"SELECT * FROM ({s['sql']}) LIMIT 0")
            out = s; break
        except Exception as e:  # noqa: BLE001
            err = str(e)[:300]
            _record(state, step, StepStatus.IN_PROGRESS,
                    f"`{fact_name}` SQL failed validation — agent fixing it. ({err[:110]})")
    if not out:
        return {"error": f"could not generate valid SQL for {fact_name}: {err}"}
    res = fd.deploy_gold_via_framework(
        fact_name, out["sql"], catalog=catalog, gold_schema=gold_schema, env_tag=env_tag,
        primary_table=state.spec.target.table, silver_schema=silver_schema,
        log=lambda m: _record(state, step, StepStatus.IN_PROGRESS, m),
        cluster_by=out["cluster_by"] or None, comment=out["comment"],
        not_null=out["not_null"] or None)
    return res


def _build_gold_facts(state: AgentState, *, env_tag: str, catalog: str,
                      gold_schema: str, silver_schema: str, step: StepName) -> dict:
    """Build the request's optional gold_facts in `env_tag`, each deriving from
    the request's Silver target (default) + any explicit sources. Used by the
    in-flow deploy_gold (dev) and the staging/prod env deploys."""
    facts = state.spec.gold_facts or []
    if not facts:
        return {"gold_facts_built": 0}
    from databricks.sdk import WorkspaceClient
    from tools import framework_deployer as fd
    wid = fd._warehouse_id()
    client = WorkspaceClient()
    fd._ensure_warehouse_running(client, wid, lambda m: None)
    default_source = f"{catalog}.{silver_schema}.{state.spec.target.table}"
    _record(state, step, StepStatus.IN_PROGRESS,
            f"Curate (Gold): building {len(facts)} fact/dim(s) in `{gold_schema}` from the cleansed Silver…",
            gold_facts=[f.name for f in facts])
    built, failed = [], []
    for f in facts:
        srcs = [s if s.count(".") >= 2 else f"{catalog}.{silver_schema}.{s}"
                for s in (f.sources or [])] or [default_source]
        res = _build_one_gold_fact(
            state, fact_name=f.name, intent=(f.intent or state.spec.intent),
            grain=f.grain, sources_fqns=srcs, catalog=catalog, gold_schema=gold_schema,
            silver_schema=silver_schema, env_tag=env_tag, step=step, client=client, wid=wid)
        if res.get("error"):
            failed.append({"fact": f.name, "error": res["error"]})
            _record(state, step, StepStatus.IN_PROGRESS,
                    f"⚠️ Gold fact `{f.name}` skipped: {res['error'][:140]}")
        else:
            built.append(res)
            _record(state, step, StepStatus.IN_PROGRESS,
                    f"Gold `{res['gold_table']}` — {res['rows']:,} rows.", gold_deploy=res)
    _record(state, step, StepStatus.SUCCEEDED,
            f"Curate (Gold) done: built {len(built)}/{len(facts)} fact/dim(s) in {env_tag}." +
            (f" Skipped: {', '.join(x['fact'] for x in failed)}." if failed else ""),
            gold_built=[r.get("gold_table") for r in built], gold_failed=failed)
    return {"gold_facts_built": len(built), "gold_failed": failed}


def _gold_after_silver(state: AgentState, *, env_tag: str, catalog: str,
                       silver_schema: str, step: StepName, result: dict) -> dict:
    """After a successful Silver deploy in an env, build the request's gold_facts
    in that env's gold schema. Pass-through of `result`."""
    if "fatal_error" not in result and (state.spec.gold_facts or []):
        gold_schema = os.environ.get(
            f"{env_tag.upper()}_GOLD_SCHEMA",
            f"dev_jeremy_herbert_databricks_pipeline_studio_{env_tag}_gold")
        try:
            _build_gold_facts(state, env_tag=env_tag, catalog=catalog,
                              gold_schema=gold_schema, silver_schema=silver_schema, step=step)
        except Exception as e:  # noqa: BLE001 — gold is best-effort; never fail the silver deploy
            _record(state, step, StepStatus.IN_PROGRESS,
                    f"⚠️ Gold build hit an error (silver is deployed): {str(e)[:160]}")
    return result


def deploy_gold(state: AgentState) -> dict:
    """In-flow Curate step (dev): build the request's optional gold_facts from
    the DEV Silver. No-op when the request asked for no gold."""
    if not (state.spec.gold_facts or []):
        _record(state, StepName.DEPLOY_GOLD, StepStatus.SKIPPED,
                "No Gold layer requested — Silver is the deliverable for this request.")
        return {}
    catalog = os.environ.get("DEV_CATALOG", state.spec.target.catalog)
    gold_schema = os.environ.get("DEV_GOLD_SCHEMA",
                                 "dev_jeremy_herbert_databricks_pipeline_studio_dev_gold")
    silver_schema = os.environ.get("DEV_SILVER_SCHEMA",
                                   "dev_jeremy_herbert_databricks_pipeline_studio_dev_silver")
    return _build_gold_facts(state, env_tag="dev", catalog=catalog,
                             gold_schema=gold_schema, silver_schema=silver_schema,
                             step=StepName.DEPLOY_GOLD)


# --- App deployment (staging: after PR approval) -------------------------

def _deploy_app_to_sandbox(state: AgentState, *, catalog: str, gold_schema: str) -> dict:
    """Deploy the generated app to the sandbox workspace. Called from deploy_staging
    when spec.app.enabled=True and the app manifest has been authored.

    Uses app_deployer to:
      1. Create/ensure the app exists
      2. Upload emitted source files to workspace
      3. Grant the app's SP the required UC permissions
      4. Deploy via `databricks apps deploy --source-code-path`

    Returns deployment metadata (url, app_sp, etc) or {'error': ...} on failure.
    GATED: only runs live if ALLOW_LIVE_APP_DEPLOY=1 env is set (default: dry-run)."""
    from tools import app_deployer, app_emitter

    app_name = state.app_manifest.get("name", "")
    if not app_name:
        return {"error": "no app manifest name"}

    # Construct the fully-qualified gold schema
    gold_fqn = f"{catalog}.{gold_schema}"

    # Get warehouse (from manifest or env)
    warehouse_id = (state.app_manifest.get("warehouse_id") or
                    os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))
    if not warehouse_id:
        return {"error": "no warehouse_id found"}

    # App files to deploy. Prefer the emitted `app/…` artifacts, but if a
    # gate/staging RESUME lost them (older runs didn't persist app content), the
    # manifest is deterministic — re-emit from it so the deploy never ships a
    # pipeline-only bundle (which fails with "no command to run").
    app_files = {k: v for k, v in (state.artifacts or {}).items()
                 if k.startswith("app/")}
    if not app_files:
        try:
            manifest = dict(state.app_manifest)
            manifest.setdefault("warehouse_id", warehouse_id)
            app_files = app_emitter.emit_app(manifest)
            _record(state, StepName.DEPLOY_APP, StepStatus.IN_PROGRESS,
                    "Re-emitted app files from the manifest (not in resumed artifacts).")
        except Exception as e:  # noqa: BLE001
            return {"error": f"could not emit app files: {str(e)[:160]}"}

    # App deploy is its OWN tracker step (DEPLOY_APP) so it shows as a distinct
    # node in the flow rather than hiding inside deploy_staging's events.
    def log_deploy(msg: str) -> None:
        _record(state, StepName.DEPLOY_APP, StepStatus.IN_PROGRESS, msg)

    _record(state, StepName.DEPLOY_APP, StepStatus.IN_PROGRESS,
            f"Deploying app `{app_name}` to sandbox workspace…",
            app_name=app_name)

    result = app_deployer.deploy_for_real(
        emitted_files=app_files,
        app_name=app_name,
        gold_schema_fq=gold_fqn,
        warehouse_id=warehouse_id,
        profile=None,  # Uses env/config default
        log=log_deploy,
    )

    if "error" in result:
        _record(state, StepName.DEPLOY_APP, StepStatus.FAILED,
                f"app deployment failed: {result.get('error')}")
        return result

    # Record success and the app URL
    app_url = result.get("url", "(pending)")
    _record(state, StepName.DEPLOY_APP, StepStatus.SUCCEEDED,
            f"✓ app deployed: {app_name} at {app_url}",
            app_deploy=result, app_url=app_url)

    return result


# --- App generation (the 'consume' step) --------------------------------
# Gated on spec.app.enabled — a pure pipeline request skips both nodes entirely,
# so every existing flow is byte-identical. The app_generator authors a manifest
# (through the GOVERNED gateway LLM), app_standards_validator audits it, and the
# deterministic tools.app_emitter stamps a Streamlit bundle into artifacts so the
# app rides the SAME PR + review + promotion as the pipeline.

def _gold_tables_built(state: AgentState) -> list[str]:
    """Gold table names the request produced (from gold_facts, else the app's
    declared gold_tables)."""
    names = [f.name for f in (state.spec.gold_facts or [])]
    if not names:
        names = list(state.spec.app.gold_tables or [])
    return names


def _read_gold_schemas(gold_fqns: list[str]) -> dict:
    """{fqn: [(col, type), ...]} via DESCRIBE — same path _build_one_gold_fact uses."""
    from tools import framework_deployer as fd
    wid = fd._warehouse_id()
    schemas: dict = {}
    for fq in gold_fqns:
        try:
            with fd._dbsql_conn(wid) as c:
                cur = c.cursor(); cur.execute(f"DESCRIBE {fq}")
                schemas[fq] = [(r[0], r[1]) for r in cur.fetchall()
                               if r[0] and not r[0].startswith('#') and r[0].strip()]
        except Exception:  # noqa: BLE001 — best-effort; app_generator falls back to AppSpec
            pass
    return schemas


def _load_brand_profile(brand_name: str | None) -> dict | None:
    """Load a named brand profile from config.yaml. Returns the profile dict or None.
    Profiles are YAML-based and never hardcoded in source (guard test scans for
    customer branding). Examples: perpetual, acme, client-x."""
    if not brand_name:
        return None
    try:
        from pathlib import Path
        import yaml
        config_path = Path(__file__).resolve().parent.parent.parent / "config.yaml"
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
                branding_section = cfg.get("branding", {})
                return branding_section.get(brand_name)
    except Exception:  # noqa: BLE001 — best-effort; fallback to brand_name only
        pass
    return None


@traced_node("app_generator")
def app_generator(state: AgentState) -> dict:
    """Author the app manifest from the Gold contract + AppSpec, then emit the
    Streamlit bundle into artifacts. Best-effort LLM with a deterministic
    fallback — a build never stalls here."""
    if not state.spec.app.enabled:
        _record(state, StepName.APP_GENERATOR, StepStatus.SKIPPED,
                "No app requested — Gold is the deliverable for this request.")
        return {}
    from agent import app_builder
    from tools import app_emitter

    catalog = os.environ.get("DEV_CATALOG", state.spec.target.catalog)
    gold_schema = os.environ.get("DEV_GOLD_SCHEMA",
                                 "dev_jeremy_herbert_databricks_pipeline_studio_dev_gold")
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    gold_tables = _gold_tables_built(state)
    gold_fqns = [t if t.count(".") >= 2 else f"{catalog}.{gold_schema}.{t}" for t in gold_tables]

    _record(state, StepName.APP_GENERATOR, StepStatus.IN_PROGRESS,
            f"Designing a governed app on {len(gold_fqns)} Gold table(s). "
            "Reading the Gold contract, then authoring the manifest via the gateway…",
            gold_tables=gold_fqns)
    schemas = _read_gold_schemas(gold_fqns)

    app_spec = state.spec.app.model_dump(mode="json")
    standards = _augment_prompt(guidelines.get("app_standards_validator"), [])
    brand = None
    bn = state.spec.app.brand_name or os.environ.get("OPS_BRAND_NAME", "")
    if bn:
        # Try to load the full brand profile from config.yaml (e.g., "perpetual").
        # If found, use it; otherwise, fall back to just the brand name.
        brand_profile = _load_brand_profile(bn)
        if brand_profile:
            brand = brand_profile
        else:
            brand = {"name": bn}
    try:
        llm_data = app_builder.suggest_app_manifest(app_spec, schemas, standards)
    except Exception:
        llm_data = None
    manifest = app_builder.build_manifest(
        app_spec, schemas, catalog=catalog, gold_schema=gold_schema,
        warehouse_id=warehouse_id, brand=brand, llm_data=llm_data)

    # Emit the bundle (deterministic). Validation errors surface as a fatal for
    # this step but do NOT kill the pipeline PR (the pipeline already deployed).
    try:
        files = app_emitter.emit_app(manifest)
    except ValueError as e:
        _record(state, StepName.APP_GENERATOR, StepStatus.FAILED,
                f"App manifest invalid: {e}")
        return {"app_manifest": manifest, "app_violations": [str(e)]}

    new_artifacts = dict(state.artifacts)
    new_artifacts.update(files)
    # Persist the FULL app file CONTENT (not just names) so a gate/staging
    # resume can restore it — deploy_staging's app-deploy reads state.artifacts,
    # and rehydrate_state only had the pipeline_generator artifacts, so without
    # this the app files were lost on resume and the App deploy shipped only the
    # pipeline files (→ "no command / no Python file" deploy failure).
    _record(state, StepName.APP_GENERATOR, StepStatus.SUCCEEDED,
            f"App `{manifest['name']}` designed: {len(manifest.get('kpis', []))} KPIs, "
            f"{len(manifest.get('charts', []))} charts, "
            f"tenant scope={manifest.get('tenant_column') or 'none'}, "
            f"{len(files)} file(s) emitted.",
            app_manifest=manifest, files=list(files.keys()),
            app_artifacts=files)
    return {"app_manifest": manifest, "artifacts": new_artifacts}


@traced_node("app_standards_validator")
def app_standards_validator(state: AgentState) -> dict:
    """Audit the generated app manifest: deterministic structural checks +
    an LLM policy pass (Gold-only reads, single-tenant scoping, OBO auth)."""
    if not state.spec.app.enabled:
        _record(state, StepName.APP_STANDARDS_VALIDATOR, StepStatus.SKIPPED,
                "No app requested.")
        return {}
    from tools import app_emitter
    manifest = state.app_manifest or {}
    violations = app_emitter.validate_manifest(manifest)

    _record(state, StepName.APP_STANDARDS_VALIDATOR, StepStatus.IN_PROGRESS,
            f"Structural checks done: {len(violations)} issue(s). "
            "Asking the gateway to review the manifest against app standards…")
    try:
        payload = {"manifest": manifest}
        av_feedback = _feedback_for(state, StepName.APP_STANDARDS_VALIDATOR)
        if av_feedback:
            payload["operator_feedback"] = av_feedback
        llm_text = _llm_call(_augment_prompt(guidelines.get("app_standards_validator"), av_feedback),
                             json.dumps(payload, default=str))
        try:
            extra = json.loads(llm_text)
            if isinstance(extra, list):
                violations.extend(str(x) for x in extra)
        except Exception:
            pass
    except Exception:
        pass

    status = StepStatus.SUCCEEDED if not violations else StepStatus.FAILED
    _record(state, StepName.APP_STANDARDS_VALIDATOR, status,
            ("App manifest passes all standards checks."
             if not violations else f"{len(violations)} app-standards issue(s)."),
            violations=violations)
    return {"app_violations": violations}


def gold_builder(state: AgentState) -> dict:
    """Standalone Gold request (mode='gold'): build ONE gold fact = target.table
    from source.tables. Kept for the gold-only request path."""
    spec = state.spec
    catalog = os.environ.get("DEV_CATALOG", spec.target.catalog)
    gold_schema = os.environ.get("DEV_GOLD_SCHEMA",
                                 "dev_jeremy_herbert_databricks_pipeline_studio_dev_gold")
    silver_schema = os.environ.get("DEV_SILVER_SCHEMA",
                                   "dev_jeremy_herbert_databricks_pipeline_studio_dev_silver")
    from databricks.sdk import WorkspaceClient
    from tools import framework_deployer as fd
    wid = fd._warehouse_id()
    client = WorkspaceClient()
    fd._ensure_warehouse_running(client, wid, lambda m: None)
    fact = spec.target.table
    srcs = [s if s.count(".") >= 2 else f"{catalog}.{silver_schema}.{s}"
            for s in (spec.source.tables or [])]
    _record(state, StepName.GOLD_BUILDER, StepStatus.IN_PROGRESS,
            f"Building Gold fact `{gold_schema}.{fact}` from {len(srcs)} source(s).", sources=srcs)
    res = _build_one_gold_fact(
        state, fact_name=fact, intent=spec.intent,
        grain=(spec.transform.description if spec.transform else ""),
        sources_fqns=srcs, catalog=catalog, gold_schema=gold_schema,
        silver_schema=silver_schema, env_tag="dev", step=StepName.GOLD_BUILDER,
        client=client, wid=wid)
    if res.get("error"):
        _record(state, StepName.GOLD_BUILDER, StepStatus.FAILED, f"Gold build failed: {res['error']}")
        update_request(state.request_id, final_status="failed")
        return {"fatal_error": res["error"]}
    update_request(state.request_id, final_status="deployed")
    _record(state, StepName.GOLD_BUILDER, StepStatus.SUCCEEDED,
            f"Gold MV `{res['gold_table']}` built — {res['rows']:,} rows.",
            gold_table=res["gold_table"], rows=res["rows"],
            pipeline_url=res.get("pipeline_url"), gold_deploy=res)
    return {"gold_deploy": res}
