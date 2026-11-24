"""Rebuild AgentState from the request row + event log.

Used whenever we re-enter the graph mid-flight in a FRESH process:
  - the gate run after human approval (verify_pr_approved → deploy_prod)
  - /rerun-from-step partial re-runs

Since the agent executes as a Databricks Job run per request, every
resume happens in a new process — rehydration is the only way state
crosses run boundaries. Everything needed lives in the `requests` row
and the `request_events` Delta log.
"""

from __future__ import annotations

import json

from agent import event_store
from agent.state import AgentState, PipelineSpec


def _last_succeeded_by_step(events: list[dict]) -> dict[str, dict]:
    """Return the latest SUCCEEDED event per step (or IN_PROGRESS if none succeeded)."""
    out: dict[str, dict] = {}
    for ev in events:
        step = ev.get("step")
        status = ev.get("status")
        if status == "succeeded":
            out[step] = ev
        elif status == "in_progress" and step not in out:
            # fall back to latest in_progress so we at least see the partial state
            out[step] = ev
    return out


def _collect_step_comments(events: list[dict]) -> dict[str, list[str]]:
    """Fold human_comment events into a dict keyed by step.value."""
    out: dict[str, list[str]] = {}
    for ev in events:
        details = _details_from_event(ev)
        if details.get("kind") == "human_comment":
            out.setdefault(ev.get("step", ""), []).append(details.get("comment", ""))
    return out


def _details_from_event(ev: dict) -> dict:
    d = ev.get("details_json")
    if isinstance(d, str):
        try:
            return json.loads(d) or {}
        except Exception:
            return {}
    if d is None:
        d = ev.get("details")
    return d or {}


def rehydrate_state(request_id: str) -> AgentState:
    """Rebuild AgentState from the request record + event log so we can
    re-enter the graph mid-flight. Raises ValueError if the request is
    unknown."""
    req = event_store.fetch_request(request_id)
    if not req:
        raise ValueError(f"request not found: {request_id}")
    spec_dict = json.loads(req["spec_json"])
    spec = PipelineSpec.model_validate(spec_dict)

    state = AgentState(
        request_id=request_id, spec=spec,
        pr_url=req.get("pr_url"),
        pr_number=int(req["pr_number"]) if req.get("pr_number") else None,
        human_decision=req.get("human_decision") or "pending",
    )

    events = event_store.fetch_events(request_id)
    state.human_feedback_per_step = _collect_step_comments(events)

    last = _last_succeeded_by_step(events)

    if (e := last.get("source_profiler")):
        d = _details_from_event(e)
        if "profile" in d:
            state.profile = d["profile"]

    if (e := last.get("pipeline_generator")):
        d = _details_from_event(e)
        if "artifacts" in d and isinstance(d["artifacts"], dict):
            state.artifacts = d["artifacts"]
        if "table_comment" in d:
            state.generator_table_comment = d["table_comment"]
        if "column_comments" in d and isinstance(d["column_comments"], dict):
            state.generator_column_comments = d["column_comments"]
        if "inline_comments" in d and isinstance(d["inline_comments"], list):
            state.generator_inline_comments = d["inline_comments"]

    if (e := last.get("standards_validator")):
        d = _details_from_event(e)
        if isinstance(d.get("violations"), list):
            state.standards_violations = d["violations"]

    if (e := last.get("test_generator")):
        d = _details_from_event(e)
        if isinstance(d.get("tests"), list):
            state.tests_generated = d["tests"]

    if (e := last.get("deploy_ingestion")):
        d = _details_from_event(e)
        if isinstance(d.get("ingestion_result"), dict):
            state.ingestion = d["ingestion_result"]

    if (e := last.get("deploy_dev")):
        d = _details_from_event(e)
        if isinstance(d.get("deploy_result"), dict):
            state.dev_deploy = d["deploy_result"]

    if (e := last.get("branch_and_pr")):
        d = _details_from_event(e)
        if d.get("branch"):
            state.branch_name = d["branch"]

    # App manifest — required by deploy_staging's app-deploy step. Without this
    # the staging-resume run has an empty app_manifest, so the guard
    # `if state.spec.app.enabled and state.app_manifest` is False and the app is
    # silently NOT deployed (the deploy_app step shows no events). The
    # app_generator success event persists the full manifest; restore it here.
    if (e := last.get("app_generator")):
        d = _details_from_event(e)
        if isinstance(d.get("app_manifest"), dict):
            state.app_manifest = d["app_manifest"]
        # Merge the app files (app.yaml/app.py/…) back into artifacts — they were
        # emitted by app_generator on top of the pipeline_generator set, so
        # without this the resume run's state.artifacts lacks the app entrypoint
        # and the App deploy fails ("no command to run / no Python file").
        if isinstance(d.get("app_artifacts"), dict):
            merged = dict(state.artifacts or {})
            merged.update(d["app_artifacts"])
            state.artifacts = merged

    return state
