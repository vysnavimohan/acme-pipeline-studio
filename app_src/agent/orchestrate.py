"""Single entry point for executing the agent graph against a request.

Three modes, mirroring how the flow crosses process boundaries now that
each execution is a Databricks Job run:

  full       — fresh request: spec_critic → … → await_human (run ENDS there;
               the request parks in `pending_review` until a human acts)
  gate       — resume after human approval: verify_pr_approved → deploy_staging
  promote    — platform release promotion of a staged request: deploy_prod
  from_step  — operator-triggered partial re-run from a chosen step

State for gate / from_step is rehydrated from the Delta event log — nothing
is held in memory between runs. Every execution is wrapped in an MLflow run
with LangGraph traces (see mlflow_utils).

This module is what the Unity-Catalog-registered agent model executes —
`agent_model.py` (models-from-code) calls execute() from its predict().
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from agent import event_store, mlflow_utils
from agent.state import AgentState, PipelineSpec, StepEvent, StepName, StepStatus

LOG = logging.getLogger("ops.orchestrate")

VALID_MODES = ("full", "gate", "promote", "from_step", "gold")


def _build_for_mode(mode: str, start_step: Optional[str]):
    """Return (compiled_graph, fail_step) for the requested mode."""
    from agent import graph as graph_mod
    if mode == "full":
        return graph_mod.build_graph(), StepName.DONE
    if mode == "gate":
        return graph_mod.build_gate_subgraph(), StepName.DEPLOY_STAGING
    if mode == "promote":
        return graph_mod.build_promote_subgraph(), StepName.DEPLOY_PROD
    if mode == "gold":
        return graph_mod.build_gold_subgraph(), StepName.GOLD_BUILDER
    if mode == "from_step":
        step = StepName(start_step)
        return graph_mod.build_subgraph_from(step), step
    raise ValueError(f"unknown mode: {mode!r} (valid: {VALID_MODES})")


def _state_for_mode(mode: str, request_id: str, start_step: Optional[str]) -> AgentState:
    req = event_store.fetch_request(request_id)
    if not req:
        raise ValueError(f"request not found: {request_id}")

    if mode in ("full", "gold"):
        spec = PipelineSpec.model_validate(json.loads(req["spec_json"]))
        return AgentState(request_id=request_id, spec=spec)

    from agent.rehydrate import rehydrate_state
    state = rehydrate_state(request_id)
    if mode in ("gate", "promote"):
        # The human approving (gate) / the platform team releasing (promote)
        # IS the approval action.
        state.human_decision = "approved"
    elif mode == "from_step":
        state.rerun_from_step = start_step
    return state


def execute(request_id: str, mode: str = "full",
            start_step: Optional[str] = None) -> dict[str, Any]:
    """Run the agent graph for `request_id`. Crash-safe: failures are
    recorded to the event log + request row, never silently swallowed."""
    if mode not in VALID_MODES:
        raise ValueError(f"unknown mode: {mode!r} (valid: {VALID_MODES})")

    # In a Job there's no FastAPI startup hook — make sure @traced_node spans
    # land in app_spans. No-op inside the App (init_otel already ran) and on
    # repeat calls.
    try:
        from agent.otel_setup import init_otel_for_job
        init_otel_for_job()
    except Exception:
        pass

    graph, fail_step = _build_for_mode(mode, start_step)
    state = _state_for_mode(mode, request_id, start_step)

    final: dict[str, Any] = {}
    try:
        with mlflow_utils.agent_run(request_id, mode, start_step, spec=state.spec) as mlf:
            try:
                final = graph.invoke(state)
            except Exception as e:
                LOG.exception("agent run crashed (request %s, mode %s)", request_id, mode)
                event_store.append_event(request_id, StepEvent(
                    step=fail_step, status=StepStatus.FAILED,
                    message=f"Agent run crashed ({mode}): {e}",
                ))
                event_store.update_request(request_id, final_status="failed")
                mlflow_utils.log_final_metrics(mlf, "failed", None)
                return {"request_id": request_id, "mode": mode, "status": "failed",
                        "error": str(e)}

            req = event_store.fetch_request(request_id) or {}
            final_status = req.get("final_status") or "unknown"
            # A node ending the graph with fatal_error (e.g. deploy failure)
            # must not leave the request looking alive.
            fatal = (final or {}).get("fatal_error")
            if fatal and final_status in ("in_progress", "unknown"):
                event_store.update_request(request_id, final_status="failed")
                final_status = "failed"
            mlflow_utils.log_final_metrics(mlf, final_status, final)
    except Exception as e:
        # MLflow wrapper itself failed AFTER the graph may have run — don't
        # mark the request failed for an observability problem.
        LOG.warning("mlflow wrapper error (non-fatal): %s", e)
        req = event_store.fetch_request(request_id) or {}
        final_status = req.get("final_status") or "unknown"

    return {"request_id": request_id, "mode": mode, "status": final_status}
