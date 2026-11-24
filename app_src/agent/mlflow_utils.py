"""MLflow experiment tracking + tracing for agent runs.

Every agent execution (full / gate / from_step) becomes one MLflow run in
the workspace experiment, with LangGraph node-level traces captured via
`mlflow.langchain.autolog()`. The run's identifiers are emitted to the
request's event log (kind="mlflow_run") so the tracker UI can deep-link
to the experiment run + its traces.

All of this is best-effort: if MLflow or workspace access is unavailable
(local dev, unit tests), agent execution proceeds untraced.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Optional

LOG = logging.getLogger("ops.mlflow")

EXPERIMENT_PATH = os.environ.get(
    "MLFLOW_EXPERIMENT_PATH", "/Shared/databricks-pipeline-studio-agent"
)


def _host() -> Optional[str]:
    host = os.environ.get("DATABRICKS_HOST")
    if host:
        return host.rstrip("/")
    try:
        from agent.event_store import _get_ws_client
        return _get_ws_client().config.host.rstrip("/")
    except Exception:
        return None


def _setup() -> Optional[Any]:
    """Configure tracking + autolog. Returns the mlflow module or None."""
    if os.environ.get("OPS_DISABLE_MLFLOW") == "1":
        return None
    try:
        import mlflow
    except Exception:
        return None
    try:
        if not os.environ.get("MLFLOW_TRACKING_URI"):
            mlflow.set_tracking_uri("databricks")
        exp = mlflow.set_experiment(EXPERIMENT_PATH)
        LOG.info("mlflow experiment: %s (%s)", EXPERIMENT_PATH, exp.experiment_id)
    except Exception as e:
        LOG.warning("mlflow experiment setup failed (%s) — running untraced", e)
        return None
    try:
        # LangGraph executions surface as traces via the langchain autolog
        # integration (each graph node = one span).
        mlflow.langchain.autolog()
    except Exception as e:
        LOG.warning("mlflow.langchain.autolog unavailable: %s", e)
    return mlflow


def run_urls(experiment_id: str, run_id: str) -> dict[str, str]:
    host = _host() or ""
    return {
        "run_url": f"{host}/ml/experiments/{experiment_id}/runs/{run_id}",
        "experiment_url": f"{host}/ml/experiments/{experiment_id}",
        "traces_url": f"{host}/ml/experiments/{experiment_id}/traces?eval.requestId={run_id}",
    }


@contextmanager
def agent_run(request_id: str, mode: str, start_step: Optional[str],
              spec: Optional[Any] = None):
    """Context manager wrapping one agent execution in an MLflow run.

    Yields a dict with run metadata ({} when MLflow is unavailable).
    Emits a kind="mlflow_run" event so the UI can link to the run.
    """
    mlflow = _setup()
    if mlflow is None:
        yield {}
        return

    t0 = time.time()
    run_name = f"{mode}-{request_id[:8]}"
    if spec is not None:
        try:
            run_name = f"{mode}-{spec.target.table}-{request_id[:8]}"
        except Exception:
            pass

    try:
        active = mlflow.start_run(run_name=run_name)
    except Exception as e:
        LOG.warning("mlflow.start_run failed (%s) — running untraced", e)
        yield {}
        return

    info: dict[str, Any] = {
        "mlflow": mlflow,
        "run_id": active.info.run_id,
        "experiment_id": active.info.experiment_id,
        **run_urls(active.info.experiment_id, active.info.run_id),
    }

    try:
        params: dict[str, Any] = {"request_id": request_id, "mode": mode}
        if start_step:
            params["start_step"] = start_step
        if spec is not None:
            try:
                tables = list(spec.source.tables or [])
                # A request can span BILLING + NETWORK — derive the distinct
                # systems from each table's prefix (fall back to primary).
                systems = sorted({(t.split(".")[0] if "." in t else spec.source.system)
                                  for t in tables}) or [spec.source.system]
                params.update({
                    "intent": (spec.intent or "")[:240],
                    "source_systems": ",".join(systems),
                    "source_table_count": len(tables),
                    "source_tables": ",".join(tables),
                    "primary_key": ",".join(spec.source.primary_key or []),
                    "target": f"{spec.target.catalog}.{spec.target.target_schema}.{spec.target.table}",
                    "target_write_mode": spec.target.write_mode,
                    "join_keys": (spec.transform.join_with or {}).get("on", "") if spec.transform.join_with else "",
                    "sla_mode": spec.sla.mode,
                    "sla_freshness_minutes": spec.sla.freshness_minutes,
                    "schedule_cron": spec.sla.schedule_cron or "",
                })
            except Exception:
                pass
        mlflow.log_params(params)
        mlflow.set_tags({
            "ops.request_id": request_id,
            "ops.mode": mode,
            "ops.app": "databricks-pipeline-studio",
        })
    except Exception:
        pass

    # Surface the run in the request's event log for the tracker UI.
    _emit_mlflow_event(request_id, mode, info)

    status = "FINISHED"
    try:
        yield info
    except Exception:
        status = "FAILED"
        raise
    finally:
        try:
            mlflow.log_metric("duration_seconds", round(time.time() - t0, 1))
            mlflow.end_run(status=status)
        except Exception:
            pass


def _emit_mlflow_event(request_id: str, mode: str, info: dict[str, Any]) -> None:
    try:
        from agent.event_store import append_event
        from agent.state import StepEvent, StepName, StepStatus
        append_event(request_id, StepEvent(
            step=StepName.INITIATED, status=StepStatus.SUCCEEDED,
            message=f"MLflow run started for this {mode} execution "
                    f"(run {info['run_id'][:8]}…) — traces capture every agent node.",
            details={
                "kind": "mlflow_run",
                "mode": mode,
                "run_id": info["run_id"],
                "experiment_id": info["experiment_id"],
                "run_url": info["run_url"],
                "experiment_url": info["experiment_url"],
                "traces_url": info["traces_url"],
            },
        ))
    except Exception as e:
        LOG.warning("could not emit mlflow_run event: %s", e)


def log_final_metrics(info: dict[str, Any], final_status: str,
                      state: Optional[Any] = None) -> None:
    """Attach outcome metrics to the active run (best-effort).

    `state` is the dict LangGraph returns from graph.invoke()."""
    mlflow = info.get("mlflow")
    if mlflow is None:
        return

    def _get(key: str, default=None):
        if state is None:
            return default
        if isinstance(state, dict):
            return state.get(key, default)
        return getattr(state, key, default)

    try:
        mlflow.set_tag("ops.final_status", final_status)
        if state is not None:
            dev = _get("dev_deploy") or {}
            prod = _get("prod_deploy") or {}
            if dev.get("silver_row_count") is not None:
                mlflow.log_metric("dev_silver_rows", dev["silver_row_count"])
            if prod.get("silver_row_count") is not None:
                mlflow.log_metric("prod_silver_rows", prod["silver_row_count"])
            mlflow.log_metric("review_iterations", _get("review_iterations", 0) or 0)
            mlflow.log_metric("critic_iterations", _get("critic_iterations", 0) or 0)
            mlflow.log_metric("standards_violations", len(_get("standards_violations") or []))
    except Exception:
        pass
