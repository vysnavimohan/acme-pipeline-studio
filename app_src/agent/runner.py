"""Launches agent executions.

Primary path: one Databricks Job run per execution (`jobs.run_now` on the
bundle's `agent_request` job). This is what gives the studio real
concurrency — N requests = N isolated serverless job runs — and survives
App restarts/redeploys (the old in-App background thread did neither).

Fallback path: a daemon thread running the same orchestrate.execute().
Used automatically when no AGENT_JOB_ID is configured (local dev, unit
tests) or when the Jobs API call fails.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any, Optional

from agent.state import StepEvent, StepName, StepStatus

LOG = logging.getLogger("ops.runner")


def _job_id() -> Optional[int]:
    raw = (os.environ.get("AGENT_JOB_ID") or "").strip()
    if not raw or os.environ.get("OPS_FORCE_THREAD_RUNNER") == "1":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


_RESOLVED_JOB_ID: Optional[int] = None  # cached by-name lookup


def _agent_job_name() -> str:
    return (os.environ.get("OPS_AGENT_JOB_NAME") or "databricks-pipeline-studio-agent").strip()


def _resolve_job_id_by_name() -> Optional[int]:
    """Resolve the agent Job by NAME (the bundle's `databricks-pipeline-studio-agent`).
    This makes the runner robust to a stale/missing AGENT_JOB_ID — e.g. after a
    fresh `bundle destroy` + redeploy on a new workspace gives the job a new id.
    Cached after the first successful lookup."""
    global _RESOLVED_JOB_ID
    if _RESOLVED_JOB_ID is not None:
        return _RESOLVED_JOB_ID
    name = _agent_job_name()
    if not name:
        return None
    try:
        from agent.event_store import _get_ws_client
        w = _get_ws_client()
        # The bundle's dev target prefixes the job name (e.g.
        # "[dev jeremy_herbert] databricks-pipeline-studio-agent"), so an exact-name
        # filter misses it — match by SUBSTRING across all jobs instead.
        for j in w.jobs.list():
            jn = (getattr(j, "settings", None) and j.settings.name) or ""
            if name in jn:
                _RESOLVED_JOB_ID = int(j.job_id)
                LOG.info("Resolved agent job (%r in %r) → %s", name, jn, _RESOLVED_JOB_ID)
                return _RESOLVED_JOB_ID
    except Exception as e:  # noqa: BLE001
        LOG.warning("agent job name resolution failed: %s", e)
    return None


def _thread_target(request_id: str, mode: str, start_step: Optional[str]) -> None:
    from agent.orchestrate import execute
    try:
        execute(request_id, mode=mode, start_step=start_step)
    except Exception as e:
        # execute() records graph crashes itself; this catches pre-graph
        # failures (e.g. request row missing).
        from agent import event_store
        LOG.exception("thread runner crashed for %s", request_id)
        event_store.append_event(request_id, StepEvent(
            step=StepName.DONE, status=StepStatus.FAILED,
            message=f"Runner crashed before the graph started: {e}",
        ))
        event_store.update_request(request_id, final_status="failed")


def _launch_thread(request_id: str, mode: str, start_step: Optional[str]) -> dict[str, Any]:
    threading.Thread(
        target=_thread_target, args=(request_id, mode, start_step),
        daemon=True, name=f"agent-{mode}-{request_id[:8]}",
    ).start()
    return {"runner": "thread", "mode": mode}


def _launch_job(job_id: int, request_id: str, mode: str,
                start_step: Optional[str]) -> dict[str, Any]:
    from agent.event_store import _get_ws_client, append_event
    w = _get_ws_client()
    waiter = w.jobs.run_now(
        job_id=job_id,
        job_parameters={
            "request_id": request_id,
            "mode": mode,
            "start_step": start_step or "",
        },
    )
    run_id = waiter.run_id
    host = w.config.host.rstrip("/")
    run_url = f"{host}/jobs/{job_id}/runs/{run_id}"
    append_event(request_id, StepEvent(
        step=StepName.INITIATED, status=StepStatus.SUCCEEDED,
        message=f"Launched Databricks Job run {run_id} ({mode}). "
                "The agent executes as an isolated serverless job — multiple "
                "requests run concurrently.",
        details={"kind": "job_run", "mode": mode, "job_id": job_id,
                 "run_id": run_id, "run_url": run_url},
    ))
    return {"runner": "job", "mode": mode, "job_id": job_id,
            "run_id": run_id, "run_url": run_url}


def launch(mode: str, request_id: str, start_step: Optional[str] = None) -> dict[str, Any]:
    """Start an agent execution. Returns metadata about how it was launched.

    Prefers the configured AGENT_JOB_ID, then resolves the job BY NAME (robust to
    a stale/missing id on a fresh workspace), then falls back to the in-App thread
    runner (local dev only — the App container has no langgraph in deployment)."""
    global _RESOLVED_JOB_ID
    if os.environ.get("OPS_FORCE_THREAD_RUNNER") == "1":
        return _launch_thread(request_id, mode, start_step)
    candidates: list[int] = []
    cfg = _job_id()
    if cfg is not None:
        candidates.append(cfg)
    by_name = _resolve_job_id_by_name()
    if by_name is not None and by_name not in candidates:
        candidates.append(by_name)
    last_err: Optional[Exception] = None
    for job_id in candidates:
        try:
            return _launch_job(job_id, request_id, mode, start_step)
        except Exception as e:
            last_err = e
            LOG.warning("Jobs API launch failed for job %s (%s) — trying next", job_id, e)
            if job_id == cfg:
                _RESOLVED_JOB_ID = None  # configured id is bad — force a fresh by-name lookup
                bn = _resolve_job_id_by_name()
                if bn is not None and bn not in candidates:
                    candidates.append(bn)
    # A job WAS expected but couldn't be launched. The thread-runner fallback below
    # has no langgraph in a deployed App, so its ImportError would otherwise MASK the
    # real cause. Surface it as a request event so it's diagnosable (the usual cause
    # on a fresh serverless deploy: the App service principal needs CAN_MANAGE_RUN on
    # the `databricks-pipeline-studio-agent` job — granted by deploy_serverless.py — and UC
    # permission propagation can lag ~1 min right after deploy, so a retry succeeds).
    if candidates and last_err is not None:
        try:
            from agent.event_store import append_event
            append_event(request_id, StepEvent(
                step=StepName.INITIATED, status=StepStatus.IN_PROGRESS,
                message=(f"Could not launch the agent Job (tried {candidates}): "
                         f"{str(last_err)[:200]}. Falling back to the in-app runner. If this is a "
                         "fresh deploy, ensure the App SP has CAN_MANAGE_RUN on the agent job "
                         "and retry in ~1 min (permission propagation)."),
                details={"kind": "job_launch_failed", "candidates": [str(c) for c in candidates],
                         "error": str(last_err)[:400]},
            ))
        except Exception:  # noqa: BLE001
            pass
    return _launch_thread(request_id, mode, start_step)
