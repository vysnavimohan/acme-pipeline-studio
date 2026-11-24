"""Observability API — reads from the Delta `app_spans` table that the
OTel DeltaSpanExporter writes to.

Endpoints:
  GET  /api/observability/traces       — list recent traces (one row per trace_id)
  GET  /api/observability/trace/{tid}  — list spans for one trace, time-ordered
  GET  /api/observability/health       — basic health (table reachable + row count)
"""

from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from agent import event_store
from agent.otel_exporter import ensure_app_spans_table

LOG = logging.getLogger("ops.observability")

router = APIRouter()


def _table_fq() -> str:
    return f"{event_store._CATALOG}.{event_store._CONTROL_SCHEMA}.app_spans"


def _query(sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
    if not event_store._can_use_dbsql():
        return []
    try:
        ensure_app_spans_table()
    except Exception as e:
        LOG.warning("ensure_app_spans_table failed: %s", e)
    with event_store._conn() as conn:
        with conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]


@router.get("/health")
def health():
    # Diagnose the OTel pipeline end-to-end
    otel_status: dict = {}
    try:
        import opentelemetry  # noqa: F401
        otel_status["sdk_installed"] = True
        try:
            from opentelemetry import trace
            tp = trace.get_tracer_provider()
            otel_status["tracer_provider"] = type(tp).__name__
            # Check processors
            try:
                processors = getattr(tp, "_active_span_processor", None)
                if processors is None:
                    processors = getattr(tp, "_span_processor", None)
                otel_status["has_processor"] = bool(processors)
            except Exception as e:
                otel_status["processor_check_error"] = str(e)
            # Issue a test span to force the exporter to fire
            tracer = trace.get_tracer("ops.diagnostic")
            with tracer.start_as_current_span("observability.health.test_span") as span:
                span.set_attribute("ops.test", True)
        except Exception as e:
            otel_status["trace_check_error"] = str(e)
    except Exception as e:
        otel_status["sdk_installed"] = False
        otel_status["error"] = str(e)
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured", "otel": otel_status}
    try:
        rows = _query(f"SELECT COUNT(*) AS n FROM {_table_fq()}")
        return {"available": True, "row_count": int(rows[0]["n"]) if rows else 0,
                "otel": otel_status}
    except Exception as e:
        return {"available": False, "reason": str(e), "otel": otel_status}


@router.get("/traces")
def list_traces(
    limit: int = Query(50, ge=1, le=500),
    request_id: str | None = Query(None, description="filter to spans tagged with this request_id"),
):
    """One row per trace_id, summarized. Newest first."""
    where = ""
    params: tuple = ()
    if request_id:
        where = "WHERE request_id = ?"
        params = (request_id,)
    sql = f"""
        SELECT
            trace_id,
            MIN(start_time) AS started_at,
            MAX(end_time)   AS ended_at,
            COUNT(*)        AS span_count,
            MAX(duration_ms) AS root_duration_ms,
            MAX(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS had_error,
            MAX(request_id) AS request_id,
            MIN(name)       AS sample_name
        FROM {_table_fq()}
        {where}
        GROUP BY trace_id
        ORDER BY started_at DESC
        LIMIT {int(limit)}
    """
    try:
        rows = _query(sql, params or None)
        return {"available": True, "traces": rows}
    except Exception as e:
        return {"available": False, "reason": str(e), "traces": []}


@router.get("/trace/{trace_id}")
def get_trace(trace_id: str):
    if not (len(trace_id) == 32 and all(c in "0123456789abcdef" for c in trace_id.lower())):
        raise HTTPException(status_code=400, detail="trace_id must be 32 hex chars")
    sql = f"""
        SELECT trace_id, span_id, parent_span_id, name, kind, service,
               start_time, end_time, duration_ms, status, request_id,
               attributes_json, events_json
        FROM {_table_fq()}
        WHERE trace_id = ?
        ORDER BY start_time
    """
    try:
        rows = _query(sql, (trace_id,))
        # parse JSON-ish columns for the UI
        for r in rows:
            for k in ("attributes_json", "events_json"):
                v = r.get(k)
                if isinstance(v, str):
                    try:
                        r[k.replace("_json", "")] = json.loads(v)
                    except Exception:
                        r[k.replace("_json", "")] = v
        return {"trace_id": trace_id, "spans": rows}
    except Exception as e:
        return {"trace_id": trace_id, "spans": [], "error": str(e)}
