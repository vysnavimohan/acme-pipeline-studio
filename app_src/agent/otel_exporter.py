"""Custom OpenTelemetry span exporter that batches spans into a Delta table
on a Databricks SQL warehouse. Reuses event_store's connection helpers so
auth + warehouse-id env wiring is shared.

The exporter is intentionally simple:
  - In-memory buffer
  - Flush every N spans OR T seconds (whichever first)
  - Falls back to a no-op if the warehouse isn't reachable (works locally)

The Delta table schema is created once at import via _ensure_table().
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import threading
import time
from typing import Any

from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

LOG = logging.getLogger("ops.otel_exporter")

# Lazy import — event_store pulls in dbsql etc which is heavy
def _table_name() -> str:
    from agent.event_store import _CATALOG, _CONTROL_SCHEMA
    return f"{_CATALOG}.{_CONTROL_SCHEMA}.app_spans"


def ensure_app_spans_table() -> None:
    from agent.event_store import _can_use_dbsql, _conn
    if not _can_use_dbsql():
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {_table_name()} (
                    trace_id STRING NOT NULL,
                    span_id STRING NOT NULL,
                    parent_span_id STRING,
                    name STRING NOT NULL,
                    kind STRING,
                    service STRING,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    duration_ms BIGINT NOT NULL,
                    status STRING,
                    request_id STRING,
                    attributes_json STRING,
                    events_json STRING
                ) USING DELTA"""
            )


class DeltaSpanExporter(SpanExporter):
    """SpanExporter that batches spans into ${OPS_CONTROL_CATALOG}.${OPS_CONTROL_SCHEMA}.app_spans."""

    def __init__(self, flush_every_n: int = 25, flush_every_s: float = 10.0):
        self._buf: list[tuple] = []
        self._lock = threading.Lock()
        self._flush_n = flush_every_n
        self._flush_s = flush_every_s
        self._last_flush = time.monotonic()
        try:
            ensure_app_spans_table()
        except Exception as e:
            LOG.warning("could not ensure app_spans table at startup: %s", e)

    @staticmethod
    def _row(s: ReadableSpan) -> tuple:
        ctx = s.get_span_context()
        trace_id_hex = f"{ctx.trace_id:032x}"
        span_id_hex = f"{ctx.span_id:016x}"
        parent_hex = f"{s.parent.span_id:016x}" if s.parent else None
        attrs = dict(s.attributes or {})
        # promote ops.request_id to a column so the tracker can deep-link
        request_id = str(attrs.get("ops.request_id") or "") or None
        service = None
        if s.resource and s.resource.attributes:
            service = s.resource.attributes.get("service.name")
        kind = s.kind.name if s.kind else None
        start = dt.datetime.fromtimestamp(s.start_time / 1_000_000_000, dt.timezone.utc)
        end = dt.datetime.fromtimestamp(s.end_time / 1_000_000_000, dt.timezone.utc)
        duration_ms = max(0, (s.end_time - s.start_time) // 1_000_000)
        status = s.status.status_code.name if s.status else "UNSET"
        events = [
            {
                "name": e.name,
                "time": dt.datetime.fromtimestamp(e.timestamp / 1_000_000_000, dt.timezone.utc).isoformat(),
                "attrs": dict(e.attributes or {}),
            }
            for e in s.events
        ]
        return (
            trace_id_hex, span_id_hex, parent_hex,
            s.name, kind, service,
            start, end, duration_ms,
            status, request_id,
            json.dumps(attrs, default=str),
            json.dumps(events, default=str),
        )

    def export(self, spans: list[ReadableSpan]) -> SpanExportResult:
        try:
            rows = [self._row(s) for s in spans]
        except Exception as e:
            LOG.warning("failed to serialize spans: %s", e)
            return SpanExportResult.FAILURE
        with self._lock:
            self._buf.extend(rows)
            due = (
                len(self._buf) >= self._flush_n
                or (time.monotonic() - self._last_flush) >= self._flush_s
            )
            if due:
                self._drain_locked()
        return SpanExportResult.SUCCESS

    def _drain_locked(self) -> None:
        from agent.event_store import _can_use_dbsql, _conn
        if not self._buf:
            return
        if not _can_use_dbsql():
            # nothing to do — silently drop. Console exporter still has them.
            self._buf.clear()
            self._last_flush = time.monotonic()
            return
        rows, self._buf = self._buf, []
        self._last_flush = time.monotonic()
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        f"""INSERT INTO {_table_name()}
                            (trace_id, span_id, parent_span_id, name, kind, service,
                             start_time, end_time, duration_ms, status, request_id,
                             attributes_json, events_json)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        rows,
                    )
        except Exception as e:
            LOG.warning("dropped %d spans — Delta INSERT failed: %s", len(rows), e)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        with self._lock:
            self._drain_locked()
        return True

    def shutdown(self) -> None:
        self.force_flush()
