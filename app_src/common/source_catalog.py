"""Persisted source-table catalog — the explicit "profiling step".

A scan of the source systems (the external Postgres systems) is
stored in a UC Delta control table `source_catalog`. This is the single
authoritative list of tables that:
  * the data browser shows,
  * the intake agent is told about (so it can't hallucinate table names),
  * spec_critic cross-checks against.

It is REFRESHED on demand (a button in the UI / `POST /api/sources/refresh`),
which is how a newly-added source table becomes visible: an operator clicks
Refresh, the catalog re-scans, and the new table appears everywhere. The
runtime deploy still validates live against information_schema, so the
catalog being slightly stale never causes a bad deploy — only the UI/agent
view lags until the next refresh.

Falls back to a live scan (no persistence) when DBSQL isn't reachable.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable, Optional

LOG = logging.getLogger("ops.source_catalog")

SYSTEMS = [
    {"key": "billing", "label": "External Postgres · system 1",
     "blurb": "Customer and invoice tables on the external Postgres source (logical schema `billing`). Live only when external Postgres is configured."},
    {"key": "network", "label": "External Postgres · system 2",
     "blurb": "Second external Postgres source (logical schema `network`). Live only when external Postgres is configured."},
]

_mem: dict[str, Any] = {"at": 0.0, "data": None}
_MEM_TTL = 300


def _table() -> str:
    from agent.event_store import _CATALOG, _CONTROL_SCHEMA
    return f"{_CATALOG}.{_CONTROL_SCHEMA}.source_catalog"


def scan_system(system: str) -> list[dict[str, Any]]:
    """Live scan of one source system: tables + est. rows + columns."""
    import psycopg
    from common import sources

    phys_schema = sources.physical_schema(system)
    tables: dict[str, dict[str, Any]] = {}
    with psycopg.connect(sources.conn_str_for(system), connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT c.relname, GREATEST(c.reltuples, 0)::bigint
                   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                   WHERE n.nspname = %s AND c.relkind = 'r' ORDER BY c.relname""",
                (phys_schema,))
            for name, est in cur.fetchall():
                tables[name] = {"name": f"{system}.{name}", "short_name": name,
                                "approx_rows": int(est), "columns": []}
            cur.execute(
                """SELECT table_name, column_name, data_type
                   FROM information_schema.columns WHERE table_schema = %s
                   ORDER BY table_name, ordinal_position""",
                (phys_schema,))
            for tname, col, dtype in cur.fetchall():
                if tname in tables:
                    tables[tname]["columns"].append({"name": col, "type": dtype})
    return list(tables.values())


def scan_connect_sources() -> list[dict[str, Any]]:
    """Attached Lakeflow Connect sources as catalog systems.

    Their tables come from the ATTACHMENT (chosen by the operator when the UC
    connection was attached), enriched with federated column metadata when a
    foreign catalog is bound. There is no psycopg scan here on purpose: a UC
    connection's credentials are write-only, so the only way to see inside one
    is UC federation.
    """
    from agent import connect_store
    from common import uc_connections

    out: list[dict[str, Any]] = []
    for src in connect_store.list_sources():
        key = src.get("system_key") or ""
        if not key:
            continue
        cat = src.get("browse_catalog") or ""
        sch = src.get("source_schema") or ""
        cols_by_table: dict[str, list[dict[str, str]]] = {}
        if cat and sch:
            try:
                cols_by_table = {t["name"]: t["columns"]
                                 for t in uc_connections.list_tables(cat, sch)}
            except Exception as e:  # noqa: BLE001 — metadata is a bonus, not a gate
                LOG.warning("federated column scan failed for %s.%s: %s", cat, sch, e)
        tables = [{"name": f"{key}.{t}", "short_name": t, "approx_rows": -1,
                   "columns": cols_by_table.get(t, [])}
                  for t in (src.get("tables") or [])]
        label = f"{src.get('name') or key} ({src.get('connection_type') or 'Lakeflow Connect'})"
        out.append({"key": key, "label": label,
                    "blurb": (f"Lakeflow Connect via UC connection "
                              f"`{src.get('connection_name')}`"
                              + (f" · {sch}" if sch else "")),
                    "tables": tables, "available": bool(tables),
                    "kind": "lakeflow_connect",
                    "connection_name": src.get("connection_name") or "",
                    "source_id": src.get("source_id") or ""})
    return out


def _degraded_scan_warning(tables: list[dict[str, Any]]) -> str:
    """A message when a scan found tables but no COLUMNS — which means the
    reader lacks Postgres privileges, not that the tables are column-less.

    Kept separate (and returning a string rather than raising) because the
    source IS partly usable in this state: the table list is real. The caller
    records it so the UI and the deploy can say so out loud.
    """
    if not tables:
        return ""
    blind = [t["name"] for t in tables if not t.get("columns")]
    if len(blind) != len(tables):
        return ""  # some columns came back — a genuinely empty table is normal
    return (f"{len(blind)} table(s) visible but NO columns readable — the "
            "connecting principal is missing `USAGE` on the schema and "
            "`SELECT` on its tables (table names come from world-readable "
            "pg_class; columns need the grant). Run "
            "`scripts/grant_lakebase_access.py`.")


def _scan_all() -> dict[str, Any]:
    systems = []
    for s in SYSTEMS:
        entry = dict(s)
        try:
            entry["tables"] = scan_system(s["key"])
            entry["available"] = True
            warning = _degraded_scan_warning(entry["tables"])
            if warning:
                # A scan that found tables but NO columns is a permission
                # signal, not an empty source: `pg_class` is world-readable
                # (table names + row estimates need no grant) while
                # `information_schema.columns` filters on privilege and simply
                # returns nothing. That half-success raises no exception, so
                # without this it persists as perfectly healthy and the only
                # tell is every table showing zero columns. KNOWN_ISSUES 6.
                entry["warning"] = warning
                LOG.warning("source scan for %s is degraded: %s", s["key"], warning)
        except Exception as e:
            LOG.warning("source scan failed for %s: %s", s["key"], e)
            entry["tables"] = []
            entry["available"] = False
            entry["error"] = str(e)[:200]
        systems.append(entry)
    try:
        systems.extend(scan_connect_sources())
    except Exception as e:  # noqa: BLE001
        LOG.warning("connect-source scan failed: %s", e)
    return {"systems": systems, "profiled_at": time.time()}


# --- persistence ------------------------------------------------------

def _ensure_table() -> None:
    from agent.event_store import _can_use_dbsql, _conn
    if not _can_use_dbsql():
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_table()} (
                    system STRING, label STRING, blurb STRING,
                    available BOOLEAN, payload_json STRING, profiled_at TIMESTAMP
                ) USING DELTA""")


def _persist(data: dict[str, Any]) -> None:
    from agent.event_store import _can_use_dbsql, _conn
    if not _can_use_dbsql():
        return
    try:
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {_table()}")
                for s in data["systems"]:
                    cur.execute(
                        f"""INSERT INTO {_table()}
                            (system, label, blurb, available, payload_json, profiled_at)
                            VALUES (?, ?, ?, ?, ?, current_timestamp())""",
                        (s["key"], s.get("label", ""), s.get("blurb", ""),
                         bool(s.get("available")), json.dumps(s.get("tables", []))))
    except Exception as e:
        LOG.warning("source_catalog persist failed: %s", e)


def _load_persisted() -> Optional[dict[str, Any]]:
    from agent.event_store import _can_use_dbsql, _conn
    if not _can_use_dbsql():
        return None
    try:
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""SELECT system, label, blurb, available, payload_json,
                                CAST(profiled_at AS STRING) FROM {_table()}""")
                rows = cur.fetchall()
        if not rows:
            return None
        by_key = {}
        profiled_at = None
        for system, label, blurb, available, payload, pat in rows:
            tables = json.loads(payload or "[]")
            entry = {"key": system, "label": label, "blurb": blurb,
                     "available": bool(available), "tables": tables}
            # Re-derive rather than store it: the warning describes the READER's
            # privileges, which can change (a grant lands) without the catalog
            # being re-scanned. Deriving on load also flags rows persisted
            # BEFORE this check existed, instead of trusting a stale verdict.
            warning = _degraded_scan_warning(tables)
            if warning:
                entry["warning"] = warning
            by_key[system] = entry
            profiled_at = pat
        # Built-in systems first, in canonical order; then anything else that
        # was persisted (attached Lakeflow Connect sources, whose keys are
        # operator-chosen and therefore NOT in SYSTEMS — filtering to SYSTEMS
        # alone would silently drop every attached connection on reload).
        builtin = [s["key"] for s in SYSTEMS]
        systems = [by_key[k] for k in builtin if k in by_key]
        systems += [v for k, v in by_key.items() if k not in builtin]
        return {"systems": systems, "profiled_at": profiled_at, "persisted": True}
    except Exception as e:
        LOG.warning("source_catalog load failed: %s", e)
        return None


# --- public API -------------------------------------------------------

def refresh(log: Optional[Callable[[str], None]] = None) -> dict[str, Any]:
    """Re-scan the source systems and persist. The explicit profiling step."""
    if log:
        log("Profiling source systems (live scan of the external Postgres systems)…")
    data = _scan_all()
    if any(s["available"] for s in data["systems"]):
        _persist(data)
    _mem["at"] = time.time()
    _mem["data"] = data
    if log:
        n = sum(len(s["tables"]) for s in data["systems"])
        log(f"Profiled {n} table(s) across {len(data['systems'])} system(s).")
    return data


def get(force: bool = False) -> dict[str, Any]:
    """Return the catalog: in-memory (TTL) → persisted Delta → live scan."""
    if not force and _mem["data"] is not None and time.time() - _mem["at"] < _MEM_TTL:
        return _mem["data"]
    if not force:
        persisted = _load_persisted()
        if persisted and any(s["tables"] for s in persisted["systems"]):
            _mem["at"] = time.time()
            _mem["data"] = persisted
            return persisted
    return refresh()


def logical_tables(data: Optional[dict[str, Any]] = None) -> list[str]:
    data = data or get()
    out: list[str] = []
    for s in data["systems"]:
        for t in s.get("tables", []):
            out.append(t["name"])
    return sorted(out)


def prompt_block(data: Optional[dict[str, Any]] = None) -> str:
    """Compact, authoritative table list for an LLM system prompt."""
    data = data or get()
    lines = ["# Real source tables (authoritative — NEVER invent table names; "
             "if a table isn't listed here it does not exist):"]
    for s in data["systems"]:
        tbls = s.get("tables", [])
        if not tbls:
            continue
        lines.append(f"  {s['key'].upper()} ({s.get('label','')}):")
        for t in tbls:
            cols = ", ".join(c["name"] for c in t.get("columns", [])[:14])
            # approx_rows is -1 for attached Lakeflow Connect sources (no row
            # count without querying the source) — don't print "-1 rows".
            rows = t.get("approx_rows", -1)
            count = f"{rows:,} rows" if isinstance(rows, int) and rows >= 0 else "row count unknown"
            lines.append(f"    - {t['name']} ({count}) [{cols}]")
    lines.append("  Join tables across the two systems only on keys both sides actually have (see the column lists above).")
    return "\n".join(lines)
