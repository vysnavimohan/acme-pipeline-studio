"""Persistence for **Lakeflow Connect sources** — UC connections attached to
the generator.

A *connect source* is an existing Unity Catalog connection (registered by the
platform team, credentials and all) that an operator has attached to the Studio
so requests can build from it. Attaching records four things:

    connection_name   which UC connection to ingest through
    system_key        the logical prefix used in a spec's `system.table` names
    source_catalog/schema   where the tables live on the source side
    tables            the tables offered to the build flow

Deliberately separate from `autoloader_store`: an Auto Loader source is a
Volume path whose datasets are *directories discovered by scanning*, whereas a
connect source is a credentialed connection whose tables are *chosen from a
catalog*. Overloading one table for both would mean a `volume_path` column
holding a connection name.

Same storage contract as its siblings — a UC Delta control table, reusing
`event_store`'s pooled connection + retry + OAuth minting, falling back to
local JSON when no SQL warehouse is reachable (local dev / unit tests).
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any, Optional

from agent.event_store import (
    _CATALOG,
    _CONTROL_SCHEMA,
    _LOCAL_DIR,
    _can_use_dbsql,
    _conn,
    _db_retry,
)

_SOURCES_FILE = _LOCAL_DIR / "_connect_sources.json"

_COLS = ("source_id", "name", "connection_name", "connection_type", "system_key",
         "source_catalog", "source_schema", "browse_catalog", "tables_json",
         "created_by", "created_at", "updated_at")


def new_source_id() -> str:
    return "cs-" + uuid.uuid4().hex[:8]


def slug_system_key(value: str) -> str:
    """A spec-safe logical system prefix. Spec tables are `system.table`, so the
    key must survive that split: lowercase, alphanumeric + underscore, no dots."""
    s = re.sub(r"[^a-z0-9_]+", "_", (value or "").strip().lower()).strip("_")
    return s or "source"


def _table() -> str:
    return f"{_CATALOG}.{_CONTROL_SCHEMA}.connect_sources"


def _ensure_table() -> None:
    if not _can_use_dbsql():
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_table()} (
                    source_id STRING NOT NULL,
                    name STRING,
                    connection_name STRING,
                    connection_type STRING,
                    system_key STRING,
                    source_catalog STRING,
                    source_schema STRING,
                    browse_catalog STRING,
                    tables_json STRING,
                    created_by STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                ) USING DELTA""")


def hydrate(row: Optional[dict]) -> Optional[dict]:
    """Parse `tables_json` + stringify timestamps so the result is
    JSON-serialisable for the API."""
    if not row:
        return row
    out = dict(row)
    raw = out.pop("tables_json", out.get("tables"))
    if isinstance(raw, str) and raw:
        try:
            out["tables"] = json.loads(raw)
        except Exception:
            out["tables"] = []
    elif isinstance(raw, list):
        out["tables"] = raw
    else:
        out["tables"] = []
    for ts in ("created_at", "updated_at"):
        if out.get(ts) is not None:
            out[ts] = str(out[ts])
    return out


def _read_local() -> dict:
    if _SOURCES_FILE.exists():
        try:
            return json.loads(_SOURCES_FILE.read_text())
        except Exception:
            return {}
    return {}


def _write_local(data: dict) -> None:
    _SOURCES_FILE.write_text(json.dumps(data, indent=2, default=str))


# --- CRUD -------------------------------------------------------------------

@_db_retry
def attach(name: str, connection_name: str, *, connection_type: str = "",
           system_key: str = "", source_catalog: str = "", source_schema: str = "",
           browse_catalog: str = "", tables: Optional[list[str]] = None,
           created_by: str = "platform-team",
           source_id: Optional[str] = None) -> dict[str, Any]:
    """Attach a UC connection to the generator (idempotent per `source_id`)."""
    sid = source_id or new_source_id()
    key = slug_system_key(system_key or name or connection_name)
    tbls = list(tables or [])
    if _can_use_dbsql():
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {_table()} WHERE source_id = ?", (sid,))
                cur.execute(
                    f"""INSERT INTO {_table()}
                        (source_id, name, connection_name, connection_type, system_key,
                         source_catalog, source_schema, browse_catalog, tables_json,
                         created_by, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                                current_timestamp(), current_timestamp())""",
                    (sid, name, connection_name, connection_type, key, source_catalog,
                     source_schema, browse_catalog, json.dumps(tbls), created_by))
    else:
        data = _read_local()
        now = datetime.utcnow().isoformat()
        data[sid] = {"source_id": sid, "name": name, "connection_name": connection_name,
                     "connection_type": connection_type, "system_key": key,
                     "source_catalog": source_catalog, "source_schema": source_schema,
                     "browse_catalog": browse_catalog, "tables": tbls,
                     "created_by": created_by, "created_at": now, "updated_at": now}
        _write_local(data)
    return {"source_id": sid, "name": name, "connection_name": connection_name,
            "system_key": key, "tables": tbls}


@_db_retry
def list_sources() -> list[dict[str, Any]]:
    if _can_use_dbsql():
        try:
            _ensure_table()
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT {', '.join(_COLS)} FROM {_table()} ORDER BY created_at")
                    return [hydrate(dict(zip(_COLS, r))) for r in cur.fetchall()]
        except Exception:  # noqa: BLE001 — a missing table must not break the page
            return []
    rows = sorted(_read_local().values(), key=lambda r: r.get("created_at", ""))
    return [hydrate(r) for r in rows]


@_db_retry
def fetch_source(source_id: str) -> Optional[dict[str, Any]]:
    if _can_use_dbsql():
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {', '.join(_COLS)} FROM {_table()} WHERE source_id = ?",
                            (source_id,))
                row = cur.fetchone()
                return hydrate(dict(zip(_COLS, row))) if row else None
    return hydrate(_read_local().get(source_id))


@_db_retry
def set_tables(source_id: str, tables: list[str]) -> None:
    """Change which of the connection's tables are offered to the build flow."""
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {_table()} SET tables_json = ?, updated_at = current_timestamp()
                        WHERE source_id = ?""",
                    (json.dumps(list(tables)), source_id))
    else:
        data = _read_local()
        if source_id in data:
            data[source_id]["tables"] = list(tables)
            data[source_id]["updated_at"] = datetime.utcnow().isoformat()
            _write_local(data)


@_db_retry
def detach(source_id: str) -> None:
    """Remove the attachment. The UC connection itself is never touched."""
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {_table()} WHERE source_id = ?", (source_id,))
    else:
        data = _read_local()
        data.pop(source_id, None)
        _write_local(data)


# --- lookups used by the build flow -----------------------------------------

def by_system_key(key: str) -> Optional[dict[str, Any]]:
    """The attached source a spec's `system.table` prefix refers to. Best-effort
    — callers fall back to the built-in BILLING/NETWORK systems."""
    key = slug_system_key(key)
    try:
        for s in list_sources():
            if s.get("system_key") == key:
                return s
    except Exception:  # noqa: BLE001
        pass
    return None


def system_keys() -> list[str]:
    """Logical system prefixes contributed by attached connections — extends the
    built-in BILLING/NETWORK set in `common.sources`."""
    try:
        return [s["system_key"] for s in list_sources() if s.get("system_key")]
    except Exception:  # noqa: BLE001
        return []


def logical_tables(source: dict[str, Any]) -> list[str]:
    """`system.table` names this attached source offers."""
    key = source.get("system_key") or ""
    return [f"{key}.{t}" for t in (source.get("tables") or [])]
