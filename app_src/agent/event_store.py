"""Persistent event store backed by a UC Delta table.

The Streamlit app polls `requests` and `request_events` tables to drive the
request tracker. The agent appends events here as it makes progress.

Falls back to local JSON files if no Databricks SQL warehouse is reachable
(useful for local dev / tests).
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from databricks import sql as dbsql

from agent.state import AgentState, PipelineSpec, StepEvent

_LOCAL_DIR = Path(os.environ.get("OPS_LOCAL_EVENT_DIR", "/tmp/ops_events"))
_LOCAL_DIR.mkdir(parents=True, exist_ok=True)

_CATALOG = os.environ.get("OPS_CONTROL_CATALOG", "vmohan_demo_catalog")
_CONTROL_SCHEMA = os.environ.get("OPS_CONTROL_SCHEMA", "studio_control")

_lock = threading.Lock()


def _resolve_warehouse_path() -> Optional[str]:
    v = (os.environ.get("DATABRICKS_WAREHOUSE_HTTP_PATH") or "").strip()
    if not v:
        wh_id = (os.environ.get("DATABRICKS_WAREHOUSE_ID") or "").strip()
        return f"/sql/1.0/warehouses/{wh_id}" if wh_id else None
    return v if v.startswith("/") else f"/sql/1.0/warehouses/{v}"


def _append_event_local(request_id: str, event: StepEvent) -> None:
    path = _LOCAL_DIR / f"{request_id}.json"
    with _lock:
        if path.exists():
            data = json.loads(path.read_text())
        else:
            data = {"events": []}
        data["events"].append({
            "timestamp": event.timestamp.isoformat(),
            "step": event.step.value if hasattr(event.step, "value") else str(event.step),
            "status": event.status.value if hasattr(event.status, "value") else str(event.status),
            "message": event.message,
            "details": event.details,
        })
        path.write_text(json.dumps(data, indent=2, default=str))


# --- Connection management -------------------------------------------
#
# Auth + warehouse sessions are expensive (an OAuth mint + a Thrift session
# handshake each add ~0.5-1s). We cache the SDK client (it refreshes its own
# OAuth token internally) and keep ONE persistent dbsql connection guarded by
# a lock, recreating it once on failure. This is what makes the UI feel
# instant instead of paying 1-2s per API call.

_ws_client = None
_dbsql_ok: Optional[bool] = None          # cached _can_use_dbsql verdict
_conn_cache: dict[str, Any] = {"conn": None}
_conn_lock = threading.RLock()


def _get_ws_client():
    global _ws_client
    if _ws_client is None:
        from databricks.sdk import WorkspaceClient
        _ws_client = WorkspaceClient()
    return _ws_client


def _resolve_host() -> Optional[str]:
    host = os.environ.get("DATABRICKS_HOST")
    if host:
        return host
    # Jobs/serverless context: the SDK knows the host even when the env
    # var isn't set.
    try:
        return _get_ws_client().config.host
    except Exception:
        return None


def _can_use_dbsql() -> bool:
    """True iff we can connect to a Databricks SQL warehouse. Result cached
    for the process lifetime (it doesn't change inside an App/Job run)."""
    global _dbsql_ok
    if _dbsql_ok is not None:
        return _dbsql_ok
    if not _resolve_warehouse_path() or not _resolve_host():
        _dbsql_ok = False
        return False
    if os.environ.get("DATABRICKS_TOKEN"):
        _dbsql_ok = True
        return True
    try:
        _get_ws_client().config.authenticate()
        _dbsql_ok = True
    except Exception:
        _dbsql_ok = False
    return _dbsql_ok


def _new_conn():
    host = (_resolve_host() or "").replace("https://", "").rstrip("/")
    http_path = _resolve_warehouse_path()
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        # Mint via the cached SDK client (it caches + refreshes internally).
        headers = _get_ws_client().config.authenticate()
        auth = headers.get("Authorization", "")
        token = auth[7:] if auth.lower().startswith("bearer ") else auth
    return dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )


class _PooledConn:
    """Context manager that yields the shared connection under a lock and
    keeps it open afterwards (unlike `with dbsql.connect(...)`)."""

    def __enter__(self):
        _conn_lock.acquire()
        if _conn_cache["conn"] is None:
            _conn_cache["conn"] = _new_conn()
        return _conn_cache["conn"]

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc is not None:
                # Connection might be stale/broken — drop it so the next call
                # reconnects fresh.
                try:
                    _conn_cache["conn"].close()
                except Exception:
                    pass
                _conn_cache["conn"] = None
        finally:
            _conn_lock.release()
        return False


def _conn():
    return _PooledConn()


def _db_retry(fn):
    """Retry a DB-touching function once. The first failure inside
    `with _conn()` drops the pooled connection, so the retry reconnects —
    this papers over warehouse-session idle timeouts."""
    import functools

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            if not _can_use_dbsql():
                raise
            return fn(*args, **kwargs)
    return wrapper


_TABLES_READY = False


@_db_retry
def ensure_tables() -> None:
    """Create control tables if missing. Idempotent; runs its DDL once per process (it used to run ~10
    statements on every submit, which made Submit take 20+ s)."""
    global _TABLES_READY
    if _TABLES_READY or not _can_use_dbsql():
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}.requests (
                    request_id STRING NOT NULL,
                    requested_by STRING,
                    intent STRING,
                    spec_json STRING NOT NULL,
                    submitted_at TIMESTAMP NOT NULL,
                    pr_url STRING,
                    pr_number BIGINT,
                    human_decision STRING,
                    final_status STRING,
                    assignee STRING,
                    release_status STRING,
                    release_id STRING,
                    updated_at TIMESTAMP NOT NULL
                ) USING DELTA
            """)
            # New columns added after the table first shipped — additive, safe
            # on tables created before these columns existed.
            for _col in ("assignee STRING", "release_status STRING", "release_id STRING"):
                try:
                    cur.execute(f"ALTER TABLE {_CATALOG}.{_CONTROL_SCHEMA}.requests "
                                f"ADD COLUMN {_col}")
                except Exception:
                    pass  # already present
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}.request_events (
                    request_id STRING NOT NULL,
                    seq BIGINT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    step STRING NOT NULL,
                    status STRING NOT NULL,
                    message STRING,
                    details_json STRING
                ) USING DELTA
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}.releases (
                    release_id STRING NOT NULL,
                    title STRING,
                    summary STRING,
                    created_by STRING,
                    request_ids STRING,
                    status STRING,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                ) USING DELTA
            """)
            # Auto Loader connectors: a "source" points at a UC Volume path; its
            # "datasets" are the directories within it, each with its own saved
            # Auto Loader config + onboarding state (new -> configured -> ready).
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources (
                    source_id STRING NOT NULL,
                    name STRING,
                    volume_path STRING,
                    created_by STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    last_refreshed_at TIMESTAMP
                ) USING DELTA
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets (
                    source_id STRING NOT NULL,
                    dataset STRING NOT NULL,
                    dir_path STRING,
                    landing_glob STRING,
                    status STRING,
                    config_json STRING,
                    profile_json STRING,
                    suggestion_json STRING,
                    last_test_status STRING,
                    last_test_message STRING,
                    last_tested_at TIMESTAMP,
                    file_count INT,
                    total_bytes BIGINT,
                    updated_at TIMESTAMP
                ) USING DELTA
            """)
    _TABLES_READY = True


@_db_retry
def submit_request(state: AgentState, final_status: str = "in_progress") -> None:
    """Insert a new request row (final_status given, default in_progress)."""
    spec_json = state.spec.model_dump_json()
    if _can_use_dbsql():
        ensure_tables()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.requests
                    (request_id, requested_by, intent, spec_json, submitted_at, human_decision, final_status, updated_at)
                    VALUES (?, ?, ?, ?, current_timestamp(), 'pending', ?, current_timestamp())
                    """,
                    (state.request_id, state.spec.requested_by, state.spec.intent, spec_json, final_status),
                )
    else:
        path = _LOCAL_DIR / f"{state.request_id}.json"
        path.write_text(json.dumps({
            "request_id": state.request_id,
            "requested_by": state.spec.requested_by,
            "intent": state.spec.intent,
            "spec_json": spec_json,
            "submitted_at": datetime.utcnow().isoformat(),
            "events": [],
            "human_decision": "pending",
            "final_status": final_status,
        }, indent=2))


def append_event(request_id: str, event: StepEvent) -> None:
    """Append a single event."""
    # Always log to stdout so the app/job logs show progress.
    print(f"[ops:{request_id[:8]}] {event.step.value if hasattr(event.step, 'value') else event.step} "
          f"-> {event.status.value if hasattr(event.status, 'value') else event.status} :: {event.message}",
          flush=True)
    # Always also write to local file (works in all environments).
    _append_event_local(request_id, event)
    # Best-effort: write to Delta via SQL warehouse if configured.
    if _can_use_dbsql():
        ensure_tables()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT COALESCE(MAX(seq), 0) + 1 FROM {_CATALOG}.{_CONTROL_SCHEMA}.request_events
                    WHERE request_id = ?
                """, (request_id,))
                seq = cur.fetchone()[0]
                cur.execute(
                    f"""
                    INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.request_events
                    (request_id, seq, timestamp, step, status, message, details_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        int(seq),
                        event.timestamp,
                        event.step.value if hasattr(event.step, "value") else str(event.step),
                        event.status.value if hasattr(event.status, "value") else str(event.status),
                        event.message,
                        json.dumps(event.details, default=str),
                    ),
                )


@_db_retry
def update_request(request_id: str, **updates: Any) -> None:
    if _can_use_dbsql():
        sets = ", ".join([f"{k} = ?" for k in updates])
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {_CATALOG}.{_CONTROL_SCHEMA}.requests SET {sets}, updated_at = current_timestamp() WHERE request_id = ?",
                    (*updates.values(), request_id),
                )
    else:
        path = _LOCAL_DIR / f"{request_id}.json"
        if path.exists():
            data = json.loads(path.read_text())
            data.update(updates)
            path.write_text(json.dumps(data, indent=2, default=str))


@_db_retry
def fetch_request(request_id: str) -> Optional[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {_CATALOG}.{_CONTROL_SCHEMA}.requests WHERE request_id = ?", (request_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
    else:
        path = _LOCAL_DIR / f"{request_id}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())


@_db_retry
def fetch_events(request_id: str) -> list[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT seq, timestamp, step, status, message, details_json
                    FROM {_CATALOG}.{_CONTROL_SCHEMA}.request_events WHERE request_id = ? ORDER BY seq""",
                    (request_id,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    else:
        path = _LOCAL_DIR / f"{request_id}.json"
        if not path.exists():
            return []
        events = json.loads(path.read_text()).get("events", [])
        # add seq for clients that filter
        for i, e in enumerate(events, start=1):
            e.setdefault("seq", i)
        return events


@_db_retry
def reset_all() -> dict:
    """Wipe request + event state.

    For DBSQL mode: TRUNCATE the two tables. For local mode: delete the JSON files.
    Returns a small dict reporting what was cleared.
    """
    if _can_use_dbsql():
        ensure_tables()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.requests")
                cur.execute(f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.request_events")
                try:
                    cur.execute(f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.releases")
                except Exception:
                    pass
        return {"mode": "delta", "schema": f"{_CATALOG}.{_CONTROL_SCHEMA}"}
    else:
        n = 0
        for f in _LOCAL_DIR.glob("*.json"):
            try:
                f.unlink()
                n += 1
            except Exception:
                pass
        return {"mode": "local", "files_removed": n}


@_db_retry
def seed_demo() -> dict:
    """Insert pre-built completed example requests so the demo starts populated.

    Idempotent — overwrites any existing rows with the same request_id.
    """
    from agent.demo_fixtures import get_seed_fixtures, get_seed_releases

    seeded = []
    for request, events in get_seed_fixtures():
        request_id = request["request_id"]
        if _can_use_dbsql():
            ensure_tables()
            with _conn() as conn:
                with conn.cursor() as cur:
                    # upsert request
                    cur.execute(
                        f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.requests WHERE request_id = ?",
                        (request_id,),
                    )
                    cur.execute(
                        f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.request_events WHERE request_id = ?",
                        (request_id,),
                    )
                    cur.execute(
                        f"""INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.requests
                            (request_id, requested_by, intent, spec_json, submitted_at,
                             pr_url, pr_number, human_decision, final_status,
                             assignee, release_status, release_id, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            request_id, request["requested_by"], request["intent"],
                            request["spec_json"], request["submitted_at"],
                            request["pr_url"], request["pr_number"],
                            request["human_decision"], request["final_status"],
                            request.get("assignee"), request.get("release_status"),
                            request.get("release_id"), request["updated_at"],
                        ),
                    )
                    for i, ev in enumerate(events, start=1):
                        cur.execute(
                            f"""INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.request_events
                                (request_id, seq, timestamp, step, status, message, details_json)
                                VALUES (?, ?, current_timestamp(), ?, ?, ?, ?)""",
                            (request_id, i, ev["step"], ev["status"], ev["message"],
                             ev.get("details_json", "{}")),
                        )
        else:
            path = _LOCAL_DIR / f"{request_id}.json"
            data = dict(request)
            # Add seq + timestamp per event so the tracker renders them
            data["events"] = [
                {**ev, "seq": i, "timestamp": request["submitted_at"]}
                for i, ev in enumerate(events, start=1)
            ]
            path.write_text(json.dumps(data, indent=2, default=str))
        seeded.append({"request_id": request_id, "intent": request["intent"][:60]})

    # Seed any pre-built release records (idempotent overwrite).
    releases = get_seed_releases()
    for rel in releases:
        rid = rel["release_id"]
        if _can_use_dbsql():
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.releases WHERE release_id = ?",
                        (rid,),
                    )
                    cur.execute(
                        f"""INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.releases
                            (release_id, title, summary, created_by, request_ids, status,
                             created_at, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, current_timestamp(), current_timestamp())""",
                        (rid, rel.get("title"), rel.get("summary"), rel.get("created_by"),
                         rel.get("request_ids"), rel.get("status", "released")),
                    )
        else:
            data = _read_local_releases()
            data[rid] = {**rel, "created_at": datetime.utcnow().isoformat(),
                         "updated_at": datetime.utcnow().isoformat()}
            _write_local_releases(data)

    # Seed the default Auto Loader source (Connectors page) so it's populated
    # out of the box — best effort, never blocks request/release seeding.
    # Telco/campaign sample feeds are opt-in (OPS_SEED_SAMPLE_FEEDS=1); Acme feeds below always seed.
    autoloader = {}
    if os.environ.get("OPS_SEED_SAMPLE_FEEDS") == "1":
        try:
            from agent import autoloader_store
            autoloader = autoloader_store.seed_default_source()
        except Exception as _e:  # noqa: BLE001
            print(f"[ops] autoloader seed skipped: {_e}", flush=True)

    # Acme Corp ERP feeds for the Data Browser (deterministic; independent
    # of the landing-volume scan above, which can fail on a Default-Storage
    # workspace). These three ready datasets let the presenter pick the source in
    # Act 1. Best-effort — never blocks the rest of the seed.
    cfo_datasets = {}
    try:
        from agent import autoloader_store
        cfo_datasets = autoloader_store.seed_cfo_datasets()
    except Exception as _e:  # noqa: BLE001
        print(f"[ops] cfo dataset seed skipped: {_e}", flush=True)

    # Perpetual/securitisation guideline overrides (the domain layer). Best-effort.
    domain_guidelines = {}
    try:
        from agent import perpetual_guidelines
        domain_guidelines = perpetual_guidelines.seed_perpetual_guidelines()
    except Exception as _e:  # noqa: BLE001
        print(f"[ops] perpetual guidelines seed skipped: {_e}", flush=True)

    return {"seeded": seeded, "count": len(seeded), "releases": len(releases),
            "autoloader": autoloader, "cfo_datasets": cfo_datasets,
            "domain_guidelines": domain_guidelines}


@_db_retry
def list_requests(limit: int = 50) -> list[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT request_id, requested_by, intent, submitted_at,
                           pr_url, pr_number, human_decision, final_status, updated_at,
                           spec_json, assignee, release_status, release_id
                       FROM {_CATALOG}.{_CONTROL_SCHEMA}.requests
                       ORDER BY submitted_at DESC LIMIT ?""",
                    (limit,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    else:
        out = []
        for f in sorted(_LOCAL_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:limit]:
            if f.name == "_releases.json":
                continue
            out.append(json.loads(f.read_text()))
        return out


# =====================================================================
# Releases — platform/ops promotion of staged requests to prod
# =====================================================================

_RELEASES_FILE = _LOCAL_DIR / "_releases.json"


@_db_retry
def create_release(release_id: str, title: str, summary: str, created_by: str,
                   request_ids: list[str]) -> None:
    """Insert a new release row in status 'in_progress'."""
    req_ids_json = json.dumps(request_ids)
    if _can_use_dbsql():
        ensure_tables()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.releases
                        (release_id, title, summary, created_by, request_ids, status,
                         created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, 'in_progress', current_timestamp(), current_timestamp())""",
                    (release_id, title, summary, created_by, req_ids_json),
                )
    else:
        data = _read_local_releases()
        data[release_id] = {
            "release_id": release_id, "title": title, "summary": summary,
            "created_by": created_by, "request_ids": req_ids_json,
            "status": "in_progress", "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        }
        _write_local_releases(data)


@_db_retry
def update_release(release_id: str, **updates: Any) -> None:
    if _can_use_dbsql():
        sets = ", ".join([f"{k} = ?" for k in updates])
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {_CATALOG}.{_CONTROL_SCHEMA}.releases SET {sets}, "
                    f"updated_at = current_timestamp() WHERE release_id = ?",
                    (*updates.values(), release_id),
                )
    else:
        data = _read_local_releases()
        if release_id in data:
            data[release_id].update(updates)
            _write_local_releases(data)


@_db_retry
def list_releases(limit: int = 50) -> list[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT release_id, title, summary, created_by, request_ids,
                           status, created_at, updated_at
                       FROM {_CATALOG}.{_CONTROL_SCHEMA}.releases
                       ORDER BY created_at DESC LIMIT ?""",
                    (limit,),
                )
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    else:
        data = _read_local_releases()
        rows = sorted(data.values(), key=lambda r: r.get("created_at", ""), reverse=True)
        return rows[:limit]


@_db_retry
def fetch_release(release_id: str) -> Optional[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {_CATALOG}.{_CONTROL_SCHEMA}.releases WHERE release_id = ?",
                    (release_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
    else:
        return _read_local_releases().get(release_id)


def _read_local_releases() -> dict:
    if _RELEASES_FILE.exists():
        try:
            return json.loads(_RELEASES_FILE.read_text())
        except Exception:
            return {}
    return {}


def _write_local_releases(data: dict) -> None:
    _RELEASES_FILE.write_text(json.dumps(data, indent=2, default=str))
