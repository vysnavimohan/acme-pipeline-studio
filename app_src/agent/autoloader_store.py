"""Persistence for Auto Loader connectors — sources and their datasets.

An **Auto Loader source** is a named pointer at a Unity Catalog Volume path
(`/Volumes/<cat>/<sch>/<vol>` or a sub-directory). Its **datasets** are the
immediate sub-directories of that path; each dataset carries its own saved
Auto Loader config + onboarding state:

    new        — discovered by a refresh, not configured yet
    configured — config saved + passed the quick (Python) parse check
    ready       — passed the deep test (real read_files execution)

Backed by two UC Delta control tables (`autoloader_sources`,
`autoloader_datasets`); reuses the pooled connection + retry + OAuth-minting
primitives from `event_store`. Falls back to local JSON when no SQL warehouse
is reachable (local dev / unit tests), exactly like the releases store.

This lives next to `event_store` (the App API *and* the agent Job both import
it: the connectors router for onboarding, `source_profiler` to prefer a
"ready" dataset's saved config).
"""

from __future__ import annotations

import json
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
    ensure_tables,
)

_SOURCES_FILE = _LOCAL_DIR / "_autoloader_sources.json"
_DATASETS_FILE = _LOCAL_DIR / "_autoloader_datasets.json"

_JSON_FIELDS = ("config", "profile", "suggestion")  # parsed in hydrate()


# --- helpers ----------------------------------------------------------------

def new_source_id() -> str:
    return "als-" + uuid.uuid4().hex[:8]


def _dataset_key(source_id: str, dataset: str) -> str:
    return f"{source_id}::{dataset}"


def hydrate(row: Optional[dict]) -> Optional[dict]:
    """Parse JSON columns to objects + stringify timestamps so the result is
    JSON-serialisable for the API."""
    if not row:
        return row
    out = dict(row)
    for base in _JSON_FIELDS:
        raw = out.pop(f"{base}_json", out.get(base))
        if isinstance(raw, str) and raw:
            try:
                out[base] = json.loads(raw)
            except Exception:
                out[base] = None
        elif not isinstance(raw, (dict, list)):
            out[base] = None
        else:
            out[base] = raw
    for ts in ("created_at", "updated_at", "last_refreshed_at", "last_tested_at"):
        if ts in out and out[ts] is not None:
            out[ts] = str(out[ts])
    return out


def _read_local(path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}
    return {}


def _write_local(path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, default=str))


# --- sources ----------------------------------------------------------------

@_db_retry
def create_source(name: str, volume_path: str, created_by: str = "platform-team",
                  source_id: Optional[str] = None) -> dict:
    sid = source_id or new_source_id()
    if _can_use_dbsql():
        ensure_tables()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources WHERE source_id = ?",
                    (sid,),
                )
                cur.execute(
                    f"""INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources
                        (source_id, name, volume_path, created_by, created_at, updated_at, last_refreshed_at)
                        VALUES (?, ?, ?, ?, current_timestamp(), current_timestamp(), NULL)""",
                    (sid, name, volume_path, created_by),
                )
    else:
        data = _read_local(_SOURCES_FILE)
        now = datetime.utcnow().isoformat()
        data[sid] = {"source_id": sid, "name": name, "volume_path": volume_path,
                     "created_by": created_by, "created_at": now, "updated_at": now,
                     "last_refreshed_at": None}
        _write_local(_SOURCES_FILE, data)
    return {"source_id": sid, "name": name, "volume_path": volume_path}


@_db_retry
def list_sources() -> list[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""SELECT source_id, name, volume_path, created_by, created_at,
                           updated_at, last_refreshed_at
                       FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources
                       ORDER BY created_at""")
                cols = [d[0] for d in cur.description]
                return [hydrate(dict(zip(cols, r))) for r in cur.fetchall()]
    rows = sorted(_read_local(_SOURCES_FILE).values(), key=lambda r: r.get("created_at", ""))
    return [hydrate(r) for r in rows]


@_db_retry
def fetch_source(source_id: str) -> Optional[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources WHERE source_id = ?",
                    (source_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return hydrate(dict(zip(cols, row)))
    return hydrate(_read_local(_SOURCES_FILE).get(source_id))


@_db_retry
def touch_source(source_id: str) -> None:
    """Stamp last_refreshed_at = now."""
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources
                        SET last_refreshed_at = current_timestamp(), updated_at = current_timestamp()
                        WHERE source_id = ?""",
                    (source_id,),
                )
    else:
        data = _read_local(_SOURCES_FILE)
        if source_id in data:
            data[source_id]["last_refreshed_at"] = datetime.utcnow().isoformat()
            _write_local(_SOURCES_FILE, data)


@_db_retry
def delete_source(source_id: str) -> None:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_sources WHERE source_id = ?",
                    (source_id,),
                )
                cur.execute(
                    f"DELETE FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets WHERE source_id = ?",
                    (source_id,),
                )
    else:
        sources = _read_local(_SOURCES_FILE)
        sources.pop(source_id, None)
        _write_local(_SOURCES_FILE, sources)
        ds = _read_local(_DATASETS_FILE)
        for k in [k for k in ds if k.startswith(f"{source_id}::")]:
            ds.pop(k, None)
        _write_local(_DATASETS_FILE, ds)


# --- datasets ---------------------------------------------------------------

@_db_retry
def list_datasets(source_id: str) -> list[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets WHERE source_id = ? ORDER BY dataset",
                    (source_id,),
                )
                cols = [d[0] for d in cur.description]
                return [hydrate(dict(zip(cols, r))) for r in cur.fetchall()]
    rows = [v for k, v in _read_local(_DATASETS_FILE).items() if v.get("source_id") == source_id]
    return [hydrate(r) for r in sorted(rows, key=lambda r: r.get("dataset", ""))]


@_db_retry
def fetch_dataset(source_id: str, dataset: str) -> Optional[dict]:
    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets WHERE source_id = ? AND dataset = ?",
                    (source_id, dataset),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return hydrate(dict(zip(cols, row)))
    return hydrate(_read_local(_DATASETS_FILE).get(_dataset_key(source_id, dataset)))


def _insert_dataset_row(cur, source_id: str, dataset: str, *, dir_path: str,
                        landing_glob: str, status: str, file_count: int,
                        total_bytes: int) -> None:
    cur.execute(
        f"""INSERT INTO {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets
            (source_id, dataset, dir_path, landing_glob, status, config_json,
             profile_json, suggestion_json, last_test_status, last_test_message,
             last_tested_at, file_count, total_bytes, updated_at)
            VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, 'untested', NULL, NULL, ?, ?, current_timestamp())""",
        (source_id, dataset, dir_path, landing_glob, status, int(file_count), int(total_bytes)),
    )


def refresh_source(source_id: str) -> list[dict]:
    """Re-scan the source volume path: register any new sub-directories as
    `new` datasets, refresh file counts on known ones (preserving their saved
    config + status), then return the dataset list. Raises if the volume path
    can't be listed (so the API can surface a clear grant error)."""
    from common import landing_zone  # lazy: avoids WorkspaceClient import in tests

    src = fetch_source(source_id)
    if not src:
        raise ValueError(f"unknown source {source_id}")
    root = src["volume_path"]
    discovered = landing_zone.list_subdirs(root)  # may raise -> surfaced by caller
    existing = {d["dataset"]: d for d in list_datasets(source_id)}

    if _can_use_dbsql():
        with _conn() as conn:
            with conn.cursor() as cur:
                for d in discovered:
                    name = d["name"]
                    glob = landing_zone.glob_for_dir(d["path"], d.get("latest_file"))
                    if name in existing:
                        cur.execute(
                            f"""UPDATE {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets
                                SET dir_path = ?, landing_glob = ?, file_count = ?,
                                    total_bytes = ?, updated_at = current_timestamp()
                                WHERE source_id = ? AND dataset = ?""",
                            (d["path"], glob, int(d["file_count"]), int(d["total_bytes"]),
                             source_id, name),
                        )
                    else:
                        _insert_dataset_row(cur, source_id, name, dir_path=d["path"],
                                            landing_glob=glob, status="new",
                                            file_count=d["file_count"], total_bytes=d["total_bytes"])
    else:
        store = _read_local(_DATASETS_FILE)
        now = datetime.utcnow().isoformat()
        for d in discovered:
            name = d["name"]
            glob = landing_zone.glob_for_dir(d["path"], d.get("latest_file"))
            key = _dataset_key(source_id, name)
            if key in store:
                store[key].update({"dir_path": d["path"], "landing_glob": glob,
                                   "file_count": d["file_count"], "total_bytes": d["total_bytes"],
                                   "updated_at": now})
            else:
                store[key] = {"source_id": source_id, "dataset": name, "dir_path": d["path"],
                              "landing_glob": glob, "status": "new", "config_json": None,
                              "profile_json": None, "suggestion_json": None,
                              "last_test_status": "untested", "last_test_message": None,
                              "last_tested_at": None, "file_count": d["file_count"],
                              "total_bytes": d["total_bytes"], "updated_at": now}
        _write_local(_DATASETS_FILE, store)

    touch_source(source_id)
    return list_datasets(source_id)


@_db_retry
def _update_dataset(source_id: str, dataset: str, fields: dict[str, Any]) -> None:
    if _can_use_dbsql():
        sets = ", ".join([f"{k} = ?" for k in fields])
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets
                        SET {sets}, updated_at = current_timestamp()
                        WHERE source_id = ? AND dataset = ?""",
                    (*fields.values(), source_id, dataset),
                )
    else:
        store = _read_local(_DATASETS_FILE)
        key = _dataset_key(source_id, dataset)
        if key in store:
            store[key].update(fields)
            store[key]["updated_at"] = datetime.utcnow().isoformat()
            _write_local(_DATASETS_FILE, store)


def save_config(source_id: str, dataset: str, config: dict, *,
                quick_ok: bool, quick_message: str = "") -> None:
    """Persist a saved Auto Loader config. A passing quick (Python) parse check
    promotes the dataset to `configured`; a failing one keeps it `new`."""
    _update_dataset(source_id, dataset, {
        "config_json": json.dumps(config),
        "status": "configured" if quick_ok else "new",
        "last_test_status": "passed" if quick_ok else "failed",
        "last_test_message": quick_message[:900] if quick_message else None,
    })


def set_suggestion(source_id: str, dataset: str, suggestion: dict) -> None:
    _update_dataset(source_id, dataset, {"suggestion_json": json.dumps(suggestion)})


def set_profile(source_id: str, dataset: str, profile: dict) -> None:
    _update_dataset(source_id, dataset, {"profile_json": json.dumps(profile)})


@_db_retry
def set_test_result(source_id: str, dataset: str, *, passed: bool, message: str) -> None:
    """Record a deep-test result. On pass, promote to `ready`."""
    fields = {
        "last_test_status": "passed" if passed else "failed",
        "last_test_message": (message or "")[:900],
    }
    if _can_use_dbsql():
        sets = ", ".join([f"{k} = ?" for k in fields])
        status_sql = "'ready'" if passed else "status"
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {_CATALOG}.{_CONTROL_SCHEMA}.autoloader_datasets
                        SET {sets}, status = {status_sql}, last_tested_at = current_timestamp(),
                            updated_at = current_timestamp()
                        WHERE source_id = ? AND dataset = ?""",
                    (*fields.values(), source_id, dataset),
                )
    else:
        extra = {**fields, "last_tested_at": datetime.utcnow().isoformat()}
        if passed:
            extra["status"] = "ready"
        _update_dataset(source_id, dataset, extra)


def mark_ready(source_id: str, dataset: str, config: dict, message: str = "seeded") -> None:
    """Force a dataset to `ready` with a known-good config (used by seeding)."""
    _update_dataset(source_id, dataset, {
        "config_json": json.dumps(config), "status": "ready",
        "last_test_status": "passed", "last_test_message": message[:900],
    })


def seed_cfo_datasets() -> dict[str, Any]:
    """Deterministically onboard the Acme Corp ERP feeds as `ready`
    Auto Loader datasets so they appear in the New Request Data Browser — without
    any landing-volume upload or profiler round-trip (which is heavy and, on a
    Default-Storage workspace, can fail on volume creation). Mirrors how the FO /
    sample feeds onboard with their DECLARED config; the names + system line up
    with scenarios._cfo_scenario() so picking one fills the SOURCE section.

    Idempotent: re-registers the source (create_source deletes-then-inserts by id)
    and upserts each dataset row to `ready`. Best-effort — never raises."""
    from agent import cfo_sample_data as cfo
    try:
        create_source(cfo.SOURCE_NAME, cfo.LANDING_ROOT,
                       created_by="platform-team", source_id=cfo.SOURCE_ID)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"create_source: {e}"}
    sid = cfo.SOURCE_ID
    ready = 0
    for feed in cfo.FEEDS:
        name = feed["dataset"]
        cfg = cfo.autoloader_config(feed)
        glob = cfg["landing_path"]
        try:
            if _can_use_dbsql():
                # Ensure a row exists (INSERT if new), then mark it ready with
                # the declared config. fetch_dataset avoids a duplicate insert.
                if not fetch_dataset(sid, name):
                    with _conn() as conn:
                        with conn.cursor() as cur:
                            _insert_dataset_row(cur, sid, name, dir_path=cfo.LANDING_ROOT,
                                                landing_glob=glob, status="new",
                                                file_count=1, total_bytes=4096)
            else:
                store = _read_local(_DATASETS_FILE)
                key = _dataset_key(sid, name)
                if key not in store:
                    store[key] = {"source_id": sid, "dataset": name,
                                  "dir_path": cfo.LANDING_ROOT, "landing_glob": glob,
                                  "status": "new", "config_json": None,
                                  "file_count": 1, "total_bytes": 4096,
                                  "updated_at": datetime.utcnow().isoformat()}
                    _write_local(_DATASETS_FILE, store)
            mark_ready(sid, name, cfg, message="seeded (Acme Corp ERP feed)")
            ready += 1
        except Exception as e:  # noqa: BLE001
            print(f"[ops] cfo onboard {name} skipped: {e}", flush=True)
    try:
        touch_source(sid)
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, "source_id": sid, "ready": ready,
            "datasets": [f["dataset"] for f in cfo.FEEDS]}


# --- build-flow feed --------------------------------------------------------

def ready_datasets() -> list[dict]:
    """All `ready` datasets across every source — feeds the New Request picker
    and the intake agent. Each item carries its saved config."""
    out: list[dict] = []
    for src in list_sources():
        for ds in list_datasets(src["source_id"]):
            if ds.get("status") == "ready":
                out.append({**ds, "source_name": src.get("name"),
                            "source_volume_path": src.get("volume_path")})
    return out


def find_ready(dataset: str, landing_glob: Optional[str] = None) -> Optional[dict]:
    """Look up a `ready` dataset by name (and optionally matching glob) — used
    by source_profiler to prefer an onboarded config. Best-effort."""
    matches = [d for d in ready_datasets() if d.get("dataset") == dataset]
    if landing_glob:
        exact = [d for d in matches if (d.get("landing_glob") or "") == landing_glob]
        if exact:
            return exact[0]
    return matches[0] if matches else None


# --- demo seeding -----------------------------------------------------------

_DEFAULT_SOURCE_ID = "als-default"
_DEMO_READY_DATASETS = ("cust_comm_hist",)  # canonical Campaign example


def seed_default_source() -> dict:
    """Idempotently register ONE Auto Loader source over the default landing
    volume, **write + onboard the sample datasets** (so a fresh deploy can play
    instantly), mark the canonical Campaign demo dataset `ready`, and seed the
    customers Silver dimension for the join → Gold example.
    Best-effort — never raises (callable from event_store.seed_demo)."""
    from common import landing_zone
    from agent import sample_data, spike_data, fo_sample_data
    try:
        landing_zone.ensure_volume()
        root = landing_zone.volume_root()
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"volume: {e}"}

    sid = _DEFAULT_SOURCE_ID
    try:
        create_source("Landing Zone (default)", root, "platform-team", source_id=sid)
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"create: {e}"}

    # Write the sample landing files (idempotent — overwrite each deploy) so the
    # sample datasets exist to discover + onboard.
    uploaded = []
    for ds in sample_data.SAMPLE_DATASETS:
        try:
            landing_zone.upload_file(ds["dataset"], ds["filename"],
                                     sample_data.file_bytes_for(ds["dataset"]))
            uploaded.append(ds["dataset"])
        except Exception as e:  # noqa: BLE001
            print(f"[ops] sample upload {ds['dataset']} skipped: {e}", flush=True)

    # Perpetual FO pool tape — the flagship reskin dataset (single CSV).
    try:
        fo_ds = fo_sample_data.SAMPLE_POOL_TAPE
        landing_zone.upload_file(fo_ds["dataset"], fo_ds["filename"],
                                 fo_sample_data.pool_tape_file_bytes())
        uploaded.append(fo_ds["dataset"])
    except Exception as e:  # noqa: BLE001
        print(f"[ops] fo pool tape upload skipped: {e}", flush=True)

    # The FOUR Tech-Spike feeds. Without all four onboarded, the headline
    # multi-feed medallion scenario does not even render as a chip (see
    # scenarios._campaign_full_medallion_scenario), which is why that scenario
    # had never been run from a vanilla deploy. Synthetic + generated in code.
    for feed in spike_data.SPIKE_FEEDS:
        try:
            landing_zone.upload_file(feed["dataset"], feed["filename"],
                                     spike_data.file_bytes_for(feed["dataset"]))
            uploaded.append(feed["dataset"])
        except Exception as e:  # noqa: BLE001
            print(f"[ops] spike upload {feed['dataset']} skipped: {e}", flush=True)

    try:
        discovered = refresh_source(sid)
    except Exception as e:  # noqa: BLE001
        return {"ok": True, "source_id": sid, "datasets": 0,
                "warning": f"scan failed: {e}", "uploaded": uploaded}

    ready = 0
    sample_by_name = {d["dataset"]: d for d in sample_data.SAMPLE_DATASETS}
    for ds in discovered:
        name = ds.get("dataset")
        if name in spike_data.SPIKE_DATASETS:
            # Onboard with the DECLARED config rather than profiling: the `@$!~`
            # delimiter and which columns hold JSON are known facts here, and a
            # profiler mis-guess would leave the dataset `ready` with a config
            # that cannot parse the file.
            try:
                cfg = spike_data.autoloader_config(name, ds.get("landing_glob") or "")
                mark_ready(sid, name, cfg, message="seeded (Tech-Spike feed)")
                ready += 1
            except Exception as e:  # noqa: BLE001
                print(f"[ops] spike onboard {name} skipped: {e}", flush=True)
        elif name in sample_by_name:
            # Sample datasets: onboard with the DECLARED config (deterministic;
            # correct for the multi-char .dat without profiler guesswork).
            try:
                cfg = sample_data.autoloader_config(
                    sample_by_name[name], ds.get("landing_glob") or "")
                mark_ready(sid, name, cfg, message="seeded (sample dataset)")
                ready += 1
            except Exception as e:  # noqa: BLE001
                print(f"[ops] sample onboard {name} skipped: {e}", flush=True)
        elif name == fo_sample_data.SAMPLE_POOL_TAPE["dataset"]:
            # Perpetual FO pool tape: onboard with the DECLARED config (plain CSV).
            try:
                cfg = fo_sample_data.autoloader_config(ds.get("landing_glob") or "")
                mark_ready(sid, name, cfg, message="seeded (FO pool tape)")
                ready += 1
            except Exception as e:  # noqa: BLE001
                print(f"[ops] fo onboard {name} skipped: {e}", flush=True)
        elif name in _DEMO_READY_DATASETS:
            # Campaign cust_comm_hist: real demo feed, onboard via the profiler.
            try:
                from agent import autoloader_advisor as advisor
                prof = landing_zone.profile_latest_file(ds.get("dir_path") or "")
                if prof.get("error"):
                    continue
                cfg = advisor.config_from_profile(prof, name, ds.get("landing_glob"))
                set_profile(sid, name, prof)
                mark_ready(sid, name, cfg, message="seeded (profiled landing file)")
                ready += 1
            except Exception:  # noqa: BLE001
                pass

    # Seed the customers Silver dimension (join target for the Gold example).
    try:
        customers_dim = sample_data.seed_customers_dim()
    except Exception as e:  # noqa: BLE001
        customers_dim = {"ok": False, "error": str(e)[:200]}

    return {"ok": True, "source_id": sid, "datasets": len(discovered),
            "ready": ready, "uploaded": uploaded, "customers_dim": customers_dim}
