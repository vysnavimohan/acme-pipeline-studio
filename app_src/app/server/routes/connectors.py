"""Connectors — source onboarding API for both ingestion paths.

**Auto Loader (file)** sources point at a UC Volume path; their **datasets**
are the directories within it. Each dataset is onboarded through:

    new  --(save config; quick Python parse check)-->  configured
         --(deep test: real read_files on the warehouse)-->  ready

A `ready` dataset's saved config feeds the pipeline build flow (New Request +
intake + source_profiler). Everything there is thin glue over `autoloader_store`
(state), `landing_zone` (volume IO), `autoloader_advisor` (agent suggestion),
and `framework_deployer` (code preview + deep test) — so the preview and test
reuse the exact builders the deploy uses.

**Lakeflow Connect** sources (`/connections`, `/connect-sources`) attach an
EXISTING Unity Catalog connection to the generator. The Studio never creates
connections or handles their credentials; it lists what UC already holds,
browses the tables via federation, and records the attachment in
`connect_store` so requests can build from it.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from agent import autoloader_advisor as advisor
from agent import autoloader_store as store
from agent import connect_store
from agent.state import FileSource
from common import file_profiler, landing_zone, uc_connections
from tools import framework_deployer

router = APIRouter()


# --- helpers ----------------------------------------------------------------

def _fs_from_config(config: dict) -> FileSource:
    try:
        return FileSource(
            dataset=config.get("dataset") or "dataset",
            landing_path=config.get("landing_path") or "",
            file_format=config.get("file_format") or "text",
            delimiter=config.get("delimiter") or "",
            has_header=bool(config.get("has_header", True)),
            columns=config.get("columns") or [],
        )
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"invalid config: {str(e)[:200]}")


def _source_summary(src: dict) -> dict:
    ds = store.list_datasets(src["source_id"])
    return {**src, "dataset_count": len(ds),
            "ready_count": sum(1 for d in ds if d.get("status") == "ready"),
            "new_count": sum(1 for d in ds if d.get("status") == "new")}


def _require_source(source_id: str) -> dict:
    src = store.fetch_source(source_id)
    if not src:
        raise HTTPException(status_code=404, detail="unknown source")
    return src


def _require_dataset(source_id: str, dataset: str) -> dict:
    d = store.fetch_dataset(source_id, dataset)
    if not d:
        raise HTTPException(status_code=404, detail="unknown dataset")
    return d


def _quick_check(config: dict, dir_path: str) -> tuple[bool, str]:
    """Fast Python parse check: split a sample with the saved config and confirm
    the column count matches. The lightweight half of the 'both' test approach."""
    if not dir_path:
        return False, "no directory to validate against"
    try:
        files = landing_zone.list_files(dir_path)
    except Exception as e:  # noqa: BLE001
        return False, f"cannot list files: {str(e)[:160]}"
    if not files:
        return False, "no sample files in the dataset directory yet"
    try:
        text = file_profiler.read_volume_head(files[0]["path"], max_bytes=64 * 1024)
    except Exception as e:  # noqa: BLE001
        return False, f"cannot read sample file: {str(e)[:160]}"
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if config.get("has_header") and lines:
        lines = lines[1:]
    if not lines:
        return False, "no data rows after the header row"
    expected = len(config.get("columns") or [])
    delim = config.get("delimiter") or ""
    if not delim:
        ok = expected <= 1
        return ok, ("single-column config validated" if ok
                    else f"config expects {expected} columns but no delimiter is set")
    counts = [len(ln.split(delim)) for ln in lines[:50]]
    modal = max(set(counts), key=counts.count)
    frac = counts.count(modal) / len(counts)
    ok = (modal == expected) and frac >= 0.6
    return ok, (f"{modal} columns parsed in {frac:.0%} of sampled lines "
                f"(config expects {expected}).")


# --- sources ----------------------------------------------------------------

@router.get("/sources")
def list_sources():
    return {"sources": [_source_summary(s) for s in store.list_sources()],
            "default_volume": landing_zone.volume_fqn(),
            "default_volume_root": landing_zone.volume_root()}


class CreateSourceBody(BaseModel):
    name: Optional[str] = None
    volume_path: str
    created_by: Optional[str] = None


@router.post("/sources")
def create_source(body: CreateSourceBody):
    path = (body.volume_path or "").strip().rstrip("/")
    err = landing_zone.validate_volume_path(path)
    if err:
        raise HTTPException(status_code=400, detail=err)
    name = (body.name or "").strip() or path.rsplit("/", 1)[-1]
    src = store.create_source(name, path, body.created_by or "platform-team")
    sid = src["source_id"]
    try:
        datasets = store.refresh_source(sid)
    except Exception as e:  # noqa: BLE001
        return {"source": _source_summary(store.fetch_source(sid)), "datasets": [],
                "warning": f"source created but initial scan failed: {str(e)[:200]}"}
    return {"source": _source_summary(store.fetch_source(sid)), "datasets": datasets}


@router.delete("/sources/{source_id}")
def delete_source(source_id: str):
    store.delete_source(source_id)
    return {"ok": True}


@router.post("/sources/{source_id}/refresh")
def refresh_source(source_id: str):
    _require_source(source_id)
    try:
        datasets = store.refresh_source(source_id)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"refresh failed: {str(e)[:300]}")
    return {"source": _source_summary(store.fetch_source(source_id)), "datasets": datasets}


@router.get("/sources/{source_id}/datasets")
def list_datasets(source_id: str):
    src = _require_source(source_id)
    return {"source": _source_summary(src), "datasets": store.list_datasets(source_id)}


# --- a single dataset -------------------------------------------------------

@router.get("/datasets/{source_id}/{dataset}")
def dataset_detail(source_id: str, dataset: str):
    d = _require_dataset(source_id, dataset)
    try:
        d["files"] = landing_zone.list_files(d.get("dir_path") or "")
    except Exception:  # noqa: BLE001
        d["files"] = []
    return d


@router.post("/datasets/{source_id}/{dataset}/suggest")
def suggest(source_id: str, dataset: str):
    d = _require_dataset(source_id, dataset)
    try:
        profile = landing_zone.profile_latest_file(d.get("dir_path") or "")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"profiling failed: {str(e)[:300]}")
    out = advisor.suggest_config(profile, dataset, landing_glob=d.get("landing_glob"))
    if out.get("error"):
        raise HTTPException(status_code=400, detail=out["error"])
    store.set_profile(source_id, dataset, profile)
    store.set_suggestion(source_id, dataset, out)
    return out


class ConfigBody(BaseModel):
    config: Optional[dict] = None


@router.post("/datasets/{source_id}/{dataset}/preview")
def preview(source_id: str, dataset: str, body: ConfigBody):
    d = _require_dataset(source_id, dataset)
    config = body.config or d.get("config")
    if not config:
        raise HTTPException(status_code=400, detail="no config to preview — suggest or fill one first")
    return framework_deployer.render_auto_loader_code_preview(_fs_from_config(config))


@router.post("/datasets/{source_id}/{dataset}/config")
def save_config(source_id: str, dataset: str, body: ConfigBody):
    d = _require_dataset(source_id, dataset)
    config = dict(body.config or {})
    config.setdefault("dataset", dataset)
    config.setdefault("landing_path", d.get("landing_glob") or "")
    _fs_from_config(config)  # validate shape
    ok, msg = _quick_check(config, d.get("dir_path") or "")
    store.save_config(source_id, dataset, config, quick_ok=ok, quick_message=msg)
    return {"ok": ok, "message": msg, "dataset": store.fetch_dataset(source_id, dataset)}


@router.post("/datasets/{source_id}/{dataset}/test")
def deep_test(source_id: str, dataset: str, body: ConfigBody):
    d = _require_dataset(source_id, dataset)
    config = body.config or d.get("config")
    if not config:
        raise HTTPException(status_code=400, detail="save a config first")
    result = framework_deployer.test_config_via_read_files(_fs_from_config(config))
    passed = bool(result.get("ok"))
    # Persist the VALIDATED config on pass — so a dataset is always usable
    # downstream (its columns are stored) even if the user tested without first
    # clicking Save. Otherwise it can end up `ready` with an empty config.
    if passed and body.config:
        try:
            store.save_config(source_id, dataset, config, quick_ok=True,
                              quick_message="validated via deep test")
        except Exception:  # noqa: BLE001
            pass
    store.set_test_result(source_id, dataset, passed=passed,
                          message=result.get("message") or result.get("error") or "")
    result["dataset"] = store.fetch_dataset(source_id, dataset)
    return result


@router.post("/datasets/{source_id}/{dataset}/upload")
async def upload(source_id: str, dataset: str, file: UploadFile = File(...)):
    d = _require_dataset(source_id, dataset)
    content = await file.read()
    try:
        path = landing_zone.upload_to_dir(d.get("dir_path") or "", file.filename, content)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"upload failed: {str(e)[:300]}")
    return {"ok": True, "path": path}


# --- Lakeflow Connect: attach an existing UC connection ---------------------

@router.get("/connections")
def list_uc_connections(all_types: bool = False, internal: bool = False,
                        refresh: bool = False, q: Optional[str] = None):
    """Unity Catalog connections available as ingestion sources.

    Filtered to the types Lakeflow Connect can actually ingest from unless
    `all_types=1` — a metastore holds plenty of connections (HTTP/MCP servers,
    Drive, Slack) that are not ingestion sources. Auto-created `internal-<uuid>`
    Lakebase system connections are hidden unless `internal=1` (they outnumber
    the real sources 3:1 on a shared metastore). `q` narrows by name/host/type
    server-side, because a shared metastore can hold hundreds.
    """
    conns = uc_connections.list_connections(
        ingestible_only=not all_types, include_internal=internal, force=refresh)
    needle = (q or "").strip().lower()
    if needle:
        conns = [c for c in conns
                 if needle in c["name"].lower()
                 or needle in c["type_label"].lower()
                 or needle in (c["host"] or "").lower()]
    attached = {s.get("connection_name") for s in connect_store.list_sources()}
    return {"connections": [{**c, "attached": c["name"] in attached} for c in conns],
            "total": len(conns),
            "hidden_internal": 0 if internal else uc_connections.internal_count(
                ingestible_only=not all_types),
            "ingestible_types": uc_connections.INGESTIBLE_TYPES}


@router.get("/connections/{connection_name}/browse")
def browse_connection(connection_name: str, catalog: Optional[str] = None,
                      schema: Optional[str] = None):
    """Federated metadata for a connection: its catalogs, a catalog's schemas,
    or a schema's tables. `browsable: false` means no foreign catalog is bound,
    so the operator names the tables manually (ingestion still works)."""
    conn = uc_connections.get_connection(connection_name)
    if not conn or not conn.get("name"):
        raise HTTPException(status_code=404, detail="unknown UC connection")
    out = {"connection": conn, **uc_connections.browse(connection_name)}
    if catalog:
        out["catalog"] = catalog
        out["schemas"] = uc_connections.list_schemas(catalog)
    if catalog and schema:
        out["schema"] = schema
        out["tables"] = uc_connections.list_tables(catalog, schema)
    return out


class AttachConnectionBody(BaseModel):
    connection_name: str
    name: Optional[str] = None
    system_key: Optional[str] = None
    source_catalog: Optional[str] = None
    source_schema: Optional[str] = None
    browse_catalog: Optional[str] = None
    tables: list[str] = []
    created_by: Optional[str] = None


@router.get("/connect-sources")
def list_connect_sources():
    """Attached Lakeflow Connect sources — the left rail + the New Request picker."""
    return {"sources": connect_store.list_sources()}


@router.post("/connect-sources")
def attach_connect_source(body: AttachConnectionBody):
    conn = uc_connections.get_connection(body.connection_name)
    if not conn or not conn.get("name"):
        raise HTTPException(status_code=404, detail="unknown UC connection")
    if not conn.get("ingestible"):
        raise HTTPException(
            status_code=400,
            detail=(f"connection type {conn['connection_type'] or 'unknown'} is not a "
                    "Lakeflow Connect ingestion source"))
    tables = [t.strip() for t in (body.tables or []) if t and t.strip()]
    if not tables:
        raise HTTPException(status_code=400, detail="select at least one source table")
    name = (body.name or "").strip() or conn["name"]
    src = connect_store.attach(
        name, conn["name"],
        connection_type=conn["connection_type"],
        system_key=body.system_key or name,
        source_catalog=(body.source_catalog or "").strip(),
        source_schema=(body.source_schema or "").strip(),
        browse_catalog=(body.browse_catalog or "").strip(),
        tables=tables,
        created_by=body.created_by or "platform-team")
    return {"source": connect_store.fetch_source(src["source_id"]) or src}


class ConnectTablesBody(BaseModel):
    tables: list[str]


@router.post("/connect-sources/{source_id}/tables")
def set_connect_tables(source_id: str, body: ConnectTablesBody):
    if not connect_store.fetch_source(source_id):
        raise HTTPException(status_code=404, detail="unknown connect source")
    tables = [t.strip() for t in (body.tables or []) if t and t.strip()]
    if not tables:
        raise HTTPException(status_code=400, detail="keep at least one source table")
    connect_store.set_tables(source_id, tables)
    return {"source": connect_store.fetch_source(source_id)}


@router.delete("/connect-sources/{source_id}")
def detach_connect_source(source_id: str):
    """Detach from the generator. The UC connection itself is untouched."""
    connect_store.detach(source_id)
    return {"ok": True}


# --- built-in demo sources (Lakebase Postgres) -------------------------------

@router.get("/builtin-sources")
def builtin_sources(refresh: bool = False):
    """The demo database sources the DEPLOYMENT provisions and seeds.

    These are neither Auto Loader sources nor attached UC connections, so
    before this endpoint existed they had no representation on this page at all
    — the deploy created them, the data browser and the prefills used them, and
    the Connectors page showed nothing, which reads as "not connected".

    Deliberately derived from `source_catalog` (the same scan the data browser
    and the intake agent read) rather than from a hardcoded list: the previous
    version of this panel WAS hardcoded, and it drifted into describing systems
    that no longer existed. Anything wrong here is wrong for the build flow too,
    which is the property worth having.
    """
    from common import source_catalog, sources

    data = source_catalog.get(force=refresh)
    builtin_keys = {s["key"] for s in source_catalog.SYSTEMS}
    out = []
    for s in data.get("systems", []):
        if s.get("key") not in builtin_keys:
            continue  # attached Lakeflow Connect sources render in their own rail
        tables = s.get("tables") or []
        out.append({
            "key": s["key"],
            "label": s.get("label") or s["key"].upper(),
            "blurb": s.get("blurb") or "",
            "available": bool(s.get("available")),
            "error": s.get("error") or "",
            # A degraded scan (tables visible, no columns) is a PERMISSION
            # signal, not an empty source — surfaced so the page says so
            # instead of quietly showing zero columns. See KNOWN_ISSUES 6.
            "warning": s.get("warning") or "",
            "instance": f"{sources.LAKEBASE_INSTANCE_PREFIX}-{s['key']}",
            "schema": sources.physical_schema(s["key"]),
            "backend": ("external Postgres" if sources.external_pg_configured()
                        else "Lakebase (managed Postgres)"),
            "table_count": len(tables),
            "column_count": sum(len(t.get("columns") or []) for t in tables),
            "row_estimate": sum(max(t.get("approx_rows") or 0, 0) for t in tables),
            "tables": [{"name": t.get("name"), "short_name": t.get("short_name"),
                        "approx_rows": t.get("approx_rows"),
                        "column_count": len(t.get("columns") or []),
                        "columns": [c.get("name") for c in (t.get("columns") or [])]}
                       for t in tables],
        })
    return {"sources": out, "profiled_at": data.get("profiled_at")}


# --- build-flow feed --------------------------------------------------------

@router.get("/ready-datasets")
def ready_datasets():
    """All `ready` datasets across sources — feeds the New Request picker + intake."""
    return {"datasets": store.ready_datasets()}
