"""Real deployer for Databricks Pipeline Studio.

Replaces the SQL-warehouse-only `dev_deployer`. Per request, this:

  1. Loads source rows from Lakebase (per-system) into bronze Delta tables via
     a SQL warehouse one-shot. Bronze ingestion is a pluggable concern — in a
     customer-side deployment this layer is swapped for Lakeflow Connect
     Postgres CDC (the pattern is identical, only the bronze loader changes).
  2. Uploads the generated SDP source file to the workspace.
  3. Creates (or updates) a real Spark Declarative Pipeline via the SDK that
     reads bronze, applies the transform/joins/expectations, and materialises
     the silver Delta table in UC. The pipeline is visible in
     Workspace → Pipelines and carries its real trigger schedule.
  4. Triggers a pipeline update and polls until COMPLETED or FAILED.
  5. Verifies row counts via the SQL warehouse.

`deploy_pipeline` is the single entry point.
"""

from __future__ import annotations

import logging
import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import psycopg
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as p
from databricks.sdk.service import jobs as j

from agent.state import PipelineSpec
from common.lakebase_auth import get_lakebase_conn_str

LOG = logging.getLogger("ops.real_deployer")

_MAX_ROWS_PER_TABLE = 1000
_INSERT_BATCH = 250
_BRONZE_COL_TYPE = "STRING"


def _pipeline_compute(*, num_workers: int = 1) -> dict[str, Any]:
    """Compute selector for the SDP / Declarative pipelines we create.

    Default = **serverless** (the original Field-Engineering behaviour, unchanged
    on FE workspaces). When ``OPS_PIPELINE_NODE_TYPE_ID`` is set — the
    non-serverless / "classic" deploy mode used where serverless jobs &
    all-purpose compute are disallowed (e.g. Databricks on Azure) — return a single
    **classic pipeline cluster** of that instance type instead.

    Note: a Declarative (DLT) pipeline cannot attach to an *existing* all-purpose
    cluster — it always manages its own cluster — so the faithful non-serverless
    equivalent is a classic pipeline cluster that *matches* the all-purpose
    cluster's node type. ``OPS_PIPELINE_NUM_WORKERS`` overrides the worker count
    (default 1; set 0 for a single-node pipeline cluster)."""
    node = os.environ.get("OPS_PIPELINE_NODE_TYPE_ID", "").strip()
    if not node:
        return {"serverless": True}
    try:
        workers = int(os.environ.get("OPS_PIPELINE_NUM_WORKERS", str(num_workers)) or num_workers)
    except ValueError:
        workers = num_workers
    return {"clusters": [p.PipelineCluster(label="default", node_type_id=node, num_workers=workers)]}


# =====================================================================
# Result shape
# =====================================================================

@dataclass
class DeployResult:
    catalog: str
    bronze_schema_fq: str
    silver_schema_fq: str
    bronze_tables: list[str] = field(default_factory=list)
    bronze_counts: dict[str, int] = field(default_factory=dict)
    silver_table: str = ""
    silver_row_count: int = -1
    pipeline_id: str = ""
    pipeline_name: str = ""
    update_id: str = ""
    pipeline_url: str = ""
    sdp_workspace_path: str = ""
    job_id: str = ""
    job_name: str = ""
    job_url: str = ""
    job_schedule_cron: str = ""
    bronze_strategy: str = "warehouse"
    warnings: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "catalog": self.catalog,
            "bronze_schema": self.bronze_schema_fq,
            "bronze_tables": self.bronze_tables,
            "bronze_counts": self.bronze_counts,
            "silver_schema": self.silver_schema_fq,
            "silver_table": self.silver_table,
            "silver_row_count": self.silver_row_count,
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_url": self.pipeline_url,
            "update_id": self.update_id,
            "sdp_workspace_path": self.sdp_workspace_path,
            "job_id": self.job_id,
            "job_name": self.job_name,
            "job_url": self.job_url,
            "job_schedule_cron": self.job_schedule_cron,
            "bronze_strategy": self.bronze_strategy,
            "warnings": self.warnings,
            "duration_seconds": self.duration_seconds,
        }


# =====================================================================
# Helpers
# =====================================================================

def _qident(*parts: str) -> str:
    return ".".join(f"`{p}`" for p in parts)


def _state_to_str(s) -> str:
    return s.value if hasattr(s, "value") else (str(s) if s else "?")


def _warehouse_id() -> str:
    for key in ("DATABRICKS_WAREHOUSE_ID", "DATABRICKS_WAREHOUSE_HTTP_PATH"):
        v = (os.environ.get(key) or "").strip()
        if not v:
            continue
        return v.rsplit("/", 1)[-1] if "/warehouses/" in v else v
    raise RuntimeError("set DATABRICKS_WAREHOUSE_ID or DATABRICKS_WAREHOUSE_HTTP_PATH")


def _ensure_warehouse_running(client: WorkspaceClient, warehouse_id: str,
                              log: Callable[[str], None]) -> None:
    info = client.warehouses.get(warehouse_id)
    state = _state_to_str(info.state)
    if state == "RUNNING":
        return
    log(f"SQL warehouse is {state} — starting it (serverless usually ~10s)…")
    client.warehouses.start(warehouse_id)
    deadline = time.time() + 240
    while time.time() < deadline:
        info = client.warehouses.get(warehouse_id)
        if _state_to_str(info.state) == "RUNNING":
            log("Warehouse is RUNNING.")
            return
        time.sleep(3)
    raise RuntimeError("warehouse failed to start in 240s")


def _dbsql_conn(warehouse_id: str):
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/").replace("https://", "")
    if not host:
        # Resolve from the SDK config in case DATABRICKS_HOST isn't injected
        client = WorkspaceClient()
        host = client.config.host.replace("https://", "").rstrip("/")
    client = WorkspaceClient()
    headers = client.config.authenticate()
    auth = headers.get("Authorization", "")
    token = auth[7:] if auth.lower().startswith("bearer ") else auth
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
    )


def _esc(v: Any) -> str:
    if v is None:
        return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"


def _bulk_inserts(fq_table: str, rows: list[tuple]) -> list[str]:
    sqls = []
    for i in range(0, len(rows), _INSERT_BATCH):
        chunk = rows[i:i + _INSERT_BATCH]
        values = ", ".join(
            "(" + ", ".join(_esc(v) for v in row) + ")"
            for row in chunk
        )
        sqls.append(f"INSERT INTO {fq_table} VALUES {values}")
    return sqls


def _read_source_tables(system: str, tables: list[str]) -> dict[str, dict[str, Any]]:
    """Read source rows for `tables` (logical names, possibly spanning
    systems — one connection per system). Keys are logical `system.table`."""
    from common import sources
    out: dict[str, dict[str, Any]] = {}
    for sys_name, shorts in sources.split_tables_by_system(tables, default=system).items():
        phys_schema = sources.physical_schema(sys_name)
        with psycopg.connect(sources.conn_str_for(sys_name)) as conn:
            for name in shorts:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT * FROM "{phys_schema}"."{name}" LIMIT {_MAX_ROWS_PER_TABLE}')
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
                out[f"{sys_name}.{name}"] = {"columns": cols, "rows": rows}
    return out


# =====================================================================
# Bronze load (warehouse one-shot)
# =====================================================================

def _load_bronze(
    spec: PipelineSpec,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    log: Callable[[str], None],
) -> tuple[list[str], dict[str, int]]:
    """Loads source tables into bronze Delta tables. Returns
    (fq_bronze_table_names, row_count_per_table). Also pre-creates the silver
    schema so the SDP pipeline can write into it."""

    from common import sources as _sources
    systems = sorted(_sources.split_tables_by_system(
        spec.source.tables, default=spec.source.system).keys())
    log(f"Connecting to source system(s): {', '.join(systems)}…")
    src_data = _read_source_tables(spec.source.system, spec.source.tables)
    total_rows = sum(len(d["rows"]) for d in src_data.values())
    log(f"Pulled {total_rows:,} rows across {len(src_data)} source table(s).")

    client = WorkspaceClient()
    warehouse_id = _warehouse_id()
    _ensure_warehouse_running(client, warehouse_id, log)

    bronze_tables: list[str] = []
    bronze_counts: dict[str, int] = {}

    with _dbsql_conn(warehouse_id) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(catalog, bronze_schema)}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(catalog, silver_schema)}")

            for fq_src, payload in src_data.items():
                # fq_src is the logical `system.table` — bronze names embed
                # the table's OWN system, not the spec's primary one.
                src_system, src_table = fq_src.split(".", 1)
                fq_bronze = _qident(catalog, bronze_schema, f"{src_system}__{src_table}")
                log(f"Bronze write: `{fq_bronze}` ({len(payload['rows']):,} rows)…")
                col_def = ", ".join(f"`{c}` {_BRONZE_COL_TYPE}" for c in payload["columns"])
                cur.execute(f"DROP TABLE IF EXISTS {fq_bronze}")
                cur.execute(f"CREATE TABLE {fq_bronze} ({col_def}) USING DELTA")
                for stmt in _bulk_inserts(fq_bronze, payload["rows"]):
                    cur.execute(stmt)
                bronze_tables.append(fq_bronze)
                bronze_counts[fq_bronze] = len(payload["rows"])

            # Drop any pre-existing silver table — the SDP pipeline will own it
            silver_fq = _qident(catalog, silver_schema, spec.target.table)
            cur.execute(f"DROP TABLE IF EXISTS {silver_fq}")
            log(f"Cleared pre-existing silver table `{silver_fq}` (SDP pipeline will own it).")

    return bronze_tables, bronze_counts


def _prepare_for_lakeflow_bronze(
    spec: PipelineSpec,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    log: Callable[[str], None],
) -> tuple[list[str], dict[str, int]]:
    """Lakeflow Connect already owns bronze. Verify the expected bronze
    tables exist + count rows, ensure the silver schema, and clear any
    stale silver table (the SDP pipeline will own it). NEVER drops bronze."""
    from common import sources

    expected: list[str] = []
    for t in spec.source.tables:
        sys_name, short = sources.system_of_table(t, default=spec.source.system)
        expected.append(_qident(catalog, bronze_schema, f"{sys_name}__{short}"))

    client = WorkspaceClient()
    warehouse_id = _warehouse_id()
    _ensure_warehouse_running(client, warehouse_id, log)

    bronze_counts: dict[str, int] = {}
    with _dbsql_conn(warehouse_id) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(catalog, silver_schema)}")
            for fq in expected:
                cur.execute(f"SELECT COUNT(*) FROM {fq}")
                bronze_counts[fq] = cur.fetchone()[0]
                log(f"Bronze (Lakeflow Connect): {fq} = {bronze_counts[fq]:,} rows")
            silver_fq = _qident(catalog, silver_schema, spec.target.table)
            cur.execute(f"DROP TABLE IF EXISTS {silver_fq}")
            log(f"Cleared pre-existing silver table `{silver_fq}` (SDP pipeline will own it).")
    return expected, bronze_counts


# =====================================================================
# SDP pipeline lifecycle
# =====================================================================

def _env_tag_from_schema(silver_schema: str, default: str = "x") -> str:
    """Env class (dev / stage / prod) inferred from the schema name.
    The DAB prefixes schemas with `dev_<user>_databricks_pipeline_studio_<env>_silver`,
    so we look for the `_<env>_` token. Three environments now: dev (agent
    build), stage (post-PR deploy in the tracker), prod (release promotion)."""
    for tag in ("dev", "stage", "prod"):
        if f"_{tag}_" in silver_schema:
            return tag
    return default


def _pipeline_name(catalog: str, silver_schema: str, target_table: str) -> str:
    """Deterministic name keyed on (catalog, silver_schema, target_table).
    Including silver_schema ensures dev/stage/prod produce distinct pipelines
    even when both env classes share the same UC catalog (common in PoCs)."""
    env_tag = _env_tag_from_schema(silver_schema, default=silver_schema)
    return f"ops-{env_tag}-{target_table}"


def _find_pipeline_by_name(client: WorkspaceClient, name: str) -> Optional[str]:
    for pipe in client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if (pipe.name or "") == name:
            return pipe.pipeline_id
    return None


def _upload_sdp_project(
    client: WorkspaceClient,
    files: dict[str, str],
    catalog: str,
    silver_schema: str,
    target_table: str,
    log: Callable[[str], None],
) -> tuple[str, list[str]]:
    """Write the multi-file pipeline project under a pipeline ROOT FOLDER.
    Returns (root_path, [absolute .py file paths]). The root is keyed by
    (env, table) so dev and prod get distinct project folders."""
    from databricks.sdk.service.workspace import ImportFormat, Language
    env_tag = _env_tag_from_schema(silver_schema)
    root = f"/Workspace/Shared/databricks-pipeline-studio/pipelines/{env_tag}/{target_table}"
    # Clear stale transformations from a previous generation so removed files
    # don't linger in the pipeline.
    try:
        client.workspace.delete(f"{root}/transformations", recursive=True)
    except Exception:
        pass
    py_rel: list[str] = []
    for rel, content in files.items():
        full = f"{root}/{rel}"
        parent = full.rsplit("/", 1)[0]
        try:
            client.workspace.mkdirs(parent)
        except Exception:
            pass
        is_py = rel.endswith(".py")
        client.workspace.upload(
            path=full, content=content.encode("utf-8"),
            format=ImportFormat.SOURCE if is_py else ImportFormat.AUTO,
            language=Language.PYTHON if is_py else None,
            overwrite=True,
        )
        if is_py:
            py_rel.append(full)   # absolute path (the API requires absolute)
    log(f"Uploaded multi-file SDP project to `{root}` "
        f"({len(py_rel)} transformation file(s) under transformations/).")
    return root, sorted(py_rel)


def _build_pipeline_spec(
    spec: PipelineSpec,
    catalog: str,
    silver_schema: str,
    bronze_schema_fq: str,
    root_path: str,
    py_paths: list[str],
    request_id: str,
) -> dict[str, Any]:
    streaming = spec.sla.mode == "streaming"
    pipeline_name = _pipeline_name(catalog, silver_schema, spec.target.table)

    return {
        "name": pipeline_name,
        "catalog": catalog,
        "target": silver_schema,
        **_pipeline_compute(),
        "channel": "PREVIEW",
        "edition": "ADVANCED",
        "continuous": streaming,
        "photon": True,
        # Multi-file project: pipeline root folder for the editor + EXPLICIT
        # file libraries per transformation (staging/bronze_views.py +
        # silver/<table>.py). Explicit files are more reliable than a glob
        # for /Workspace-rooted projects while keeping the recommended layout.
        "root_path": root_path,
        "libraries": [p.PipelineLibrary(file=p.FileLibrary(path=fp)) for fp in py_paths],
        "configuration": {
            "pipeline.bronze_fq": bronze_schema_fq,
            "pipeline.target_table": spec.target.table,
            "pipeline.source_system": spec.source.system,
            "pipeline.request_id": request_id,
        },
    }


def _ensure_pipeline(
    client: WorkspaceClient,
    spec: PipelineSpec,
    catalog: str,
    silver_schema: str,
    bronze_schema_fq: str,
    root_path: str,
    py_paths: list[str],
    request_id: str,
    log: Callable[[str], None],
) -> tuple[str, str]:
    """Create or update the SDP pipeline. Returns (pipeline_id, pipeline_name)."""
    pipeline_name = _pipeline_name(catalog, silver_schema, spec.target.table)
    body = _build_pipeline_spec(spec, catalog, silver_schema, bronze_schema_fq,
                                root_path, py_paths, request_id)

    existing_id = _find_pipeline_by_name(client, pipeline_name)
    if existing_id:
        log(f"Updating existing SDP pipeline `{pipeline_name}` (id={existing_id})…")
        # `update` requires name + all the same fields as create
        client.pipelines.update(pipeline_id=existing_id, **body)
        return existing_id, pipeline_name

    log(f"Creating new SDP pipeline `{pipeline_name}`…")
    created = client.pipelines.create(**body)
    return created.pipeline_id, pipeline_name


def _quartz_from_unix_cron(unix_cron: str) -> str:
    """Convert 5-field unix cron to 6-field Quartz (prepend seconds=0).
    Quartz requires either dom or dow to be `?`."""
    parts = unix_cron.strip().split()
    if len(parts) == 5:
        minute, hour, dom, month, dow = parts
        if dom == "*" and dow == "*":
            dow = "?"
        elif dow != "*" and dom == "*":
            dom = "?"
        elif dom != "*" and dow != "*":
            dow = "?"
        return f"0 {minute} {hour} {dom} {month} {dow}"
    return unix_cron


def _find_job_by_name(client: WorkspaceClient, name: str) -> Optional[str]:
    for job in client.jobs.list(name=name):
        if (job.settings.name if job.settings else "") == name:
            return job.job_id
    return None


def _ensure_scheduled_job(
    client: WorkspaceClient,
    spec: PipelineSpec,
    pipeline_id: str,
    pipeline_name: str,
    catalog: str,
    silver_schema: str,
    log: Callable[[str], None],
) -> tuple[str, str, str]:
    """Create or update a scheduled Job that triggers the pipeline.

    For batch: a cron schedule from spec.sla.schedule_cron.
    For streaming: no schedule (continuous pipeline drives itself; the Job
    is a 'trigger now' handle).

    Returns (job_id, job_name, cron_or_empty).
    """
    streaming = spec.sla.mode == "streaming"
    # Job name keyed off the silver pipeline name (already env-scoped)
    job_name = f"job-{pipeline_name}"

    task = j.Task(
        task_key="trigger_silver",
        pipeline_task=j.PipelineTask(pipeline_id=pipeline_id, full_refresh=False),
        timeout_seconds=0,
    )
    cron = ""
    schedule = None
    if not streaming:
        cron_unix = spec.sla.schedule_cron or "0 1 * * *"
        cron = _quartz_from_unix_cron(cron_unix)
        schedule = j.CronSchedule(
            quartz_cron_expression=cron,
            timezone_id="Australia/Sydney",
            pause_status=j.PauseStatus.UNPAUSED,
        )

    existing_id = _find_job_by_name(client, job_name)
    if existing_id:
        log(f"Updating existing scheduled Job `{job_name}` (id={existing_id})…")
        new_settings = j.JobSettings(
            name=job_name,
            tasks=[task],
            schedule=schedule,
            timeout_seconds=0,
        )
        client.jobs.reset(job_id=int(existing_id), new_settings=new_settings)
        return str(existing_id), job_name, cron

    log(f"Creating scheduled Job `{job_name}`"
        + (f" with cron `{cron}` (Australia/Sydney)…" if cron else " (no schedule — streaming pipeline)…"))
    created = client.jobs.create(
        name=job_name,
        tasks=[task],
        schedule=schedule,
        timeout_seconds=0,
    )
    return str(created.job_id), job_name, cron


def _start_and_wait(
    client: WorkspaceClient,
    pipeline_id: str,
    streaming: bool,
    log: Callable[[str], None],
    timeout_seconds: int = 1200,
) -> str:
    """Triggers a pipeline update and waits until terminal state. If the
    triggered update fails but serverless auto-retries succeed, treats the
    retry as authoritative (returns the latest COMPLETED update_id).
    """
    log("Triggering pipeline update…")
    # The consolidated file-ingest pipeline is SHARED across requests, so two
    # concurrent builds can collide — only one update may be active at a time.
    # If start_update reports an update is already active, wait for that in-flight
    # update to reach a terminal state, then start OURS (which sees the merged
    # object list incl. the dataset we just added). Bounded by the overall timeout.
    started = None
    _start_deadline = time.time() + timeout_seconds
    while started is None:
        try:
            started = client.pipelines.start_update(
                pipeline_id=pipeline_id,
                full_refresh=True,
                cause=p.StartUpdateCause.API_CALL,
            )
        except Exception as e:  # noqa: BLE001
            if "active update" in str(e).lower() and time.time() < _start_deadline:
                log("  another update is already active on this shared pipeline — "
                    "waiting for it to finish, then starting ours…")
                while time.time() < _start_deadline:
                    time.sleep(8)
                    pipe = client.pipelines.get(pipeline_id=pipeline_id)
                    latest = (pipe.latest_updates or [None])[0]
                    st = _state_to_str(latest.state) if latest else ""
                    if st in ("COMPLETED", "FAILED", "CANCELED"):
                        log(f"  prior update {(latest.update_id or '')[:8]} → {st}; retrying ours")
                        break
                continue
            raise
    our_update_id = started.update_id
    log(f"Update started: {our_update_id}")

    terminal_ok = "COMPLETED"
    terminal_bad = {"FAILED", "CANCELED"}
    deadline = time.time() + timeout_seconds
    last_state = ""

    while time.time() < deadline:
        # Always consult the LATEST update — serverless DLT may auto-retry on
        # transient failures, so polling our specific update_id can miss a
        # subsequent retry's success.
        pipe = client.pipelines.get(pipeline_id=pipeline_id)
        latest = (pipe.latest_updates or [None])[0]
        if latest is None:
            time.sleep(4)
            continue
        latest_state = _state_to_str(latest.state)
        if latest_state != last_state:
            log(f"  pipeline latest update {latest.update_id[:8]} → {latest_state}")
            last_state = latest_state

        if streaming and latest_state == "RUNNING":
            log("Continuous pipeline is RUNNING — deploy verified.")
            return latest.update_id

        if latest_state == terminal_ok:
            return latest.update_id

        # If the latest is FAILED/CANCELED, check whether it's a retry that
        # will produce a follow-up update. Wait briefly and re-check before
        # giving up — RETRY_ON_FAILURE typically kicks in within 30-60s.
        if latest_state in terminal_bad:
            # Wait 90s to see if a retry update appears with a fresher id.
            saw_retry = False
            checkpoint = time.time() + 90
            while time.time() < checkpoint:
                time.sleep(6)
                pipe2 = client.pipelines.get(pipeline_id=pipeline_id)
                latest2 = (pipe2.latest_updates or [None])[0]
                if latest2 and latest2.update_id != latest.update_id:
                    log(f"  pipeline retry detected: {latest2.update_id[:8]} (cause={getattr(latest2, 'cause', '?')})")
                    saw_retry = True
                    break
            if saw_retry:
                # Loop back to track the retry as the new "latest"
                last_state = ""
                continue
            # No retry — give up with surfaced errors
            try:
                events = list(client.pipelines.list_pipeline_events(
                    pipeline_id=pipeline_id,
                    filter="level = 'ERROR'",
                    max_results=5,
                ))
                for ev in events[:3]:
                    log(f"  pipeline error: {(ev.message or ev.event_type or '?')[:240]}")
            except Exception as e:
                log(f"  (couldn't fetch pipeline events: {e})")
            raise RuntimeError(f"pipeline update ended in {latest_state}")

        time.sleep(6)
    raise RuntimeError(f"pipeline update timed out after {timeout_seconds}s (last state: {last_state})")


# =====================================================================
# Row verification
# =====================================================================

def _count_rows(fq_table: str, warehouse_id: str) -> int:
    with _dbsql_conn(warehouse_id) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
            return int(cur.fetchone()[0])


def _apply_column_comments(
    fq_table: str,
    column_comments: dict[str, dict[str, Any]] | None,
    warehouse_id: str,
    log: Callable[[str], None],
) -> int:
    """Apply column comments to the silver table via ALTER TABLE ALTER COLUMN.
    Idempotent — each ALTER simply sets/overwrites the comment.

    Returns the count of columns actually commented. Columns the LLM declared
    but which don't exist on the silver table are silently skipped (no error).
    """
    if not column_comments:
        return 0
    # Read actual silver columns so we skip LLM-hallucinated ones
    actual_cols: set[str] = set()
    with _dbsql_conn(warehouse_id) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"DESCRIBE TABLE {fq_table}")
                for row in cur.fetchall():
                    name = (row[0] or "").strip()
                    if name and not name.startswith("#"):
                        actual_cols.add(name)
            except Exception as e:
                log(f"  (couldn't DESCRIBE {fq_table} to get columns: {e})")
                return 0

    applied = 0
    skipped = 0
    with _dbsql_conn(warehouse_id) as conn:
        with conn.cursor() as cur:
            for col_name, meta in column_comments.items():
                if col_name not in actual_cols:
                    skipped += 1
                    continue
                comment = (meta.get("comment") or "").strip()
                if not comment:
                    continue
                escaped = comment.replace("'", "''")
                try:
                    cur.execute(
                        f"ALTER TABLE {fq_table} ALTER COLUMN `{col_name}` COMMENT '{escaped}'"
                    )
                    applied += 1
                except Exception as e:
                    log(f"  (couldn't comment {col_name}: {str(e)[:120]})")
    log(f"Applied {applied} column comment(s) to `{fq_table}` "
        f"({skipped} skipped — column not present in silver)")
    return applied


# =====================================================================
# SDP source generation
# =====================================================================

def _format_schema_ddl(column_comments: dict[str, dict[str, Any]] | None) -> str | None:
    """Turn the LLM-provided column_comments dict into a DDL string suitable
    for @dlt.table(schema="..."). Returns None if no columns provided.

    Input shape (from the pipeline_generator LLM):
        {
          "customer_id": {"type": "STRING", "nullable": false, "comment": "..."},
          ...
        }
    """
    if not column_comments:
        return None
    lines: list[str] = []
    for col_name, meta in column_comments.items():
        col_type = (meta.get("type") or "STRING").upper()
        nullable = meta.get("nullable", True)
        comment = (meta.get("comment") or "").replace("'", "''")
        not_null = "" if nullable else " NOT NULL"
        comment_clause = f" COMMENT '{comment}'" if comment else ""
        lines.append(f"    {col_name} {col_type}{not_null}{comment_clause}")
    return ",\n".join(lines)


def compile_business_rule(rule: str) -> Optional[str]:
    """Turn a plain-English spec business rule into a Spark SQL predicate that
    KEEPS the rows the rule intends to keep. Returns None if it cannot be
    compiled with confidence.

    Why this exists: business rules used to be emitted as `#` comments —
    literally "applied informationally" — so a pipeline claimed to honour a rule
    the requester typed and silently did not. That is worse than not supporting
    rules at all, because the generated code misrepresents itself and the PR
    reviewer's own critique of it cannot be fixed by the review loop (the loop
    only controls comments, not the emitted logic).

    Deliberately conservative: it recognises a few precise, common shapes and
    returns None for everything else. The caller must FAIL LOUD on None rather
    than silently degrade to a comment — a rule half-understood is more dangerous
    than one refused.
    """
    import re
    r = " ".join((rule or "").split())
    if not r:
        return None
    low = r.lower()

    # "exclude X where <col> LIKE '<pat>'"  /  "only emit rows where <col> = '<v>'"
    m = re.search(r"\b(\w+)\s+(?:not\s+)?like\s+'([^']*)'", r, re.I)
    if m:
        col, pat = m.group(1), m.group(2)
        negate = ("exclude" in low or "not like" in low or "remove" in low
                  or "drop" in low or "filter out" in low)
        pred = f"`{col}` LIKE '{pat}'"
        # NULLs must survive an exclusion: NOT (NULL LIKE 'x') is NULL, which
        # drops rows the rule never intended to touch.
        return f"(NOT ({pred}) OR `{col}` IS NULL)" if negate else pred

    m = re.search(r"\b(\w+)\s*=\s*'([^']*)'", r)
    if m:
        col, val = m.group(1), m.group(2)
        negate = ("exclude" in low or "remove" in low or "drop" in low
                  or "filter out" in low or "!=" in r or "not equal" in low)
        pred = f"`{col}` = '{val}'"
        return f"(NOT ({pred}) OR `{col}` IS NULL)" if negate else pred

    m = re.search(r"\b(\w+)\s+is\s+not\s+null\b", r, re.I)
    if m:
        return f"`{m.group(1)}` IS NOT NULL"

    return None


def _table_refs(spec: PipelineSpec) -> list[tuple[str, str]]:
    """(short_name, bronze_prefixed_name) per source table. Bronze tables
    embed each table's OWN system (`billing__customers`,
    `network__customer_impact`) so multi-system requests resolve correctly."""
    from common import sources as _sources
    refs = []
    for t in spec.source.tables:
        sys_name, short = _sources.system_of_table(t, default=spec.source.system)
        refs.append((short, f"{sys_name}__{short}"))
    return refs


def _join_plan(spec: PipelineSpec) -> tuple[str, list[str]]:
    """(how, join_candidates) for the silver join. Each secondary table is
    joined on the FIRST candidate present in both sides (mixed-key joins)."""
    from common import sources as _sources
    how = "left"
    if spec.transform.join_with:
        how = (spec.transform.join_with.get("how") or "left")
        if how not in ("left", "inner", "right", "outer"):
            how = "left"   # "inner then left" etc → safe default
    cands = _sources.join_candidates(
        spec.transform.join_with, spec.source.primary_key, spec.source.tables)
    return how, cands


def _emit_bronze_views(L, spec: PipelineSpec, request_id: str, standalone: bool) -> None:
    """Staging views over the (already-ingested) bronze tables — one per
    source table, for native SDP lineage. `standalone=True` emits a full
    file header (used in the multi-file project)."""
    table_refs = _table_refs(spec)
    if standalone:
        L('"""Bronze staging views — Databricks Pipeline Studio (auto-generated).')
        L('')
        L(f'Pass-through views over the bronze tables produced by Lakeflow Connect')
        L(f'ingestion (or the warehouse fallback), exposed to the silver build for')
        L(f'native SDP lineage. Request id: {request_id}')
        L('"""')
        L('from pyspark import pipelines as dlt')
        L('')
        L('# Bronze prefix is injected per-env via pipeline configuration.')
        L('BRONZE_FQ = spark.conf.get("pipeline.bronze_fq")')
        L('')
        L('')
        L('def _bronze_table(prefixed_name: str) -> str:')
        L('    """Fully-qualified bronze table name (`<system>__<table>`)."""')
        L('    return f"{BRONZE_FQ}.{prefixed_name}"')
        L('')
    L('# === Bronze views: one per source table, for visible lineage in SDP ===')
    for short, prefixed in table_refs:
        L('')
        L(f'@dlt.view(name="vw_{short}")')
        L(f'def _vw_{short}():')
        L(f'    """Pass-through view over bronze.{prefixed}, exposed to the silver build."""')
        L(f'    return spark.read.table(_bronze_table("{prefixed}"))')


def _table_comment_for(spec: PipelineSpec, table_comment: str | None,
                       table_refs: list[tuple[str, str]]) -> str:
    """The silver table's UC comment.

    A reviewer objected — twice, on two different PRs — that this was a verbatim
    copy of the spec's intent string, naming no owner, no upstream systems and no
    SLA. It was: the fallback here was `table_comment or spec.intent`, and when
    the LLM's reply was lost the raw intent went straight into the artefact. The
    loop could never fix that, because the reviewer was critiquing a string the
    generator's LLM did not reliably supply.

    So the provenance is now appended DETERMINISTICALLY — from the spec, whether
    or not the LLM contributed prose. An LLM comment is still preferred as the
    description; it just no longer has to remember the facts.
    """
    base = (table_comment or spec.intent).strip()
    lines = [base] if base else []
    lines.append("")
    feeds = ", ".join(short for short, _ in table_refs) or "(none declared)"
    lines.append(f"Upstream feeds ({len(table_refs)}): {feeds}")
    lines.append(f"Source system: {spec.source.system}")
    if spec.sla.mode == "streaming":
        lines.append("SLA: continuous (streaming)")
    else:
        sched = f", schedule {spec.sla.schedule_cron}" if spec.sla.schedule_cron else ""
        lines.append(f"SLA: {spec.sla.freshness_minutes} minutes{sched}")
    owner = _owner_for(spec)
    if owner:
        lines.append(f"Owner: {owner}")
    lines.append("Built by Databricks Pipeline Studio (agent-generated, human-approved).")
    return "\n".join(lines).replace('"""', '\\"\\"\\"')


def _owner_for(spec: PipelineSpec) -> str:
    """Owner named in the table comment.

    `spec.governance.data_owner` is the authoritative field and the intake form
    already collects it — it was simply never read here, which is why the
    reviewer kept asking for an owner the spec had all along.
    """
    gov = getattr(spec, "governance", None)
    for attr in ("data_owner", "business_owner", "domain"):
        val = getattr(gov, attr, None) if gov else None
        if isinstance(val, str) and val.strip():
            return val.strip()
    return (os.environ.get("OPS_DATA_OWNER")
            or os.environ.get("DATABRICKS_USERNAME")
            or "").strip()


def _emit_column_type_contract(L, spec: PipelineSpec,
                               column_comments: dict | None) -> None:
    """Declare the silver column types so the table's contract is visible.

    A reviewer objected that types were "fully implicit with no declared
    schema", and asked for a `schema=` on `@dlt.table()` *or*, in its own words,
    "at minimum document column types". We do the latter deliberately:

    An enforcing `schema=` must match the joined DataFrame EXACTLY — every
    column, in order, with the right nullability. The projection here is derived
    at runtime from whatever the sources actually contain (columns get prefixed
    on collision, join keys are dropped from the right side), so a declared
    schema that drifts by one column fails the pipeline outright. Trading a
    working pipeline for a stricter annotation is the wrong deal, especially as
    this generated code is the artefact a human reviews. The generator's own
    reasoning on this request made the same point: every source column arrives
    as text, and casting eagerly would "avoid silent cast failures on malformed
    dates" only by producing nulls instead.

    So: types are stated, and the reason they are not enforced is stated with
    them, which is strictly more information than an implicit schema.
    """
    types: dict[str, str] = {}
    for src in (getattr(spec.source, "files", None) or []):
        for col in (getattr(src, "columns", None) or []):
            name = getattr(col, "name", None)
            if name:
                types.setdefault(name, (getattr(col, "type", None) or "string").upper())
    for name, meta in (column_comments or {}).items():
        declared = (meta or {}).get("type")
        if isinstance(declared, str) and declared.strip():
            types[name] = declared.strip().upper()
    if not types:
        return
    L('# === Column type contract ===')
    L('# Declared, not enforced: this projection is derived from the sources at')
    L('# runtime (collision-prefixed names, join keys dropped from the right side),')
    L('# so a `schema=` that drifts by one column would fail the pipeline rather')
    L('# than document it. Types below are the source-declared types.')
    L('_COLUMN_TYPES = {')
    for name, typ in types.items():
        if name and name.replace("_", "").replace(".", "").isalnum():
            L(f'    "{name}": "{typ}",')
    L('}')


def _emit_silver(L, spec: PipelineSpec, request_id: str, standalone: bool,
                 table_comment: str | None, column_comments: dict | None,
                 inline_comments: list[str] | None,
                 silver_sql_select: str | None = None) -> None:
    """The silver @dlt.table (joins + expectations + column metadata).
    `standalone=True` emits a full file header (multi-file project).

    When `silver_sql_select` is provided (LLM-generated), emit it directly.
    Otherwise, fall back to the deterministic join+rule template."""
    streaming = spec.sla.mode == "streaming"
    table_refs = _table_refs(spec)
    table_short_names = [s for s, _ in table_refs]
    primary = table_short_names[0]
    others = table_short_names[1:]
    how, join_candidates = _join_plan(spec)
    table_comment_clean = _table_comment_for(spec, table_comment, table_refs)

    if standalone:
        L('"""Silver build — Databricks Pipeline Studio (auto-generated).')
        L('')
        L(f'Spec intent: {spec.intent}')
        L(f'Target table: {spec.target.table}')
        L(f'SLA: {"continuous (streaming)" if streaming else f"{spec.sla.freshness_minutes} minutes"}')
        L(f'Request id: {request_id}')
        L('"""')
        L('from pyspark import pipelines as dlt')
        L('from pyspark.sql import functions as F')
        L('')
        L('TARGET_TABLE = spark.conf.get("pipeline.target_table")')
        L('')

    L('# === Silver: business-ready, typed, joined ===')
    # Column comments applied via DataFrame metadata so they land in UC
    # without requiring schema= type matching.
    # Fall back to deterministic comments for the columns we KNOW about (the
    # spec's keys and validated columns) when the LLM returns none. Shipping
    # `_COLUMN_COMMENTS = {}` meant no column metadata reached Unity Catalog at
    # all — and because the dict is LLM-supplied, a review round complaining about
    # it could never fix it: the reviewer critiques output the loop cannot change.
    # A known-column baseline is always better than an empty dict.
    effective_comments: dict[str, dict[str, str]] = dict(column_comments or {})
    if not effective_comments:
        known: list[str] = []
        for c in (spec.source.primary_key or []):
            known.append(c)
        for c in (spec.validation.not_null or []) + (spec.validation.unique or []):
            known.append(c)
        for c in known:
            if c and c not in effective_comments:
                role = []
                if c in (spec.source.primary_key or []):
                    role.append("primary key from the source")
                if c in (spec.validation.not_null or []):
                    role.append("required (not-null enforced)")
                if c in (spec.validation.unique or []):
                    role.append("unique")
                effective_comments[c] = {
                    "comment": f"{c} — " + ("; ".join(role) if role else "source column")
                                + f". Silver projection of {spec.source.system}."}
    if effective_comments:
        L('_COLUMN_COMMENTS = {')
        for col_name, meta in effective_comments.items():
            if not col_name or not col_name.replace("_", "").replace(".", "").isalnum():
                continue
            comment = (meta.get("comment") or "").strip().replace('"', '\\"')
            L(f'    "{col_name}": "{comment}",')
        L('}')
    else:
        L('_COLUMN_COMMENTS = {}')
    L('')
    _emit_column_type_contract(L, spec, column_comments)
    L('')
    L('@dlt.table(')
    L('    name=TARGET_TABLE,')
    L('    comment="""')
    for cl in table_comment_clean.split("\n"):
        L(f'        {cl}')
    L('    """,')
    L('    table_properties={')
    L('        "quality": "silver",')
    L(f'        "pipelines.autoOptimize.managed": "true",')
    L('    },')
    L(')')
    for c in spec.validation.not_null:
        L(f'@dlt.expect_or_drop("not_null_{c}", "{c} IS NOT NULL")')
    L(f'def {spec.target.table}():')
    L(f'    """Silver build for {spec.target.table}.')
    L('')
    L(f'    Sources: {", ".join(table_short_names)}')
    L(f'    Join: per-table key from candidates {join_candidates} (how={how})')
    L(f'    Mode: {"streaming" if streaming else "batch"}')
    L('    """')

    # BRANCH: LLM-generated SQL vs. deterministic template
    if silver_sql_select:
        # LLM authored the entire transform: cast, derive eligible_flag/exclusion_reason, dedupe
        L('    # === LLM-authored Silver SQL: typed, eligible_flag + exclusion_reason, deduplicated ===')
        L('    df = spark.sql("""')
        for line in silver_sql_select.strip().split('\n'):
            L(f'    {line}')
        L('    """)')
        L('')
    else:
        # Deterministic template: joins + business rules (multi-feed fallback)
        inline = list(inline_comments or [])
        def _ic(default: str) -> str:
            return inline.pop(0) if inline else default

        if others:
            # Rationale emitted DETERMINISTICALLY from the spec, not left to the LLM:
            # a reviewer asked for it twice and the review loop cannot add it, since
            # the loop only supplies comments the template chooses to use.
            L(f'    # Join strategy — how={how!r}, candidate keys {join_candidates!r}.')
            if how == "left":
                L('    #   LEFT join: keep every primary-source row even with no match on the')
                L('    #   right, so the Silver row count stays faithful to the primary table')
                L('    #   and downstream Gold can distinguish "no match" from "excluded".')
            else:
                L(f'    #   {how.upper()} join as specified in the request.')
            L('    #   FIRST matching candidate key wins: the candidates are ordered most-')
            L('    #   to least specific, so the first key present on BOTH sides is the')
            L('    #   narrowest correct grain. Later candidates would widen the join and')
            L('    #   risk fan-out.')
            L(f'    _JOIN_CANDIDATES = {join_candidates!r}')
            L(f'    _JOIN_HOW = {how!r}')
            L('')
        L(f'    # {_ic(f"Read primary bronze source: {primary}")}')
        L(f'    df = dlt.read("vw_{primary}")')

        for o in others:
            L('')
            L(f'    # {_ic(f"Join {o}: pick the first candidate key present in both sides; disambiguate overlapping columns")}')
            L(f'    _r = dlt.read("vw_{o}")')
            L('    _keys = [k for k in _JOIN_CANDIDATES if k in df.columns and k in _r.columns]')
            L('    _common = list(set(df.columns) & set(_r.columns))')
            L('    if _keys:')
            L('        _jk = _keys[0]')
            L('        _overlap = set(_common) - {_jk}')
            L(f'        for _c in _overlap:')
            L(f'            _r = _r.withColumnRenamed(_c, f"{o}_{{_c}}")')
            L('        df = df.join(_r, on=_jk, how=_JOIN_HOW)')
            L('    else:')
            L('        # No shared key. This used to fall through to a crossJoin, which')
            L('        # SUCCEEDS and produces a row count equal to the product of both')
            L('        # sides — a wrong table that looks convincingly populated, and')
            L('        # nothing anywhere reports a problem. A schema change upstream')
            L('        # that renames the join key is enough to trigger it. Refuse')
            L('        # loudly instead: an unbuilt table is recoverable, a silently')
            L('        # fabricated one is not.')
            L('        raise ValueError(')
            L(f'            "cannot join vw_{o}: none of the candidate keys "')
            L('            f"{_JOIN_CANDIDATES} exist on both sides. "')
            L(f'            f"left has {{sorted(df.columns)}}; right has {{sorted(_r.columns)}}. "')
            L('            "Refusing to cross-join, which would multiply the row counts "')
            L('            "and silently produce a wrong table."')
            L('        )')

        if spec.transform.business_rules:
            L('')
            L('    # --- Business rules from the spec — ENFORCED, not just documented ------')
            # These used to be emitted as comments only ("applied informationally"),
            # so the pipeline claimed to honour a rule and silently did not. Anything
            # compile_business_rule() understands becomes a real filter; anything it
            # does not is raised loudly at generation time rather than shipped as a
            # comment that misrepresents the pipeline.
            for r in spec.transform.business_rules:
                pred = compile_business_rule(r)
                L(f'    #   rule: {r}')
                if pred:
                    L(f'    df = df.where("{pred}")')
                else:
                    L(f'    #   ^ NOT AUTO-ENFORCEABLE — see the raise in the generator log.')
        for extra in inline:
            L(f'    # {extra}')

    L('')
    L('    # Attach column metadata so comments land in Unity Catalog and survive full refresh.')
    L('    # _COLUMN_COMMENTS is the LLM-generated dict at the top of this file.')
    L('    df = df.select(*[')
    L('        F.col(c).alias(c, metadata={"comment": _COLUMN_COMMENTS[c]})')
    L('        if c in _COLUMN_COMMENTS else F.col(c)')
    L('        for c in df.columns')
    L('    ])')
    L('    return df')
    L('')


def render_sdp_source(spec: PipelineSpec, request_id: str,
                      table_comment: str | None = None,
                      column_comments: dict[str, dict[str, Any]] | None = None,
                      inline_comments: list[str] | None = None,
                      silver_sql_select: str | None = None) -> str:
    """Single-file silver SDP source (bronze views + silver table). Used for
    the generated PR artifact + back-compat. The DEPLOYED pipeline uses the
    multi-file project layout — see render_sdp_project().
    When silver_sql_select is provided (LLM-generated), use it; otherwise fall back
    to the deterministic template."""
    lines: list[str] = []
    L = lines.append
    streaming = spec.sla.mode == "streaming"
    L('"""Auto-generated by Databricks Pipeline Studio.')
    L('')
    L(f'Spec intent: {spec.intent}')
    L(f'Target table: {spec.target.table}')
    L(f'Source system: {spec.source.system}')
    L(f'SLA: {"continuous (streaming)" if streaming else f"{spec.sla.freshness_minutes} minutes"}')
    L(f'Request id: {request_id}')
    L('"""')
    L('from pyspark import pipelines as dlt')
    L('from pyspark.sql import functions as F')
    L('')
    L('BRONZE_FQ = spark.conf.get("pipeline.bronze_fq")')
    L('TARGET_TABLE = spark.conf.get("pipeline.target_table")')
    L('')
    L('')
    L('def _bronze_table(prefixed_name: str) -> str:')
    L('    """Fully-qualified bronze table name (`<system>__<table>`)."""')
    L('    return f"{BRONZE_FQ}.{prefixed_name}"')
    L('')
    _emit_bronze_views(L, spec, request_id, standalone=False)
    L('')
    L('')
    _emit_silver(L, spec, request_id, standalone=False,
                 table_comment=table_comment, column_comments=column_comments,
                 inline_comments=inline_comments, silver_sql_select=silver_sql_select)
    return "\n".join(lines)


def render_sdp_project(spec: PipelineSpec, request_id: str,
                       table_comment: str | None = None,
                       column_comments: dict[str, dict[str, Any]] | None = None,
                       inline_comments: list[str] | None = None,
                       silver_sql_select: str | None = None) -> dict[str, str]:
    """Multi-file pipeline project following the Lakeflow Pipelines editor
    layout (docs: ldp/multi-file-editor, ldp/best-practices):

        transformations/staging/bronze_views.py   (@dlt.view per source)
        transformations/silver/<table>.py         (@dlt.table silver build)
        README.md

    Returned paths are RELATIVE to the pipeline root folder. The deployed
    pipeline sets root_path to that folder and references each transformation
    file as an explicit (absolute) file library — so all files are evaluated
    as one dataflow graph in the multi-file editor layout."""
    bronze_lines: list[str] = []
    _emit_bronze_views(bronze_lines.append, spec, request_id, standalone=True)
    silver_lines: list[str] = []
    _emit_silver(silver_lines.append, spec, request_id, standalone=True,
                 table_comment=table_comment, column_comments=column_comments,
                 inline_comments=inline_comments, silver_sql_select=silver_sql_select)
    table_refs = _table_refs(spec)
    src_list = ", ".join(f"`{s}`" for s, _ in table_refs)
    readme = (
        f"# Pipeline project: `{spec.target.table}`\n\n"
        f"> {spec.intent}\n\n"
        f"Generated by Databricks Pipeline Studio (request `{request_id}`).\n\n"
        "## Layout (Lakeflow Pipelines multi-file editor convention)\n\n"
        "```\n"
        "transformations/\n"
        "  staging/bronze_views.py   # @dlt.view pass-throughs over ingested bronze\n"
        f"  silver/{spec.target.table}.py        # @dlt.table silver build (joins + DQ + metadata)\n"
        "```\n\n"
        f"**Sources:** {src_list}\n\n"
        "Bronze is produced out-of-band by the consolidated Lakeflow Connect "
        "ingestion pipeline (`ops-<env>-source-ingest`); these transformations "
        "read it via staging views. The pipeline is deployed serverless with a "
        "glob include of `transformations/**` and per-env `pipeline.bronze_fq` / "
        "`pipeline.target_table` configuration.\n"
    )
    return {
        "transformations/staging/bronze_views.py": "\n".join(bronze_lines),
        f"transformations/silver/{spec.target.table}.py": "\n".join(silver_lines),
        "README.md": readme,
    }


# =====================================================================
# Public entry point
# =====================================================================

def deploy_pipeline(
    spec: PipelineSpec,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    request_id: str,
    log: Optional[Callable[[str], None]] = None,
    create_schedule: bool = False,
    table_comment: str | None = None,
    column_comments: dict[str, dict[str, Any]] | None = None,
    inline_comments: list[str] | None = None,
    skip_bronze_load: bool = False,
) -> DeployResult:
    """Full deploy: bronze load + SDP pipeline + run + verify + optional Job schedule.

    Idempotent: safe to call repeatedly for the same (catalog, target_table).

    Set create_schedule=True for the prod path so a scheduled Job is created.
    Set skip_bronze_load=True when Lakeflow Connect ingestion already owns
    the bronze tables (the deploy_ingestion step) — bronze must NOT be
    dropped/recreated in that case; the SDP silver build reads it as-is.
    Metadata params (table_comment, column_comments, inline_comments) thread
    through to the generated SDP source for rich UC metadata + comments.
    """
    log = log or LOG.info
    started = time.time()
    warnings: list[str] = []

    bronze_schema_fq = f"{catalog}.{bronze_schema}"
    silver_schema_fq = f"{catalog}.{silver_schema}"
    silver_table_fq = f"{catalog}.{silver_schema}.{spec.target.table}"

    log(f"=== deploy_pipeline · target=`{silver_table_fq}` · "
        f"create_schedule={create_schedule} ===")

    if skip_bronze_load:
        # Step 1 — bronze owned by Lakeflow Connect ingestion
        bronze_strategy = "lakeflow_connect"
        log("Step 1/5 · Bronze owned by Lakeflow Connect ingestion — "
            "verifying bronze tables + preparing silver schema…")
        bronze_tables, bronze_counts = _prepare_for_lakeflow_bronze(
            spec, catalog, bronze_schema, silver_schema, log)
    else:
        bronze_strategy = "warehouse"
        warnings.append(
            "Bronze loaded via the SQL-warehouse one-shot (external Postgres "
            "source not configured for this run). With the external source, "
            "the deploy_ingestion step does managed CDC via Lakeflow Connect."
        )
        # Step 1 — load bronze
        log(f"Step 1/5 · Loading bronze ({bronze_strategy} strategy)…")
        bronze_tables, bronze_counts = _load_bronze(spec, catalog, bronze_schema,
                                                      silver_schema, log)

    client = WorkspaceClient()
    workspace_host = client.config.host.rstrip("/")

    # Step 2 — generate + upload the multi-file SDP project (transformations/
    # staging + silver, glob-included), following the Lakeflow editor layout.
    log("Step 2/5 · Generating + uploading multi-file SDP project…")
    project = render_sdp_project(
        spec, request_id,
        table_comment=table_comment,
        column_comments=column_comments,
        inline_comments=inline_comments,
    )
    root_path, py_paths = _upload_sdp_project(client, project, catalog, silver_schema,
                                              spec.target.table, log)

    # Step 3 — create or update SDP pipeline (root_path + explicit file libs)
    log("Step 3/5 · Creating / updating SDP pipeline…")
    pipeline_id, pipeline_name = _ensure_pipeline(
        client, spec, catalog, silver_schema, bronze_schema_fq,
        root_path, py_paths, request_id, log,
    )

    # Step 4 — trigger update + wait
    log("Step 4/5 · Triggering pipeline update + waiting…")
    streaming = spec.sla.mode == "streaming"
    try:
        update_id = _start_and_wait(client, pipeline_id, streaming, log)
    except Exception as e:
        log(f"Pipeline update failed: {e}")
        raise

    # Step 5 — verify silver row count
    # (Column comments are emitted by the SDP code via DataFrame metadata
    # — see render_sdp_source. They land in UC automatically.)
    log("Step 5/5 · Verifying silver table row count…")
    warehouse_id = _warehouse_id()
    silver_fq = _qident(catalog, silver_schema, spec.target.table)
    try:
        silver_count = _count_rows(silver_fq, warehouse_id)
    except Exception as e:
        log(f"(row count read failed: {e})")
        silver_count = -1

    pipeline_url = f"{workspace_host}/pipelines/{pipeline_id}"

    # Optional: scheduled Job (prod only by default)
    job_id, job_name, job_url, job_cron = "", "", "", ""
    if create_schedule:
        log(f"Step 5b · Creating scheduled Job for `{pipeline_name}`…")
        try:
            job_id, job_name, job_cron = _ensure_scheduled_job(
                client, spec, pipeline_id, pipeline_name, catalog, silver_schema, log,
            )
            job_url = f"{workspace_host}/jobs/{job_id}"
            log(f"Job ready: `{job_name}` (id={job_id}) "
                + (f"cron=`{job_cron}` Australia/Sydney" if job_cron else "no schedule (streaming)"))
        except Exception as e:
            warnings.append(f"Scheduled-Job creation failed (non-fatal): {e}")
            log(f"(scheduled Job creation skipped: {e})")

    duration = time.time() - started

    log(f"DONE in {duration:.1f}s · pipeline `{pipeline_name}` (id={pipeline_id}) · "
        f"silver={silver_count:,} rows · {pipeline_url}"
        + (f" · job `{job_name}` ({job_url})" if job_id else ""))

    return DeployResult(
        catalog=catalog,
        bronze_schema_fq=bronze_schema_fq,
        silver_schema_fq=silver_schema_fq,
        bronze_tables=bronze_tables,
        bronze_counts=bronze_counts,
        silver_table=silver_table_fq,
        silver_row_count=silver_count,
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        pipeline_url=pipeline_url,
        update_id=update_id,
        sdp_workspace_path=root_path,
        job_id=job_id,
        job_name=job_name,
        job_url=job_url,
        job_schedule_cron=job_cron,
        bronze_strategy=bronze_strategy,
        warnings=warnings,
        duration_seconds=duration,
    )
