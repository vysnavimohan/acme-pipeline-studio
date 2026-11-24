"""Actually deploy the generated pipeline.

For the PoC we don't run a full Lakeflow Connect ingest + SDP pipeline
(too much setup for a short demo loop). Instead we use:

  1. psycopg → read source rows from Lakebase.
  2. Databricks SDK → start the SQL warehouse if it's stopped.
  3. databricks-sql-connector → bulk-load bronze + build silver.

The PR still contains the real Lakeflow Connect + SDP code, so the
customer sees both the production pattern (in the PR) and the actual
data (in UC).
"""

from __future__ import annotations

import logging
import os
import time
import traceback
from typing import Any, Callable

import psycopg
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

from agent.state import PipelineSpec
from common.lakebase_auth import get_lakebase_conn_str

LOG = logging.getLogger("ops.deployer")

_BRONZE_COL_TYPE = "STRING"
_MAX_ROWS_PER_TABLE = 1000
_INSERT_BATCH = 250


def _qident(*parts: str) -> str:
    return ".".join(f"`{p}`" for p in parts)


# ---- Lakebase read ----------------------------------------------------

def _read_source_tables(system: str, tables: list[str]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    conn_str = get_lakebase_conn_str(f"databricks-pipeline-studio-{system}")
    with psycopg.connect(conn_str) as conn:
        for t in tables:
            schema, name = t.split(".", 1) if "." in t else (system, t)
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM "{schema}"."{name}" LIMIT {_MAX_ROWS_PER_TABLE}')
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
            out[f"{schema}.{name}"] = {"columns": cols, "rows": rows}
    return out


# ---- Warehouse warm-up + dbsql connection -----------------------------

def _warehouse_id() -> str:
    for key in ("DATABRICKS_WAREHOUSE_ID", "DATABRICKS_WAREHOUSE_HTTP_PATH"):
        v = (os.environ.get(key) or "").strip()
        if not v:
            continue
        return v.rsplit("/", 1)[-1] if "/warehouses/" in v else v
    raise RuntimeError("set DATABRICKS_WAREHOUSE_ID or DATABRICKS_WAREHOUSE_HTTP_PATH")


def _state_to_str(s) -> str:
    return s.value if hasattr(s, "value") else (str(s) if s else "?")


def _ensure_warehouse_running(client: WorkspaceClient, warehouse_id: str,
                              log: Callable[[str], None]):
    info = client.warehouses.get(warehouse_id)
    state = _state_to_str(info.state)
    if state == "RUNNING":
        return
    log(f"SQL warehouse is {state} — starting it (~10s on serverless)…")
    client.warehouses.start(warehouse_id)
    deadline = time.time() + 180
    while time.time() < deadline:
        info = client.warehouses.get(warehouse_id)
        if _state_to_str(info.state) == "RUNNING":
            log("Warehouse is RUNNING.")
            return
        time.sleep(3)
    raise RuntimeError("warehouse failed to start in 180s")


def _dbsql_conn(warehouse_id: str):
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/").replace("https://", "")
    if not host:
        raise RuntimeError("DATABRICKS_HOST not set")
    # Mint a fresh OAuth token via SDK — works in App SP context AND local.
    client = WorkspaceClient()
    token_provider = client.config.authenticate
    # Provider returns a dict of headers; pull just the bearer.
    headers = token_provider()
    auth = headers.get("Authorization", "")
    token = auth[7:] if auth.lower().startswith("bearer ") else auth
    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        access_token=token,
    )


# ---- INSERT batching --------------------------------------------------

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


# ---- Silver build -----------------------------------------------------

def _build_silver_sql(spec: PipelineSpec, catalog: str, silver_schema: str,
                      bronze_tables: list[str],
                      bronze_columns: dict[str, list[str]]) -> str:
    """Build the silver CREATE TABLE statement.

    Multi-table joins are common (e.g. customers and invoices both
    have a `status` column), so we prefix every column from non-first tables
    with the bronze table's short name to avoid duplicate-column errors.
    """
    silver_table = _qident(catalog, silver_schema, spec.target.table)
    if len(bronze_tables) == 1:
        return (f"CREATE OR REPLACE TABLE {silver_table} USING DELTA AS "
                f"SELECT * FROM {bronze_tables[0]}")

    join_on = None
    if spec.transform.join_with:
        join_on = spec.transform.join_with.get("on")
    if not join_on and spec.source.primary_key:
        join_on = spec.source.primary_key[0]
    if not join_on:
        return (f"CREATE OR REPLACE TABLE {silver_table} USING DELTA AS "
                f"SELECT * FROM {bronze_tables[0]}")

    def _short(b: str) -> str:
        # `catalog`.`schema`.`prefix__tablename` → tablename
        last = b.rstrip("`").rsplit("`", 1)[-1] or b.rsplit(".", 1)[-1]
        return last.split("__", 1)[-1]

    first = bronze_tables[0]
    # Take all columns from t0 as-is
    select_parts = [f"t0.`{c}` AS `{c}`" for c in bronze_columns[first]]
    join_clause = ""
    for i, b in enumerate(bronze_tables[1:], start=1):
        prefix = _short(b)
        for c in bronze_columns[b]:
            if c == join_on:
                continue  # the key column is already on t0
            select_parts.append(f"t{i}.`{c}` AS `{prefix}_{c}`")
        join_clause += f" LEFT JOIN {b} t{i} ON t0.`{join_on}` = t{i}.`{join_on}`"

    return (
        f"CREATE OR REPLACE TABLE {silver_table} USING DELTA AS "
        f"SELECT {', '.join(select_parts)} FROM {first} t0{join_clause}"
    )


# ---- Top-level deploy --------------------------------------------------

def deploy_to_catalog(
    spec: PipelineSpec,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    log = log or LOG.info

    try:
        client = WorkspaceClient()
        warehouse_id = _warehouse_id()
        _ensure_warehouse_running(client, warehouse_id, log)
    except Exception as e:
        log(f"Warehouse prep error: {e}\n{traceback.format_exc()}")
        raise

    log(f"Connecting to Lakebase instance `databricks-pipeline-studio-{spec.source.system}`…")
    try:
        src_data = _read_source_tables(spec.source.system, spec.source.tables)
    except Exception as e:
        log(f"Lakebase read error: {e}")
        raise
    total_rows = sum(len(d["rows"]) for d in src_data.values())
    log(f"Pulled {total_rows:,} rows across {len(src_data)} source table(s).")

    bronze_tables: list[str] = []
    bronze_counts: dict[str, int] = {}
    silver_count = -1

    try:
        with _dbsql_conn(warehouse_id) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(catalog, bronze_schema)}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(catalog, silver_schema)}")

                bronze_columns: dict[str, list[str]] = {}
                for fq_src, payload in src_data.items():
                    src_table = fq_src.split(".", 1)[-1]
                    fq_bronze = _qident(catalog, bronze_schema, f"{spec.source.system}__{src_table}")
                    log(f"Bronze write: `{fq_bronze}` ({len(payload['rows']):,} rows)…")
                    col_def = ", ".join(f"`{c}` {_BRONZE_COL_TYPE}" for c in payload["columns"])
                    cur.execute(f"DROP TABLE IF EXISTS {fq_bronze}")
                    cur.execute(f"CREATE TABLE {fq_bronze} ({col_def}) USING DELTA")
                    for stmt in _bulk_inserts(fq_bronze, payload["rows"]):
                        cur.execute(stmt)
                    bronze_tables.append(fq_bronze)
                    bronze_counts[fq_bronze] = len(payload["rows"])
                    bronze_columns[fq_bronze] = payload["columns"]

                silver_sql = _build_silver_sql(spec, catalog, silver_schema, bronze_tables, bronze_columns)
                fq_silver = _qident(catalog, silver_schema, spec.target.table)
                log(f"Silver build: `{fq_silver}`…")
                cur.execute(silver_sql)
                cur.execute(f"SELECT COUNT(*) FROM {fq_silver}")
                silver_count = cur.fetchone()[0]
                log(f"Silver `{fq_silver}` has {silver_count:,} rows.")
    except Exception as e:
        log(f"DBSQL error: {e}\n{traceback.format_exc()[:1000]}")
        raise

    return {
        "catalog": catalog,
        "bronze_schema": f"{catalog}.{bronze_schema}",
        "bronze_tables": bronze_tables,
        "bronze_counts": bronze_counts,
        "silver_schema": f"{catalog}.{silver_schema}",
        "silver_table": f"{catalog}.{silver_schema}.{spec.target.table}",
        "silver_row_count": silver_count,
    }
