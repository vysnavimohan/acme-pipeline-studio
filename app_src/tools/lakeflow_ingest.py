"""Lakeflow Connect ingestion for the external Postgres source.

CONSOLIDATED architecture — ONE ingestion pipeline per (env, source):

  UC connection  `databricks-pipeline-studio-pg`     (created once, holds creds)
        │
  ingestion pipeline `ops-{env}-source-ingest`  (ONE per env; Direct CDC)
        │   objects = UNION of every bronze table any request has needed
        ▼
  bronze `{catalog}.{bronze_schema}.{system}__{table}`  (streaming tables)

Why consolidated (not one pipeline per request):
  * Lakeflow Connect enforces "a streaming table is owned by exactly ONE
    pipeline". Per-request pipelines fight over shared bronze tables
    (e.g. two requests both needing `billing__customers`) and the second
    one fails with "already managed by pipeline X".
  * Bronze is 1:1 with the source — there is no reason to ingest
    `billing.customers` twice. Consolidating de-duplicates ingestion.
  * Adding a table to an existing managed pipeline is incremental: the
    existing tables keep their CDC state, the new table gets an initial
    snapshot. Re-running when nothing changed is skipped entirely — so a
    request whose tables are already ingested costs ~zero ingestion time.

The SDP silver pipeline reads those bronze tables exactly as it does under
the warehouse strategy — the bronze *producer* is swappable.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any, Callable, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as P

from common import sources

CONNECTION_NAME = os.environ.get("OPS_PG_CONNECTION_NAME", "databricks-pipeline-studio-pg")
GATEWAY_NAME = os.environ.get("OPS_PG_GATEWAY_NAME", "ops-gateway-pg")
# "direct" = Direct CDC managed ingestion (Beta preview; no gateway cluster —
# the ingestion pipeline talks to the source itself). "gateway" = the classic
# always-on gateway architecture.
INGEST_MODE = os.environ.get("OPS_INGEST_MODE", "direct")

_POLL_SECONDS = 10
_TIMEOUT_SECONDS = 30 * 60


def consolidated_name(env_tag: str, connection_name: Optional[str] = None) -> str:
    """The consolidated ingestion pipeline name for an env.

    One pipeline per (env, connection): an ingestion pipeline binds to exactly
    ONE `connection_name`, so tables reached through a different UC connection
    (an operator-attached Lakeflow Connect source) cannot share the built-in
    pipeline and get their own, still consolidated across requests.
    """
    if connection_name and connection_name != CONNECTION_NAME:
        slug = re.sub(r"[^a-z0-9]+", "-", connection_name.lower()).strip("-")
        return f"ops-{env_tag}-{slug}-ingest"
    return f"ops-{env_tag}-source-ingest"


def _find_pipeline(client: WorkspaceClient, name: str) -> Optional[str]:
    for pipe in client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if (pipe.name or "") == name:
            return pipe.pipeline_id
    return None


def _dest_key(obj: P.IngestionConfig) -> str:
    t = obj.table
    if not t:
        return ""
    return f"{t.destination_schema}.{t.destination_table}"


def _table_spec(source_catalog: Optional[str], source_schema: str, short: str,
                dest_catalog: str, dest_schema: str, dest_table: str) -> P.IngestionConfig:
    return P.IngestionConfig(table=P.TableSpec(
        source_catalog=source_catalog,
        source_schema=source_schema,
        source_table=short,
        destination_catalog=dest_catalog,
        destination_schema=dest_schema,
        destination_table=dest_table,
        table_configuration=P.TableSpecificConfig(
            scd_type=P.TableSpecificConfigScdType.SCD_TYPE_1,
        ),
    ))


def _ingestion_objects(spec_tables: list[str], primary_system: str,
                       dest_catalog: str, dest_schema: str) -> list[P.IngestionConfig]:
    cfg = sources.pg_config()
    objects: list[P.IngestionConfig] = []
    for sys_name, shorts in sources.split_tables_by_system(
            spec_tables, default=primary_system).items():
        phys_schema = sources.physical_schema(sys_name)
        for short in shorts:
            objects.append(_table_spec(cfg["database"], phys_schema, short,
                                       dest_catalog, dest_schema,
                                       f"{sys_name}__{short}"))
    return objects


def _existing_objects(client: WorkspaceClient, pipeline_id: str) -> list[P.IngestionConfig]:
    spec = client.pipelines.get(pipeline_id).spec
    idef = getattr(spec, "ingestion_definition", None) if spec else None
    return list(idef.objects) if idef and idef.objects else []


def _definition(gateway_id: Optional[str], objects: list[P.IngestionConfig],
                connection_name: Optional[str] = None) -> P.IngestionPipelineDefinition:
    if gateway_id:
        return P.IngestionPipelineDefinition(ingestion_gateway_id=gateway_id, objects=objects)
    return P.IngestionPipelineDefinition(
        connection_name=connection_name or CONNECTION_NAME, objects=objects)


# --- gateway (only used when OPS_INGEST_MODE=gateway) ------------------

def ensure_gateway(client: WorkspaceClient, storage_catalog: str,
                   storage_schema: str, log: Callable[[str], None]) -> str:
    existing = _find_pipeline(client, GATEWAY_NAME)
    if existing:
        log(f"Reusing existing ingestion gateway `{GATEWAY_NAME}` ({existing}).")
        return existing
    conn = client.connections.get(CONNECTION_NAME)
    log(f"Creating ingestion gateway `{GATEWAY_NAME}` on connection `{CONNECTION_NAME}`…")
    created = client.pipelines.create(
        name=GATEWAY_NAME,
        gateway_definition=P.IngestionGatewayPipelineDefinition(
            connection_id=conn.connection_id, connection_name=CONNECTION_NAME,
            gateway_storage_catalog=storage_catalog,
            gateway_storage_schema=storage_schema, gateway_storage_name=GATEWAY_NAME,
        ),
    )
    return created.pipeline_id


def ensure_gateway_running(client: WorkspaceClient, gateway_id: str,
                           log: Callable[[str], None]) -> None:
    info = client.pipelines.get(gateway_id)
    if "RUNNING" in str(info.state or ""):
        return
    try:
        client.pipelines.start_update(pipeline_id=gateway_id)
    except Exception as e:
        log(f"(gateway start note: {str(e)[:120]})")
    for _ in range(30):
        time.sleep(10)
        if "RUNNING" in str(client.pipelines.get(gateway_id).state or ""):
            return


# --- run + freshness ----------------------------------------------------

def _wait_for_update(client: WorkspaceClient, pipeline_id: str, deadline: float,
                     log: Callable[[str], None], label: str) -> str:
    """Poll the LATEST update until terminal; return its final state string.
    Does not raise on FAILED/CANCELED — caller decides (a concurrent update
    we merely waited on failing is not our failure)."""
    last_state = ""
    while time.time() < deadline:
        time.sleep(_POLL_SECONDS)
        info = client.pipelines.get(pipeline_id)
        updates = info.latest_updates or []
        state = str(updates[0].state) if updates else "?"
        if state != last_state:
            log(f"{label}: update state {state.split('.')[-1]}")
            last_state = state
        if any(s in state for s in ("COMPLETED", "FAILED", "CANCELED")):
            return state
    raise TimeoutError(f"{label} did not complete within {_TIMEOUT_SECONDS}s")


def run_pipeline_to_completion(client: WorkspaceClient, pipeline_id: str,
                               log: Callable[[str], None], label: str) -> None:
    """start_update + poll the LATEST update until terminal.

    The consolidated ingestion pipeline is shared across requests, so two
    concurrent gate/full runs can collide: only one update may be active at a
    time. If start_update reports an update is already active, wait for that
    in-flight update to finish, then start OUR update (which sees the merged
    object list, including the table we just added) and wait for it. Bounded
    by the overall timeout."""
    deadline = time.time() + _TIMEOUT_SECONDS
    while time.time() < deadline:
        try:
            client.pipelines.start_update(pipeline_id=pipeline_id)
        except Exception as e:  # noqa: BLE001
            if "active update" in str(e).lower():
                log(f"{label}: another update is already active — waiting for it "
                    "to finish before starting ours")
                _wait_for_update(client, pipeline_id, deadline, log, label)
                continue
            raise
        state = _wait_for_update(client, pipeline_id, deadline, log, label)
        if "COMPLETED" in state:
            return
        if any(s in state for s in ("FAILED", "CANCELED")):
            raise RuntimeError(f"{label} update ended in {state}")
    raise TimeoutError(f"{label} did not complete within {_TIMEOUT_SECONDS}s")


def _tables_present_with_rows(catalog: str, bronze_schema: str,
                              dest_tables: list[str], log: Callable[[str], None]) -> bool:
    """True iff every dest table exists AND has >0 rows (lets us skip a run)."""
    from tools.real_deployer import (_dbsql_conn, _ensure_warehouse_running,
                                      _qident, _warehouse_id)
    client = WorkspaceClient()
    wid = _warehouse_id()
    _ensure_warehouse_running(client, wid, log)
    with _dbsql_conn(wid) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT table_name FROM `{catalog}`.information_schema.tables
                    WHERE table_schema = '{bronze_schema}'""")
            existing = {r[0] for r in cur.fetchall()}
            for short in dest_tables:
                if short not in existing:
                    return False
                cur.execute(f"SELECT COUNT(*) FROM {_qident(catalog, bronze_schema, short)}")
                if (cur.fetchone()[0] or 0) == 0:
                    return False
    return True


def _free_conflicting_tables(catalog: str, bronze_schema: str,
                             dest_short_names: list[str],
                             log: Callable[[str], None]) -> None:
    """Drop physical bronze tables that the consolidated pipeline is about to
    take ownership of but doesn't yet own — legacy MANAGED tables (old
    warehouse strategy) or STREAMING tables orphaned from a retired
    per-request pipeline. Without this, the pipeline update fails with
    'already managed by pipeline X'."""
    from tools.real_deployer import (_dbsql_conn, _ensure_warehouse_running,
                                      _qident, _warehouse_id)
    if not dest_short_names:
        return
    client = WorkspaceClient()
    wid = _warehouse_id()
    _ensure_warehouse_running(client, wid, log)
    with _dbsql_conn(wid) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(catalog, bronze_schema)}")
            cur.execute(
                f"""SELECT table_name FROM `{catalog}`.information_schema.tables
                    WHERE table_schema = '{bronze_schema}'""")
            existing = {r[0] for r in cur.fetchall()}
            for short in dest_short_names:
                if short in existing:
                    log(f"Freeing bronze table `{catalog}.{bronze_schema}.{short}` "
                        "so the consolidated ingestion pipeline can own it.")
                    cur.execute(f"DROP TABLE IF EXISTS {_qident(catalog, bronze_schema, short)}")


def _retire_legacy_pipelines(client: WorkspaceClient, env_tag: str,
                             keep_name: str, log: Callable[[str], None],
                             keep_names: Optional[set[str]] = None) -> None:
    """Delete superseded per-request ingestion pipelines (`ops-{env}-*-ingest`
    that aren't a consolidated one). One-time migration from the old
    per-request scheme; a no-op afterwards.

    `keep_names` must list EVERY live consolidated pipeline, not just ours: the
    per-connection pipelines for operator-attached Lakeflow Connect sources
    match the same `ops-{env}-*-ingest` shape, and deleting one would tear down
    another connection's bronze mid-flight.
    """
    keep = set(keep_names or ()) | {keep_name}
    for pipe in client.pipelines.list_pipelines(filter=f"name LIKE 'ops-{env_tag}-%-ingest'"):
        nm = pipe.name or ""
        if nm and nm not in keep:
            try:
                client.pipelines.delete(pipeline_id=pipe.pipeline_id)
                log(f"Retired superseded per-request ingestion pipeline `{nm}`.")
            except Exception as e:
                log(f"(could not delete legacy pipeline {nm}: {str(e)[:120]})")


def _ensure_consolidated(client: WorkspaceClient, name: str, gateway_id: Optional[str],
                         desired: list[P.IngestionConfig], catalog: str, bronze_schema: str,
                         log: Callable[[str], None], attempts: int = 6,
                         connection_name: Optional[str] = None
                         ) -> tuple[str, list[str], list[str]]:
    """Find-or-create-or-update the ONE consolidated pipeline so it manages
    `desired`. Concurrency-safe against simultaneous requests:
      - create races → catch 'name already used' → re-find + update;
      - update races (a concurrent writer dropped our table) → re-read +
        re-merge + update, up to `attempts` times.
    Returns (pipeline_id, added_dest_tables, reused_dest_tables)."""
    import random
    desired_by_key = {_dest_key(o): o for o in desired}
    for attempt in range(attempts):
        existing_id = _find_pipeline(client, name)
        if existing_id:
            current = _existing_objects(client, existing_id)
            current_keys = {_dest_key(o) for o in current}
            new_objs = [o for k, o in desired_by_key.items() if k not in current_keys]
            added = [o.table.destination_table for o in new_objs]
            reused = [o.table.destination_table for k, o in desired_by_key.items() if k in current_keys]
            if not new_objs:
                return existing_id, [], reused  # already complete
            _free_conflicting_tables(catalog, bronze_schema,
                                     [o.table.destination_table for o in new_objs], log)
            merged = current + new_objs
            log(f"Consolidated pipeline `{name}`: reusing {len(reused)} existing "
                f"table(s), adding {len(added)} new ({', '.join(added)}).")
            try:
                client.pipelines.update(
                    pipeline_id=existing_id, name=name,
                    ingestion_definition=_definition(gateway_id, merged, connection_name),
                    catalog=catalog, schema=bronze_schema)
            except Exception as e:
                log(f"(update raced — retrying: {str(e)[:100]})")
                time.sleep(2 + random.random() * 3)
                continue
            # verify our tables stuck (a concurrent update may have clobbered)
            after = {_dest_key(o) for o in _existing_objects(client, existing_id)}
            if all(k in after for k in desired_by_key):
                return existing_id, added, reused
            log("(concurrent update dropped a table — re-merging)")
            time.sleep(1 + random.random() * 2)
            continue
        # No pipeline yet — try to create it.
        _free_conflicting_tables(catalog, bronze_schema,
                                 [o.table.destination_table for o in desired], log)
        log(f"Creating consolidated ingestion pipeline `{name}` "
            f"({len(desired)} table(s), mode={'gateway' if gateway_id else 'direct CDC'})…")
        try:
            created = client.pipelines.create(
                name=name,
                ingestion_definition=_definition(gateway_id, desired, connection_name),
                serverless=True, continuous=False, channel="PREVIEW",
                catalog=catalog, schema=bronze_schema)
            return created.pipeline_id, [o.table.destination_table for o in desired], []
        except Exception as e:
            if "already" in str(e).lower():   # lost the create race — loop re-finds + updates
                log("(another request created the consolidated pipeline first — joining it)")
                time.sleep(1 + random.random() * 2)
                continue
            raise
    raise RuntimeError(f"could not converge consolidated pipeline `{name}` after {attempts} attempts")


# --- routing tables to the connection that reaches them -----------------

def _group_by_connection(spec_tables: list[str], primary_system: str,
                         dest_catalog: str, dest_schema: str
                         ) -> tuple[dict[str, list[P.IngestionConfig]], list[str]]:
    """Group the request's tables by the UC connection that can reach them.

    Tables whose `system.` prefix names an operator-attached Lakeflow Connect
    source ingest through THAT connection (and its recorded source
    catalog/schema). An ingestion pipeline binds to one connection, so this
    grouping is what decides how many pipelines a cross-source request needs.

    Returns (groups, builtin_tables) — the leftovers belong to the built-in
    external Postgres source, which the caller can only ingest when external
    Postgres is configured.
    """
    from agent import connect_store

    groups: dict[str, list[P.IngestionConfig]] = {}
    builtin: list[str] = []
    for sys_name, shorts in sources.split_tables_by_system(
            spec_tables, default=primary_system).items():
        attached = connect_store.by_system_key(sys_name)
        if not attached or not attached.get("connection_name"):
            builtin.extend(f"{sys_name}.{s}" for s in shorts)
            continue
        conn = attached["connection_name"]
        src_schema = attached.get("source_schema") or sys_name
        src_catalog = attached.get("source_catalog") or None
        for short in shorts:
            groups.setdefault(conn, []).append(
                _table_spec(src_catalog, src_schema, short, dest_catalog,
                            dest_schema, f"{sys_name}__{short}"))
    return groups, builtin


def can_ingest(spec_tables: list[str], primary_system: str) -> bool:
    """True when EVERY requested table is reachable through Lakeflow Connect —
    either via an attached UC connection or via the built-in external Postgres
    source. False means the caller should fall back to the warehouse one-shot
    bronze load, which is the pre-existing behaviour."""
    try:
        _, builtin = _group_by_connection(spec_tables, primary_system, "", "")
    except Exception:  # noqa: BLE001
        return sources.external_pg_configured()
    if builtin:
        return sources.external_pg_configured()
    return True


# --- public entry point -------------------------------------------------

def ingest_to_bronze(
    spec_tables: list[str],
    primary_system: str,
    catalog: str,
    bronze_schema: str,
    env_tag: str,
    target_table: str,
    log: Callable[[str], None],
    storage_catalog: Optional[str] = None,
    storage_schema: Optional[str] = None,
) -> dict[str, Any]:
    """Ensure the request's source tables are in the CONSOLIDATED ingestion
    pipeline for this env, running it only if new tables were added (or
    existing bronze is empty). Returns a result dict for the event log.

    A request whose tables span more than one UC connection drives one
    consolidated pipeline per connection (see `_group_by_connection`); the
    common single-connection case behaves exactly as before.
    """
    client = WorkspaceClient()

    gateway_id: Optional[str] = None
    if INGEST_MODE == "gateway":
        storage_catalog = storage_catalog or catalog
        storage_schema = storage_schema or os.environ.get(
            "OPS_CONTROL_SCHEMA", "databricks_pipeline_studio_dev_control")
        gateway_id = ensure_gateway(client, storage_catalog, storage_schema, log)
        ensure_gateway_running(client, gateway_id, log)

    groups, builtin = _group_by_connection(spec_tables, primary_system,
                                           catalog, bronze_schema)
    if builtin:
        # The built-in external Postgres connection needs the external-Postgres
        # config (its ingestion objects carry that database name). Callers gate
        # on external_pg_configured() before choosing this path; assert it here
        # too so a mixed request fails with a clear message rather than a
        # KeyError deep inside _ingestion_objects.
        if not sources.external_pg_configured():
            raise RuntimeError(
                "tables " + ", ".join(f"`{t}`" for t in builtin) + " are not reachable: "
                "no attached Lakeflow Connect source claims their `system.` prefix and "
                "the built-in external Postgres source is not configured.")
        groups[CONNECTION_NAME] = _ingestion_objects(
            builtin, primary_system, catalog, bronze_schema)
    if not groups:
        raise RuntimeError("no source tables to ingest")

    if not gateway_id:
        log("Direct CDC mode — consolidated pipeline(s) connect via UC connection "
            + ", ".join(f"`{c}`" for c in groups) + " (no gateway cluster).")

    # Every consolidated pipeline that must survive the legacy sweep.
    keep_names = {consolidated_name(env_tag, c) for c in groups}
    _retire_legacy_pipelines(client, env_tag, keep_name=consolidated_name(env_tag),
                             log=log, keep_names=keep_names)

    host = client.config.host.rstrip("/")
    per_connection: list[dict[str, Any]] = []
    all_added: list[str] = []
    all_reused: list[str] = []
    ran_any = False

    for conn_name, desired in groups.items():
        name = consolidated_name(env_tag, conn_name)
        desired_shorts = [o.table.destination_table for o in desired]

        # Ensure the consolidated pipeline manages our desired tables.
        # Concurrency-safe: multiple requests may initiate at once and all
        # target this ONE pipeline, so creates/updates race. We retry on
        # name-conflict (loser re-finds + updates) and re-merge if a concurrent
        # update dropped one of our tables.
        pipeline_id, added, reused = _ensure_consolidated(
            client, name, gateway_id, desired, catalog, bronze_schema, log,
            connection_name=conn_name)

        if added:
            needs_run = True
        else:
            log(f"Consolidated pipeline `{name}` already manages all "
                f"{len(desired)} requested table(s) — checking freshness…")
            needs_run = not _tables_present_with_rows(
                catalog, bronze_schema, desired_shorts, log)
            if not needs_run:
                log("Bronze already populated — skipping ingestion run (reuse, ~0s).")

        if needs_run:
            log(f"Running consolidated ingestion `{name}` → {catalog}.{bronze_schema}…")
            run_pipeline_to_completion(client, pipeline_id, log, label=name)

        all_added.extend(added)
        all_reused.extend(reused)
        ran_any = ran_any or needs_run
        per_connection.append({
            "connection": conn_name, "ingestion_pipeline_id": pipeline_id,
            "ingestion_pipeline_name": name,
            "ingestion_pipeline_url": f"{host}/pipelines/{pipeline_id}",
            "tables_added": added, "tables_reused": reused, "ran": needs_run,
        })

    primary = per_connection[0]
    result: dict[str, Any] = {
        "strategy": "lakeflow_connect",
        "mode": "gateway" if gateway_id else "direct_cdc",
        "consolidated": True,
        # Top-level keys describe the FIRST pipeline so existing event-log
        # consumers keep working; `per_connection` has the full picture.
        "connection": primary["connection"],
        "ingestion_pipeline_id": primary["ingestion_pipeline_id"],
        "ingestion_pipeline_name": primary["ingestion_pipeline_name"],
        "ingestion_pipeline_url": primary["ingestion_pipeline_url"],
        "per_connection": per_connection,
        "connections": [g["connection"] for g in per_connection],
        "tables": spec_tables,
        "tables_added": all_added,
        "tables_reused": all_reused,
        "ran": ran_any,
    }
    if gateway_id:
        result["gateway_id"] = gateway_id
        result["gateway_url"] = f"{host}/pipelines/{gateway_id}"
    return result
