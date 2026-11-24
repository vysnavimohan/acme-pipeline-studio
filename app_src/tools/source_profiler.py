"""Profiles source tables across one or MORE source systems.

A request's tables may span systems (`billing.customers` joined with
`network.customer_kpi` on customer_id) — tables are grouped per system and
each system gets its own connection. Logical schema names (billing/network)
are mapped to physical Postgres schemas by common.sources.

Returns column names, types, row counts, and a small sample, PLUS a
`missing_tables` list + `available_tables` catalog so the agent can fail
fast (with a helpful message) when the spec names a table that doesn't
exist — instead of burning a whole deploy run on a typo or an
LLM-hallucinated name.
"""

from __future__ import annotations

from typing import Any

from common import sources

# NOTE (2026-08-03): `psycopg` is imported LAZILY inside the functions that use
# it, not at module top level. Its binary build loads native libpq at import
# time, which on serverless job compute intermittently SIGABRTs the whole
# Python kernel (exit 134 — "The Python kernel is unresponsive"), taking the
# agent run down before any node executes. Deferring the import keeps the
# crash confined to the Postgres/Lakebase code path, so the file/Auto-Loader
# requests (which never touch psycopg) can't be brought down by it.
# `common/source_catalog.py` already does the same.


def _conn_str_for(system: str) -> str:
    """Kept for back-compat with older callers/tests."""
    return sources.conn_str_for(system)


def list_available_tables(system: str) -> list[str]:
    """Logical table names (`system.table`) that actually exist."""
    import psycopg

    with psycopg.connect(sources.conn_str_for(system), connect_timeout=15) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' ORDER BY table_name",
                (sources.physical_schema(system),),
            )
            return [f"{system}.{r[0]}" for r in cur.fetchall()]


def _profile_via_federation(attached: dict[str, Any], sys_name: str,
                            shorts: list[str], out: dict[str, Any]) -> None:
    """Profile an operator-attached Lakeflow Connect source through UC.

    A UC connection's credentials are WRITE-ONLY, so there is no psycopg path
    here: the columns come from the federated catalog bound to the connection.
    When nothing is bound we can't verify the tables, so we accept the
    operator's list as-is (typed by the LLM downstream) rather than failing the
    run on the source_profiler hard gate.
    """
    from common import uc_connections

    cat = attached.get("browse_catalog") or ""
    sch = attached.get("source_schema") or ""
    declared = list(attached.get("tables") or [])
    federated: dict[str, list[dict[str, str]]] = {}
    if cat and sch:
        federated = {t["name"]: t["columns"] for t in uc_connections.list_tables(cat, sch)}

    # Known names: what federation sees, else what the operator attached.
    known = set(federated) or set(declared)
    out["available_tables"].extend(f"{sys_name}.{t}" for t in sorted(known))
    for name in shorts:
        logical = f"{sys_name}.{name}"
        if known and name not in known:
            out["missing_tables"].append(logical)
            continue
        out["tables"][logical] = {
            "columns": [{"name": c["name"], "type": c["type"], "nullable": True}
                        for c in federated.get(name, [])],
            # Row count + sample would need to query the source; Lakeflow
            # Connect does that during ingestion, not during profiling.
            "row_count": -1,
            "sample": [],
            "source_system": sys_name,
            "source_kind": "lakeflow_connect",
            "connection_name": attached.get("connection_name") or "",
        }


def profile_source(system: str, tables: list[str]) -> dict[str, Any]:
    """Profile `tables` (logical names, possibly spanning systems).

    `system` is the spec's primary system — bare table names default to it.
    Tables belonging to an attached UC connection are profiled through UC
    federation; the built-in external Postgres systems are profiled over psycopg.
    """
    import psycopg

    from agent import connect_store

    out: dict[str, Any] = {"system": system, "tables": {},
                           "missing_tables": [], "available_tables": []}
    by_system = sources.split_tables_by_system(tables, default=system)

    for sys_name, shorts in by_system.items():
        attached = connect_store.by_system_key(sys_name)
        if attached:
            _profile_via_federation(attached, sys_name, shorts, out)
            continue
        phys_schema = sources.physical_schema(sys_name)
        with psycopg.connect(sources.conn_str_for(sys_name), connect_timeout=15) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE'",
                    (phys_schema,),
                )
                existing = {r[0] for r in cur.fetchall()}
                out["available_tables"].extend(f"{sys_name}.{t}" for t in sorted(existing))

                for name in shorts:
                    logical = f"{sys_name}.{name}"
                    if name not in existing:
                        out["missing_tables"].append(logical)
                        continue
                    cur.execute(
                        "SELECT column_name, data_type, is_nullable "
                        "FROM information_schema.columns "
                        "WHERE table_schema = %s AND table_name = %s "
                        "ORDER BY ordinal_position",
                        (phys_schema, name),
                    )
                    cols = [{"name": r[0], "type": r[1], "nullable": r[2] == "YES"}
                            for r in cur.fetchall()]
                    cur.execute(f'SELECT COUNT(*) FROM "{phys_schema}"."{name}"')
                    row_count = cur.fetchone()[0]
                    cur.execute(f'SELECT * FROM "{phys_schema}"."{name}" LIMIT 3')
                    sample = [list(r) for r in cur.fetchall()]
                    out["tables"][logical] = {
                        "columns": cols,
                        "row_count": row_count,
                        "sample": sample,
                        "source_system": sys_name,
                    }
    return out
