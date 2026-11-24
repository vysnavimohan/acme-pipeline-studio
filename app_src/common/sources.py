"""Source-system registry: the single place that knows how to reach the
external operational systems (billing, network) and how logical table names map
to physical Postgres schemas.

Logical naming (used in specs, UI, bronze tables):  `billing.customers`
Physical location, one of two backends per system:

  * external Postgres (the FE shared Lakeflow-Connect-capable instance) —
    used when OPS_SOURCE_PG_HOST is configured. Logical schema `billing`
    maps to physical schema `{OPS_SOURCE_PG_SCHEMA_PREFIX}billing` so the
    shared instance stays namespaced.
  * Lakebase (legacy/fallback) — instance `databricks-pipeline-studio-{system}`,
    physical schema == system name.

A request's tables may span BOTH systems (e.g. billing customers joined
with network KPIs on customer_id) — callers group tables per system via
`split_tables_by_system` and open one connection per system.
"""

from __future__ import annotations

import os
import re
from typing import Optional

SYSTEMS = ("billing", "network")

# Lakebase instance naming: `<prefix>-<system>`. Defined once here because four
# separate places derived it independently (the connection string, the scenario
# availability probe, the grant script, the bundle) and a rename would have had
# to find all four.
LAKEBASE_INSTANCE_PREFIX = "databricks-pipeline-studio"


def lakebase_instance(system: str) -> str:
    """The Lakebase instance holding a logical system's demo data."""
    return f"{LAKEBASE_INSTANCE_PREFIX}-{system}"

# Common Databricks join keys, appended as fallbacks so multi-table joins resolve
# even when the spec's declared key is vague.
_COMMON_JOIN_KEYS = ["customer_id", "invoice_id", "branch_id", "product_id"]
_JOIN_NOISE = {"on", "how", "left", "inner", "right", "outer", "then", "and", "join"}


def join_candidates(join_with: Optional[dict], primary_key: Optional[list[str]],
                    source_tables: Optional[list[str]] = None) -> list[str]:
    """Ordered candidate join keys. Each secondary table in a multi-table
    join is joined on the FIRST candidate present in both sides — so a 3-way
    join with mixed keys works (customers⨝subscriptions on customer_id,
    customers⨝customer_kpi on customer_id). `join_with.on` may be a tidy column
    or a messy english string; we extract snake_case tokens from it and drop
    table-name tokens (which are never join keys)."""
    table_shorts = {(t.split(".", 1)[-1] if "." in t else t).lower()
                    for t in (source_tables or [])}
    cands: list[str] = []
    if join_with:
        cands += re.findall(r"[a-z_][a-z0-9_]*", str(join_with.get("on") or "").lower())
    cands += [str(k).lower() for k in (primary_key or [])]
    cands += _COMMON_JOIN_KEYS
    out: list[str] = []
    for c in cands:
        if c and c not in _JOIN_NOISE and c not in table_shorts and c not in out:
            out.append(c)
    return out


def expected_silver_columns(source_tables: list[str], primary_system: str,
                            join_with: Optional[dict], primary_key: Optional[list[str]],
                            columns_by_logical: dict[str, list[str]]) -> list[str]:
    """Deterministic mirror of the runtime join in real_deployer.render_sdp_source:
    primary cols as-is, each secondary joined on the first shared candidate key
    (dropped — it lives on primary), other overlaps prefixed `{short}_`.
    `columns_by_logical` maps logical table name → ordered column names."""
    cands = join_candidates(join_with, primary_key, source_tables)
    if not source_tables:
        return []
    running = list(columns_by_logical.get(source_tables[0], []))
    out = list(running)
    running_set = set(running)
    for t in source_tables[1:]:
        short = t.split(".", 1)[-1] if "." in t else t
        cols = list(columns_by_logical.get(t, []))
        jk = next((k for k in cands if k in running_set and k in cols), None)
        for c in cols:
            if jk is not None and c == jk:
                continue  # join key already present from the left side
            name = f"{short}_{c}" if c in running_set else c
            out.append(name)
            running_set.add(name)
    return out

# All source-pg-* config (incl. host) lives in the secret scope, not in git
# or env blocks. Loaded once per process, env vars win.
_PG_KEYS = ("host", "port", "database", "user", "password")
_pg_cfg: dict[str, str] | None = None


def schema_prefix() -> str:
    return os.environ.get("OPS_SOURCE_PG_SCHEMA_PREFIX", "demo_")


def _load_pg_cfg() -> dict[str, str]:
    global _pg_cfg
    if _pg_cfg is not None:
        return _pg_cfg
    cfg: dict[str, str] = {}
    for k in _PG_KEYS:
        env_val = os.environ.get(f"OPS_SOURCE_PG_{k.upper()}")
        if env_val:
            cfg[k] = env_val
    missing = [k for k in _PG_KEYS if k not in cfg]
    if missing and os.environ.get("OPS_SOURCE_BACKEND", "external_pg") == "external_pg":
        scope = os.environ.get("OPS_SECRET_SCOPE", "databricks-pipeline-studio")
        try:
            import base64
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            for k in missing:
                try:
                    resp = w.secrets.get_secret(scope=scope, key=f"source-pg-{k}")
                    cfg[k] = base64.b64decode(resp.value).decode("utf-8")
                except Exception:
                    pass
        except Exception:
            pass
    _pg_cfg = cfg
    return cfg


def external_pg_configured() -> bool:
    if os.environ.get("OPS_SOURCE_BACKEND") == "lakebase":
        return False
    cfg = _load_pg_cfg()
    return all(k in cfg for k in _PG_KEYS)


def known_systems() -> tuple[str, ...]:
    """The built-in systems PLUS the logical keys of any Lakeflow Connect source
    an operator has attached on the Connectors page.

    Without this, `system_of_table` would fold an attached source's prefix into
    the default system — `pagila.customer` would silently become
    `billing.customer` and the request would try to read the wrong database.
    Best-effort: a store that isn't reachable just yields the built-ins.
    """
    try:
        from agent import connect_store
        extra = tuple(k for k in connect_store.system_keys() if k not in SYSTEMS)
    except Exception:  # noqa: BLE001
        extra = ()
    return SYSTEMS + extra


def system_of_table(table: str, default: str = "billing") -> tuple[str, str]:
    """`billing.customers` -> ("billing", "customers"); bare names use default.
    An unrecognised prefix falls back to `default` (which is how a bare or
    mistyped name still routes somewhere sensible)."""
    if "." in table:
        sys_name, short = table.split(".", 1)
        return (sys_name if sys_name in known_systems() else default), short
    return default, table


def split_tables_by_system(tables: list[str], default: str = "billing") -> dict[str, list[str]]:
    """Group logical table names by their source system, preserving order."""
    out: dict[str, list[str]] = {}
    for t in tables:
        sys_name, short = system_of_table(t, default)
        out.setdefault(sys_name, []).append(short)
    return out


def physical_schema(system: str) -> str:
    """Physical Postgres schema for a logical system."""
    if external_pg_configured():
        return f"{schema_prefix()}{system}"
    return system


def pg_config() -> dict[str, str]:
    """The external PG config (host/port/database/user/password). Raises if
    not configured — call external_pg_configured() first."""
    cfg = _load_pg_cfg()
    missing = [k for k in _PG_KEYS if k not in cfg]
    if missing:
        raise RuntimeError(f"external Postgres config incomplete (missing: {missing})")
    return cfg


def conn_str_for(system: str) -> str:
    """psycopg connection string for the system's physical database.

    Only the BUILT-IN systems have one: an attached Lakeflow Connect source is
    reached through its UC connection, whose credentials are write-only, so
    there is deliberately no direct connection string for it.
    """
    if system not in SYSTEMS:
        try:
            from agent import connect_store
            if connect_store.by_system_key(system):
                raise ValueError(
                    f"{system!r} is an attached Lakeflow Connect source — it has no "
                    "direct connection string (UC connection credentials are "
                    "write-only). Profile it via UC federation instead.")
        except ImportError:
            pass
        raise ValueError(f"unknown source system: {system!r} (valid: {SYSTEMS})")
    # Local-dev override wins
    env_url = os.environ.get(f"DATABASE_URL_{system.upper()}")
    if env_url:
        return env_url
    if external_pg_configured():
        import urllib.parse
        cfg = pg_config()
        u = urllib.parse.quote(cfg["user"], safe="")
        p = urllib.parse.quote(cfg["password"], safe="")
        return (f"postgresql://{u}:{p}@{cfg['host']}:{cfg['port']}/"
                f"{cfg['database']}?sslmode=require")
    # Lakebase fallback
    from common.lakebase_auth import get_lakebase_conn_str
    return get_lakebase_conn_str(lakebase_instance(system))
