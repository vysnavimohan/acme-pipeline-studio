"""Read-only discovery of Unity Catalog **connections** — the pool the
Lakeflow Connect picker draws from.

The Studio does not create connections. It assumes a platform team has
already registered them in UC (with their credentials) and offers an
affordance to attach an existing one to the generator, which then ingests
from it through Lakeflow Connect (`tools/lakeflow_ingest`).

Two things this module deliberately does NOT do:

  * **Read credentials.** A UC connection's secrets are write-only — you can
    never get the password back out. That is why a connection-backed source is
    profiled through UC (below) rather than by opening a psycopg connection the
    way `common/sources.py` does for the external Postgres systems.
  * **Use the typed SDK for listing.** `WorkspaceClient.connections.list()`
    deserialises `connection_type` into the `ConnectionType` enum, and any
    value the installed SDK doesn't know about silently becomes `None` — on the
    shared FE metastore that is 557 of 806 connections (`MANAGED_POSTGRESQL`,
    i.e. every Lakebase instance, is not in databricks-sdk 0.112.0). We call
    the REST API directly so the server's own string survives.

Table discovery goes through UC federation: a connection may have one or more
**foreign catalogs** bound to it, and those list schemas/tables/columns with no
credentials at all. When a connection has no foreign catalog bound, the caller
falls back to the operator naming the tables (see the picker's manual mode).
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Optional

LOG = logging.getLogger("ops.uc_connections")

# Connection types the generator's Bronze ingestion can actually drive via
# Lakeflow Connect (database CDC + the managed SaaS connectors). Anything else
# in UC — HTTP/MCP servers, Google Drive, Slack, Power BI — is a real
# connection but not an ingestion source, so the picker hides it rather than
# offering a choice that cannot build.
INGESTIBLE_TYPES: dict[str, str] = {
    "POSTGRESQL": "PostgreSQL",
    "MANAGED_POSTGRESQL": "Lakebase (managed Postgres)",
    "MYSQL": "MySQL",
    "SQLSERVER": "SQL Server",
    "ORACLE": "Oracle",
    "SNOWFLAKE": "Snowflake",
    "REDSHIFT": "Amazon Redshift",
    "SQLDW": "Azure Synapse",
    "TERADATA": "Teradata",
    "SALESFORCE": "Salesforce",
    "SERVICENOW": "ServiceNow",
    "WORKDAY_RAAS": "Workday RaaS",
    "GENERIC_LAKEFLOW_CONNECT": "Lakeflow Connect (generic)",
}

# Types whose logical namespace is `schema.table` (relational). The SaaS
# connectors expose flat object lists instead, which the picker labels
# differently.
RELATIONAL_TYPES = frozenset({
    "POSTGRESQL", "MANAGED_POSTGRESQL", "MYSQL", "SQLSERVER", "ORACLE",
    "SNOWFLAKE", "REDSHIFT", "SQLDW", "TERADATA",
})

_CACHE: dict[str, Any] = {"at": 0.0, "connections": None}
_CACHE_TTL = 120  # the connection list changes rarely; the picker polls it

# Lakebase instances and managed database catalogs each get an auto-created
# system connection (`internal-<uuid>` / `managed-database-catalog-<uuid>`)
# pointing at the internal host. They are real MANAGED_POSTGRESQL connections,
# but they're plumbing for someone else's instance — on the shared FE metastore
# there are 536 of them against 162 genuine sources, so they'd bury the picker.
# Matched on BOTH the name shape and the internal host: a connection someone
# deliberately named `internal-something` must never be hidden.
_INTERNAL_NAME = re.compile(
    r"^(?:internal|managed-database-catalog)"
    r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_INTERNAL_HOST = "https://internal/"


def _client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


def _raw_type(c: dict) -> str:
    """The server's own connection_type string, uppercased. Falls back to
    `securable_kind` (`CONNECTION_POSTGRESQL` → `POSTGRESQL`), which is
    populated on some connections where connection_type is absent."""
    t = (c.get("connection_type") or "").strip().upper()
    if t:
        return t
    kind = (c.get("securable_kind") or "").strip().upper()
    return kind[len("CONNECTION_"):] if kind.startswith("CONNECTION_") else ""


def _host_of(c: dict) -> str:
    opts = c.get("options") or {}
    host = opts.get("host") or ""
    port = opts.get("port") or ""
    if host and port:
        return f"{host}:{port}"
    return host or (c.get("url") or "")


def _is_internal(name: str, host: str) -> bool:
    """An auto-created per-Lakebase-instance system connection, not a source
    anyone would deliberately ingest from."""
    return bool(_INTERNAL_NAME.match(name or "")) and host == _INTERNAL_HOST


def _summarise(c: dict) -> dict[str, Any]:
    ctype = _raw_type(c)
    name = c.get("name") or c.get("full_name") or ""
    host = _host_of(c)
    return {
        "name": name,
        "internal": _is_internal(name, host),
        "connection_type": ctype,
        "type_label": INGESTIBLE_TYPES.get(ctype, ctype.title() if ctype else "Unknown"),
        "relational": ctype in RELATIONAL_TYPES,
        "ingestible": ctype in INGESTIBLE_TYPES,
        "owner": c.get("owner") or "",
        "comment": c.get("comment") or "",
        "host": host,
        "read_only": bool(c.get("read_only")),
        "state": ((c.get("provisioning_info") or {}).get("state") or "").upper(),
        "credential_type": c.get("credential_type") or "",
    }


def _list_raw() -> list[dict]:
    """Every connection the caller can see, as raw API dicts (all pages)."""
    client = _client()
    out: list[dict] = []
    token: Optional[str] = None
    for _ in range(50):  # hard page cap — 50 * 200 is far beyond any metastore
        query: dict[str, Any] = {"max_results": 200}
        if token:
            query["page_token"] = token
        resp = client.api_client.do(
            "GET", "/api/2.1/unity-catalog/connections", query=query) or {}
        out.extend(resp.get("connections") or [])
        token = resp.get("next_page_token")
        if not token:
            break
    return out


def list_connections(*, ingestible_only: bool = True, include_internal: bool = False,
                     force: bool = False) -> list[dict[str, Any]]:
    """UC connections available as ingestion sources, sorted by name.

    Cached briefly — the shared FE metastore holds ~800 connections and the
    picker re-reads this on every modal open. Auto-created `internal-<uuid>`
    Lakebase system connections are excluded unless `include_internal`.
    """
    if not force and _CACHE["connections"] is not None \
            and time.time() - _CACHE["at"] < _CACHE_TTL:
        conns = _CACHE["connections"]
    else:
        try:
            conns = [_summarise(c) for c in _list_raw()]
        except Exception as e:  # noqa: BLE001 — never break the page over this
            LOG.warning("UC connection list failed: %s", e)
            return []
        _CACHE["connections"] = conns
        _CACHE["at"] = time.time()
    if ingestible_only:
        conns = [c for c in conns if c["ingestible"]]
    if not include_internal:
        conns = [c for c in conns if not c["internal"]]
    return sorted(conns, key=lambda c: c["name"].lower())


def internal_count(*, ingestible_only: bool = True) -> int:
    """How many auto-created Lakebase system connections are being hidden — the
    picker shows this as an opt-in toggle rather than silently dropping them."""
    return sum(1 for c in list_connections(ingestible_only=ingestible_only,
                                           include_internal=True) if c["internal"])


def get_connection(name: str) -> Optional[dict[str, Any]]:
    try:
        c = _client().api_client.do(
            "GET", f"/api/2.1/unity-catalog/connections/{name}")
    except Exception as e:  # noqa: BLE001
        LOG.warning("UC connection %s not readable: %s", name, e)
        return None
    return _summarise(c or {})


# --- table discovery via UC federation --------------------------------------

def foreign_catalogs_for(connection_name: str) -> list[str]:
    """Catalogs bound to this connection. Browsing these needs no source
    credentials — UC federates the metadata read."""
    try:
        return sorted(
            c.name for c in _client().catalogs.list()
            if (c.connection_name or "") == connection_name and c.name
        )
    except Exception as e:  # noqa: BLE001
        LOG.warning("catalog list failed: %s", e)
        return []


def list_schemas(catalog: str) -> list[str]:
    try:
        return sorted(s.name for s in _client().schemas.list(catalog)
                      if s.name and s.name != "information_schema")
    except Exception as e:  # noqa: BLE001
        LOG.warning("schema list failed for %s: %s", catalog, e)
        return []


def list_tables(catalog: str, schema: str) -> list[dict[str, Any]]:
    """Tables in a federated schema, with columns — enough for the picker to
    show what will be ingested and for the profiler to type the Silver table."""
    try:
        tables = _client().tables.list(catalog_name=catalog, schema_name=schema)
    except Exception as e:  # noqa: BLE001
        LOG.warning("table list failed for %s.%s: %s", catalog, schema, e)
        return []
    out: list[dict[str, Any]] = []
    for t in tables:
        if not t.name:
            continue
        out.append({
            "name": t.name,
            "full_name": t.full_name or f"{catalog}.{schema}.{t.name}",
            "comment": t.comment or "",
            "columns": [{"name": c.name, "type": (c.type_name.value
                                                  if hasattr(c.type_name, "value")
                                                  else str(c.type_name or "")).lower()}
                        for c in (t.columns or []) if c.name],
        })
    return sorted(out, key=lambda x: x["name"])


def browse(connection_name: str) -> dict[str, Any]:
    """What the picker shows after a connection is chosen: the federated
    catalogs bound to it and, for the first one, its schemas.

    `browsable` False means no foreign catalog is bound — the connection is
    still perfectly usable for ingestion (Lakeflow Connect talks to the source
    directly), the operator just has to name the tables themselves.
    """
    catalogs = foreign_catalogs_for(connection_name)
    if not catalogs:
        return {"browsable": False, "catalogs": [], "schemas": [],
                "reason": ("No federated catalog is bound to this connection, so "
                           "its tables can't be listed here. Enter the source "
                           "schema + tables manually — ingestion still works.")}
    return {"browsable": True, "catalogs": catalogs,
            "catalog": catalogs[0], "schemas": list_schemas(catalogs[0])}
