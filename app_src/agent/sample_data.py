"""Pre-onboarded **sample datasets** so a fresh deployment can instantly play
with the accelerator end-to-end (Source → Ingest → Prepare → Curate) without a
single connected source system.

Two sample landing files mirror the shapes Databricks actually receives:
  * ``sample_orders``   — a native **CSV** (comma-delimited, header row)
  * ``sample_signups``  — a multi-character-delimited **.dat** (``@#@``) with an
                          embedded **JSON** column, exactly like the Campaign
                          Prepaid Response Feed (exercises the Silver flatten)

Plus a small ``customers`` **Silver dimension** that the "join → Gold"
ready-made example aggregates the signups against (a fact ⋈ dimension join,
materialised to Gold in one request).

This module is the SINGLE SOURCE OF TRUTH:
  * the seeder (``autoloader_store.seed_default_source``) writes these files to
    the landing Volume and onboards them as *ready* Auto Loader datasets, and
  * ``scenarios.py`` builds its file-based ready-made examples from the same
    definitions — so the onboarded config and the prefilled form never drift.
"""

from __future__ import annotations

import os
from typing import Any

# =====================================================================
# Environment-resolved targets (match the Job/App env in resources/*.yml)
# =====================================================================

def dev_catalog() -> str:
    return os.environ.get("DEV_CATALOG", "pipeline_studio_catalog")


def dev_silver_schema() -> str:
    return os.environ.get(
        "DEV_SILVER_SCHEMA",
        "dev_jeremy_herbert_databricks_pipeline_studio_dev_silver")


# =====================================================================
# Sample landing dataset 1 — sample_orders (CSV)
# =====================================================================

ORDERS_COLUMNS = [
    {"name": "order_id", "type": "string", "is_json": False},
    {"name": "customer_id", "type": "string", "is_json": False},
    {"name": "order_ts", "type": "timestamp", "is_json": False},
    {"name": "product", "type": "string", "is_json": False},
    {"name": "channel", "type": "string", "is_json": False},
    {"name": "quantity", "type": "long", "is_json": False},
    {"name": "unit_price", "type": "double", "is_json": False},
    {"name": "region", "type": "string", "is_json": False},
]

# (order_id, customer_id, order_ts, product, channel, quantity, unit_price, region)
ORDERS_ROWS = [
    ("O0001", "C001", "2026-06-01 08:14:22", "Handset",   "online",      1, 1299.00, "NSW"),
    ("O0002", "C002", "2026-06-01 09:02:10", "SIM",       "store",       2,   10.00, "VIC"),
    ("O0003", "C003", "2026-06-01 10:31:55", "DataPack",  "online",      1,   30.00, "NSW"),
    ("O0004", "C004", "2026-06-01 11:19:03", "Accessory", "store",       3,   24.95, "QLD"),
    ("O0005", "C005", "2026-06-02 07:45:41", "Modem",     "call_centre", 1,  149.00, "NSW"),
    ("O0006", "C006", "2026-06-02 12:08:19", "SIM",       "online",      1,   10.00, "WA"),
    ("O0007", "C007", "2026-06-02 13:52:37", "Handset",   "store",       1,  999.00, "VIC"),
    ("O0008", "C008", "2026-06-02 15:27:48", "DataPack",  "online",      2,   30.00, "SA"),
    ("O0009", "C009", "2026-06-03 08:11:09", "Accessory", "online",      1,   49.95, "QLD"),
    ("O0010", "C010", "2026-06-03 09:40:55", "Handset",   "call_centre", 1, 1499.00, "NSW"),
    ("O0011", "C001", "2026-06-03 10:22:31", "DataPack",  "online",      1,   30.00, "NSW"),
    ("O0012", "C011", "2026-06-03 14:05:12", "SIM",       "store",       4,   10.00, "VIC"),
    ("O0013", "C012", "2026-06-04 08:33:44", "Modem",     "online",      1,  149.00, "WA"),
    ("O0014", "C003", "2026-06-04 11:48:26", "Handset",   "store",       1,  899.00, "NSW"),
    ("O0015", "C005", "2026-06-04 13:14:50", "Accessory", "online",      2,   24.95, "NSW"),
    ("O0016", "C007", "2026-06-05 09:27:38", "DataPack",  "call_centre", 1,   45.00, "VIC"),
    ("O0017", "C002", "2026-06-05 10:55:02", "SIM",       "online",      1,   10.00, "VIC"),
    ("O0018", "C009", "2026-06-05 12:19:44", "Handset",   "store",       1, 1099.00, "QLD"),
    ("O0019", "C010", "2026-06-06 08:02:17", "Modem",     "online",      2,  149.00, "NSW"),
    ("O0020", "C008", "2026-06-06 15:41:29", "Accessory", "online",      1,   49.95, "SA"),
    ("O0021", "C004", "2026-06-06 16:08:53", "DataPack",  "store",       1,   30.00, "QLD"),
    ("O0022", "C012", "2026-06-07 09:33:11", "Handset",   "online",      1,  799.00, "WA"),
]

SAMPLE_ORDERS: dict[str, Any] = {
    "dataset": "sample_orders",
    "filename": "sample_orders_2026-06-07.csv",
    "file_format": "csv",
    "delimiter": ",",
    "has_header": True,
    "columns": ORDERS_COLUMNS,
    "description": "Retail order lines (CSV) — one row per order line.",
}


# =====================================================================
# Sample landing dataset 2 — sample_signups (.dat, @#@, embedded JSON)
# =====================================================================

SIGNUPS_DELIM = "@#@"

SIGNUPS_COLUMNS = [
    {"name": "event_id", "type": "string", "is_json": False},
    {"name": "customer_id", "type": "string", "is_json": False},
    {"name": "event_ts", "type": "timestamp", "is_json": False},
    {"name": "plan", "type": "string", "is_json": False},
    {"name": "device_type", "type": "string", "is_json": False},
    {"name": "attributes", "type": "string", "is_json": True},
]

# (event_id, customer_id, event_ts, plan, device_type, attributes_json)
SIGNUPS_ROWS = [
    ("E0001", "C001", "2026-06-01 08:20:00", "postpaid", "ios",     '{"promo":"WINTER25","store":"Bondi","autopay":true,"referral":false}'),
    ("E0002", "C002", "2026-06-01 09:10:00", "prepaid",  "android", '{"promo":"NONE","store":"Chadstone","autopay":false,"referral":true}'),
    ("E0003", "C003", "2026-06-01 10:40:00", "postpaid", "ios",     '{"promo":"WINTER25","store":"online","autopay":true,"referral":false}'),
    ("E0004", "C004", "2026-06-01 11:25:00", "prepaid",  "android", '{"promo":"SAVE10","store":"Chermside","autopay":false,"referral":false}'),
    ("E0005", "C005", "2026-06-02 07:50:00", "postpaid", "ios",     '{"promo":"BIZ50","store":"online","autopay":true,"referral":true}'),
    ("E0006", "C006", "2026-06-02 12:15:00", "mvno",     "android", '{"promo":"NONE","store":"Perth City","autopay":false,"referral":false}'),
    ("E0007", "C007", "2026-06-02 14:00:00", "postpaid", "ios",     '{"promo":"BIZ50","store":"Highpoint","autopay":true,"referral":false}'),
    ("E0008", "C008", "2026-06-02 15:35:00", "prepaid",  "android", '{"promo":"SAVE10","store":"online","autopay":false,"referral":true}'),
    ("E0009", "C009", "2026-06-03 08:15:00", "postpaid", "ios",     '{"promo":"WINTER25","store":"Carindale","autopay":true,"referral":false}'),
    ("E0010", "C010", "2026-06-03 09:45:00", "postpaid", "android", '{"promo":"BIZ50","store":"online","autopay":true,"referral":true}'),
    ("E0011", "C011", "2026-06-03 14:10:00", "prepaid",  "ios",     '{"promo":"NONE","store":"Chadstone","autopay":false,"referral":false}'),
    ("E0012", "C012", "2026-06-04 08:40:00", "postpaid", "android", '{"promo":"SAVE10","store":"Perth City","autopay":true,"referral":false}'),
    ("E0013", "C001", "2026-06-04 11:50:00", "postpaid", "ios",     '{"promo":"NONE","store":"online","autopay":true,"referral":true}'),
    ("E0014", "C003", "2026-06-04 13:20:00", "postpaid", "ios",     '{"promo":"WINTER25","store":"Bondi","autopay":true,"referral":false}'),
    ("E0015", "C005", "2026-06-05 09:30:00", "postpaid", "android", '{"promo":"BIZ50","store":"online","autopay":true,"referral":false}'),
    ("E0016", "C007", "2026-06-05 11:00:00", "postpaid", "ios",     '{"promo":"BIZ50","store":"Highpoint","autopay":true,"referral":true}'),
    ("E0017", "C008", "2026-06-05 12:25:00", "prepaid",  "android", '{"promo":"SAVE10","store":"Chermside","autopay":false,"referral":false}'),
    ("E0018", "C009", "2026-06-06 08:05:00", "postpaid", "ios",     '{"promo":"WINTER25","store":"online","autopay":true,"referral":false}'),
    ("E0019", "C010", "2026-06-06 15:45:00", "postpaid", "android", '{"promo":"BIZ50","store":"Carindale","autopay":true,"referral":true}'),
    ("E0020", "C002", "2026-06-07 09:35:00", "prepaid",  "ios",     '{"promo":"NONE","store":"online","autopay":false,"referral":false}'),
]

SAMPLE_SIGNUPS: dict[str, Any] = {
    "dataset": "sample_signups",
    "filename": "sample_signups_2026-06-07.dat",
    "file_format": "text",
    "delimiter": SIGNUPS_DELIM,
    "has_header": True,
    "columns": SIGNUPS_COLUMNS,
    "description": ("Plan-signup events (multi-char-delimited .dat with an "
                    "embedded JSON attributes column — like the Campaign feed)."),
}


SAMPLE_DATASETS: list[dict[str, Any]] = [SAMPLE_ORDERS, SAMPLE_SIGNUPS]


# =====================================================================
# Customers Silver dimension (pre-seeded so the join → Gold example works)
# =====================================================================

CUSTOMERS_COLUMNS = ["customer_id", "customer_name", "segment", "plan", "region", "status"]

# (customer_id, customer_name, segment, plan, region, status)
CUSTOMERS_ROWS = [
    ("C001", "Aanya Sharma",   "consumer",   "postpaid", "NSW", "active"),
    ("C002", "Liam O'Brien",   "consumer",   "prepaid",  "VIC", "active"),
    ("C003", "Mei Tan",        "smb",        "postpaid", "NSW", "active"),
    ("C004", "Noah Williams",  "consumer",   "prepaid",  "QLD", "churned"),
    ("C005", "Olivia Chen",    "enterprise", "postpaid", "NSW", "active"),
    ("C006", "Ethan Brown",    "consumer",   "mvno",     "WA",  "suspended"),
    ("C007", "Sofia Rossi",    "smb",        "postpaid", "VIC", "active"),
    ("C008", "Jack Nguyen",    "consumer",   "prepaid",  "SA",  "active"),
    ("C009", "Isla Murphy",    "consumer",   "postpaid", "QLD", "active"),
    ("C010", "Lucas Kim",      "enterprise", "postpaid", "NSW", "active"),
    ("C011", "Mia Patel",      "smb",        "prepaid",  "VIC", "churned"),
    ("C012", "Charlie Wilson", "consumer",   "postpaid", "WA",  "active"),
]


# =====================================================================
# File-content builders (written verbatim to the landing Volume)
# =====================================================================

def _csv_line(values: tuple) -> str:
    return ",".join(str(v) for v in values)


def orders_file_bytes() -> bytes:
    header = _csv_line(tuple(c["name"] for c in ORDERS_COLUMNS))
    lines = [header] + [_csv_line(r) for r in ORDERS_ROWS]
    return ("\n".join(lines) + "\n").encode("utf-8")


def signups_file_bytes() -> bytes:
    header = SIGNUPS_DELIM.join(c["name"] for c in SIGNUPS_COLUMNS)
    lines = [header] + [SIGNUPS_DELIM.join(str(v) for v in r) for r in SIGNUPS_ROWS]
    return ("\n".join(lines) + "\n").encode("utf-8")


def file_bytes_for(dataset: str) -> bytes:
    if dataset == SAMPLE_ORDERS["dataset"]:
        return orders_file_bytes()
    if dataset == SAMPLE_SIGNUPS["dataset"]:
        return signups_file_bytes()
    raise KeyError(dataset)


def autoloader_config(ds: dict[str, Any], landing_glob: str) -> dict[str, Any]:
    """The FileSource-shaped Auto Loader config for a sample dataset, matching
    what ``autoloader_advisor.config_from_profile`` would produce — used by the
    seeder (``mark_ready``) so onboarding is deterministic for the multi-char
    .dat (no profiler guesswork)."""
    return {
        "dataset": ds["dataset"],
        "landing_path": landing_glob,
        "file_format": ds["file_format"],
        "delimiter": ds["delimiter"],
        "has_header": ds["has_header"],
        "columns": [{"name": c["name"], "type": c["type"],
                     "is_json": c["is_json"], "source_name": c["name"]}
                    for c in ds["columns"]],
    }


# =====================================================================
# Customers dimension seeding (idempotent CREATE OR REPLACE)
# =====================================================================

def _sql_str(v: str) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def customers_dim_fqn() -> str:
    return f"{dev_catalog()}.{dev_silver_schema()}.customers"


def seed_customers_dim() -> dict[str, Any]:
    """Create the ``customers`` Silver dimension (small, deterministic) so the
    join → Gold ready-made example has a dimension to aggregate against on a
    fresh deploy. Best-effort; never raises. Local/dev (no warehouse) → skip."""
    from agent.event_store import _can_use_dbsql, _conn
    if not _can_use_dbsql():
        return {"ok": True, "skipped": "no warehouse (local dev)"}
    fqn = customers_dim_fqn()
    schema_fqn = f"{dev_catalog()}.{dev_silver_schema()}"
    values = ",\n  ".join(
        "(" + ", ".join([
            _sql_str(r[0]), _sql_str(r[1]), _sql_str(r[2]),
            _sql_str(r[3]), _sql_str(r[4]), _sql_str(r[5]),
        ]) + ")"
        for r in CUSTOMERS_ROWS
    )
    cols = ", ".join(CUSTOMERS_COLUMNS)
    ddl = (
        f"CREATE OR REPLACE TABLE {fqn} ({', '.join(c + ' STRING' for c in CUSTOMERS_COLUMNS)})\n"
        "COMMENT 'Sample customers dimension (seeded) — join target for the "
        "Gold ready-made example'"
    )
    insert = f"INSERT INTO {fqn} ({cols}) VALUES\n  {values}"
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}")
                cur.execute(ddl)
                cur.execute(insert)
        return {"ok": True, "table": fqn, "rows": len(CUSTOMERS_ROWS)}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)[:200], "table": fqn}
