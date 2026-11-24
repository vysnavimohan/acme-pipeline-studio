"""Acme Corp ERP landing feeds for the Data Browser.

The Margin & Cash request (REQ-0142) reads Acme Corp's ERP extracts that land as
CSV files in the `raw_erp` Volume. They are surfaced as ONBOARDED ("ready") Auto
Loader datasets so the analyst can see the sources during the grill, with their
DECLARED column shapes (no landing scan / profiler round-trip on the seed path).

The files really exist at LANDING_ROOT and already feed the medallion
(`cfo_workbench` bronze → silver → gold), so a dataset picked here maps to real,
already-built tables. Module name kept for import compatibility.
"""
from __future__ import annotations

from typing import Any

SOURCE_NAME = "Acme Corp ERP (acme_erp)"
SOURCE_ID = "als-acme-erp"
SYSTEM = "acme_erp"


def _cols(spec: str) -> list[dict[str, str]]:
    out = []
    for part in spec.split(","):
        name, typ = part.split(":")
        out.append({"name": name.strip(), "type": typ.strip().lower()})
    return out


FEEDS: list[dict[str, Any]] = [
    {"dataset": "sales_invoice_lines", "file_format": "csv", "delimiter": ",", "has_header": True,
     "incremental_column": "invoice_date", "primary_key": ["invoice_id", "line_no"],
     "columns": _cols("invoice_id:string,line_no:int,invoice_date:date,customer_id:string,branch_id:string,"
                      "product_id:string,category:string,quantity:int,list_unit_price_usd:double,"
                      "discount_pct:double,net_revenue_usd:double,currency:string,cogs_usd:double")},
    {"dataset": "ar_transactions", "file_format": "csv", "delimiter": ",", "has_header": True,
     "incremental_column": "as_of_date", "primary_key": ["invoice_id", "as_of_date"],
     "columns": _cols("invoice_id:string,customer_id:string,invoice_date:date,due_date:date,"
                      "invoice_amount_usd:double,paid_amount_usd:double,paid_date:date,"
                      "open_balance_usd:double,dispute_date:date,as_of_date:date")},
    {"dataset": "inventory_on_hand", "file_format": "csv", "delimiter": ",", "has_header": True,
     "incremental_column": "snapshot_date", "primary_key": ["snapshot_date", "branch_id", "product_id"],
     "columns": _cols("snapshot_date:date,branch_id:string,product_id:string,quantity_on_hand:double,"
                      "unit_cost_usd:double,last_receipt_date:date")},
    {"dataset": "freight_charges", "file_format": "csv", "delimiter": ",", "has_header": True,
     "incremental_column": "week_start", "primary_key": ["week_start", "branch_id"],
     "columns": _cols("week_start:date,branch_id:string,freight_cost_usd:double")},
    {"dataset": "supplier_rebates", "file_format": "csv", "delimiter": ",", "has_header": True,
     "incremental_column": "period_month", "primary_key": ["supplier_id", "category", "period_month"],
     "columns": _cols("supplier_id:string,category:string,period_month:date,rebate_accrued_usd:double,"
                      "rebate_received_usd:double")},
    {"dataset": "fpa_plan", "file_format": "csv", "delimiter": ",", "has_header": True,
     "incremental_column": "period_month", "primary_key": ["period_month", "category"],
     "columns": _cols("period_month:date,category:string,plan_gross_margin_pct:double")},
]

# Real landing Volume (the files exist; the seed does not scan it).
LANDING_ROOT = "/Volumes/vmohan_demo_catalog/cfo_workbench/raw_erp"


def _landing_glob(dataset: str) -> str:
    return f"{LANDING_ROOT}/{dataset}/*.csv"


def autoloader_config(feed: dict[str, Any]) -> dict[str, Any]:
    """The saved Auto Loader config for a feed, matching the shape the store +
    SOURCE panel expect (dataset, landing_path, file_format, delimiter,
    has_header, columns[{name,type,is_json,source_name}])."""
    return {
        "dataset": feed["dataset"],
        "landing_path": _landing_glob(feed["dataset"]),
        "file_format": feed["file_format"],
        "delimiter": feed["delimiter"],
        "has_header": feed["has_header"],
        "incremental_column": feed.get("incremental_column", ""),
        "primary_key": feed.get("primary_key", []),
        "columns": [
            {"name": c["name"], "type": c["type"], "is_json": False,
             "source_name": c["name"]}
            for c in feed["columns"]
        ],
    }
