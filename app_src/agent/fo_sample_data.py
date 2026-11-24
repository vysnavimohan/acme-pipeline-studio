"""Perpetual Corporate Trust — Funding Optimisation (FO) sample data.

The Perpetual-themed reskin of the accelerator. PCT runs the same funding process
across many securitisation **facilities** (warehouses / deals), each with different
eligibility rules, concentration limits and advance rates — literally "one framework,
many parameters, many clients". The sample here is a daily **loan-level pool tape**:
one row per loan, landing as a CSV in the landing Volume (Auto Loader), that builds
Bronze → Silver (eligible collateral) → Gold (pool allocation / borrowing base).

Synthetic data only. Mirrors the fields the real PCT Funding Optimisation Console
(the fo-app) reads, so the generated app matches the deployed reference.
"""

from __future__ import annotations

import os
import random
from typing import Any


def dev_catalog() -> str:
    return os.environ.get("DEV_CATALOG", "pipeline_studio_catalog")


# =====================================================================
# Loan pool tape — one row per loan (the securitisation "tape")
# =====================================================================

POOL_TAPE_COLUMNS = [
    {"name": "loan_id", "type": "string", "is_json": False},
    {"name": "facility", "type": "string", "is_json": False},        # the funding program / warehouse (tenant)
    {"name": "asset_class", "type": "string", "is_json": False},     # rmbs | auto | sme
    {"name": "originator", "type": "string", "is_json": False},
    {"name": "state", "type": "string", "is_json": False},           # AU state
    {"name": "current_balance", "type": "double", "is_json": False},
    {"name": "credit_score", "type": "long", "is_json": False},
    {"name": "ltv", "type": "double", "is_json": False},             # loan-to-value ratio
    {"name": "arrears_days", "type": "long", "is_json": False},
    {"name": "interest_rate", "type": "double", "is_json": False},
    {"name": "seasoning_months", "type": "long", "is_json": False},
    {"name": "as_of_date", "type": "date", "is_json": False},
]

_FACILITIES = [
    ("warehouse_rmbs_a", "rmbs", "Pepper Home Loans"),
    ("warehouse_auto_b", "auto", "Angle Finance"),
]
_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "ACT", "NT"]
_STATE_W = [0.33, 0.26, 0.19, 0.09, 0.07, 0.02, 0.02, 0.02]  # NSW/VIC/QLD dominate (realistic)


def _gen_rows(n: int = 600, seed: int = 7) -> list[tuple]:
    """Deterministic synthetic pool tape. RMBS = larger balances, lower LVR;
    auto = smaller balances, shorter seasoning."""
    rng = random.Random(seed)
    rows: list[tuple] = []
    for i in range(n):
        fac, asset, originator = rng.choice(_FACILITIES)
        state = rng.choices(_STATES, weights=_STATE_W)[0]
        if asset == "rmbs":
            bal = round(rng.uniform(180_000, 950_000), 2)
            ltv = round(rng.uniform(0.40, 0.95), 3)
            rate = round(rng.uniform(0.045, 0.079), 4)
            seasoning = rng.randint(3, 120)
        else:  # auto
            bal = round(rng.uniform(8_000, 65_000), 2)
            ltv = round(rng.uniform(0.60, 1.05), 3)
            rate = round(rng.uniform(0.069, 0.135), 4)
            seasoning = rng.randint(1, 60)
        score = rng.randint(520, 830)
        # most loans current; a tail in arrears
        arrears = 0 if rng.random() < 0.82 else rng.choice([15, 30, 45, 60, 90, 120])
        rows.append((
            f"L{i:05d}", fac, asset, originator, state,
            bal, score, ltv, arrears, rate, seasoning, "2026-07-31",
        ))
    return rows


POOL_TAPE_ROWS = _gen_rows()

SAMPLE_POOL_TAPE: dict[str, Any] = {
    "dataset": "fo_pool_tape",
    "filename": "fo_pool_tape_2026-07-31.csv",
    "file_format": "csv",
    "delimiter": ",",
    "has_header": True,
    "columns": POOL_TAPE_COLUMNS,
    "description": ("Monthly loan-level pool tape (CSV) — one row per loan across "
                    "PCT securitisation facilities. The FO medallion source."),
}


def _csv_line(values: tuple) -> str:
    return ",".join(str(v) for v in values)


def pool_tape_file_bytes() -> bytes:
    header = _csv_line(tuple(c["name"] for c in POOL_TAPE_COLUMNS))
    lines = [header] + [_csv_line(r) for r in POOL_TAPE_ROWS]
    return ("\n".join(lines) + "\n").encode("utf-8")


def autoloader_config(landing_glob: str) -> dict[str, Any]:
    ds = SAMPLE_POOL_TAPE
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
