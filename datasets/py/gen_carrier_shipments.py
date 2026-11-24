"""Deterministic synthetic carrier shipments for the RAW delivery-times demo (CONTRACT3, owner P).

Writes one CSV per order day: shipments_YYYY-MM-DD.csv for the 26 weeks ending 2026-11-15 (182 days),
into a local folder, and with --upload into the volume
  /Volumes/$CATALOG/studio_control/raw_landing/carrier_shipments/

Planted story (fixed seed, so re-runs give the same bytes):
  - overall on-time (delivered_date <= promised_date) about 91%
  - carrier FastFreight is late about 18% of the time in the East region (other lanes ~8.5%)
  - average order-to-delivery about 3.2 days
  - latest full week is the week of 2026-11-09 (Mon-Sun)
Dirty rows on purpose (for the DQ expectations to catch): duplicate shipment_id rows, null branch_id,
delivered_date before despatch_date.

Usage:
  python gen_carrier_shipments.py --out /tmp/acme_carrier --stats
  CATALOG=<catalog> DATABRICKS_CONFIG_PROFILE=<profile> python gen_carrier_shipments.py --upload
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import pathlib
import random
import statistics
import sys
import tempfile

SEED = 20261115
END = dt.date(2026, 11, 15)          # last day of the 26-week window (a Sunday)
DAYS = 182                            # 26 weeks, Monday 2026-05-18 .. Sunday 2026-11-15
START = END - dt.timedelta(days=DAYS - 1)
import os
VOLUME_DIR = f"/Volumes/{os.environ.get('CATALOG', 'main')}/studio_control/raw_landing/carrier_shipments"
COLUMNS = ["shipment_id", "order_id", "branch_id", "carrier", "order_date", "promised_date", "despatch_date",
           "delivered_date", "status", "weight_kg", "freight_cost_usd"]

# Branch ids and regions match <catalog>.cfo_workbench.gold_dim_branch.
REGIONS = {**{f"BR{i:02d}": "North" for i in (1, 2, 3)}, **{f"BR{i:02d}": "South" for i in (4, 5, 6)},
           **{f"BR{i:02d}": "East" for i in (7, 8, 9)}, **{f"BR{i:02d}": "West" for i in (10, 11, 12)}}
CARRIERS = [("FastFreight", 0.22, 1.12), ("SwiftLine", 0.22, 1.00), ("NorthWay", 0.20, 0.97),
            ("CargoBridge", 0.19, 1.03), ("BlueRoute", 0.17, 0.95)]
ON_TIME_BASE = 0.915                  # every lane except the planted one
PLANTED = ("FastFreight", "East")
PLANTED_ON_TIME = 0.825               # -> about 18% late on that lane
PROMISE_LEAD = [(2, 0.15), (3, 0.45), (4, 0.28), (5, 0.12)]
DESPATCH_OFFSET = [(0, 0.50), (1, 0.40), (2, 0.10)]
RETURNED_RATE = 0.015
PER_DAY = (230, 320)
DUP_ROWS, NULL_BRANCH_ROWS, DELIVERED_BEFORE_DESPATCH_ROWS = 25, 20, 15


def _pick(rng: random.Random, pairs):
    x, acc = rng.random(), 0.0
    for value, p in pairs:
        acc += p
        if x < acc:
            return value
    return pairs[-1][0]


def _p_on_time(carrier: str, region: str) -> float:
    return PLANTED_ON_TIME if (carrier, region) == PLANTED else ON_TIME_BASE


def generate() -> tuple[dict[dt.date, list[list[str]]], dict]:
    """Return {order_date: [row, ...]} (CSV rows as strings, header excluded) plus planted-story stats."""
    rng = random.Random(SEED)
    branches = sorted(REGIONS)
    carrier_names = [c[0] for c in CARRIERS]
    carrier_w = [(c[0], c[1]) for c in CARRIERS]
    factor = {c[0]: c[2] for c in CARRIERS}
    by_day: dict[dt.date, list[list[str]]] = {}
    clean_rows: list[dict] = []   # structured, for the stats (before dirty rows are injected)
    seq = 0
    for k in range(DAYS):
        day = START + dt.timedelta(days=k)
        rows: list[list[str]] = []
        for _ in range(rng.randint(*PER_DAY)):
            seq += 1
            branch = rng.choice(branches)
            region = REGIONS[branch]
            carrier = _pick(rng, carrier_w)
            promised = day + dt.timedelta(days=_pick(rng, PROMISE_LEAD))
            despatch = min(day + dt.timedelta(days=_pick(rng, DESPATCH_OFFSET)), promised)
            status, delivered = "delivered", None
            if rng.random() < RETURNED_RATE:
                status = "returned"
            elif rng.random() < _p_on_time(carrier, region):
                # On time: arrives on the promised day or the day before (never before despatch).
                delivered = max(despatch, promised - dt.timedelta(days=rng.choice([0, 0, 1])))
            else:
                delivered = promised + dt.timedelta(days=rng.choice([1, 1, 2, 3]))
            if delivered is not None and delivered > END:
                status, delivered = "in_transit", None
            weight = round(min(max(rng.lognormvariate(2.6, 0.9), 0.5), 900.0), 1)
            freight = round((12.0 + 1.15 * weight) * factor[carrier] * rng.uniform(0.92, 1.08), 2)
            row = {
                "shipment_id": f"SHP-{seq:06d}", "order_id": f"SO-{700000 + seq}", "branch_id": branch,
                "carrier": carrier, "order_date": day.isoformat(), "promised_date": promised.isoformat(),
                "despatch_date": despatch.isoformat(),
                "delivered_date": delivered.isoformat() if delivered else "",
                "status": status, "weight_kg": f"{weight:.1f}", "freight_cost_usd": f"{freight:.2f}",
            }
            clean_rows.append(row)
            rows.append([row[c] for c in COLUMNS])
        by_day[day] = rows

    # Dirty rows, placed deterministically.
    picker = random.Random(SEED + 1)
    days = sorted(by_day)
    for _ in range(DUP_ROWS):          # duplicate shipment_id, landed in the next day's file
        src = picker.choice(clean_rows)
        d = dt.date.fromisoformat(src["order_date"]) + dt.timedelta(days=1)
        target = by_day.get(d, by_day[days[-1]])
        target.append([src[c] for c in COLUMNS])
    for _ in range(NULL_BRANCH_ROWS):  # null branch
        d = picker.choice(days)
        r = picker.choice(by_day[d])
        r[COLUMNS.index("branch_id")] = ""
    for _ in range(DELIVERED_BEFORE_DESPATCH_ROWS):  # delivered before despatch
        d = picker.choice(days)
        r = picker.choice(by_day[d])
        i_dsp, i_del, i_st = COLUMNS.index("despatch_date"), COLUMNS.index("delivered_date"), COLUMNS.index("status")
        if r[i_st] == "delivered":
            r[i_del] = (dt.date.fromisoformat(r[i_dsp]) - dt.timedelta(days=1)).isoformat()

    stats = story_stats(clean_rows)
    stats["rows_total"] = sum(len(v) for v in by_day.values())
    return by_day, stats


def story_stats(rows: list[dict]) -> dict:
    """Planted-story numbers on the clean rows (before the dirty rows were injected)."""
    delivered = [r for r in rows if r["delivered_date"]]
    on_time = [r for r in delivered if r["delivered_date"] <= r["promised_date"]]
    fast_east = [r for r in delivered if (r["carrier"], REGIONS[r["branch_id"]]) == PLANTED]
    fast_east_late = [r for r in fast_east if r["delivered_date"] > r["promised_date"]]
    lead = [(dt.date.fromisoformat(r["delivered_date"]) - dt.date.fromisoformat(r["order_date"])).days
            for r in delivered]
    latest_week = dt.date(2026, 11, 9)
    lw = [r for r in rows if latest_week <= dt.date.fromisoformat(r["order_date"]) <= latest_week + dt.timedelta(days=6)]
    lw_delivered = [r for r in lw if r["delivered_date"]]
    return {
        "on_time_pct": round(100 * len(on_time) / len(delivered), 2),
        "fastfreight_east_late_pct": round(100 * len(fast_east_late) / len(fast_east), 2) if fast_east else None,
        "avg_order_to_delivery_days": round(statistics.fmean(lead), 2),
        "latest_full_week": latest_week.isoformat(),
        "latest_week_on_time_pct": round(100 * sum(r["delivered_date"] <= r["promised_date"] for r in lw_delivered)
                                         / len(lw_delivered), 2) if lw_delivered else None,
        "latest_week_shipments": len(lw),
        "carriers": sorted({r["carrier"] for r in rows}),
    }


def write_files(by_day: dict[dt.date, list[list[str]]], out: pathlib.Path) -> list[pathlib.Path]:
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for day in sorted(by_day):
        p = out / f"shipments_{day.isoformat()}.csv"
        with p.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(COLUMNS)
            w.writerows(by_day[day])
        paths.append(p)
    return paths


def upload(paths: list[pathlib.Path]) -> None:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    try:
        w.files.create_directory(VOLUME_DIR)
    except Exception:  # noqa: BLE001  (already exists)
        pass
    for p in paths:
        w.files.upload(f"{VOLUME_DIR}/{p.name}", io.BytesIO(p.read_bytes()), overwrite=True)
    print(f"uploaded {len(paths)} files to {VOLUME_DIR}")


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", help="local folder for the CSVs (default: a new temp dir)")
    ap.add_argument("--upload", action="store_true", help="also upload the files to the raw_landing volume")
    ap.add_argument("--stats", action="store_true", help="print planted-story numbers")
    args = ap.parse_args(argv)
    by_day, stats = generate()
    out = pathlib.Path(args.out) if args.out else pathlib.Path(tempfile.mkdtemp(prefix="acme_carrier_"))
    paths = write_files(by_day, out)
    print(f"wrote {len(paths)} files, {stats['rows_total']} rows to {out}")
    if args.stats or not args.upload:
        for k, v in stats.items():
            print(f"  {k}: {v}")
    if args.upload:
        upload(paths)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
