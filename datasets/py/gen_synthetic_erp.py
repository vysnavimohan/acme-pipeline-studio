#!/usr/bin/env python3
"""SYNTHETIC Dynamics-ERP-shaped landing files. No real data.
Calibrates to PLAN §4 contract values and writes a manifest of achieved values.
Usage: python3 gen_synthetic_erp.py <out_dir>
"""
import datetime as dt, json, sys
from pathlib import Path
import numpy as np, pandas as pd

ANCHOR = dt.date(2026, 11, 16)            # Monday of the pulse
assert ANCHOR.weekday() == 0
CUTOFF = ANCHOR - dt.timedelta(days=1)    # 2026-11-15 extract cut-off
LAST_WEEK = ANCHOR - dt.timedelta(weeks=1)  # 2026-11-09 .. 2026-11-15
N_WEEKS = 104
WEEKS = [ANCHOR - dt.timedelta(weeks=k) for k in range(N_WEEKS, 0, -1)]
PY_WEEK = LAST_WEEK - dt.timedelta(weeks=52)
OUT = Path(sys.argv[1])
rng = np.random.default_rng(20261116)

T_REV = 27.40e6; T_YOY = 0.031; T_GM = 31.8; T_PLAN_GM = 32.4
T_FAST_DELTA = -1.4; T_DSO = 47.0; T_OVERDUE = 6.2e6; T_INV180 = 4.1e6

CATS = {"Fasteners": (0.22, 31.5), "Power Tools": (0.18, 33.0), "Hand Tools": (0.16, 34.5),
        "Electrical": (0.16, 30.5), "Plumbing": (0.14, 31.0), "Safety": (0.14, 32.8)}
cat_names = list(CATS); cat_p = np.array([v[0] for v in CATS.values()])

# ---------- dimensions ----------
branches = pd.DataFrame({"branch_id": [f"BR{i:02d}" for i in range(1, 13)],
                         "branch_name": [f"Branch {i:02d}" for i in range(1, 13)],
                         "region": [r for r in ["North", "South", "East", "West"] for _ in range(3)]})
rows, pid = [], 0
for cat, (_, base) in CATS.items():
    for k in range(40):
        pid += 1
        cost = float(np.round(rng.lognormal(3.0, 0.7), 2))
        rows.append(dict(product_id=f"P{pid:04d}", sku=f"{cat[:3].upper()}-{k+1:03d}",
                         product_name=f"{cat} item {k+1:02d}", category=cat,
                         unit_cost_usd=cost, list_price_usd=float(np.round(cost / (1 - base / 100), 2))))
products = pd.DataFrame(rows)
prod_by_cat = {c: products[products.category == c].reset_index(drop=True) for c in cat_names}

N_CUST = 1200
ranks = np.arange(1, N_CUST + 1)
w_cust = ranks ** -0.9; w_cust = w_cust / w_cust.sum()
U_LATE = rng.random(N_CUST)  # internal truth (late payers), not landed
PL_BASE = rng.beta(0.8, 2.6, N_CUST)  # per-account late propensity (noisy per invoice, internal truth)
INDUSTRY = rng.choice(["Civil construction", "Manufacturing", "Mining", "Agriculture", "Retail trade", "Government services"],
                      N_CUST, p=[.14, .26, .14, .10, .20, .16])
DISPUTE_P = np.where(INDUSTRY == "Civil construction", 0.01, 0.07)  # disputes are sparse in civil construction
PROMO_CATS = ("Power Tools", "Electrical")
BR_W = np.array([1.6, 1.4, 1.3, 1.2, 1.1, 1.0, 0.9, 0.9, 0.8, 0.8, 0.7, 0.6]); BR_W = BR_W / BR_W.sum()
def promo_week(wk):  # known in advance: every 6th week (promo calendar)
    return ((wk - WEEKS[0]).days // 7) % 6 == 3
def late_for(q):  # per-account late probability; top-200 accounts shifted by q
    return np.where(ranks <= 200, np.clip(PL_BASE + q - 0.5, 0, 1), PL_BASE)
customers = pd.DataFrame({"customer_id": [f"C{r:04d}" for r in ranks], "customer_name": [f"Customer {r:04d}" for r in ranks],
                          "segment": rng.choice(["Trade", "Industrial", "Retail", "Government"], N_CUST, p=[.45, .25, .2, .1]),
                          "industry_segment": INDUSTRY,
                          "payment_terms_days": rng.choice([30, 45, 60], N_CUST, p=[.5, .3, .2]),
                          "credit_limit_usd": np.round(rng.lognormal(11, 0.6, N_CUST), -2), "account_rank": ranks})
terms_c = customers.payment_terms_days.values

# ---------- weekly revenue target series ----------
wi_all = np.arange(N_WEEKS)
T = 26.0e6 * (1 + 0.05 * np.sin(2 * np.pi * wi_all / 52 + 0.6)) * (1 + 0.0005 * wi_all) * (1 + rng.normal(0, 0.012, N_WEEKS))
li = WEEKS.index(LAST_WEEK); pyi = WEEKS.index(PY_WEEK)
T[li] = T_REV; T[pyi] = T_REV / (1 + T_YOY)

# ---------- sales invoices + lines ----------
N_INV = 4000
inv_frames, line_frames, ar_frames = [], [], []
fast_top20_last = None
for wi, wk in enumerate(WEEKS):
    cust_i = rng.choice(N_CUST, N_INV, p=w_cust)
    br = rng.choice(12, N_INV, p=BR_W)
    day = rng.choice(7, N_INV, p=[.22, .22, .2, .18, .14, .02, .02])
    inv_date = np.array([wk + dt.timedelta(days=int(d)) for d in day], dtype=object)
    nl = np.minimum(1 + rng.poisson(2, N_INV), 6)
    inv_idx = np.repeat(np.arange(N_INV), nl); n = len(inv_idx)
    line_no = np.concatenate([np.arange(1, k + 1) for k in nl])
    cats = rng.choice(cat_names, n, p=cat_p)
    pids = np.empty(n, dtype=object); lp = np.empty(n)
    for c in cat_names:
        m = cats == c; pool = prod_by_cat[c]; pick = rng.integers(0, len(pool), m.sum())
        pids[m] = pool.product_id.values[pick]; lp[m] = pool.list_price_usd.values[pick]
    qty = rng.integers(1, 21, n)
    disc = rng.uniform(0, 0.03, n) + np.where(rng.random(n) < 0.5, rng.uniform(0, 0.07, n), 0)
    promo = promo_week(wk)
    if promo:  # promo weeks: more volume and deeper discount in the promo categories
        pm = np.isin(cats, PROMO_CATS)
        qty = np.where(pm, np.round(qty * 1.4), qty).astype(int)
        disc = np.where(pm, disc + 0.04, disc)
    cust_line = cust_i[inv_idx]
    if wi == li:  # discount leakage: Fasteners to top-20 accounts in the anchor week
        leak = (cats == "Fasteners") & (cust_line < 20)
        disc[leak] = rng.uniform(0.12, 0.20, leak.sum())
    r = qty * lp * (1 - disc)
    k = T[wi] / r.sum()  # scale price and volume together so realisation = 1 - discount
    net = r * k
    lp = lp * k
    gm = {c: CATS[c][1] + rng.normal(0, 0.4) for c in cat_names}
    cogs = np.array([net[i] * (1 - gm[cats[i]] / 100) for i in range(0)]) if False else None
    line_frames.append(pd.DataFrame({
        "invoice_id": [f"INV{wi:03d}{j:05d}" for j in inv_idx], "line_no": line_no,
        "invoice_date": inv_date[inv_idx], "customer_id": [f"C{c+1:04d}" for c in cust_line],
        "branch_id": [f"BR{b+1:02d}" for b in br[inv_idx]], "product_id": pids,
        "category": cats, "quantity": qty, "list_unit_price_usd": lp, "discount_pct": np.round(disc, 4),
        "net_revenue_usd": np.round(net, 2), "gm_target_pct": 0.0, "currency": "USD", "_wi": wi, "_top": cust_line < 20}))
    # invoice header (AR basis = invoice total)
    tot = pd.Series(net).groupby(inv_idx).sum()
    inv_frames.append(pd.DataFrame({"invoice_id": [f"INV{wi:03d}{j:05d}" for j in range(N_INV)],
                                    "customer_id_i": cust_i, "invoice_date": inv_date,
                                    "amount_usd": np.round(tot.reindex(range(N_INV)).fillna(0).values, 2), "_wi": wi}))

lines = pd.concat(line_frames, ignore_index=True)
invs = pd.concat(inv_frames, ignore_index=True)
invs["customer_id"] = [f"C{c+1:04d}" for c in invs.customer_id_i]
invs["invoice_date"] = pd.to_datetime(invs.invoice_date)
invs["cust_rank"] = invs.customer_id_i + 1
invs["payment_terms_days"] = terms_c[invs.customer_id_i]
invs["due_date"] = invs.invoice_date + pd.to_timedelta(invs.payment_terms_days, unit="D")
invs["invoice_amount_usd"] = invs.amount_usd

# ---------- gross margin calibration (anchor week solves category offsets) ----------
lines["invoice_date"] = pd.to_datetime(lines.invoice_date)
lw = lines[lines._wi == li]
rev_c = lw.groupby("category").net_revenue_usd.sum(); share = rev_c / rev_c.sum()
F = "Fasteners"; sF = share[F]
others = [c for c in cat_names if c != F]
# actual: Fasteners fixed at base-1.4 => 30.1 (matches plan_F - 1.4); solve uniform delta for others
act_F = CATS[F][1] + T_FAST_DELTA
d = (T_GM - sF * act_F) / (1 - sF) - np.dot(share[others], [CATS[c][1] for c in others]) / (1 - sF)
plan_F = CATS[F][1]
p = (T_PLAN_GM - sF * plan_F) / (1 - sF) - np.dot(share[others], [CATS[c][1] for c in others]) / (1 - sF)
gm_anchor = {F: act_F, **{c: CATS[c][1] + d for c in others}}
plan_gm = {F: plan_F, **{c: CATS[c][1] + p for c in others}}

def gm_for_week(wi):
    return {c: (gm_anchor[c] if wi == li else CATS[c][1] + rng.normal(0, 0.4)) for c in cat_names}

# recompute cogs per week with the category GM (anchor exact)
gm_table = {wi: gm_for_week(wi) for wi in range(N_WEEKS)}
lines["gm_target_pct"] = [gm_table[w][c] for w, c in zip(lines._wi, lines.category)]
lines["cogs_usd"] = np.round(lines.net_revenue_usd * (1 - lines.gm_target_pct / 100), 2)
lines = lines.drop(columns=["gm_target_pct", "_wi", "_top"])
lines["line_key_check"] = 1  # placeholder removed below
lines = lines.drop(columns=["line_key_check"])
lines["invoice_date"] = lines.invoice_date.dt.date
lines["cust_rank"] = lines.customer_id.str[1:].astype(int)

# ---------- AR calibration: DSO and past-due exposure (top 200) ----------
CUT = pd.Timestamp(CUTOFF)
inv_ord = (invs.invoice_date - pd.Timestamp("2000-01-01")).dt.days.values
cut_ord = (CUT - pd.Timestamp("2000-01-01")).days
amt = invs.invoice_amount_usd.values
terms_i = invs.payment_terms_days.values
rank_i = invs.cust_rank.values
noise_i = rng.normal(0, 6, len(invs))  # invoice-level payment noise (makes lateness noisy, not a switch)
disp_i = rng.random(len(invs)) < DISPUTE_P[invs.customer_id_i.values]  # disputed invoices (landed as dispute_date)
U_INV = rng.random(len(invs))
_rev90_c = invs[(inv_ord >= cut_ord - 89) & (inv_ord <= cut_ord)].groupby("customer_id_i").invoice_amount_usd.sum()
_top_set = set(_rev90_c.sort_values(ascending=False).index[:200])
top200_i = invs.customer_id_i.isin(_top_set).values

def ar_metrics(s, m, q):
    p_late = np.clip(late_for(q)[invs.customer_id_i.values] + 0.25 * disp_i, 0, 1)
    late_i = U_INV < p_late
    days = terms_i + noise_i + late_i * m + s
    pay = inv_ord + np.round(days).astype(int)
    is_open = pay > cut_ord
    rev90 = amt[(inv_ord >= cut_ord - 89) & (inv_ord <= cut_ord)].sum()
    open_bal = amt[is_open].sum()
    dso = open_bal / (rev90 / 90)
    dpd = cut_ord - (inv_ord + terms_i)
    over = is_open & (dpd > 30) & top200_i  # top-200 by trailing 90-day revenue (same rule as gold_customer_rank)
    return dso, amt[over].sum(), is_open, pay

def solve_s(m, q):
    lo, hi = -60.0, 120.0
    for _ in range(60):
        mid = (lo + hi) / 2
        if ar_metrics(mid, m, q)[0] < T_DSO: lo = mid
        else: hi = mid
    return (lo + hi) / 2

best = None
for q in np.arange(0.50, 0.71, 0.02):  # refined around the coarse optimum (0.6, ~46 days)
    for m in np.arange(36, 57, 2.0):
        s_ = solve_s(m, q); dso_, ov, _, _ = ar_metrics(s_, m, q)
        err = abs(ov - T_OVERDUE) + 1e3 * abs(dso_ - T_DSO)
        if best is None or err < best[0]: best = (err, m, s_, q)
_, m_best, s_best, q_best = best
dso, overdue, is_open, pay = ar_metrics(s_best, m_best, q_best)
invs["paid_date"] = [(pd.Timestamp("2000-01-01") + pd.Timedelta(days=int(x))).date() if not o else None
                     for x, o in zip(pay, is_open)]
invs["paid_amount_usd"] = np.where(is_open, 0.0, amt)
invs["open_balance_usd"] = np.where(is_open, amt, 0.0)
invs["dispute_date"] = [(d + dt.timedelta(days=int(k))) if x else None for d, k, x in
                        zip(invs.invoice_date.dt.date, rng.integers(5, 31, len(invs)), disp_i)]
ar = invs[["invoice_id", "customer_id", "invoice_date", "due_date", "invoice_amount_usd", "paid_amount_usd", "paid_date", "open_balance_usd", "dispute_date"]].copy()
ar["invoice_date"] = ar.invoice_date.dt.date; ar["due_date"] = ar.due_date.dt.date
ar["as_of_date"] = CUTOFF

# ---------- inventory snapshot (aged >180d value calibrated) ----------
inv_rows = []
for b in branches.branch_id:
    keep = rng.random(len(products)) < 0.45
    sub = products[keep]
    for _, pr in sub.iterrows():
        inv_rows.append(dict(snapshot_date=CUTOFF, branch_id=b, product_id=pr.product_id,
                             quantity_on_hand=float(rng.integers(20, 300)), unit_cost_usd=pr.unit_cost_usd,
                             last_receipt_date=CUTOFF - dt.timedelta(days=int(min(rng.exponential(70), 400)))))
inv = pd.DataFrame(inv_rows)
inv["age_days"] = (pd.Timestamp(CUTOFF) - pd.to_datetime(inv.last_receipt_date)).dt.days
aged = inv.age_days > 180
aged_val = (inv.quantity_on_hand * inv.unit_cost_usd)[aged].sum()
f = T_INV180 / aged_val
inv.loc[aged, "quantity_on_hand"] = np.round(inv.loc[aged, "quantity_on_hand"] * f, 2)
inv["last_receipt_date"] = pd.to_datetime(inv.last_receipt_date).dt.date
inv = inv.drop(columns=["age_days"])

# ---------- freight, rebates, plan ----------
wk_br = lines.copy(); wk_br["week_start"] = pd.to_datetime(wk_br.invoice_date) - pd.to_timedelta(pd.to_datetime(wk_br.invoice_date).dt.day_of_week, unit="D")
fr = wk_br.groupby(["week_start", "branch_id"]).net_revenue_usd.sum().reset_index()
fr["freight_cost_usd"] = np.round(fr.net_revenue_usd * (0.031 + rng.normal(0, 0.004, len(fr))), 2)
fr["week_start"] = fr.week_start.dt.date
fr = fr[["week_start", "branch_id", "freight_cost_usd"]]
sup = pd.DataFrame({"supplier_id": [f"S{i:02d}" for i in range(1, 9)], "supplier_name": [f"Supplier {i:02d}" for i in range(1, 9)],
                    "category": [cat_names[i % 6] for i in range(8)]})
reb = []
for _, s_ in sup.iterrows():
    for mth in ["2026-08", "2026-09", "2026-10", "2026-11"]:
        reb.append(dict(supplier_id=s_.supplier_id, category=s_.category, period_month=mth,
                        rebate_accrued_usd=round(float(rng.uniform(40e3, 90e3)), 2),
                        rebate_received_usd=round(float(rng.uniform(20e3, 80e3)), 2)))
rebates = pd.DataFrame(reb)
plan = pd.DataFrame({"period_month": "2026-11", "category": cat_names,
                     "plan_gross_margin_pct": [round(plan_gm[c], 4) for c in cat_names]})

# ---------- achieved manifest ----------
lw2 = lines[pd.to_datetime(lines.invoice_date) >= pd.Timestamp(LAST_WEEK)]
gm_total = (lw.net_revenue_usd.sum() - (lw.net_revenue_usd * (1 - lw.gm_target_pct / 100)).sum()) / lw.net_revenue_usd.sum() * 100
actual_gm = 100 * (1 - lw2.cogs_usd.sum() / lw2.net_revenue_usd.sum())
lw_rev = lw2.net_revenue_usd.sum()
py_rev = lines[pd.to_datetime(lines.invoice_date) >= pd.Timestamp(PY_WEEK)].query("invoice_date < @PY_WEEK + pd.Timedelta(days=7)").net_revenue_usd.sum() if False else float(T[pyi])
fast_df = lw2[lw2.category == F]
fast_gm = 100 * (1 - fast_df.cogs_usd.sum() / fast_df.net_revenue_usd.sum())
plan_total = sum(share[c] * plan_gm[c] for c in cat_names)
inv_aged_val = (inv.quantity_on_hand * inv.unit_cost_usd)[aged.values].sum() if False else float(inv.loc[inv.last_receipt_date.apply(lambda x: (CUTOFF - x).days > 180), ["quantity_on_hand", "unit_cost_usd"]].prod(axis=1).sum())
ov_check = float(overdue)
manifest = {
    "anchor_monday": str(ANCHOR), "cutoff": str(CUTOFF), "last_full_week": str(LAST_WEEK), "prior_year_week": str(PY_WEEK),
    "weekly_revenue_usd": {"target": T_REV, "achieved": round(float(lw_rev), 2)},
    "yoy_pct": {"target": 3.1, "achieved": round(100 * (lw_rev / float(T[pyi]) - 1), 3)},
    "gm_pct": {"target": T_GM, "achieved": round(float(actual_gm), 3)},
    "plan_gm_pct": {"target": T_PLAN_GM, "achieved": round(float(plan_total), 3)},
    "gm_vs_plan_bps": {"target": -60, "achieved": round((float(actual_gm) - float(plan_total)) * 100, 1)},
    "fasteners_gm_vs_plan_bps": {"target": -140, "achieved": round((fast_gm - plan_F) * 100, 1)},
    "dso_days": {"target": T_DSO, "achieved": round(float(dso), 2)},
    "overdue_top200_usd": {"target": T_OVERDUE, "achieved": round(ov_check, 2), "definition": "open balance of invoices >30 days past due, top-200 ranked accounts, as of cutoff"},
    "inventory_aged_gt180_usd": {"target": T_INV180, "achieved": round(float(inv_aged_val), 2)},
    "calibration": {"late_payer_delay_days": float(m_best), "payment_shift_days": round(float(s_best), 3), "top200_late_share": float(q_best)},
}
manifest["forecast_13w_sum_target_usd"] = 356e6  # checked after ML fit, see forecast manifest
OUT.mkdir(parents=True, exist_ok=True)
for sub in ["dim_branch", "dim_product", "dim_customer", "dim_supplier", "sales_invoice_lines", "ar_transactions", "inventory_on_hand", "freight_charges", "supplier_rebates", "fpa_plan"]:
    (OUT / sub).mkdir(exist_ok=True)
branches.to_csv(OUT / "dim_branch/branches.csv", index=False)
products.to_csv(OUT / "dim_product/products.csv", index=False)
# Dataverse-style change feed: one row per change. Current truth is the latest version per customer.
# ~15% of customers changed credit terms / industry / account manager (earlier version = previous values);
# three archived duplicate records are created and then deleted (is_deleted flag). versionnumber is monotonic.
AM = [f"AM{k:02d}" for k in range(1, 9)]
changed = rng.random(N_CUST) < 0.15
truth = customers.assign(account_manager=rng.choice(AM, N_CUST), is_deleted=False,
                         modifiedon=pd.Timestamp("2026-10-01") + pd.to_timedelta(rng.integers(0, 45, N_CUST), unit="D"),
                         versionnumber=5000 + np.arange(N_CUST))
prev = truth[changed].copy()
prev["payment_terms_days"] = [int(rng.choice([t for t in (30, 45, 60) if t != x])) for x in prev.payment_terms_days]
INDUSTRY_LIST = ["Civil construction", "Manufacturing", "Mining", "Agriculture", "Retail trade", "Government services"]
prev["industry_segment"] = [str(rng.choice([s for s in INDUSTRY_LIST if s != x])) for x in prev.industry_segment]
prev["account_manager"] = rng.choice(AM, len(prev))
prev["modifiedon"] = pd.Timestamp("2025-06-01") + pd.to_timedelta(rng.integers(0, 300, len(prev)), unit="D")
prev["versionnumber"] = 1000 + np.arange(len(prev))
truth.loc[changed, "versionnumber"] = 5000 + np.flatnonzero(changed)
deleted = pd.DataFrame({"customer_id": [f"C9{k:03d}" for k in range(1, 4)], "customer_name": [f"Duplicate record {k}" for k in range(1, 4)],
                        "segment": "Trade", "industry_segment": "Manufacturing", "payment_terms_days": 30,
                        "credit_limit_usd": 0.0, "account_rank": 0, "account_manager": "AM01", "is_deleted": False,
                        "modifiedon": pd.Timestamp("2026-02-01"), "versionnumber": 100 + np.arange(3)})
deleted_flag = deleted.copy(); deleted_flag["is_deleted"] = True
deleted_flag["modifiedon"] = pd.Timestamp("2026-03-01"); deleted_flag["versionnumber"] = 200 + np.arange(3)
feed = pd.concat([prev, truth, deleted, deleted_flag], ignore_index=True)
feed = feed.drop_duplicates(subset=["customer_id", "versionnumber"]).sort_values("versionnumber")
feed["modifiedon"] = pd.to_datetime(feed.modifiedon).dt.strftime("%Y-%m-%d %H:%M:%S")
feed.to_csv(OUT / "dim_customer/customers.csv", index=False)
sup.to_csv(OUT / "dim_supplier/suppliers.csv", index=False)
for w in range(N_WEEKS):
    ws = WEEKS[w]
    lines[pd.to_datetime(lines.invoice_date) - pd.to_timedelta(pd.to_datetime(lines.invoice_date).dt.day_of_week, unit="D") == pd.Timestamp(ws)].to_csv(
        OUT / f"sales_invoice_lines/sales_invoice_lines_{ws}.csv", index=False)
ar.to_csv(OUT / f"ar_transactions/ar_transactions_{CUTOFF}.csv", index=False)
inv.to_csv(OUT / f"inventory_on_hand/inventory_on_hand_{CUTOFF}.csv", index=False)
fr.to_csv(OUT / "freight_charges/freight_charges_weekly.csv", index=False)
rebates.to_csv(OUT / "supplier_rebates/supplier_rebates_monthly.csv", index=False)
plan.to_csv(OUT / "fpa_plan/plan_gross_margin.csv", index=False)
(OUT / "manifest.json").write_text(json.dumps(manifest, indent=2))
print(json.dumps(manifest, indent=2))
