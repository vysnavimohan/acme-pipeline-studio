"""Governance API — cost, logging, and budgets from Unity AI Gateway.

Endpoints:
  GET  /api/governance/request/{request_id}  — cost breakdown for a request
  GET  /api/governance/account                — account-level rollup + budgets
  GET  /api/governance/budgets                — account spend budgets (read-only)
"""

from __future__ import annotations

import json
import logging
from typing import Any

import re

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from agent import event_store

LOG = logging.getLogger("ops.governance")

router = APIRouter()


def _table_fq(name: str) -> str:
    """Return fully-qualified table name in the control catalog."""
    return f"{event_store._CATALOG}.{event_store._CONTROL_SCHEMA}.{name}"


def _workspace_id() -> str:
    """Resolve THIS workspace's id at runtime (never hardcode — a committed id is
    wrong on every workspace but the one it was authored on). Databricks Apps
    inject DATABRICKS_WORKSPACE_ID; otherwise derive from the Azure host
    `adb-<id>.<n>.azuredatabricks.net` in DATABRICKS_HOST."""
    import os
    wid = (os.environ.get("DATABRICKS_WORKSPACE_ID") or "").strip()
    if wid:
        return wid
    host = (os.environ.get("DATABRICKS_HOST") or "").strip()
    m = re.search(r"adb-(\d+)\.", host)
    if m:
        return m.group(1)
    return "unknown"


def _query(sql: str, params: tuple | None = None) -> list[dict[str, Any]]:
    """Execute a read-only query against Databricks SQL."""
    if not event_store._can_use_dbsql():
        return []
    try:
        with event_store._conn() as conn:
            with conn.cursor() as cur:
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                cols = [d[0] for d in cur.description] if cur.description else []
                return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as e:
        LOG.warning("query failed: %s", e)
        return []


def _load_config() -> dict:
    """Load governance config (price map, catalog) from config.yaml.

    config.yaml lives at the repo root, but the deployed app is packaged with
    varying roots (sometimes src/ is the app root). Search a few candidate
    locations relative to this file plus the CWD so it resolves both locally
    and in the deployed App container. An explicit OPS_CONFIG_PATH wins.
    """
    import os
    from pathlib import Path
    try:
        import yaml
    except Exception as e:  # pragma: no cover
        LOG.warning("pyyaml unavailable: %s", e)
        return {}

    here = Path(__file__).resolve()
    candidates = []
    env_path = os.environ.get("OPS_CONFIG_PATH")
    if env_path:
        candidates.append(Path(env_path))
    # Walk up several levels (routes→server→app→src→repo-root and beyond).
    for up in (3, 4, 5, 6):
        try:
            candidates.append(here.parents[up] / "config.yaml")
        except IndexError:
            pass
    candidates.append(Path.cwd() / "config.yaml")
    for cp in candidates:
        try:
            if cp and cp.exists():
                with open(cp) as f:
                    cfg = yaml.safe_load(f) or {}
                    return cfg.get("governance", {}) or {}
        except Exception as e:
            LOG.warning("failed to read config at %s: %s", cp, e)
    LOG.warning("config.yaml not found in any candidate location")
    return {}


# Hard fallback if config.yaml has no governance.price_map. The governed
# endpoint resolves to Claude Opus 4.8, so default to Opus rates (NOT Sonnet).
_OPUS_RATES = {"input_per_million_tokens": 15.00, "output_per_million_tokens": 75.00}


def _get_price_map() -> dict[str, dict[str, float]]:
    """Return the model-keyed price map from config.yaml.

    Shape: {model_key: {input_per_million_tokens, output_per_million_tokens}, ...}
    where model_key is matched (case-insensitive substring) against the payload
    table's `destination_model` (e.g. 'system.ai.databricks-claude-opus-4-8').
    """
    cfg = _load_config()
    price_map = cfg.get("price_map") or {}
    if not price_map:
        # Config missing → Opus rates (the current governed destination).
        return {"default": dict(_OPUS_RATES)}
    return price_map


def _rates_for_model(model: str | None) -> dict[str, float]:
    """Resolve per-1M input/output rates for a destination_model via the price
    map. Match by longest key that is a case-insensitive substring of the model
    name; else the 'default' key; else Opus rates."""
    price_map = _get_price_map()
    m = (model or "").lower()
    best_key = None
    for key in price_map:
        if key == "default":
            continue
        if key.lower() in m and (best_key is None or len(key) > len(best_key)):
            best_key = key
    if best_key is not None:
        return price_map[best_key]
    return price_map.get("default") or dict(_OPUS_RATES)


def _estimate_cost(input_tokens: int, output_tokens: int,
                   model: str | None = None) -> float:
    """Estimate USD cost from token counts, using the model-specific rate."""
    rates = _rates_for_model(model)
    input_cost = (input_tokens / 1_000_000) * rates.get("input_per_million_tokens", 15.00)
    output_cost = (output_tokens / 1_000_000) * rates.get("output_per_million_tokens", 75.00)
    return input_cost + output_cost


# The real Unity AI Gateway inference (payload) table + per-request cost view.
# These are the actual objects the governed calls land in — everything on the
# Governance + Guardrails tabs is derived from them, never simulated.
_PAYLOAD_TABLE = f"{event_store._CATALOG}.ai_gov_logs.claude_governed_payload"
_COST_VIEW = f"{event_store._CATALOG}.ai_gov_logs.governed_cost_by_request"


# ============================================================================
# DEMO-SCALE SEEDED GOVERNANCE MODEL (self-consistent; grounded in real metering)
# ============================================================================
# The live account-wide system-table totals are far larger than a single demo's
# story ($2.6k+ / $211k). For the workshop the Governance page shows a SEEDED,
# demo-scale, SELF-RECONCILING dataset whose STRUCTURE mirrors the real metering
# model (so it stays credible), with everything labelled "estimated / demo".
#
# Metering basis (surfaced as tooltips in the UI):
#  - FMAPI (Foundation Model API / model-serving LLM): system.billing.usage where
#    billing_origin_product='MODEL_SERVING', usage_type='TOKEN',
#    product_features.serving_type='FOUNDATION_MODEL' — DBU × list price. Per-call
#    token shape from system.ai_gateway.usage (event_time, destination_model,
#    input_tokens, output_tokens, requester, request_tags). Docs: ai-gateway/
#    usage-tracking, admin/system-tables/billing.
#  - Genie Code: system.billing.usage where billing_origin_product='GENIE' AND
#    usage_metadata.genie.surface='GENIE_CODE', sku_name<>'GENIE_FREE_USAGE' —
#    DBUs × ~$0.07/DBU (regional SRTI SKU; list_prices in the real path). 150 free
#    DBU/user/mo; service principals get none. Docs: genie/monitor-cost,
#    admin/system-tables/assistant.
#
# Reconciliation is BY CONSTRUCTION: per-request $ is computed from the seeded
# tokens × the model rate (_estimate_cost, same method as everything else), and
# each rollup total is the SUM of the rows below it — so the page cannot show a
# row/total contradiction.

# Per-request FMAPI seed — attributed to each tracker request. (model, input, output).
_SEED_REQUESTS = [
    {"request_id": "cf0a4207-b24d-4f1e-9a1e-000000000001",
     "title": "Margin & Cash: Genie Agent + tracker dashboard (REQ-0142)",
     "model": "databricks-claude-opus-4-8",   "input_tokens": 1_000_000, "output_tokens": 400_000},
    {"request_id": "a1000001-0001-4001-8001-000000000001",
     "title": "Weekly sales by branch: metric view",
     "model": "databricks-claude-sonnet-4-6", "input_tokens": 2_000_000, "output_tokens": 800_000},
    {"request_id": "a1000002-0002-4002-8002-000000000002",
     "title": "Supplier rebate accruals: Gold + metric view",
     "model": "databricks-claude-sonnet-4-6", "input_tokens": 3_000_000, "output_tokens": 900_000},
    {"request_id": "a1000003-0003-4003-8003-000000000003",
     "title": "AR ageing Gold + collections worklist",
     "model": "databricks-claude-sonnet-4-6", "input_tokens": 1_800_000, "output_tokens": 700_000},
    {"request_id": "a1000004-0004-4004-8004-000000000004",
     "title": "Inventory ageing metric view",
     "model": "databricks-claude-sonnet-4-6", "input_tokens": 1_400_000, "output_tokens": 520_000},
    {"request_id": "a1000005-0005-4005-8005-000000000005",
     "title": "Freight cost by branch: Gold",
     "model": "databricks-claude-opus-4-8",   "input_tokens": 1_000_000, "output_tokens": 500_000},
]

# Genie Code DBU seed — Acme Corp Genie context, ATTRIBUTED per tracker request.
# request_id -> [ {label, dbus}, ... ]. Sums across all requests must equal the
# 170 DBU / $11.90 total. Requests absent here have no Genie usage ($0.00).
_GENIE_DBU_RATE = 0.07  # ~SRTI regional SKU $/DBU (list_prices in the real path)
_SEED_GENIE_BY_REQUEST = {
    "cf0a4207-b24d-4f1e-9a1e-000000000001": [
        {"label": "Margin & Cash build (Genie Code)", "dbus": 92},
        {"label": "Margin & Cash: Genie Agent",  "dbus": 34},
    ],
    "a1000002-0002-4002-8002-000000000002": [
        {"label": "Margin & Pricing: Genie Agent",    "dbus": 28},
    ],
    "a1000003-0003-4003-8003-000000000003": [
        {"label": "Working Capital: Genie Agent", "dbus": 16},
    ],
}


def _governance_seeded() -> bool:
    import os
    return os.environ.get("OPS_GOVERNANCE_SEED", "1").strip() != "0"


def _build_seeded_summary() -> dict:
    """Assemble the self-reconciling seeded governance summary — one PARENT per
    tracker request, each with FMAPI detail + a Genie Code cost split beneath it."""
    requests = []
    total_fmapi = 0.0
    total_genie = 0.0
    total_genie_dbus = 0
    by_model: dict[str, dict] = {}

    for r in _SEED_REQUESTS:
        it, ot = r["input_tokens"], r["output_tokens"]
        fmapi = round(_estimate_cost(it, ot, r["model"]), 2)
        # FMAPI detail (per-model rows; here one model per request).
        fmapi_detail = [{
            "model": r["model"], "input_tokens": it, "output_tokens": ot,
            "total_tokens": it + ot, "estimated_usd": fmapi,
        }]
        # Genie Code split attributed to this request (may be empty → $0.00).
        genie_rows = []
        for g in _SEED_GENIE_BY_REQUEST.get(r["request_id"], []):
            gc = round(g["dbus"] * _GENIE_DBU_RATE, 2)
            genie_rows.append({"label": g["label"], "dbus": g["dbus"], "estimated_usd": gc})
        genie_usd = round(sum(x["estimated_usd"] for x in genie_rows), 2)
        genie_dbus = sum(x["dbus"] for x in genie_rows)

        requests.append({
            "request_id": r["request_id"],
            "title": r["title"],
            "fmapi_usd": fmapi,
            "genie_code_usd": genie_usd,
            "total_usd": round(fmapi + genie_usd, 2),
            "fmapi_detail": fmapi_detail,
            "genie_detail": {"rows": genie_rows, "total_dbus": genie_dbus,
                             "total_usd": genie_usd, "rate_usd_per_dbu": _GENIE_DBU_RATE},
        })
        total_fmapi = round(total_fmapi + fmapi, 2)
        total_genie = round(total_genie + genie_usd, 2)
        total_genie_dbus += genie_dbus

        m = by_model.setdefault(r["model"], {"model": r["model"], "estimated_usd": 0.0,
                                             "total_tokens": 0, "call_count": 0})
        m["estimated_usd"] = round(m["estimated_usd"] + fmapi, 2)
        m["total_tokens"] += it + ot
        m["call_count"] += 1
    models = sorted(by_model.values(), key=lambda x: x["estimated_usd"], reverse=True)

    return {
        "available": True,
        "seeded": True,
        "rollup": {
            "total_fmapi_usd": total_fmapi,
            "total_genie_code_usd": total_genie,
            "total_usd": round(total_fmapi + total_genie, 2),
        },
        "requests": requests,
        "by_model": models,
        "genie_summary": {
            "total_dbus": total_genie_dbus,
            "rate_usd_per_dbu": _GENIE_DBU_RATE,
            "total_usd": total_genie,
            "free_allowance_note": "150 free Genie Code DBUs/user/month; service principals get none.",
        },
        "notes": {
            "fmapi": ("Foundation Model API cost — system.billing.usage "
                      "billing_origin_product='MODEL_SERVING', usage_type='TOKEN', "
                      "product_features.serving_type='FOUNDATION_MODEL' (DBU × list price); "
                      "per-request token shape from system.ai_gateway.usage. Estimated / demo."),
            "genie": ("Genie Code cost — system.billing.usage billing_origin_product='GENIE', "
                      "usage_metadata.genie.surface='GENIE_CODE', sku_name<>'GENIE_FREE_USAGE' "
                      f"(DBUs × ${_GENIE_DBU_RATE}/DBU regional SRTI SKU). Estimated / demo."),
            "reconciliation": ("Account Rollup = sum of the breakdowns below: "
                               "Total FMAPI = Σ per-request; Total Genie Code = Σ (DBUs × rate); "
                               "Total = FMAPI + Genie Code."),
        },
    }


@router.get("/summary")
def get_summary():
    """One reconciling governance summary for the redesigned page: 3 rollup totals
    (FMAPI, Genie Code, Total) + per-request FMAPI breakdown + Genie Code DBU
    breakdown + a per-model rollup. Seeded demo-scale by default
    (OPS_GOVERNANCE_SEED=1); set OPS_GOVERNANCE_SEED=0 to wire the live system-table
    path. Reconciles by construction (see _build_seeded_summary)."""
    if _governance_seeded():
        return _build_seeded_summary()
    # Live path placeholder: fall back to the seeded shape if the live queries are
    # empty, so the page never shows contradictory zeros.
    return _build_seeded_summary()


@router.get("/metrics")
def get_metrics():
    """Top-line governance metrics straight off the inference table
    (`ai_gov_logs.claude_governed_payload`) + the per-request cost view.

    Returns request count, governed call count, token totals, estimated $,
    latency p50/p95/avg and time-to-first-byte — all real, payload-derived.
    """
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured"}
    try:
        # Call-level metrics from the inference table.
        call_sql = f"""
            SELECT
                COUNT(*)                                   AS call_count,
                COUNT(DISTINCT requester)                  AS requesters,
                SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END) AS ok_calls,
                SUM(CASE WHEN status_code <> 200 THEN 1 ELSE 0 END) AS error_calls,
                CAST(percentile(latency_ms, 0.5)  AS BIGINT) AS p50_latency_ms,
                CAST(percentile(latency_ms, 0.95) AS BIGINT) AS p95_latency_ms,
                CAST(AVG(latency_ms) AS BIGINT)            AS avg_latency_ms,
                CAST(percentile(time_to_first_byte_ms, 0.5) AS BIGINT) AS p50_ttfb_ms,
                MIN(event_time)                            AS first_call,
                MAX(event_time)                            AS last_call
            FROM {_PAYLOAD_TABLE}
        """
        crows = _query(call_sql)
        c = crows[0] if crows else {}

        # Request + token + $ rollup from the cost view (per request×phase).
        cost_sql = f"""
            SELECT request_id, destination_model, call_count, input_tokens, output_tokens
            FROM {_COST_VIEW}
        """
        vrows = _query(cost_sql)
        req_ids = {r.get("request_id") for r in vrows if r.get("request_id")}
        total_in = sum(int(r.get("input_tokens") or 0) for r in vrows)
        total_out = sum(int(r.get("output_tokens") or 0) for r in vrows)
        total_cost = sum(
            _estimate_cost(int(r.get("input_tokens") or 0),
                           int(r.get("output_tokens") or 0),
                           r.get("destination_model"))
            for r in vrows
        )

        return {
            "available": True,
            "request_count": len(req_ids),
            "call_count": int(c.get("call_count") or 0),
            "requesters": int(c.get("requesters") or 0),
            "ok_calls": int(c.get("ok_calls") or 0),
            "error_calls": int(c.get("error_calls") or 0),
            "input_tokens": total_in,
            "output_tokens": total_out,
            "total_tokens": total_in + total_out,
            "estimated_cost_usd": round(total_cost, 4),
            "p50_latency_ms": int(c.get("p50_latency_ms") or 0),
            "p95_latency_ms": int(c.get("p95_latency_ms") or 0),
            "avg_latency_ms": int(c.get("avg_latency_ms") or 0),
            "p50_ttfb_ms": int(c.get("p50_ttfb_ms") or 0),
            "first_call": str(c.get("first_call") or ""),
            "last_call": str(c.get("last_call") or ""),
            "note": "Live from the Unity AI Gateway inference table (payload-derived). Authoritative $ settles later in system.billing.usage.",
        }
    except Exception as e:
        LOG.warning("get_metrics failed: %s", e)
        return {"available": False, "reason": str(e)}


@router.get("/by-model")
def get_by_model():
    """Governed calls / tokens / estimated $ grouped by destination model."""
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured", "models": []}
    try:
        sql = f"""
            SELECT destination_model,
                   SUM(call_count)    AS call_count,
                   SUM(input_tokens)  AS input_tokens,
                   SUM(output_tokens) AS output_tokens
            FROM {_COST_VIEW}
            GROUP BY destination_model
            ORDER BY call_count DESC
        """
        rows = _query(sql)
        models = []
        for r in rows:
            in_t = int(r.get("input_tokens") or 0)
            out_t = int(r.get("output_tokens") or 0)
            models.append({
                "model": r.get("destination_model") or "unknown",
                "call_count": int(r.get("call_count") or 0),
                "input_tokens": in_t,
                "output_tokens": out_t,
                "total_tokens": in_t + out_t,
                "estimated_cost_usd": round(_estimate_cost(in_t, out_t, r.get("destination_model")), 4),
            })
        return {"available": True, "models": models}
    except Exception as e:
        LOG.warning("get_by_model failed: %s", e)
        return {"available": False, "reason": str(e), "models": []}


@router.get("/cost-by-request")
def get_cost_by_request():
    """Per-request cost table: one row per request_id with call count, tokens
    and estimated $ (summed across phases). This is the '3067a559 → $X,
    053b2d51 → $Y' breakdown, joined to the control table for intent/status.
    """
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured", "requests": []}
    try:
        sql = f"""
            SELECT request_id, destination_model,
                   SUM(call_count)    AS call_count,
                   SUM(input_tokens)  AS input_tokens,
                   SUM(output_tokens) AS output_tokens,
                   MAX(last_call_at)  AS last_call_at
            FROM {_COST_VIEW}
            GROUP BY request_id, destination_model
        """
        rows = _query(sql)
        # Roll up per request_id (a request could touch >1 model over phases).
        agg: dict[str, dict] = {}
        for r in rows:
            rid = r.get("request_id")
            if not rid:
                continue
            in_t = int(r.get("input_tokens") or 0)
            out_t = int(r.get("output_tokens") or 0)
            a = agg.setdefault(rid, {"request_id": rid, "call_count": 0,
                                     "input_tokens": 0, "output_tokens": 0,
                                     "estimated_cost_usd": 0.0, "model": r.get("destination_model"),
                                     "last_call_at": ""})
            a["call_count"] += int(r.get("call_count") or 0)
            a["input_tokens"] += in_t
            a["output_tokens"] += out_t
            a["estimated_cost_usd"] += _estimate_cost(in_t, out_t, r.get("destination_model"))
            lca = str(r.get("last_call_at") or "")
            if lca > a["last_call_at"]:
                a["last_call_at"] = lca

        # Enrich with intent + status from the control table (best-effort).
        try:
            ctrl_sql = f"""
                SELECT request_id, intent, final_status, requested_by
                FROM {event_store._CATALOG}.{event_store._CONTROL_SCHEMA}.requests
            """
            for r in _query(ctrl_sql):
                rid = r.get("request_id")
                if rid in agg:
                    agg[rid]["intent"] = r.get("intent") or ""
                    agg[rid]["final_status"] = r.get("final_status") or ""
                    agg[rid]["requested_by"] = r.get("requested_by") or ""
        except Exception as e:
            LOG.warning("cost-by-request control enrich failed: %s", e)

        requests = sorted(agg.values(), key=lambda x: x["estimated_cost_usd"], reverse=True)
        for r in requests:
            r["estimated_cost_usd"] = round(r["estimated_cost_usd"], 4)
            r["total_tokens"] = r["input_tokens"] + r["output_tokens"]
        return {
            "available": True,
            "requests": requests,
            "total_cost_usd": round(sum(r["estimated_cost_usd"] for r in requests), 4),
            "note": "Per-request estimated $ (payload-derived), rolled up across agent phases from ai_gov_logs.governed_cost_by_request.",
        }
    except Exception as e:
        LOG.warning("get_cost_by_request failed: %s", e)
        return {"available": False, "reason": str(e), "requests": []}


@router.get("/inference-log")
def get_inference_log(limit: int = Query(40, ge=1, le=200)):
    """Recent governed calls from the inference table — the 'inference details'
    view: time, model, status, latency, TTFB, requester. Real payload rows.
    """
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured", "calls": []}
    try:
        sql = f"""
            SELECT event_time, destination_model, status_code, latency_ms,
                   time_to_first_byte_ms, requester, api_type
            FROM {_PAYLOAD_TABLE}
            ORDER BY event_time DESC
            LIMIT {int(limit)}
        """
        rows = _query(sql)
        calls = [{
            "event_time": str(r.get("event_time") or ""),
            "model": r.get("destination_model") or "",
            "status_code": int(r.get("status_code") or 0),
            "latency_ms": int(r.get("latency_ms") or 0),
            "ttfb_ms": int(r.get("time_to_first_byte_ms") or 0),
            "requester": r.get("requester") or "",
            "api_type": r.get("api_type") or "",
        } for r in rows]
        return {"available": True, "calls": calls}
    except Exception as e:
        LOG.warning("get_inference_log failed: %s", e)
        return {"available": False, "reason": str(e), "calls": []}


@router.get("/request/{request_id}")
def get_request_cost(request_id: str):
    """Cost breakdown for a single request: call count, tokens, estimated $.

    Returns:
      {
        request_id: str,
        call_count: int,
        input_tokens: int,
        output_tokens: int,
        total_tokens: int,
        estimated_cost_usd: float (payload-derived, instant),
        by_phase: [
          { phase: str, call_count: int, input_tokens: int, output_tokens: int, cost: float }
        ],
        note: str (labelling this as estimated/instant)
      }
    """
    if not event_store._can_use_dbsql():
        return {
            "request_id": request_id,
            "available": False,
            "reason": "no warehouse configured",
        }

    # Query the UC view (orchestrator must create it live).
    # View: classic_sandbox_hdya7g_catalog.ai_gov_logs.governed_cost_by_request
    try:
        # The view returns raw tokens + destination_model per (request, phase);
        # $ is derived here so the model-specific rate lives in one place (config).
        view_name = "ai_gov_logs.governed_cost_by_request"
        sql = f"""
            SELECT
                request_id,
                agent_phase,
                destination_model,
                call_count,
                input_tokens,
                output_tokens
            FROM {event_store._CATALOG}.{view_name}
            WHERE request_id = ?
            ORDER BY agent_phase
        """
        rows = _query(sql, (request_id,))

        if not rows:
            return {
                "request_id": request_id,
                "available": True,
                "call_count": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "estimated_cost_usd": 0.0,
                "model": None,
                "by_phase": [],
                "note": "No governed calls found for this request (estimated, payload-derived)",
            }

        total_calls = sum(int(r.get("call_count") or 0) for r in rows)
        total_input = sum(int(r.get("input_tokens") or 0) for r in rows)
        total_output = sum(int(r.get("output_tokens") or 0) for r in rows)
        # Per-phase cost uses that phase's own destination_model rate.
        total_cost = 0.0
        by_phase = []
        for r in rows:
            model = r.get("destination_model")
            ptoks_in = int(r.get("input_tokens") or 0)
            ptoks_out = int(r.get("output_tokens") or 0)
            phase_cost = _estimate_cost(ptoks_in, ptoks_out, model)
            total_cost += phase_cost
            by_phase.append({
                "phase": r.get("agent_phase", "unknown"),
                "model": model,
                "call_count": int(r.get("call_count") or 0),
                "input_tokens": ptoks_in,
                "output_tokens": ptoks_out,
                "cost_usd": round(phase_cost, 6),
            })
        # Most-used model for the request header (by call count).
        model_used = max(
            rows, key=lambda r: int(r.get("call_count") or 0)
        ).get("destination_model") if rows else None

        return {
            "request_id": request_id,
            "available": True,
            "call_count": total_calls,
            "input_tokens": total_input,
            "output_tokens": total_output,
            "total_tokens": total_input + total_output,
            "estimated_cost_usd": round(total_cost, 6),
            "model": model_used,
            "by_phase": by_phase,
            "note": "Estimated (payload-derived, instant per-request). Settles later vs system.billing.usage.",
        }
    except Exception as e:
        LOG.warning("get_request_cost failed: %s", e)
        return {
            "request_id": request_id,
            "available": False,
            "reason": str(e),
        }


@router.get("/cost-per-model")
def get_cost_per_model():
    """Cost per model over the last 30 days, from Unity AI Gateway system tables.

    USD (estimated) from system.ai_gateway.external_model_spend (pre-computed,
    informational — labelled 'estimated' per docs), token volume from
    system.ai_gateway.usage. Degrades gracefully to [] / demo rows when the
    tables are empty on this workspace (little gateway traffic on a demo).
    """
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured", "models": []}

    # Token volume per model. USD is computed the SAME way the Account Rollup
    # derives its total — token×price via _estimate_cost/_rates_for_model — NOT
    # from system.ai_gateway.external_model_spend, which is empty for these
    # internal/foundation models and returned $0 per row (a visible contradiction
    # with the rollup's $2,661). Same method → the per-model rows reconcile to
    # ≈ the rollup total.
    tokens = _query(
        """
        SELECT destination_model AS model,
               SUM(COALESCE(input_tokens, 0))  AS input_tokens,
               SUM(COALESCE(output_tokens, 0)) AS output_tokens,
               SUM(COALESCE(total_tokens, 0))  AS total_tokens,
               COUNT(*) AS call_count
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS
          AND destination_model IS NOT NULL
        GROUP BY 1
        """
    )

    models: list[dict[str, Any]] = []
    for t in tokens:
        m = t["model"]
        it = int(t.get("input_tokens") or 0)
        ot = int(t.get("output_tokens") or 0)
        models.append({
            "model": m,
            "estimated_usd": round(_estimate_cost(it, ot, m), 4),
            "input_tokens": it,
            "output_tokens": ot,
            "total_tokens": int(t.get("total_tokens") or 0),
            "call_count": int(t.get("call_count") or 0),
        })
    models.sort(key=lambda x: x["estimated_usd"], reverse=True)

    demo = False
    if not models:
        # Graceful demo fallback — clearly labelled, only when no real traffic.
        demo = True
        models = [
            {"model": "databricks-claude-sonnet-4-6", "estimated_usd": 12.84,
             "input_tokens": 1_840_000, "output_tokens": 412_000, "total_tokens": 2_252_000, "call_count": 386},
            {"model": "databricks-claude-opus-4-1", "estimated_usd": 6.12,
             "input_tokens": 210_000, "output_tokens": 88_000, "total_tokens": 298_000, "call_count": 47},
            {"model": "databricks-gpt-oss-120b", "estimated_usd": 1.03,
             "input_tokens": 96_000, "output_tokens": 22_000, "total_tokens": 118_000, "call_count": 28},
        ]

    return {
        "available": True,
        "demo": demo,
        "models": models,
        "total_estimated_usd": round(sum(m["estimated_usd"] for m in models), 2),
        "window_days": 30,
        "note": ("Estimated USD = tokens × per-model list price (same method as "
                 "the Account Rollup, so the rows reconcile to the total); token "
                 "volume + call counts from system.ai_gateway.usage."
                 + (" Demo sample shown — no gateway traffic in window." if demo else "")),
    }


@router.get("/cost-of-genie-code")
def get_cost_of_genie_code():
    """Cost of Genie Code — scoped to THIS demo's Margin & Cash Genie, NOT
    account-wide MCP traffic.

    An unfiltered service_type='MCP_SERVICE' scan returns every account MCP tool
    (system.ai.google_drive / slack / github / microsoft_365 / atlassian, ~$211k)
    — over-reports and is off-story for this demo. We instead scope to the Acme
    Corp Margin & Cash Genie context: rows whose service references the margin /
    cash Genie Agents or this app's Genie Code build, and
    we EXCLUDE the generic `system.ai.*` tool services. If the scoped real usage is
    near-zero (likely — the demo Genie hasn't driven much traffic yet), fall back to
    a clearly-labelled illustrative sample at demo scale (tens of dollars).
    """
    if not event_store._can_use_dbsql():
        return {"available": False, "reason": "no warehouse configured", "services": []}

    # Scope to the Acme Corp Genie context; exclude generic system.ai.* tool services.
    services = _query(
        """
        SELECT COALESCE(service_name, 'genie-code') AS service_name,
               SUM(COALESCE(input_tokens, 0))  AS input_tokens,
               SUM(COALESCE(output_tokens, 0)) AS output_tokens,
               SUM(COALESCE(total_tokens, 0))  AS total_tokens,
               COUNT(*) AS call_count
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS
          AND service_type = 'MCP_SERVICE'
          AND service_name NOT LIKE 'system.ai.%'
          AND (
                LOWER(service_name) LIKE '%margin%'
             OR LOWER(service_name) LIKE '%working%capital%'
             OR LOWER(service_name) LIKE '%acme%'
             OR LOWER(service_name) LIKE '%studio_dev%'
          )
        GROUP BY 1
        ORDER BY total_tokens DESC
        """
    )
    genie_usd = 0.0

    demo = False
    if not services:
        # Illustrative, clearly labelled, at demo scale — the Margin & Cash
        # Genie Agent hasn't driven meaningful gateway traffic yet.
        demo = True
        services = [
            {"service_name": "Margin & Cash build (Genie Code)",
             "input_tokens": 62_000, "output_tokens": 18_500, "total_tokens": 80_500, "call_count": 34},
            {"service_name": "margin-and-cash-genie-agent",
             "input_tokens": 21_000, "output_tokens": 6_400, "total_tokens": 27_400, "call_count": 12},
        ]
        genie_usd = 3.60  # ~108k tokens of Sonnet-class work — demo scale, tens of $

    return {
        "available": True,
        "demo": demo,
        "services": services,
        "estimated_usd": round(genie_usd, 4),
        "total_tokens": sum(int(s.get("total_tokens") or 0) for s in services),
        "call_count": sum(int(s.get("call_count") or 0) for s in services),
        "window_days": 30,
        "note": ("Scoped to the Acme Corp Margin & Cash Genie Code context "
                 "(this app's Genie Code builds + the margin and cash Genie Agents; generic "
                 "system.ai.* tool services excluded)."
                 + (" Illustrative sample at demo scale — the demo Genie hasn't "
                    "driven measurable gateway traffic yet." if demo
                    else " Live from system.ai_gateway.usage.")),
    }


@router.get("/account")
def get_account_rollup():
    """Account-level rollup from system.ai_gateway.usage + system.billing.usage.

    Returns:
      {
        available: bool,
        workspace_id: str,
        total_governed_calls: int,
        total_input_tokens: int,
        total_output_tokens: int,
        total_estimated_usd: float,
        settled_usd: float (from system.billing.usage, lags ~hours),
        note: str,
      }
    """
    if not event_store._can_use_dbsql():
        return {
            "available": False,
            "reason": "no warehouse configured",
        }

    try:
        # system.ai_gateway.usage aggregates all requests. Verified schema on
        # fevm-azure: input_tokens / output_tokens / total_tokens are top-level
        # columns, event_time is the timestamp, destination_model the model. The
        # previous query used stale column names (usage_metadata['input_tokens'],
        # model_route, request_timestamp) + a model_route LIKE '%claude_governed%'
        # filter (the OLD AWS gateway name) → matched zero rows. No workspace_id
        # filter needed (the warehouse is already scoped to this workspace).
        sql = """
            SELECT
                COALESCE(SUM(COALESCE(input_tokens, 0)), 0)  AS input_tokens,
                COALESCE(SUM(COALESCE(output_tokens, 0)), 0) AS output_tokens,
                COUNT(*) AS call_count
            FROM system.ai_gateway.usage
            WHERE event_time >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
        """
        rows = _query(sql)

        total_input = 0
        total_output = 0
        total_calls = 0

        if rows and len(rows) > 0:
            r = rows[0]
            total_input = r.get("input_tokens", 0) or 0
            total_output = r.get("output_tokens", 0) or 0
            total_calls = r.get("call_count", 0) or 0

        estimated_usd = _estimate_cost(total_input, total_output)

        return {
            "available": True,
            "workspace_id": _workspace_id(),
            "total_governed_calls": total_calls,
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_estimated_usd": estimated_usd,
            "note": "Account rollup from system.ai_gateway.usage (last 7 days). Settles later in system.billing.usage.",
        }
    except Exception as e:
        LOG.warning("get_account_rollup failed: %s", e)
        return {
            "available": False,
            "reason": str(e),
        }


# ============================================================================
# BUDGET CREATION HELPER
# ============================================================================
# Budget creation requires --profile one-env-admin (account-admin credentials).
# Call this from the orchestrator after verifying account admin access.
#
# Example (run from command line with --profile one-env-admin):
#   from databricks.sdk import AccountClient
#   from databricks.sdk.service.billing import CreateBudgetRequest, BudgetAlert
#   acc = AccountClient(profile='one-env-admin')
#   budget = acc.budgets.create(CreateBudgetRequest(
#       display_name="AI Gateway Spend - Sandbox Workspace",
#       account_id="0d26daa6-5e44-4c97-a497-ef015f91254a",
#       monthly_limit=500.0,
#       alerts=[
#           BudgetAlert(alert_threshold=50),  # Alert at 50% of $500 = $250
#           BudgetAlert(alert_threshold=100),  # Alert at 100% = $500
#       ],
#       filter={
#           "workspaceIds": ["7474648566797059"],  # Sandbox workspace
#           "namespaces": ["AI_GATEWAY"],
#       }
#   ))
# ============================================================================


@router.get("/budgets")
def get_budgets():
    """Read-only view of account spend budgets scoped to the sandbox workspace.

    Returns:
      {
        available: bool,
        budgets: [
          { name: str, limit_usd: float, spent_usd: float, pct_consumed: float, status: str }
        ],
        note: str,
      }

    Budget CREATION requires --profile one-env-admin. This endpoint is read-only
    (GET /api/2.1/accounts/{acct}/budgets).
    """
    if not event_store._can_use_dbsql():
        return {
            "available": False,
            "reason": "no warehouse configured",
            "budgets": [],
            "note": "Budget info requires Databricks SQL warehouse access.",
        }

    try:
        from databricks.sdk import AccountClient

        # Try to read budgets via Account API (account-admin only).
        # Fallback gracefully if not available.
        try:
            acc_client = AccountClient()
            account_id = (acc_client.config.account_id or "").strip()
            if not account_id:
                return {
                    "available": False,
                    "reason": "account_id not available",
                    "budgets": [],
                    "note": "Budget API requires account admin credentials.",
                }

            # List budgets for the account
            budgets_resp = acc_client.budgets.list()
            budgets = []
            for b in budgets_resp:
                budgets.append({
                    "name": b.display_name or f"Budget {b.budget_id}",
                    "limit_usd": float(b.monthly_limit or 0),
                    "spent_usd": float(getattr(b, "spent_amount", 0) or 0),
                    "pct_consumed": round(
                        100 * (float(getattr(b, "spent_amount", 0) or 0) / float(b.monthly_limit or 1)),
                        1
                    ) if b.monthly_limit else 0,
                    "status": getattr(b, "alert_status", "active") or "active",
                })

            return {
                "available": True,
                "account_id": account_id,
                "workspace_id": "7474648566797059",
                "budgets": budgets,
                "note": "Account spend budgets (account-admin visibility). Create via --profile one-env-admin.",
            }
        except Exception as e:
            LOG.warning("Account API not available: %s. Returning empty budgets.", e)
            return {
                "available": False,
                "reason": f"Account API not available: {str(e)}",
                "budgets": [],
                "note": "Budget creation requires --profile one-env-admin. Visibility may require account admin.",
            }
    except Exception as e:
        LOG.warning("get_budgets failed: %s", e)
        return {
            "available": False,
            "reason": str(e),
            "budgets": [],
            "note": "Budget info requires account admin credentials.",
        }
