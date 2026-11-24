"""Similar / duplicate request detection (shown at the queued step).

When a request is queued, the operator who decides whether to initiate it
sees prior already-built requests that are similar — so they can:
  * avoid building a true duplicate,
  * notice that the source tables overlap (consolidated ingestion will
    reuse them — cheaper/faster),
  * avoid colliding on a silver target-table name.

Hybrid approach: deterministic signals (source-table overlap, identical
target table) are always computed; an LLM adds a semantic similarity
verdict + one-line reason. If the LLM is unavailable the deterministic
signals stand on their own.
"""

from __future__ import annotations

import json
import logging
from typing import Any

LOG = logging.getLogger("ops.similarity")

# Requests in these states represent real, already-built pipelines worth
# comparing against (a queued/failed request isn't a dedup target).
_BUILT_STATES = {"deployed", "pending_review", "blocked_on_pr_approval", "in_progress"}


def _spec(r: dict) -> dict:
    try:
        return json.loads(r.get("spec_json") or "{}")
    except Exception:
        return {}


def _summary(r: dict) -> dict[str, Any]:
    sp = _spec(r)
    src = sp.get("source") or {}
    tgt = sp.get("target") or {}
    return {
        "request_id": r.get("request_id"),
        "intent": (r.get("intent") or sp.get("intent") or "")[:300],
        "source_tables": sorted(src.get("tables") or []),
        "target_table": tgt.get("table"),
        "final_status": r.get("final_status"),
    }


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def find_similar(request_id: str, max_results: int = 5) -> dict[str, Any]:
    """Return {target, candidates:[...]} ranked by similarity. Each candidate
    carries deterministic overlap + (best-effort) an LLM verdict."""
    from agent import event_store
    target = event_store.fetch_request(request_id)
    if not target:
        return {"target": None, "candidates": []}
    t = _summary(target)
    t_tables = set(t["source_tables"])

    others = [r for r in event_store.list_requests(limit=100)
              if r.get("request_id") != request_id
              and (r.get("final_status") in _BUILT_STATES)]

    cands: list[dict[str, Any]] = []
    for r in others:
        s = _summary(r)
        c_tables = set(s["source_tables"])
        overlap = sorted(t_tables & c_tables)
        cands.append({
            **s,
            "overlap_tables": overlap,
            "table_overlap_pct": round(100 * _jaccard(t_tables, c_tables)),
            "same_target_table": bool(s["target_table"] and s["target_table"] == t["target_table"]),
        })

    # Pre-rank by deterministic signal so the LLM (and the UI fallback) see
    # the most promising first.
    cands.sort(key=lambda c: (c["same_target_table"], c["table_overlap_pct"]), reverse=True)
    cands = cands[:max_results]

    if cands:
        _add_llm_verdicts(t, cands)

    return {"target": t, "candidates": cands}


_VERDICT_PROMPT = """You compare a NEW data-pipeline request against prior ones.
For each prior request, decide how similar it is to the new request in PURPOSE
(what business question / dataset it produces), not just table overlap.

Return STRICT JSON: {"verdicts": [{"request_id": "...", "similarity": "duplicate|high|medium|low", "reason": "<= 18 words"}]}
- "duplicate" = same dataset/purpose (would produce essentially the same silver table)
- "high" = strongly overlapping purpose
- "medium" = related, shares some sources or goal
- "low" = mostly unrelated
Only JSON. No prose."""


def _add_llm_verdicts(target: dict, cands: list[dict]) -> None:
    try:
        from agent.llm import get_llm
        from langchain_core.messages import HumanMessage, SystemMessage
        payload = {
            "new_request": {"intent": target["intent"],
                            "source_tables": target["source_tables"],
                            "target_table": target["target_table"]},
            "prior_requests": [
                {"request_id": c["request_id"], "intent": c["intent"],
                 "source_tables": c["source_tables"], "target_table": c["target_table"]}
                for c in cands],
        }
        resp = get_llm().invoke([SystemMessage(content=_VERDICT_PROMPT),
                                 HumanMessage(content=json.dumps(payload))])
        raw = resp.content if isinstance(resp.content, str) else str(resp.content)
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```", 2)[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip().rstrip("`").strip()
        if not raw.startswith("{"):
            raw = raw[raw.find("{"): raw.rfind("}") + 1]
        verdicts = {v["request_id"]: v for v in json.loads(raw).get("verdicts", [])}
        for c in cands:
            v = verdicts.get(c["request_id"])
            if v:
                c["similarity"] = v.get("similarity", "low")
                c["reason"] = v.get("reason", "")
    except Exception as e:
        LOG.warning("similarity LLM verdict unavailable: %s", e)
        # Deterministic fallback verdict
        for c in cands:
            if c["same_target_table"]:
                c["similarity"], c["reason"] = "duplicate", "same target table name"
            elif c["table_overlap_pct"] >= 60:
                c["similarity"], c["reason"] = "high", "shares most source tables"
            elif c["overlap_tables"]:
                c["similarity"], c["reason"] = "medium", "shares some source tables"
            else:
                c["similarity"], c["reason"] = "low", "no shared sources"
