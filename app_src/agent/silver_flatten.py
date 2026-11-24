"""Agentic, request-aware JSON-flatten selection for the Silver layer.

Given a request's intent/target/transform and, per embedded-JSON column, its
sampled keys + a sample value, an LLM picks the fields MOST relevant to the
request to promote into typed Silver columns. This is a DEDICATED call (not
bundled with the generator's metadata call) so it stays reliable for large
schemas, and it is told to PREFIX aliases to avoid colliding with existing
columns — the bulk hand-coding the platform replaces.

Best-effort: any failure returns {} and the JSON column is kept as raw string.
"""

from __future__ import annotations

import json
import re
from typing import Optional

from agent import llm

_CASTABLE = {"long", "int", "integer", "double", "float", "decimal", "boolean",
             "timestamp", "date"}


def _loads_lenient(text: str) -> Optional[dict]:
    """Parse JSON that may be wrapped in ```json fences or surrounding prose."""
    if not text:
        return None
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", t, re.DOTALL)
    if m:
        t = m.group(1)
    else:
        i, j = t.find("{"), t.rfind("}")
        if i != -1 and j != -1 and j > i:
            t = t[i:j + 1]
    try:
        return json.loads(t)
    except Exception:
        return None


def clean_flatten(data: dict, json_columns: dict, avoid_aliases: set) -> dict:
    """Validate + de-collide an LLM flatten response into
    {json_col: [{path, alias, type, comment}]}. Pure (no LLM) → unit-testable.

    - drops fields with no path/alias, unknown columns, non-castable types→string
    - skips an alias that collides with an existing column (avoid_aliases) or a
      previously-emitted alias; the deploy-side `_silver_select_exp` dedups too,
      but doing it here keeps the count honest.
    """
    if not isinstance(data, dict):
        return {}
    payload = data.get("json_flatten") if isinstance(data.get("json_flatten"), dict) else data
    avoid = {str(a).lower() for a in (avoid_aliases or set())}
    out: dict = {}
    for col, fields in (payload or {}).items():
        if col not in json_columns or not isinstance(fields, list):
            continue
        clean = []
        seen: set = set()
        for f in fields:
            if not isinstance(f, dict):
                continue
            path = str(f.get("path") or f.get("alias") or "").strip()
            alias = str(f.get("alias") or f.get("path") or "").strip()
            if not path or not alias:
                continue
            al = alias.lower()
            if al in avoid or al in seen:
                continue
            seen.add(al)
            typ = str(f.get("type") or "string").strip().lower()
            if typ not in _CASTABLE:
                typ = "string"
            clean.append({"path": path, "alias": alias, "type": typ,
                          "comment": str(f.get("comment") or "")[:200]})
        if clean:
            out[col] = clean
    return out


_SYSTEM = (
    "You are a senior Databricks data engineer building the SILVER layer of a "
    "telco data pipeline. You are given a data request (intent / target / "
    "transform) and, for each embedded-JSON column in the source, its available "
    "top-level keys plus a sample value. Your job: select ONLY the fields that "
    "are MEANINGFUL for THIS request and promote them to typed Silver columns. "
    "Skip internal/system flags, opaque ids, and noise. Prefer business-relevant "
    "fields (amounts, ids, statuses, timestamps, names, codes). "
    "CRITICAL — avoid column-name collisions: alias every field with a SHORT "
    "prefix derived from its JSON column so it can't clash with an existing "
    "column (e.g. column 'fulfillment_json' -> 'fj_sender', 'fj_message_type'; "
    "'initial_json' -> 'ij_cam_name'; 'reward_params' -> 'rp_offer_id'). Never "
    "reuse a name from avoid_aliases. Use snake_case. "
    "For nested keys use a dotted json-path (e.g. 'a.b'). Choose 4-10 fields per "
    "column. Return STRICT JSON ONLY: "
    "{\"<json_column>\": [{\"path\":..,\"alias\":..,\"type\":"
    "\"string|long|double|timestamp|date|boolean\",\"comment\":\"..\"}]}."
)


def suggest_flatten(json_columns: dict, context: dict, *, use_llm: bool = True) -> dict:
    """json_columns: {col: {"keys":[...], "sample": ".."}}. context: {intent,
    target, transform, business_rules, avoid_aliases:[...]}. Returns
    {col: [{path, alias, type, comment}]}; {} on any failure."""
    if not json_columns or not use_llm:
        return {}
    from langchain_core.messages import HumanMessage, SystemMessage
    user = json.dumps({"request": context, "json_columns": json_columns},
                      default=str)[:9000]
    try:
        resp = llm.get_llm().invoke([SystemMessage(content=_SYSTEM),
                                     HumanMessage(content=user)])
        data = _loads_lenient(resp.content) or {}
    except Exception:
        return {}
    return clean_flatten(data, json_columns, set(context.get("avoid_aliases") or []))
