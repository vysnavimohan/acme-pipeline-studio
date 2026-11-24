"""Agentic Gold fact/dimension SQL generation.

Given a fact's name + intent, the SOURCE Silver table(s) with their columns, and
optional join / grain / measure hints, an LLM writes ONE Spark SQL SELECT that
produces the Gold fact/dim from the (flattened) Silver tables. This is the
"platform generates the Gold transform" step. The caller validates the SQL
(EXPLAIN / LIMIT 0) before deploying it as an MV.

Best-effort: returns None on failure (caller can fall back / retry).
"""

from __future__ import annotations

import json
import re
from typing import Optional

from agent import llm

_SYSTEM = (
    "You are a senior Databricks data engineer building a GOLD fact or dimension "
    "table in a medallion pipeline (Bronze->Silver->Gold). You are given the "
    "fact's name + intent, the SOURCE Silver table(s) by FULLY-QUALIFIED name with "
    "their columns, and optional join / grain / measures hints. Write ONE Spark SQL "
    "SELECT that produces the fact/dim by reading the Silver table(s) by their FULL "
    "names. RULES: (1) only reference columns that exist in the provided schemas; "
    "(2) project business-meaningful columns with clean snake_case names; (3) for an "
    "aggregation fact, GROUP BY the stated grain and compute the measures (counts, "
    "sums, status flags); (4) for a join, use the stated keys; (5) derive the stated "
    "flags/measures (e.g. UPPER(status), a derived flag via CASE); (6) it must be a "
    "SINGLE statement, no trailing semicolon, no CTE-only output; (7) project each "
    "grain/join key EXACTLY ONCE (qualify it with ONE source alias) — the output must "
    "have NO duplicate column names; (8) for a tiered measure, key the CASE on the "
    "actual numeric amount column (CAST to double) so the tiers actually differ; "
    "(9) you MUST read and USE EVERY source table listed in silver_schemas — when two "
    "or more sources are given you MUST JOIN them (on the stated join key, else a shared "
    "id such as customer_id / facility_id / entity_id) and pull dimension attributes FROM the "
    "joined dimension; NEVER silently drop a provided source table; (10) NEVER fabricate "
    "a grouping/dimension value as a string literal (e.g. 'active'/'pending'/'archived') — "
    "every grain/dimension column in the output MUST be a real column from a provided "
    "source (e.g. a `status` grain MUST come from the dimension table's `status` "
    "column via the join, not an invented CASE); if a requested grain attribute has no "
    "backing column in any provided source, omit that attribute rather than inventing it. "
    "Return STRICT JSON "
    "ONLY: {\"sql\": \"<one SELECT>\", \"cluster_by\": [<output col(s)>], "
    "\"not_null\": [<grain/key col(s)>], \"comment\": \"<table comment>\"}."
)


def _loads_lenient(text: str) -> Optional[dict]:
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


def parse_gold_sql(data: dict, output_cols_allowed: Optional[set] = None) -> Optional[dict]:
    """Validate an LLM gold-sql response into {sql, cluster_by, not_null, comment}.
    Pure (no LLM, no warehouse) → unit-testable. Returns None if there is no usable
    single SELECT statement."""
    if not isinstance(data, dict):
        return None
    sql = str(data.get("sql") or "").strip().rstrip(";").strip()
    if not sql or not re.match(r"(?is)^\s*(with|select)\b", sql):
        return None
    # one statement only — reject anything after a statement separator
    if ";" in sql:
        sql = sql.split(";")[0].strip()
    cluster_by = [str(c).strip() for c in (data.get("cluster_by") or []) if str(c).strip()]
    not_null = [str(c).strip() for c in (data.get("not_null") or []) if str(c).strip()]
    if output_cols_allowed is not None:
        cluster_by = [c for c in cluster_by if c in output_cols_allowed]
        not_null = [c for c in not_null if c in output_cols_allowed]
    return {"sql": sql, "cluster_by": cluster_by[:4], "not_null": not_null[:4],
            "comment": str(data.get("comment") or "")[:300]}


def suggest_gold_sql(fact: dict, silver_schemas: dict, *,
                     feedback: Optional[str] = None, use_llm: bool = True) -> Optional[dict]:
    """fact: {name, intent, sources:[fqn...], grain?, join?, measures?}.
    silver_schemas: {fqn: [[col, type], ...]}. feedback: a prior SQL error to fix.
    Returns {sql, cluster_by, not_null, comment} or None."""
    if not use_llm:
        return None
    from langchain_core.messages import HumanMessage, SystemMessage
    payload = {"fact": fact,
               "silver_schemas": {k: [c[0] if isinstance(c, (list, tuple)) else c for c in v]
                                  for k, v in silver_schemas.items()}}
    if feedback:
        payload["fix_this_error_from_your_previous_sql"] = feedback
    user = json.dumps(payload, default=str)[:11000]
    try:
        resp = llm.get_llm().invoke([SystemMessage(content=_SYSTEM), HumanMessage(content=user)])
        data = _loads_lenient(resp.content) or {}
    except Exception:
        return None
    return parse_gold_sql(data)
