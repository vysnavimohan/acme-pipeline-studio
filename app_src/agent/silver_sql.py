"""Agentic Silver cast/derive/dedupe SQL generation for Funding Optimisation.

Given Bronze columns + declared types + business rules (specifically eligibility
criteria), the LLM writes ONE Spark SQL SELECT that (a) casts each column to its
declared type (numeric/date), null-safe; (b) derives `eligible_flag` from the
credit box AND `exclusion_reason` via CASE; (c) dedupes to latest `as_of_date`
per `loan_id` (window row_number). This is the "platform generates the Silver
transform" step. The caller validates the SQL (EXPLAIN / LIMIT 0) before
deploying it as a @dlt.table.

Best-effort: returns None on failure (caller falls back to deterministic template).
"""

from __future__ import annotations

import json
import re
from typing import Optional

from agent import llm

_SYSTEM = (
    "You are a senior data engineer building a Silver transformation table in a "
    "Funding Optimisation (FO) medallion pipeline (Bronze→Silver→Gold). You are "
    "given the Bronze table columns, their declared types, and a structured "
    "eligibility credit box (thresholds for credit_score, ltv, arrears_days). "
    "Write ONE Spark SQL SELECT that produces a typed, deduplicated Silver table "
    "ready for Gold aggregation. RULES: (1) cast each Bronze column to its "
    "declared type (numeric/date), using NULL-safe casting; (2) derive "
    "`eligible_flag` = TRUE when ALL of these hold: credit_score >= 620 AND "
    "ltv <= 0.95 AND arrears_days <= 30, else FALSE; (3) when a row fails "
    "eligibility, populate `exclusion_reason` with ONE of: 'low_score' "
    "(credit_score < 620), 'high_ltv' (ltv > 0.95), 'arrears' (arrears_days > 30); "
    "if multiple thresholds fail, pick the first one in that order; if eligible, "
    "`exclusion_reason` = NULL; (4) deduplicate to the LATEST as_of_date per "
    "loan_id using ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY as_of_date "
    "DESC, <tie-breaker>) = 1; (5) project all casted columns + the two derived "
    "columns (eligible_flag, exclusion_reason) in snake_case; (6) it must be a "
    "SINGLE statement, no trailing semicolon; (7) only reference columns that exist "
    "in the provided Bronze schema; (8) NEVER fabricate columns or use string "
    "literals for the thresholds — apply the EXACT credit box values provided in "
    "the context. Return STRICT JSON ONLY: {\"sql\": \"<one SELECT>\", "
    "\"not_null\": [<col(s)>], \"comment\": \"<table comment>\"}."
)

# Column types the LLM will use for casting. Maps Bronze column name → type hint.
_COLUMN_TYPES = {
    "loan_id": "string",  # primary key, keep as string
    "facility": "string",  # dimension
    "asset_class": "string",
    "originator": "string",
    "state": "string",
    "current_balance": "decimal(18,2)",  # numeric
    "credit_score": "int",  # numeric
    "ltv": "decimal(5,4)",  # numeric, loan-to-value ratio
    "arrears_days": "int",  # numeric
    "interest_rate": "decimal(5,4)",  # numeric, basis points
    "seasoning_months": "int",  # numeric
    "as_of_date": "date",  # date
}


def _loads_lenient(text: str) -> Optional[dict]:
    """Parse JSON from LLM response, lenient to markdown/wrapper."""
    if not text:
        return None
    t = text.strip()
    # Try markdown JSON code block
    m = re.search(r"```(?:json)?\s*(\{.*\})\s*```", t, re.DOTALL)
    if m:
        t = m.group(1)
    else:
        # Try bare JSON
        i, j = t.find("{"), t.rfind("}")
        if i != -1 and j != -1 and j > i:
            t = t[i:j + 1]
    try:
        return json.loads(t)
    except Exception:
        return None


def parse_silver_sql(data: dict, bronze_columns: Optional[set] = None,
                     output_cols_required: Optional[set] = None) -> Optional[dict]:
    """Validate an LLM silver-sql response into {sql, not_null, comment}.
    Pure (no LLM, no warehouse) → unit-testable. Returns None if there is no
    usable single SELECT statement.

    Args:
        data: the parsed JSON response
        bronze_columns: set of column names from Bronze (used for guardrail validation)
        output_cols_required: set of output columns that MUST be present (e.g. eligible_flag, exclusion_reason)

    Returns:
        {sql, not_null, comment} or None
    """
    if not isinstance(data, dict):
        return None
    sql = str(data.get("sql") or "").strip().rstrip(";").strip()
    if not sql or not re.match(r"(?is)^\s*(with|select)\b", sql):
        return None
    # one statement only
    if ";" in sql:
        sql = sql.split(";")[0].strip()
    not_null = [str(c).strip() for c in (data.get("not_null") or []) if str(c).strip()]
    not_null = not_null[:4]  # cap to 4 NOT NULL columns
    comment = str(data.get("comment") or "")[:300]

    # Guardrails: if we know the Bronze columns, warn if the SQL references
    # columns that don't exist. If we know required output columns, warn if they're missing.
    if bronze_columns is not None:
        # Simple heuristic: look for column names in backticks or quoted identifiers
        mentioned = set(re.findall(r"`(\w+)`", sql)) | set(re.findall(r'"(\w+)"', sql))
        if mentioned:
            unknown = mentioned - bronze_columns - {"loan_id", "eligible_flag", "exclusion_reason"}
            if unknown:
                # Non-fatal but worth flagging: the LLM referenced columns that may not exist
                pass
    if output_cols_required is not None:
        # Required output columns may be emitted as bare identifiers (the common
        # Spark-SQL case: `... AS eligible_flag`), backtick-quoted, or double-quoted.
        # Match on word boundaries so a valid bare-identifier SELECT is not rejected —
        # the earlier quoted-only check returned None for every realistic LLM SELECT,
        # which silently forced the deterministic pass-through fallback.
        missing = {c for c in output_cols_required
                   if not re.search(rf"(?i)\b{re.escape(c)}\b", sql)}
        if missing:
            return None  # Critical: missing derived columns

    return {"sql": sql, "not_null": not_null, "comment": comment}


def suggest_silver_sql(bronze_columns: list[str], credit_box: dict,
                       business_rules: Optional[list[str]] = None, *,
                       feedback: Optional[str] = None, use_llm: bool = True) -> Optional[dict]:
    """Generate Silver SELECT to cast, derive eligible_flag/exclusion_reason, and dedupe.

    Args:
        bronze_columns: list of column names in Bronze table
        credit_box: dict with keys credit_score_min, ltv_max, arrears_days_max (the thresholds)
        business_rules: optional list of additional business rules to apply
        feedback: prior SQL error to fix (retry loop)
        use_llm: if False, return None (caller uses fallback)

    Returns:
        {sql, not_null, comment} or None
    """
    if not use_llm:
        return None
    from langchain_core.messages import HumanMessage, SystemMessage

    # Prepare the payload for the LLM
    payload = {
        "bronze_columns": bronze_columns,
        "column_types": _COLUMN_TYPES,
        "credit_box": credit_box,
        "business_rules": business_rules or [],
        "context": (
            "You are building the Silver layer for a Funding Optimisation pipeline. "
            "The Bronze data contains loan pool information. Each row must be typed, "
            "assessed for eligibility (eligible_flag + exclusion_reason), and deduplicated "
            "to the latest as_of_date per loan_id. The resulting Silver table is the "
            "source for the Gold layer's borrowing-base fact."
        ),
    }
    if feedback:
        payload["fix_this_error_from_your_previous_sql"] = feedback
    user = json.dumps(payload, default=str)[:11000]
    try:
        resp = llm.get_llm().invoke([SystemMessage(content=_SYSTEM), HumanMessage(content=user)])
        data = _loads_lenient(resp.content) or {}
    except Exception:
        return None

    # Validate the output
    bronze_set = set(bronze_columns)
    output_required = {"eligible_flag", "exclusion_reason"}
    return parse_silver_sql(data, bronze_columns=bronze_set, output_cols_required=output_required)
