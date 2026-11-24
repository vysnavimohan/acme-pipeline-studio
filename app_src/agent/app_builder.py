"""Agentic app-manifest generation (the 'consume' step).

Given the request's AppSpec + the Gold contract (tables the request built, with
their columns), an LLM authors an app MANIFEST: which Gold columns become KPIs,
which become charts, tenancy scoping, Genie, writeback. The deterministic
`tools.app_emitter` then stamps a Streamlit app from that manifest — config not
code, mirroring how `gold_sql` + the pipeline generator work.

Best-effort: `suggest_app_manifest` returns None on failure; the node falls back
to a deterministic manifest built straight from the AppSpec so a build never
stalls on the LLM.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from agent import llm

_SYSTEM = (
    "You are a senior Databricks solutions engineer designing a governed Databricks "
    "App on top of a GOLD medallion layer. You are given: the app INTENT + audience, "
    "the GOLD tables the pipeline built (fully-qualified) with their columns, and app "
    "STANDARDS the app must obey. Author a single JSON APP MANIFEST the deterministic "
    "emitter will render into a Streamlit app. RULES: (1) reference ONLY columns that "
    "exist in the provided gold schemas; (2) pick 3-5 KPIs that answer the audience's "
    "core decision — each is {label, column, agg(sum|avg|count|max|min), "
    "format(money|pct|int|float), help}; (3) pick 2-4 charts — each is {title, "
    "kind(bar|line|table), x, y, source_table, limit}; x/y MUST be real gold columns; "
    "(4) if the standards require single-tenant scoping, set tenant_column to the gold "
    "column that identifies a tenant/facility/program (else \"\"); (5) NEVER invent a "
    "column or a table; (6) choose money format for balances/amounts, pct for rates/"
    "ratios, int for counts. Return STRICT JSON ONLY with keys: name (kebab-case), "
    "title, subtitle, primary_table (one of the gold tables, unqualified), "
    "tenant_column, kpis, charts, include_genie (bool), include_writeback (bool)."
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


def build_manifest(app_spec: dict, gold_schemas: dict, *,
                   catalog: str, gold_schema: str,
                   warehouse_id: str = "", brand: Optional[dict] = None,
                   llm_data: Optional[dict] = None) -> dict[str, Any]:
    """Merge the AppSpec (what the user asked for) with any LLM suggestions and
    the deployment facts (catalog/schema/warehouse) into a complete manifest the
    emitter accepts. PURE (no LLM/IO) → unit-testable. `llm_data` is the parsed
    LLM output or None (then we fall back to the AppSpec fields)."""
    gold_tables = [t.split(".")[-1] for t in gold_schemas.keys()] or \
        [str(t) for t in (app_spec.get("gold_tables") or [])]
    primary = ""
    if llm_data and llm_data.get("primary_table"):
        primary = str(llm_data["primary_table"]).split(".")[-1]
    if primary not in gold_tables:
        primary = gold_tables[0] if gold_tables else ""

    def _from(key, default):
        if llm_data and llm_data.get(key) not in (None, "", [], {}):
            return llm_data[key]
        return app_spec.get(key, default)

    manifest: dict[str, Any] = {
        "name": (_from("name", app_spec.get("name")) or "generated-console").strip().replace(" ", "-"),
        "title": _from("title", app_spec.get("title")) or "Generated Console",
        "subtitle": _from("subtitle", app_spec.get("subtitle")) or "",
        "catalog": catalog,
        "gold_schema": gold_schema,
        "primary_table": primary,
        "tenant_column": _from("tenant_column", app_spec.get("tenant_column", "")) or "",
        "kpis": _from("kpis", [k for k in (app_spec.get("kpis") or [])]),
        "charts": _from("charts", [c for c in (app_spec.get("charts") or [])]),
        "include_genie": bool(_from("include_genie", app_spec.get("include_genie", True))),
        "include_writeback": bool(_from("include_writeback", app_spec.get("include_writeback", False))),
        "warehouse_id": warehouse_id,
        "genie_space_id": app_spec.get("genie_space_id", ""),
    }
    if brand:
        manifest["brand"] = brand
    return manifest


def suggest_app_manifest(app_spec: dict, gold_schemas: dict, standards: str, *,
                         use_llm: bool = True) -> Optional[dict]:
    """Ask the LLM (through the governed gateway) to author the manifest core.
    Returns the parsed LLM dict or None. The caller passes this to
    build_manifest() to merge with deployment facts."""
    if not use_llm:
        return None
    from langchain_core.messages import HumanMessage, SystemMessage
    payload = {
        "app_intent": app_spec.get("audience") or app_spec.get("title") or app_spec.get("name"),
        "app_spec": {k: app_spec.get(k) for k in
                     ("title", "subtitle", "tenant_column", "include_genie",
                      "include_writeback", "kpis", "charts")},
        "gold_schemas": {k: [c[0] if isinstance(c, (list, tuple)) else c for c in v]
                         for k, v in gold_schemas.items()},
        "app_standards": standards,
    }
    user = json.dumps(payload, default=str)[:12000]
    try:
        resp = llm.get_llm().invoke([SystemMessage(content=_SYSTEM),
                                     HumanMessage(content=user)])
        return _loads_lenient(resp.content)
    except Exception:
        return None
