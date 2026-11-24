"""Acme Corp REQ-0142 journey: Margin & Cash (Genie Agent + tracking dashboard).

The intake grill (LLM) captures the ask. The app then walks the analyst through
four reviewed steps, each backed by real objects in `vmohan_demo_catalog`:

  reuse        existing metric views / Genie Agents / dashboards, REUSE / EXTEND / GAP
  metric_view  the gap becomes a governed metric view (`mv_margin_leakage`)
  dq           existing rules from the rules repo first, then gap rules; review saved
  design       Genie Agent + dashboard design (sample questions + benchmarks, PII)

Everything here is deterministic except the natural-language DQ rule, which an LLM
turns into SQL; that SQL is then checked against the real table columns.
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import uuid
from typing import Any, Optional

COMPANY = os.environ.get("OPS_COMPANY_NAME", "Acme Corp")
CATALOG = os.environ.get("DEV_CATALOG", "vmohan_demo_catalog")
SRC_SCHEMA = os.environ.get("OPS_ACME_SOURCE_SCHEMA", "cfo_workbench")
DEV_SCHEMA = os.environ.get("DEV_GOLD_SCHEMA", "studio_dev")
STAGE_SCHEMA = os.environ.get("STAGE_SILVER_SCHEMA", "studio_stage")
PROD_SCHEMA = os.environ.get("PROD_SILVER_SCHEMA", "studio_prod")
CONTROL_SCHEMA = os.environ.get("OPS_CONTROL_SCHEMA", "studio_control")
WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "7464666eb7d50c27")
SRC = f"{CATALOG}.{SRC_SCHEMA}"
SCENARIO_KEY = "acme_margin_cash"
LATEST_WEEK = "2026-11-09"

# Real, already-deployed assets (fevm-azure). Overridable per deploy.
GENIE_MARGIN = os.environ.get("OPS_ACME_GENIE_MARGIN", "01f1e1fd7b9a1eac82c012943fb8e157")
GENIE_WC = os.environ.get("OPS_ACME_GENIE_WC", "01f1e1fd7c9212429be16be8140e9545")
DASH_MWC = os.environ.get("OPS_ACME_DASH_MWC", "01f1e201c7491dbb86c6c3d9085aaebd")
DASH_SALES = os.environ.get("OPS_ACME_DASH_SALES", "01f1e201d0271694a45ac8a8ced55606")

ASSET_DIR = pathlib.Path(__file__).resolve().parents[1] / "build_assets"


# --------------------------------------------------------------------------- utils
def host() -> str:
    h = (os.environ.get("DATABRICKS_HOST") or "").strip()
    if not h:
        try:
            from databricks.sdk import WorkspaceClient
            h = WorkspaceClient().config.host or ""
        except Exception:  # noqa: BLE001
            h = ""
    if h and not h.startswith("http"):
        h = f"https://{h}"
    return h.rstrip("/")


def table_link(fq: str) -> str:
    c, s, t = fq.split(".")
    return f"{host()}/explore/data/{c}/{s}/{t}"


def genie_link(space_id: str) -> str:
    return f"{host()}/genie/rooms/{space_id}"


def dashboard_link(dash_id: str, published: bool = False) -> str:
    return f"{host()}/dashboardsv3/{dash_id}" + ("/published" if published else "")


_WS = None


def _ws():
    """One WorkspaceClient per process (building one costs ~2.5 s)."""
    global _WS
    if _WS is None:
        from databricks.sdk import WorkspaceClient
        _WS = WorkspaceClient()
    return _WS


def sql(stmt: str) -> list[dict[str, Any]]:
    """Run one statement on the warehouse; return rows as dicts. Raises on failure."""
    w = _ws()
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=stmt, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        msg = r.status.error.message if r.status.error else r.status.state.value
        raise RuntimeError(msg)
    if not (r.manifest and r.manifest.schema and r.result):
        return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in (r.result.data_array or [])]


def lit(v: Any) -> str:
    if v is None:
        return "NULL"
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


# --------------------------------------------------------------------------- reuse
NEEDS = [
    # (need, decision, asset, why)
    ("Gross margin vs plan by week, category, branch", "REUSE", "mv_revenue_margin",
     "Governed metric view already has Gross Margin Pct, Plan Gross Margin Pct, GM vs Plan Bps."),
    ("Discounting (list vs net revenue)", "REUSE", "mv_revenue_margin",
     "Price Realisation Pct exists; leakage in dollars is added in the new view."),
    ("Rebate dependency (margin before supplier rebates)", "EXTEND", "mv_revenue_margin_draft",
     "Draft view has pre-rebate margin but is not certified and has no plan or freight."),
    ("Freight cost eating margin", "GAP", None,
     "No governed measure: freight sits in gold_fct_freight at week x branch grain."),
    ("DSO, past due > 30 days, top-200 exposure", "REUSE", "mv_working_capital",
     "Governed metric view: DSO Days, Past Due Over 30 Days, Collections Exposure Top 200."),
    ("Inventory older than 180 days", "REUSE", "mv_inventory_ageing",
     "Governed metric view: Inventory Over 180 Days."),
    ("One agent for margin AND cash questions", "EXTEND", "Margin & Pricing + Working Capital",
     "Two separate Genie Agents today; neither can answer both, neither sees leakage drivers."),
    ("Weekly tracking dashboard", "EXTEND", "Margin & Working Capital (draft)",
     "Draft dashboard has no leakage page; it is the starting layout for the tracker."),
]


def existing_coverage() -> dict[str, Any]:
    cards = [
        {"kind": "metric_view", "icon": "📐", "title": "mv_revenue_margin", "decision": "REUSE",
         "id": f"{SRC}.mv_revenue_margin", "open_url": table_link(f"{SRC}.mv_revenue_margin")},
        {"kind": "metric_view", "icon": "📐", "title": "mv_working_capital", "decision": "REUSE",
         "id": f"{SRC}.mv_working_capital", "open_url": table_link(f"{SRC}.mv_working_capital")},
        {"kind": "metric_view", "icon": "📐", "title": "mv_inventory_ageing", "decision": "REUSE",
         "id": f"{SRC}.mv_inventory_ageing", "open_url": table_link(f"{SRC}.mv_inventory_ageing")},
        {"kind": "metric_view", "icon": "📐", "title": "mv_revenue_margin_draft", "decision": "EXTEND",
         "id": f"{SRC}.mv_revenue_margin_draft", "open_url": table_link(f"{SRC}.mv_revenue_margin_draft")},
        {"kind": "genie", "icon": "🧞", "title": "Margin & Pricing", "decision": "EXTEND",
         "id": GENIE_MARGIN, "open_url": genie_link(GENIE_MARGIN)},
        {"kind": "genie", "icon": "🧞", "title": "Working Capital", "decision": "EXTEND",
         "id": GENIE_WC, "open_url": genie_link(GENIE_WC)},
        {"kind": "dashboard", "icon": "📊", "title": "Margin & Working Capital (draft)", "decision": "EXTEND",
         "id": DASH_MWC, "open_url": dashboard_link(DASH_MWC)},
    ]
    covered = sum(1 for n in NEEDS if n[1] == "REUSE") + 0.5 * sum(1 for n in NEEDS if n[1] == "EXTEND")
    pct = int(round(100 * covered / len(NEEDS)))
    return {
        "coverage_pct": pct,
        "headline": "What already exists for this ask",
        "agent_line": (f"Before we build anything, I checked the catalog. About {pct}% of what you asked for "
                       "is already covered by governed assets. Each need is marked REUSE, EXTEND or GAP. "
                       "Only the gap gets built."),
        "needs": [{"need": n, "decision": d, "asset": a, "why": w} for n, d, a, w in NEEDS],
        "cards": cards,
        "method": "Matched against the Unity Catalog metric views, Genie Agents and dashboards in this workspace.",
        "reuse_label": "Agree: extend these, build only the gap →",
        "build_label": "Build a new Genie space on the raw tables instead",
    }


def pushback_raw_tables() -> str:
    return ("I won't do that one. A Genie Agent on raw or gold tables would recompute margin and DSO its own "
            "way, and the answers would drift from the governed numbers the dashboards already use "
            "(GM 31.8%, DSO 47.0). The agent and the tracker must sit on the metric views. "
            "I've kept the plan: reuse the three metric views, extend the agents and dashboard, and build "
            "only the gap.")


# --------------------------------------------------------------------------- metric view
def metric_view_sql(schema: str) -> str:
    return (ASSET_DIR / "mv_margin_leakage.sql").read_text().replace("{schema}", schema)


def metric_view_yaml() -> str:
    s = metric_view_sql(DEV_SCHEMA)
    return s.split("AS $$", 1)[1].rsplit("$$", 1)[0].strip()


def _yaml_names(section: str) -> list[str]:
    y = metric_view_yaml()
    part = y.split(f"\n{section}:", 1)[1]
    part = re.split(r"\n[a-z_]+:\n", part, maxsplit=1)[0]
    return re.findall(r"- name: (.+)", part)


def metric_view_reco() -> dict[str, Any]:
    name = f"{CATALOG}.{DEV_SCHEMA}.mv_margin_leakage"
    return {
        "eyebrow": "Recommended · Unity Catalog metric view (fills the gap)",
        "agent_line": ("Margin leakage is a reusable business metric. I recommend defining it once as a "
                       "governed metric view, so the Genie Agent and the tracker dashboard compute leakage "
                       "the same way: gap to plan in bps, discount leakage, freight, and rebate dependency."),
        "name": name,
        "measures": _yaml_names("measures"),
        "dimensions": _yaml_names("dimensions"),
        "yaml": metric_view_yaml(),
        "why": ("One definition shared by the agent and the dashboard. Built on the existing gold tables "
                f"in {SRC}; freight is allocated to lines pro-rata to revenue within week and branch."),
        "spec_block": {"metric_view": {"name": name, "measures": _yaml_names("measures"),
                                       "dimensions": _yaml_names("dimensions"),
                                       "sql_template": "build_assets/mv_margin_leakage.sql"}},
        "add_label": "Add to spec",
        "skip_label": "Skip",
    }


def pushback_powerbi() -> str:
    return ("No. If Power BI calculates DSO, the dashboard and the Genie Agent will disagree the first time "
            "someone filters differently. DSO stays in mv_working_capital (Open AR / billed last 90 days x 90) "
            "and every consumer reads it from there.")


# --------------------------------------------------------------------------- DQ
GAP_TABLES = {
    "gold_fct_freight": f"{SRC}.gold_fct_freight",
    "silver_sales_invoice_line": f"{SRC}.silver_sales_invoice_line",
    "gold_plan_gross_margin": f"{SRC}.gold_plan_gross_margin",
}

PROPOSED_RULES = [
    {"table_name": "gold_fct_freight", "rule_name": "freight non-negative",
     "constraint_sql": "freight_cost_usd >= 0", "severity": "warn",
     "why": "negative freight would make contribution margin look better than it is"},
    {"table_name": "silver_sales_invoice_line", "rule_name": "discount within policy",
     "constraint_sql": "discount_pct BETWEEN 0 AND 60", "severity": "warn",
     "why": "discounts above 60% are outside pricing policy and distort leakage"},
    {"table_name": "gold_plan_gross_margin", "rule_name": "plan has every category",
     "constraint_sql": "plan_gross_margin_pct IS NOT NULL", "severity": "drop",
     "why": "GM vs plan needs a plan value for each category"},
]


def existing_rules() -> list[dict[str, Any]]:
    rows = sql(f"SELECT table_name, rule_name, constraint_sql, severity, why, owner "
               f"FROM {SRC}.dq_rules_repo WHERE enabled ORDER BY table_name, rule_name")
    return rows


def _table_columns(table_name: str) -> list[str]:
    rows = sql(f"SELECT column_name FROM {CATALOG}.information_schema.columns "
               f"WHERE table_schema = {lit(SRC_SCHEMA)} AND table_name = {lit(table_name)}")
    return [r["column_name"] for r in rows]


_SQL_WORDS = {"and", "or", "not", "is", "null", "between", "in", "like", "true", "false", "case", "when",
              "then", "else", "end", "coalesce", "abs", "length", "trim", "lower", "upper", "date",
              "current_date", "current_timestamp", "datediff", "cast", "as", "double", "int", "string"}


def check_constraint(table_name: str, constraint_sql: str) -> dict[str, Any]:
    """Reject a rule that references a column the table doesn't have."""
    from agent.genie_engine import _dq_rule_ok  # same guard the build uses (lazy: avoids an import cycle)
    refused = _dq_rule_ok(table_name, constraint_sql)
    if refused:
        return {"ok": False, "reason": refused}
    cols = {c.lower() for c in _table_columns(table_name)}
    if not cols:
        return {"ok": False, "reason": f"table {table_name} not found in {SRC}"}
    stripped = re.sub(r"'[^']*'", " ", constraint_sql)
    idents = {t.lower() for t in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", stripped)}
    missing = sorted(i for i in idents if i not in _SQL_WORDS and i not in cols)
    if missing:
        return {"ok": False, "reason": (f"column(s) {', '.join(missing)} don't exist on {table_name}. "
                                        f"Available: {', '.join(sorted(cols))}"), "missing": missing}
    try:
        n = sql(f"SELECT COUNT(*) AS n FROM {SRC}.{table_name} WHERE NOT ({constraint_sql})")[0]["n"]
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reason": "the rule did not run on the warehouse; check the column names and syntax"}
    return {"ok": True, "violations_now": int(n)}


def nl_to_rule(text: str) -> dict[str, Any]:
    """Natural-language rule → {table_name, rule_name, constraint_sql, severity, why} via the LLM."""
    from agent.llm import get_llm
    from langchain_core.messages import HumanMessage, SystemMessage
    tables = {t: _table_columns(t) for t in ("silver_sales_invoice_line", "gold_fct_freight",
                                             "silver_ar_transaction", "silver_inventory_on_hand",
                                             "gold_plan_gross_margin")}
    sys_msg = ("Turn a data-quality rule written in plain English into JSON with keys table_name, rule_name, "
               "constraint_sql (a SQL boolean expression that must be TRUE for good rows), severity "
               "('drop' or 'warn'), why. Use ONLY these tables and use the column names the user says, "
               "even if they are not in the list (the app checks them): "
               + json.dumps(tables) + ". Reply with the JSON only.")
    resp = get_llm().invoke([SystemMessage(content=sys_msg), HumanMessage(content=text)])
    raw = resp.content if isinstance(resp.content, str) else str(resp.content)
    m = re.search(r"\{.*\}", raw, re.S)
    rule = json.loads(m.group(0)) if m else {}
    rule.setdefault("severity", "warn")
    return rule


def ensure_control_tables() -> None:
    sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{CONTROL_SCHEMA}.dq_review "
        f"(review_id STRING, session_id STRING, request_id STRING, review_json STRING, ts TIMESTAMP)")


def save_dq_review(session_id: str, review: dict[str, Any]) -> str:
    ensure_control_tables()
    rid = str(uuid.uuid4())
    sql(f"INSERT INTO {CATALOG}.{CONTROL_SCHEMA}.dq_review VALUES ({lit(rid)}, {lit(session_id)}, NULL, "
        f"{lit(json.dumps(review))}, current_timestamp())")
    return rid


# --------------------------------------------------------------------------- design
def default_design() -> dict[str, Any]:
    return {
        "genie_agent": {
            "title": "Margin & Cash",
            "description": f"{COMPANY} finance agent: where margin is leaking and what cash is doing.",
            "data_sources": [f"{CATALOG}.{DEV_SCHEMA}.mv_margin_leakage", f"{SRC}.mv_working_capital",
                             f"{SRC}.mv_inventory_ageing"],
            "instructions": (f"{COMPANY} finance. Margin leakage = gross margin gap to plan in bps "
                             "(GM vs Plan Bps, negative = leaking), plus Discount Leakage USD and Freight Cost USD. "
                             f"Weeks start Monday; the latest full week is {LATEST_WEEK}. Cash = DSO Days, Past Due "
                             "Over 30 Days, Collections Exposure Top 200, and inventory older than 180 days. Always "
                             "use the metric views with MEASURE(); never recompute these in SQL. "
                             "Answering rules: (1) 'Where are we leaking margin' means: for the latest full week, "
                             "group GM vs Plan Bps and Gross Margin Pct by Category, name the worst category first "
                             "with its GM % and bps vs plan, then give the company total; break down by branch only "
                             "if asked. (2) DSO means company-wide DSO Days with no filter unless the user asks for a "
                             "segment; the top-200 filter applies only to overdue exposure (Collections Exposure "
                             "Top 200). The DSO target is 42 days."),
            "benchmarks": [
                {"question": "Where are we leaking margin this week, by category?",
                 "expected": "Fasteners: gross margin 30.1%, -140 bps vs plan; total GM 31.8%, -60 bps vs plan"},
                # Worded exactly like the grill's day-one questions so the reviewer can match them.
                # "Is it discounting, freight or rebates?" deliberately has no benchmark yet: the AI
                # reviewer flags that gap at the PR.
                {"question": "What is DSO against the 42-day target?",
                 "expected": "DSO 47.0 days (target 42)"},
                {"question": "How much is overdue more than 30 days in the top-200 accounts?",
                 "expected": "$6.26M"},
                {"question": "How much stock is older than 180 days?", "expected": "$4.10M"},
            ],
            "access": "account users: finance-analysts (CAN_RUN)",
        },
        "dashboard": {
            "title": "Margin & Cash Tracker",
            "pages": [
                {"name": "Margin", "widgets": ["Gross Margin Pct (latest week)", "GM vs Plan Bps (latest week)",
                                               "GM % vs plan by week (26 weeks)", "GM vs Plan Bps by category",
                                               "Discount leakage and freight by category"]},
                {"name": "Cash", "widgets": ["DSO Days", "Past Due Over 30 Days", "Collections Exposure Top 200",
                                             "Past due by segment", "Inventory older than 180 days"]},
            ],
            "filters": ["Week Start", "Region"],
            "refresh": "weekly, Monday 07:00 Sydney",
            "include_customer_names": False,
        },
    }


def pushback_pii() -> str:
    return ("Not on the shared tracker. Customer names are personal and commercially sensitive, and this "
            "dashboard is shared with the whole finance team. The Cash page shows segments and the top-200 "
            "total; anyone who needs names asks the Genie Agent, which applies their own permissions.")


# --------------------------------------------------------------------------- scenario + plan
def base_spec() -> dict[str, Any]:
    """The flat spec every intake session starts from (copy of the scenario spec)."""
    import copy
    return copy.deepcopy(scenario()["spec"])


def scenario() -> dict[str, Any]:
    spec = {
        "intent": ("Margin & Cash: a Genie Agent the finance team can ask where margin is leaking and what "
                   "is happening with cash, plus a weekly tracking dashboard."),
        "source_kind": "file",
        "source_system": "acme_erp",
        "source_tables": ["sales_invoice_lines", "ar_transactions", "inventory_on_hand", "freight_charges",
                          "supplier_rebates", "fpa_plan"],
        "source_incremental_column": "",
        "source_primary_key": [],
        "target_catalog": CATALOG,
        "target_schema": DEV_SCHEMA,
        "target_table": "mv_margin_leakage",
        "target_write_mode": "overwrite",
        "transform_description": "",
        "transform_business_rules": [],
        "gold_facts": [],
        "validation_not_null": [],
        "validation_unique": [],
        "validation_row_count_tolerance_pct": 5.0,
        "governance_data_owner": "vaishnavi.mohan@databricks.com",
        "governance_business_owner": "Head of FP&A",
        "governance_domain": "finance",
        "governance_classification": "confidential",
        "sla_mode": "batch",
        "sla_freshness_minutes": 10080,
        "sla_schedule_cron": "0 7 * * 1",
        "required_approvers": ["finance-data-owner", "data-platform-engineering"],
    }
    return {"key": SCENARIO_KEY,
            "label": f"{COMPANY} · Margin & Cash (Genie Agent + tracking dashboard)",
            "description": ("A finance analyst wants a Genie Agent that answers where margin is leaking and "
                            "what cash is doing, plus a weekly dashboard to track both. Reuse first; build only "
                            "the gap; data quality from the rules repo; Genie Code builds."),
            "spec": spec}


def plan_steps(data_product: dict[str, Any]) -> list[dict[str, Any]]:
    """The build plan the analyst approves, derived from a data_product v2 (see CONTRACT2.md).

    One step per thing the grill asked for: the metric view only if the plan has a new one, the Genie
    Agent only if need.outputs asks for it, the dashboard only if it asks for one. The DQ step always runs.
    A v1 (or unreadable) data product gets an empty plan; the engine refuses to build it anyway."""
    if (data_product or {}).get("version") not in (2, 3):
        return []
    need = data_product.get("need") or {}
    outputs = need.get("outputs") or []
    design = data_product.get("design") or {}
    steps: list[dict[str, Any]] = []
    pipe = data_product.get("pipeline") or None
    if pipe and pipe.get("name"):
        # RAW data (a feed with no bronze/silver/gold yet): the pipeline is the first build step; the metric
        # view, DQ, agent and dashboard all read the tables it creates.
        steps.append({"key": "pipeline", "node": "deploy_ingestion",
                      "title": f"Create the pipeline {pipe['name']}: raw feed to bronze, silver and gold in {DEV_SCHEMA}",
                      "engine": "genie_code", "skills": ["medallion-sdp", "databricks-spark-declarative-pipelines"],
                      "asset": pipe["name"]})
    mv = data_product.get("metric_view") or None
    if mv and mv.get("name"):
        steps.append({"key": "metric_view", "node": "pipeline_generator",
                      "title": f"Create the metric view {mv['name'].split('.')[-1]}",
                      "engine": "genie_code", "skills": ["metric-view-author", "governance-by-construction"],
                      "asset": mv["name"]})
    rules = [r for r in ((data_product.get("dq") or {}).get("rules") or []) if r.get("enabled", True)]
    steps.append({"key": "dq", "node": "standards_validator", "title": "Run the agreed DQ rules",
                  "engine": "app", "skills": ["dq-standards"], "asset": f"{len(rules)} rules"})
    ga = design.get("genie_agent") if "genie_agent" in outputs else None
    if ga:
        steps.append({"key": "genie_agent", "node": "test_generator",
                      "title": f"Create the Genie Agent '{ga.get('title', '')}'",
                      "engine": "genie_code", "skills": ["genie-agent-author", "business-glossary"],
                      "asset": ga.get("title", "")})
    db = design.get("dashboard") if "dashboard" in outputs else None
    if db:
        steps.append({"key": "dashboard", "node": "deploy_dev",
                      "title": f"Create the dashboard '{db.get('title', '')}'",
                      "engine": "genie_code", "skills": ["aibi-dashboard-standards"],
                      "asset": db.get("title", "")})
    return steps
