"""Genie Code build engine for Acme Corp data products (spec.data_product.engine == "genie_code", version 2).

The grill decides WHAT is built (data_product v2, see CONTRACT2.md). The engine builds only that:

  metric view   only if data_product.metric_view (SQL from metric_view.sql_template, per env)
  Genie Agent   only if need.outputs has "genie_agent" (design.genie_agent)
  dashboard     only if need.outputs has "dashboard" (design.dashboard, widget SQL validated at intake)

The tracker (app/static/tracker.js) polls request_events, so everything the engine does is an event.
Two human gates, both handled by the request routes:

  initiate  (gate 1, "Approve plan") -> start_build(): spec check, source profile, Genie Code metric
                                        view, DQ rules, live check of benchmarks + widgets, Genie Code
                                        Genie Agent and dashboard in parallel, PR artefacts, AI review;
                                        parks at pending_review.
  approve   (gate 2)                 -> start_promote(): verify, staging (new metric view + widget and
                                        benchmark re-run), prod (metric view, Genie Agent, published
                                        dashboard, benchmarks), release record, done.

Every Genie Code step: IN_PROGRESS with the job run link at dispatch, then SUCCEEDED/FAILED after the
asset is verified through the API. One retry on a failed or unverified run; then the app creates the
asset itself and says so ("fallback: created by the app, not Genie Code").

Event contract (details.kind):
  spec_check, source_profile, genie_code_run (asset/env/job_run_url/conversation_url/asset_id/
  asset_url/response_excerpt/verified/fallback), dq_check, live_check (benchmark + widget SQL re-run),
  mock_pr (PR files + artifacts), pr_review_comment (verdict, checklist, comment_md), await_human,
  stage_asset / stage_check, benchmark_results (env, rows), prod_asset, prod_release, done.
"""

from __future__ import annotations

import base64
import datetime as dt
import difflib
import json
import logging
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Optional

import yaml

from agent import acme_journey as aj
from agent import event_store
from agent import sql_guard
from agent.state import StepEvent, StepName, StepStatus

LOG = logging.getLogger("ops.genie_engine")

ROOT = Path(__file__).resolve().parents[1]
PROMPT_DIR = ROOT / "build_assets" / "genie_prompts"

JOB_ID = int(os.environ.get("OPS_GENIE_CODE_JOB_ID", "28927436286061"))
ORG_ID = os.environ.get("OPS_WORKSPACE_ORG_ID", "7405604635797033")
FOLDER = os.environ.get("OPS_DASHBOARD_FOLDER",
                        "/Workspace/Users/vaishnavi.mohan@databricks.com/acme_studio")
# Dashboards the APP creates itself (prod copy, fallbacks) run as the app identity, so they need a
# folder that identity owns. Genie Code (runs as the job owner) keeps using FOLDER.
APP_FOLDER = os.environ.get("OPS_APP_DASHBOARD_FOLDER", FOLDER)
WAREHOUSE = aj.WAREHOUSE
CAT = aj.CATALOG
DEV, STAGE, PROD = aj.DEV_SCHEMA, aj.STAGE_SCHEMA, aj.PROD_SCHEMA
SRC_SCHEMA = aj.SRC_SCHEMA
SRC = aj.SRC
MAX_ATTEMPTS = 2
TERMINAL = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
PII_RE = r"customer_name|customer name|dim_customer\.name|customer_full_name|\bcust_name\b"
OUTPUTS = ("dashboard", "genie_agent")
# Raw landing volume: the pipeline may read only the feeds listed here (the folder under RAW_LANDING).
RAW_LANDING = os.environ.get("OPS_RAW_LANDING", "/Volumes/vmohan_demo_catalog/studio_control/raw_landing")
RAW_FEEDS = {"carrier_shipments"}
APP_SP = os.environ.get("OPS_APP_SP_ID", "709e095d-db7e-4d9f-a243-a51ccbcb5764")
PUBLIC_SCHEMAS = {SRC_SCHEMA, DEV, STAGE, PROD}          # the only schemas a listed source may come from

S = StepStatus
N = StepName


class _Ctx:
    """Tracks which step a thread is on, so an unexpected crash lands on the right step."""

    def __init__(self) -> None:
        self.step: StepName = N.INITIATED


# --- input guards (security review 2026-11-18) ------------------------------------------
# DQ rules, sources and queries arrive in the request spec, so treat them as untrusted.
DQ_TABLES = {"silver_sales_invoice_line", "silver_ar_transaction", "silver_inventory_on_hand",
             "silver_customer_hist", "gold_fct_freight", "gold_plan_gross_margin",
             "gold_sales_line_rebate_alloc", "gold_fct_sales_line", "gold_fct_ar_invoice"}
_DQ_FORBIDDEN = re.compile(r"(;|--|/\*|\b(select|insert|update|delete|drop|alter|create|grant|revoke|merge|"
                           r"call|exec|union|from|join|with)\b)", re.I)
_DQ_ALLOWED_CHARS = re.compile(r"^[A-Za-z0-9_ .,'<>=!()%+\-*/]+$")
_DQ_FUNCTIONS = {"coalesce", "abs", "length", "trim", "lower", "upper", "round", "nullif", "datediff", "date",
                 "current_date", "isnull", "isnotnull", "in"}
_FQ_OK = re.compile(r"^" + re.escape(CAT) + r"\.(" + "|".join(sorted(PUBLIC_SCHEMAS)) + r")\.[a-z0-9_]+$")
_FQ_TOKEN = re.compile(re.escape(CAT) + r"\.[`\w]+\.[`\w]+", re.I)


def _dq_rule_ok(table_name: str, constraint_sql: str, extra: tuple = ()) -> Optional[str]:
    """None if the rule may run; otherwise the reason it is refused. Three layers: the table allowlist (plus the
    tables of this request's pipeline, passed as extra), the text guards, and the parser (agent/sql_guard: the
    condition must parse as a plain boolean expression)."""
    if table_name not in DQ_TABLES and table_name not in extra:
        return f"table {table_name!r} is not an allowed DQ source table"
    c = constraint_sql or ""
    if not c.strip() or len(c) > 400:
        return "constraint is empty or too long"
    if not _DQ_ALLOWED_CHARS.match(c) or _DQ_FORBIDDEN.search(c):
        return "constraint must be a simple column condition (no statements, subqueries or comments)"
    if re.search(r"\.\s*[A-Za-z0-9_]+\s*\(", c) or re.search(r"[`\x22\[\]]", c):
        return "qualified or quoted function calls are not allowed in a DQ rule"
    bad_fns = sorted({f.lower() for f in re.findall(r"(?<![A-Za-z0-9_])([A-Za-z0-9_]+)\s*\(", c)} - _DQ_FUNCTIONS)
    if bad_fns:
        return f"function(s) not allowed in a DQ rule: {', '.join(bad_fns)}"
    return sql_guard.check_expression(c)


def _nfq(name: Any) -> str:
    """Normalise a fully-qualified name: lower case, no backticks, no spaces."""
    return str(name or "").replace("`", "").strip().lower()


def _fq_ok(fq: str) -> bool:
    return bool(_FQ_OK.match(fq or ""))


def _listed_table_ok(v: dict, schema: str = DEV) -> Callable[[str], bool]:
    """Tables a query may read: ONLY the data product's listed sources (exact names; the new metric view in
    the env's schema). Dashboards and benchmarks read metric views, never raw gold tables."""
    allowed = {_to_env(x, v, schema).lower() for x in v["listed"]}
    return lambda fq: fq.lower() in allowed


def _guard_sql(sql: str, v: dict, schema: str = DEV) -> Optional[str]:
    """sql_guard on a benchmark or widget query, after the environment's metric view name is applied."""
    return sql_guard.check(_sub_mv(sql, v, schema), _listed_table_ok(v, schema))


def _refs(sqls: list[str]) -> list[str]:
    out: list[str] = []
    for s in sqls:
        for tok in _FQ_TOKEN.findall(s or ""):
            n = _nfq(tok)
            if n not in out:
                out.append(n)
    return out


# ----------------------------------------------------------------------------- pipeline (RAW data)
# Data that exists only as raw files gets a Lakeflow Spark Declarative Pipeline (raw -> bronze -> silver ->
# gold) built by Genie Code in the dev schema, then created or updated and run per env by the app identity.
# The SQL is checked HERE, on the server, before anything runs: the source must be an allowlisted raw feed,
# the targets must be the planned bronze/silver/gold tables (created in the env schema only, since the
# pipeline's target schema is set per env), every SELECT must pass agent/sql_guard, and the CONSTRAINT ...
# EXPECT clauses are the data-quality gate (ON VIOLATION DROP ROW = drop, none = warn).
_PIPE_NAME = re.compile(r"^acme-[a-z0-9][a-z0-9-]{2,50}$")
_PIPE_TBL = re.compile(r"^(bronze|silver|gold)_[a-z0-9_]{1,50}$")
_PIPE_HEAD = re.compile(r"^CREATE\s+OR\s+REFRESH\s+(?:STREAMING\s+TABLE|MATERIALIZED\s+VIEW)\s+([a-z_][a-z0-9_]*)\s*", re.I)
_PIPE_KIND = re.compile(r"^CREATE\s+OR\s+REFRESH\s+(STREAMING\s+TABLE|MATERIALIZED\s+VIEW)", re.I)
_PIPE_READ = re.compile(r"read_files\s*\(([^()]*)\)", re.I)
_PIPE_REF = re.compile(r"\b(?:FROM|JOIN)\s+(?:STREAM\s+)?((?:bronze|silver|gold)_[a-z0-9_]+)\b", re.I)
_PIPE_CONS = re.compile(r"CONSTRAINT\s+([a-z_][a-z0-9_]{0,60})\s+EXPECT\s*\(", re.I)
_RAW_FQ = "guard_catalog.guard_schema.raw_feed"


def _feed_of(path: str) -> Optional[str]:
    """The feed name when path is exactly an allowlisted raw landing folder, else None."""
    p = (path or "").strip().rstrip("/")
    for feed in RAW_FEEDS:
        if p == f"{RAW_LANDING}/{feed}":
            return feed
    return None


def _balanced(text: str, start: int) -> int:
    """Index just past the ')' that closes the '(' at text[start] (quoted strings skipped); -1 if unbalanced."""
    depth, in_str = 0, False
    for i in range(start, len(text)):
        c = text[i]
        if c == "'":
            in_str = not in_str
        elif not in_str and c == "(":
            depth += 1
        elif not in_str and c == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    return -1


def _split_top_level(text: str) -> Optional[list[str]]:
    """Split at commas outside parentheses and quotes; None if parentheses or quotes are unbalanced."""
    parts, buf, depth, quote = [], [], 0, None
    for ch in text:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
            continue
        if ch in ("'", '"', "`"):
            quote = ch
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return None
        elif ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    if depth != 0 or quote:
        return None
    parts.append("".join(buf))
    return [x.strip() for x in parts]


_EXPECT_ELEM = re.compile(r"CONSTRAINT\s+([a-z_][a-z0-9_]{0,60})\s+EXPECT\s*\((.*)\)"
                          r"(?:\s+ON\s+VIOLATION\s+(DROP\s+ROW|FAIL\s+UPDATE))?", re.I | re.S)


def _expectations_of(defn: str, table: str) -> tuple[list[dict], list[str]]:
    """The CONSTRAINT ... EXPECT clauses of one table definition. The definition is tokenised at top-level commas
    and EVERY element must be exactly `CONSTRAINT name EXPECT (cond) [ON VIOLATION DROP ROW]` - nothing else."""
    out: list[dict] = []
    probs: list[str] = []
    elems = _split_top_level(defn or "")
    if elems is None:
        return out, [f"{table}: unbalanced parentheses or quotes in the table definition"]
    for el in [e for e in elems if e]:
        m = _EXPECT_ELEM.fullmatch(el)
        if not m:
            probs.append(f"{table}: only CONSTRAINT name EXPECT (condition) [ON VIOLATION DROP ROW] may appear")
            continue
        name, cond, action = m.group(1), m.group(2).strip(), (m.group(3) or "")
        if _split_top_level(cond) is None or _balanced("(" + cond + ")", 0) != len(cond) + 2:
            probs.append(f"{table}: expectation {name} condition is not a single balanced expression")
            continue
        if action.upper().startswith("FAIL"):
            probs.append(f"{table}: expectation {name} uses FAIL UPDATE; use DROP ROW or warn")
            continue
        why = sql_guard.check_expression(cond)
        if why:
            probs.append(f"{table}: expectation {name} refused: {why}")
            continue
        out.append({"table": table, "name": name, "constraint_sql": cond, "action": "drop" if action else "warn"})
    return out, probs


def _pipeline_check(pl: Any) -> tuple[list[str], Optional[dict]]:
    """Server-side check of the pipeline block. (problems, info). info is None when anything is refused."""
    if not isinstance(pl, dict) or not pl:
        return ["no pipeline block"], None
    probs: list[str] = []
    name = str(pl.get("name") or "")
    if not _PIPE_NAME.match(name):
        probs.append(f"pipeline name {name!r} must start with acme- and use lower case, digits and dashes")
    src = pl.get("source") or {}
    path = str(src.get("path") or "").strip()
    feed = _feed_of(path)
    if not feed:
        probs.append(f"source {path!r} is not an allowlisted raw feed")
    if str(src.get("format") or "csv").lower() != "csv":
        probs.append("the raw feed must be csv")
    tabs = pl.get("tables") or {}
    bronze, silver = str(tabs.get("bronze") or ""), str(tabs.get("silver") or "")
    gold = [str(g) for g in (tabs.get("gold") or [])]
    planned = [bronze, silver] + gold
    if not gold:
        probs.append("at least one gold table is needed")
    for t in planned:
        if not _PIPE_TBL.match(t):
            probs.append(f"table name {t!r} must be bronze_, silver_ or gold_ plus lower case letters, digits, underscores")
    if not bronze.startswith("bronze_") or not silver.startswith("silver_") or any(not g.startswith("gold_") for g in gold):
        probs.append("tables must be one bronze_, one silver_ and gold_ table(s)")
    if len(set(planned)) != len(planned):
        probs.append("table names must be unique")
    sql = str(pl.get("sql") or "").strip()
    if not sql or len(sql) > 20000:
        probs.append("the pipeline SQL is empty or too long")
    elif re.search(r"--|/\*|\*/|`|\"|\\", sql):
        probs.append("the pipeline SQL may not contain comments, backticks, double quotes or backslashes")
    if probs:
        return probs, None

    stmts = [x.strip() for x in sql.split(";") if x.strip()]
    guard_names = {f"guard_catalog.guard_schema.{t}" for t in planned} | {_RAW_FQ}
    defined: list[str] = []
    exps: list[dict] = []
    for st in stmts:
        m = _PIPE_HEAD.match(st)
        if not m:
            probs.append("every statement must be CREATE OR REFRESH STREAMING TABLE or MATERIALIZED VIEW")
            continue
        table = m.group(1).lower()
        if table not in planned:
            probs.append(f"{table} is not one of the planned tables")
            continue
        if table in defined:
            probs.append(f"{table} is defined twice")
            continue
        rest = st[m.end():].lstrip()
        defn = ""
        if rest.startswith("("):
            close = _balanced(rest, 0)
            if close < 0:
                probs.append(f"{table}: unbalanced parentheses")
                continue
            defn, rest = rest[1:close - 1], rest[close:]
        am = re.search(r"\bAS\s+(SELECT|WITH)\b", rest, re.I)
        if not am:
            probs.append(f"{table} has no AS SELECT body")
            continue
        head = re.sub(r"COMMENT\s+'[^']*'", "", rest[:am.start()], flags=re.I).strip()
        if head:
            probs.append(f"{table}: only a COMMENT may come before AS")
        body = rest[am.start() + 2:].strip()
        e_list, e_probs = _expectations_of(defn, table)
        exps.extend(e_list)
        probs.extend(e_probs)
        reads = _PIPE_READ.findall(body)
        if table == bronze:
            if len(reads) != 1 or not re.search(r"\bSTREAM\s+read_files", body, re.I):
                probs.append(f"{table} must read the raw feed once, as STREAM read_files(...)")
            for args in reads:
                pm = re.match(r"\s*'([^']*)'", args)
                if not pm or _feed_of(pm.group(1)) != feed:
                    probs.append(f"{table} reads a path other than the allowlisted raw feed")
        elif reads:
            probs.append(f"{table}: only the bronze table may read the raw feed")
        for r in (x.lower() for x in _PIPE_REF.findall(body)):
            if r == table or r not in defined:
                probs.append(f"{table} reads {r}, which must be defined before it")
        g = _PIPE_READ.sub(lambda _m: _RAW_FQ, body)
        g = re.sub(r"\bSTREAM\s+(?=guard_catalog|bronze_|silver_|gold_)", "", g, flags=re.I)
        g = re.sub(r"\b(FROM|JOIN)\s+((?:bronze|silver|gold)_[a-z0-9_]+)\b", r"\1 guard_catalog.guard_schema.\2", g,
                   flags=re.I)
        why = sql_guard.check(g, lambda fq: fq in guard_names)
        if why:
            probs.append(f"{table} SELECT refused: {why}")
        defined.append(table)
    missing = [t for t in planned if t not in defined]
    if missing:
        probs.append("missing table definitions: " + ", ".join(missing))
    names = [e["name"] for e in exps]
    if len(set(names)) != len(names):
        probs.append("expectation names must be unique")
    if probs:
        return probs, None
    return [], {"name": name, "feed": feed, "path": f"{RAW_LANDING}/{feed}", "sql": sql,
                "tables": {"bronze": bronze, "silver": silver, "gold": gold}, "planned": planned,
                "expectations": exps}


# ----------------------------------------------------------------------------- plumbing
def _ws():
    return event_store._get_ws_client()


def _host() -> str:
    return (aj.host() or (_ws().config.host or "")).rstrip("/")


def _api(method: str, path: str, body: Any = None, query: Optional[dict] = None) -> Any:
    """REST call with retries on transient errors (timeouts, 429, 5xx). GETs retry 4x, PATCH 2x, POST never."""
    from databricks.sdk.errors import DeadlineExceeded, TooManyRequests, InternalError, TemporarilyUnavailable
    tries = {"GET": 4, "PATCH": 2}.get(method, 1)   # never retry a POST create (duplicates)
    for i in range(tries):
        try:
            return _ws().api_client.do(method, path, body=body, query=query)
        except (DeadlineExceeded, TooManyRequests, InternalError, TemporarilyUnavailable) as e:
            if i == tries - 1:
                raise RuntimeError(f"{type(e).__name__} calling {method} {path} after {tries} tries") from e
            LOG.warning("transient %s on %s %s, retry %d", type(e).__name__, method, path, i + 1)
            time.sleep(3 * (i + 1))


def _sql_cols(stmt: str) -> tuple[list[str], list[dict[str, Any]]]:
    w = _ws()
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=stmt, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        raise RuntimeError((r.status.error.message if r.status.error else r.status.state.value) or "SQL failed")
    if not (r.manifest and r.manifest.schema and r.result):
        return [], []
    cols = [c.name for c in r.manifest.schema.columns]
    return cols, [dict(zip(cols, row)) for row in (r.result.data_array or [])]


def _sql(stmt: str) -> list[dict[str, Any]]:
    return _sql_cols(stmt)[1]


def _emit(rid: str, step: StepName, status: StepStatus, message: str, **details: Any) -> None:
    event_store.append_event(rid, StepEvent(step=step, status=status, message=message, details=details))


def _fail(rid: str, step: StepName, message: str, **details: Any) -> None:
    _emit(rid, step, S.FAILED, message, kind=details.pop("kind", "genie_failed"), **details)
    event_store.update_request(rid, final_status="failed")


def _details(e: dict) -> dict:
    d = e.get("details")
    if not isinstance(d, dict):
        try:
            d = json.loads(e.get("details_json") or "{}")
        except Exception:  # noqa: BLE001
            d = {}
    return d if isinstance(d, dict) else {}


def _events(rid: str) -> list[dict]:
    return [dict(e, _d=_details(e)) for e in event_store.fetch_events(rid)]


def _last(rid: str, pred: Callable[[dict], bool]) -> Optional[dict]:
    hit = None
    for e in _events(rid):
        if pred(e["_d"]):
            hit = e["_d"]
    return hit


def _spec_dp(req: dict) -> tuple[dict, dict]:
    sj = req.get("spec_json")
    spec = json.loads(sj) if isinstance(sj, str) else (sj or {})
    return spec, (spec.get("data_product") or {})


def is_genie(req: dict) -> bool:
    try:
        return _spec_dp(req)[1].get("engine") == "genie_code"
    except Exception:  # noqa: BLE001
        return False


def refusal_for(req: dict) -> Optional[str]:
    """Why the engine will not build this request (None = it can build it)."""
    try:
        _, dp = _spec_dp(req)
    except Exception:  # noqa: BLE001
        return "The request spec could not be read."
    if dp.get("version") not in (2, 3):
        return ("This request uses data_product v1, which the engine no longer builds. Submit a new request "
                "from the intake; it is built from the v2 plan.")
    reuse = dp.get("reuse") or {}
    if reuse.get("buildable") is False:
        gaps = "; ".join(f"{x.get('need')}: {x.get('missing')}" for x in (reuse.get("no_data") or [])) or "see the plan"
        return f"The Acme data cannot answer this ask yet ({gaps}). Nothing was built."
    need = dp.get("need") or {}
    outs = [o for o in (need.get("outputs") or []) if o in OUTPUTS]
    if not outs:
        return "Nothing to build: the plan asks for no output. Choose a dashboard, a Genie Agent, or both."
    design = dp.get("design") or {}
    missing = [o for o in outs if not design.get(o)]
    if missing:
        return "The design is missing the requested output(s): " + ", ".join(missing) + "."
    if dp.get("pipeline"):
        probs, _info = _pipeline_check(dp.get("pipeline"))
        if probs:
            return "The pipeline SQL was refused by the server checks: " + "; ".join(probs[:3]) + ". Nothing was built."
        if not (dp.get("metric_view") or {}).get("name"):
            return "A pipeline needs a metric view over its gold table. Nothing was built."
    return None


def _fill(name: str, **vars: Any) -> str:
    text = (PROMPT_DIR / name).read_text()
    # The app verifies and reads what Genie Code builds, as its own service principal, so every
    # prompt tells Genie Code (running as the job owner) to share the new asset with it.
    vars.setdefault("APP_SP", os.environ.get("OPS_APP_SP_ID", "709e095d-db7e-4d9f-a243-a51ccbcb5764"))
    for k, v in vars.items():
        text = text.replace("{{" + k + "}}", str(v))
    return text


def _hex() -> str:
    return uuid.uuid4().hex


def _ref(rid: str) -> str:
    return f"REQ-{rid[:8].upper()}"


def _norm(q: str) -> list[str]:
    return re.sub(r"[^a-z0-9 ]+", " ", q.lower()).split()  # tokens


def _same_question(a: str, b: str) -> bool:
    ta, tb = _norm(a), _norm(b)
    if not ta or not tb:
        return False
    if ta == tb:
        return True
    return difflib.SequenceMatcher(None, " ".join(ta), " ".join(tb)).ratio() >= 0.9


def _one_line(sql: str) -> str:
    return " ".join((sql or "").split())


# ----------------------------------------------------------------------------- v2 view of a request
def _view(spec: dict, dp: dict) -> dict[str, Any]:
    """Everything the build needs, derived once from the v2 data product."""
    need = dp.get("need") or {}
    design = dp.get("design") or {}
    requested = [o for o in (need.get("outputs") or []) if o in OUTPUTS]
    mv = dp.get("metric_view") or None
    mv_dev = _nfq(mv.get("name")) if mv else ""
    listed = [_nfq(s) for s in (dp.get("data_sources") or []) if _nfq(s)]
    if mv_dev and mv_dev not in listed:
        listed.append(mv_dev)
    listed = list(dict.fromkeys(listed))
    ga = design.get("genie_agent") if "genie_agent" in requested else None
    db = design.get("dashboard") if "dashboard" in requested else None
    questions = [str(q).strip() for q in (need.get("questions") or []) if str(q).strip()]
    if not questions and ga:
        questions = [str(q).strip() for q in (ga.get("sample_questions") or []) if str(q).strip()]
    widgets = [w for p in ((db or {}).get("pages") or []) for w in (p.get("widgets") or [])]
    product = (ga or {}).get("title") or (db or {}).get("title") or need.get("slug") or "data product"
    mv_base = mv_dev.split(".")[-1] if mv_dev else ""
    pipe_probs, pipe = _pipeline_check(dp.get("pipeline")) if dp.get("pipeline") else ([], None)
    gold = [f"{CAT}.{DEV}.{g}" for g in pipe["tables"]["gold"]] if pipe else []
    return {
        "pipe": pipe, "pipe_problems": pipe_probs, "pipe_tables": list(pipe["planned"]) if pipe else [], "gold": gold,
        "need": need, "outputs": requested, "mv": mv, "mv_dev": mv_dev, "mv_base": mv_base,
        "listed": listed, "sources": [s for s in listed if _fq_ok(s)],
        "ga": ga, "db": db, "widgets": widgets, "questions": questions, "product": product,
        "slug": re.sub(r"[^a-z0-9_]+", "_", str(need.get("slug") or mv_base or "data_product").lower()),
        "ref_sources_dash": _refs([w.get("sql") or "" for w in widgets]),
    }


def _env_fq(v: dict, schema: str) -> str:
    return f"{CAT}.{schema}.{v['mv_base']}"


def _to_env(fq: str, v: dict, schema: str) -> str:
    """The new metric view maps to its env name; reused metric views keep their own name."""
    return _env_fq(v, schema) if (v["mv_dev"] and _nfq(fq) == v["mv_dev"]) else _nfq(fq)


def _sub_mv(sql: str, v: dict, schema: str) -> str:
    if not v["mv_dev"] or schema == DEV:
        return sql
    return re.sub(re.escape(v["mv_dev"]), _env_fq(v, schema), sql or "", flags=re.I)


_MV_BASE_RE = re.compile(r"^mv_[a-z0-9_]{1,60}$")
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9 _%&()./+-]{1,80}$")


def _mv_base(mv: dict) -> str:
    base = str(mv.get("name") or "").split(".")[-1].strip().lower()
    if not _MV_BASE_RE.match(base):
        raise RuntimeError(f"metric view name not allowed: {base!r}")
    return base


def _check_mv_yaml_local(body: str, gold: tuple) -> tuple[Optional[str], Optional[dict]]:
    """dp_planner._check_mv_yaml for a metric view over a pipeline's gold table: the same rules, but the allowed
    sources are the cfo_workbench gold tables AND the planned gold table(s) of this request's pipeline."""
    from agent import dp_planner as dpp
    if ";" in body or "$$" in body:
        return "YAML must not contain semicolons or $$", None
    try:
        y = yaml.safe_load(body)
    except yaml.YAMLError:
        return "YAML does not parse", None
    if not isinstance(y, dict) or str(y.get("version")) != "1.1":
        return "version must be 1.1", None
    source = y.get("source")
    if not isinstance(source, str) or not source.strip():
        return "source: is required (a SQL SELECT in a | block)", None
    dims, meas = y.get("dimensions") or [], y.get("measures") or []
    if not dims or not meas:
        return "need at least one dimension and one measure", None
    names_by_kind: dict[str, list[str]] = {}
    for kind, items in (("dimension", dims), ("measure", meas)):
        names = []
        for it in items:
            if not isinstance(it, dict) or not it.get("name") or not it.get("expr"):
                return f"every {kind} needs name and expr", None
            if not str(it.get("comment") or "").strip():
                return f"{kind} '{it['name']}' needs a comment", None
            names.append(str(it["name"]).strip())
        if len(set(names)) != len(names):
            return f"duplicate {kind} names", None
        names_by_kind[kind] = names
    for ref in re.findall(r"MEASURE\(\s*`([^`]+)`\s*\)", body):
        if ref not in names_by_kind["measure"]:
            return f"MEASURE(`{ref}`) refers to a measure that is not in this view", None
    known = {f"{CAT}.{SRC_SCHEMA}.{t}".lower() for t in dpp.catalog()["columns"].keys()} | {g.lower() for g in gold}
    for fq in re.findall(rf"\b{re.escape(CAT)}\.\w+\.\w+\b", body):
        if fq.lower() not in known:
            return f"{fq} is not an allowed source (the cfo_workbench gold tables and the new gold table)", None
    from agent import sql_guard as _sg
    allowed_top = {"version", "source", "joins", "dimensions", "measures", "comment", "filter"}
    extra = sorted(str(k) for k in y if k not in allowed_top)
    if extra:
        return f"unsupported YAML keys: {', '.join(extra)}", None
    src_ok = lambda fq: fq.lower() in known
    why = _sg.check(source, src_ok)
    if why:
        return f"source SQL refused: {why}", None
    guard_ok = lambda fq: fq == "guard_catalog.guard_schema.guard_table"
    for j in y.get("joins") or []:
        if not isinstance(j, dict) or True in j:
            return "each join must be a mapping with a quoted \"on\" key", None
        if set(map(str, j)) - {"name", "source", "on"}:
            return "joins may only have name, source and on", None
        if not src_ok(str(j.get("source") or "").strip()):
            return f"join source not allowed: {j.get('source')}", None
        on = str(j.get("on") or "")
        why = _sg.check(f"SELECT 1 FROM guard_catalog.guard_schema.guard_table WHERE {on}", guard_ok)
        if why or not on.strip():
            return f"join '{j.get('name')}' condition refused: {why or 'empty'}", None
    if y.get("filter") is not None:
        why = _sg.check(f"SELECT 1 FROM guard_catalog.guard_schema.guard_table WHERE {y['filter']}", guard_ok)
        if why:
            return f"filter refused: {why}", None
    for kind, items in (("dimension", dims), ("measure", meas)):
        for it in items:
            if set(map(str, it)) - {"name", "expr", "comment", "display_name", "format", "synonyms"}:
                return f"{kind} '{it.get('name')}' has unsupported keys", None
            why = _sg.check(f"SELECT {it['expr']} FROM guard_catalog.guard_schema.guard_table", guard_ok)
            if why:
                return f"{kind} '{it['name']}' expr refused: {why}", None
    return None, y


def _mv_checked_yaml(mv: dict, gold: tuple = ()) -> dict:
    """Re-validate the YAML body on the SERVER (never trust the spec's sql_template or validated flag)."""
    from agent import dp_planner as dpp
    body = str(mv.get("yaml") or "").strip()
    if not body:
        raise RuntimeError("metric view has no YAML body")
    if gold:
        problem, parsed = _check_mv_yaml_local(body, tuple(gold))
    else:
        problem, parsed = dpp._check_mv_yaml(body, dpp.catalog())
    if problem:
        raise RuntimeError(f"metric view YAML refused: {problem}")
    names = [m.get("name") for m in parsed.get("measures") or []] + [d.get("name") for d in parsed.get("dimensions") or []]
    bad = [n for n in names if not _SAFE_NAME_RE.match(str(n or ""))]
    if bad:
        raise RuntimeError(f"measure/dimension names not allowed: {bad[:3]}")
    return {"body": body, "parsed": parsed}


def _mv_sql(mv: dict, schema: str, gold: tuple = ()) -> str:
    """CREATE OR REPLACE VIEW rebuilt here from the re-validated YAML and a server-derived name. For a pipeline
    metric view the gold source moves to the env's schema (gold is the planned dev names)."""
    from agent import dp_planner as dpp
    if schema not in (DEV, STAGE, PROD):
        raise RuntimeError(f"schema not allowed: {schema}")
    checked = _mv_checked_yaml(mv, gold)
    body = checked["body"]
    if gold and schema != DEV:
        body = body.replace(f"{CAT}.{DEV}.", f"{CAT}.{schema}.")
    fq = f"{CAT}.{schema}.{_mv_base(mv)}"
    comment = "Acme Corp data product metric view, built from the reviewed spec. SYNTHETIC data."
    return dpp._create_stmt(fq, comment, body)


def _mv_measures(mv: dict, gold: tuple = ()) -> list[str]:
    """Measure names from the server-validated YAML (safe characters only)."""
    return [m["name"] for m in _mv_checked_yaml(mv, gold)["parsed"].get("measures") or []]


def _chk(name: str, ok: bool, detail: str) -> dict:
    return {"name": name, "pass": bool(ok), "detail": detail}


def _instr_points_at_views(instr: str, sources: list[str]) -> bool:
    """The agent is told to answer from the governed metric views: either it names MEASURE(), or it names
    every metric view it reads (the planner's wording says "using the mv_x metric view")."""
    text = instr or ""
    if "MEASURE" in text:
        return True
    return bool(sources) and all(s.split(".")[-1] in text for s in sources)


# ----------------------------------------------------------------------------- links
def _job_url(run_id: Any) -> str:
    return f"{_host()}/jobs/{JOB_ID}/runs/{run_id}"


def _conv_url(conv: str) -> str:
    return f"{_host()}/editor/folders/workspace?mode=chat&agent_session={conv}&o={ORG_ID}" if conv else ""


def _mv_url(full: str) -> str:
    c, s, t = full.split(".")
    return f"{_host()}/explore/data/{c}/{s}/{t}"


def _genie_url(sid: str) -> str:
    return f"{_host()}/genie/rooms/{sid}"


def _dash_url(did: str, published: bool = False) -> str:
    return f"{_host()}/dashboardsv3/{did}" + ("/published" if published else "")


# ----------------------------------------------------------------------------- Genie Code runs
def _run_genie_code(rid: str, step: StepName, kind: str, env: str, prompt: str, label: str,
                    title: str) -> dict[str, Any]:
    w = _ws()
    run = w.jobs.run_now(job_id=JOB_ID, job_parameters={"prompt": prompt})
    run_id = int(run.run_id)
    job_url = _job_url(run_id)
    _emit(rid, step, S.IN_PROGRESS,
          f"{label}: dispatched to Genie Code, job run {run_id}. Live run: {job_url}",
          kind="genie_code_run", engine="genie_code", asset=kind, env=env, title=title,
          run_id=run_id, job_run_url=job_url, state="RUNNING", verified=False, fallback=False)

    deadline = time.time() + 3300
    life, result, r = "PENDING", None, None
    while True:
        try:
            r = w.jobs.get_run(run_id=run_id)
            life = r.state.life_cycle_state.value if r.state and r.state.life_cycle_state else "PENDING"
            result = r.state.result_state.value if r.state and r.state.result_state else None
        except Exception as e:  # noqa: BLE001 — a transient poll error must not kill the build
            LOG.warning("get_run %s failed: %s", run_id, e)
        if life in TERMINAL or time.time() > deadline:
            break
        time.sleep(15)

    out: dict = {}
    task_run = r.tasks[0].run_id if (r is not None and r.tasks) else None
    if task_run:
        try:
            out = _api("GET", "/api/2.2/jobs/runs/get-output", query={"run_id": task_run}) or {}
        except Exception as e:  # noqa: BLE001
            out = {"error": f"get-output failed: {e}"}
    g = out.get("genie_task_output") or {}
    conv = g.get("conversation_id") or ""
    response = (g.get("agent_output") or {}).get("response") or ""
    state_msg = (r.state.state_message if (r is not None and r.state) else "") or ""
    return {"run_id": run_id, "job_run_url": job_url, "life": life, "result": result or life,
            "response": response, "conversation_id": conv, "conversation_url": _conv_url(conv),
            "error": out.get("error") or state_msg}


_ID_PAT = {
    "genie_agent": re.compile(r"GENIE_SPACE_OK\s+([0-9a-f]{32})"),
    "dashboard": re.compile(r"DASHBOARD_OK\s+([0-9a-f]{32})"),
    "pipeline": re.compile(r"PIPELINE_OK\s+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", re.I),
}


def _parse_id(response: str, kind: str) -> Optional[str]:
    pat = _ID_PAT.get(kind)
    m = pat.search(response or "") if pat else None
    return m.group(1) if m else None


def _asset_details(kind: str, env: str, title: str, asset: dict, run: dict, verified: bool,
                   fallback: bool, attempt: int) -> dict[str, Any]:
    return {
        "kind": "genie_code_run", "engine": "genie_code", "asset": kind, "env": env, "title": title,
        "job_run_url": run.get("job_run_url"), "run_id": run.get("run_id"),
        "conversation_url": run.get("conversation_url") or "",
        "conversation_id": run.get("conversation_id") or "",
        "asset_id": asset.get("id"), "asset_url": asset.get("url"),
        "response_excerpt": (run.get("response") or "")[:600],
        "verified": verified, "fallback": fallback, "attempt": attempt, "state": "TERMINATED",
        "result": run.get("result"),
    }


def _build_asset(rid: str, step: StepName, kind: str, env: str, *, label: str, title: str,
                 prompt: str, prompt_vars: dict, verify: Callable[[Optional[str]], Optional[dict]],
                 fallback: Callable[[], dict]) -> dict:
    """Genie Code run -> verify via API -> retry once -> fallback created by the app."""
    run: dict = {}
    for attempt in range(1, MAX_ATTEMPTS + 1):
        sfx = "" if attempt == 1 else " (retry)"
        run = _run_genie_code(rid, step, kind, env, _fill(prompt, **prompt_vars), label + sfx, title)
        try:
            asset = verify(_parse_id(run["response"], kind))
        except Exception as e:  # noqa: BLE001
            LOG.warning("verify %s failed: %s", kind, e)
            asset = None
        if asset and asset.get("ok", True):
            asset["fallback"] = False
            _emit(rid, step, S.SUCCEEDED,
                  f"{label} created by Genie Code and verified: {asset['url']}",
                  **_asset_details(kind, env, title, asset, dict(run, run_id=run["run_id"]), True, False, attempt))
            return asset
        more = "Retrying once." if attempt < MAX_ATTEMPTS else "Falling back to creating it through the API."
        reasons = "; ".join((asset or {}).get("reasons") or [])[:400]
        _emit(rid, step, S.FAILED,
              f"{label}: Genie Code run {run['run_id']} ended {run['result']} and {kind} '{title}' "
              f"did not verify.{(' Not verified: ' + reasons + '.') if reasons else ''} {more}",
              **_asset_details(kind, env, title, {}, run, False, False, attempt))

    _emit(rid, step, S.IN_PROGRESS,
          f"{label}: fallback: creating '{title}' through the API, not Genie Code.",
          kind="genie_code_run", engine="genie_code", asset=kind, env=env, title=title,
          job_run_url=run.get("job_run_url"), run_id=run.get("run_id"), state="FALLBACK",
          verified=False, fallback=True)
    asset = fallback()
    asset["fallback"] = True
    _emit(rid, step, S.SUCCEEDED,
          f"fallback: created by the app, not Genie Code. {label}: {asset['url']}",
          **_asset_details(kind, env, title, asset, run, True, True, MAX_ATTEMPTS))
    return asset


# ----------------------------------------------------------------------------- verification
def _mv_yaml(show_stmt: str) -> dict:
    if "$$" not in show_stmt:
        return {}
    body = show_stmt.split("$$", 1)[1].rsplit("$$", 1)[0]
    try:
        return yaml.safe_load(body) or {}
    except Exception:  # noqa: BLE001
        return {}


def _verify_mv(full: str, expect: Optional[list[str]] = None) -> Optional[dict]:
    """A METRIC_VIEW that exposes every expected measure and answers a query for all of them."""
    schema, name = full.split(".")[1], full.split(".")[2]
    rows = _sql(f"SELECT table_type FROM {CAT}.information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{name}'")
    if not rows or rows[0].get("table_type") != "METRIC_VIEW":
        return None
    try:
        names = [m.get("name") for m in (_mv_yaml(_sql(f"SHOW CREATE TABLE {full}")[0]["createtab_stmt"])
                                         .get("measures") or [])]
    except Exception:  # noqa: BLE001
        return None
    if expect and any(m not in names for m in expect):
        return None
    if names:
        try:
            _sql("SELECT " + ", ".join(f"MEASURE(`{m}`)" for m in names) + f" FROM {full}")
        except Exception:  # noqa: BLE001
            return None
    return {"id": full, "url": _mv_url(full), "title": full, "measures": names}


def _find_space(title: str) -> Optional[str]:
    token = None
    for _ in range(20):
        q = {"page_size": "100"}
        if token:
            q["page_token"] = token
        r = _api("GET", "/api/2.0/genie/spaces", query=q) or {}
        for s in r.get("spaces") or []:
            if s.get("title") == title:
                return s.get("space_id")
        token = r.get("next_page_token")
        if not token:
            break
    return None


def _space_ss(g: dict) -> dict:
    try:
        return json.loads(g.get("serialized_space") or "{}")
    except Exception:  # noqa: BLE001
        return {}


def _verify_space(title: str, hint: Optional[str], must: Optional[list[str]] = None,
                  search: bool = True) -> Optional[dict]:
    """The Genie Agent with this title whose data sources include every name in `must`.
    search=False: only accept the id we created (prod); a title search could return an older space."""
    must = must or []
    for sid in [hint] + ([_find_space(title)] if search else []):
        if not sid:
            continue
        try:
            g = _api("GET", f"/api/2.0/genie/spaces/{sid}", query={"include_serialized_space": "true"})
        except Exception:  # noqa: BLE001
            continue
        if not g:
            continue
        # the id we created or updated is authoritative; Genie may add a suffix to a duplicate title
        if sid != hint and g.get("title") != title:
            continue
        ss = _space_ss(g)
        tables = [t.get("identifier") for t in (ss.get("data_sources") or {}).get("tables", [])]
        if not all(m in tables for m in must):
            continue
        instr = " ".join(
            "".join(t.get("content") or []) for t in (ss.get("instructions") or {}).get("text_instructions", []))
        qs = [" ".join(q.get("question") or []) for q in (ss.get("config") or {}).get("sample_questions", [])]
        return {"id": sid, "url": _genie_url(sid), "title": title, "tables": tables,
                "instructions": instr, "sample_questions": qs, "serialized": ss}
    return None


def _find_dash(title: str) -> Optional[str]:
    token = None
    for _ in range(20):
        q = {"page_size": "100"}
        if token:
            q["page_token"] = token
        r = _api("GET", "/api/2.0/lakeview/dashboards", query=q) or {}
        for d in r.get("dashboards") or []:
            if d.get("display_name") == title and d.get("lifecycle_state", "ACTIVE") == "ACTIVE":
                return d.get("dashboard_id")
        token = r.get("next_page_token")
        if not token:
            break
    return None


def _dash_summary(d: dict, did: str) -> dict:
    ser = d.get("serialized_dashboard") or ""
    try:
        sd = json.loads(ser) if ser else {}
    except Exception:  # noqa: BLE001
        sd = {}
    datasets = sd.get("datasets") or []
    return {"id": did, "url": _dash_url(did), "title": d.get("display_name"),
            "serialized_text": ser, "serialized": sd,
            "datasets_text": json.dumps(datasets),
            "pages": [p.get("displayName") or p.get("name") or "" for p in (sd.get("pages") or [])],
            "dataset_names": [x.get("displayName") or x.get("name") or "" for x in datasets]}


def _verify_dashboard(title: str, hint: Optional[str], must: Optional[list[str]] = None,
                      search: bool = True) -> Optional[dict]:
    """The dashboard with this title whose serialized form reads every name in `must`."""
    must = must or []
    for did in [hint] + ([_find_dash(title)] if search else []):
        if not did:
            continue
        try:
            d = _api("GET", f"/api/2.0/lakeview/dashboards/{did}")
        except Exception:  # noqa: BLE001
            continue
        if not d or d.get("lifecycle_state", "ACTIVE") != "ACTIVE" or (did != hint and d.get("display_name") != title):
            continue
        ser = d.get("serialized_dashboard") or ""
        if not all(m.lower() in ser.lower() for m in must):
            continue
        return _dash_summary(d, did)
    return None


# ----------------------------------------------------------------------------- fallbacks (API)
def _grant_view_to_owner(full: str) -> None:
    """The app identity owns the views it (re)creates; give the data owner SELECT so the Genie Agent and
    dashboard work for them. Best effort."""
    owner = os.environ.get("OPS_PROD_OWNER", "vaishnavi.mohan@databricks.com")
    if not re.fullmatch(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+", owner or ""):
        return
    try:
        _sql(f"GRANT SELECT ON TABLE {full} TO `{owner}`")
    except Exception as e:  # noqa: BLE001
        LOG.warning("could not grant SELECT on %s to %s: %s", full, owner, e)


def _fallback_mv(full: str, mv: dict, gold: tuple = ()) -> dict:
    _sql(_mv_sql(mv, DEV, gold))
    v = _verify_mv(full, mv.get("measures") or [])
    if not v:
        raise RuntimeError(f"{full} is not a METRIC_VIEW with the planned measures after the fallback create")
    return v


def _space_payload(title: str, desc: str, tables: list[str], instructions: str,
                   questions: list[str]) -> dict:
    ss = {
        "version": 2,
        "config": {"sample_questions": [{"id": _hex(), "question": [q]} for q in questions]},
        "data_sources": {"tables": [{"identifier": t} for t in sorted(set(tables))]},
        "instructions": {"text_instructions": [{"id": _hex(), "content": [instructions + "\n"]}],
                         "example_question_sqls": []},
    }
    return {"title": title, "description": desc, "warehouse_id": WAREHOUSE,
            "serialized_space": json.dumps(ss)}


def _fallback_space(title: str, desc: str, tables: list[str], instructions: str,
                    questions: list[str]) -> dict:
    body = _space_payload(title, desc, tables, instructions, questions)
    body["parent_path"] = APP_FOLDER
    sid = (_api("POST", "/api/2.0/genie/spaces", body=body) or {}).get("space_id")
    v = _verify_space(title, sid, must=tables)
    if not v:
        raise RuntimeError(f"Genie space '{title}' did not verify after the fallback create")
    return v


def _fallback_dashboard(title: str, pages: list[dict], wh: str, v: dict) -> dict:
    """Fallback layout (only if Genie Code fails twice): one table per planned widget, in planned order,
    each reading its validated query. Columns come from running the query, so every table renders."""
    datasets, layout_pages = [], []
    for pi, page in enumerate(pages):
        layout, y, n = [], 0, 0
        for wi, w in enumerate(page.get("widgets") or []):
            sql = _one_line(w.get("sql") or "")
            try:
                if _guard_sql(w.get("sql") or "", v, DEV):
                    raise RuntimeError("query refused by the guard")
                cols, _rows = _sql_cols(sql)
            except Exception as e:  # noqa: BLE001
                LOG.warning("fallback dashboard: widget %r failed: %s", w.get("title"), e)
                continue
            if not cols:
                continue
            ds = f"ds_{pi}_{wi}"
            datasets.append({"name": ds, "displayName": str(w.get("title") or ds)[:60], "queryLines": [sql]})
            fields = [{"name": c, "expression": f"`{c}`"} for c in cols]
            encodings = {"columns": [{"fieldName": c, "displayName": c, "type": "string", "order": i,
                                      "title": c, "visible": True} for i, c in enumerate(cols)]}
            layout.append({"widget": {"name": f"w_{pi}_{wi}", "queries": [{"name": "main_query", "query": {
                "datasetName": ds, "fields": fields, "disaggregated": True}}],
                "spec": {"version": 2, "widgetType": "table", "encodings": encodings,
                         "frame": {"showTitle": True, "title": str(w.get("title") or "")}}},
                "position": {"x": 0, "y": y, "width": 6, "height": 8}})
            y += 8
            n += 1
        if n:
            layout_pages.append({"name": f"page_{pi}", "displayName": str(page.get("name") or f"Page {pi + 1}"),
                                 "pageType": "PAGE_TYPE_CANVAS", "layout": layout})
    ser = json.dumps({"datasets": datasets, "pages": layout_pages})
    d = _api("POST", "/api/2.0/lakeview/dashboards",
             body={"display_name": title, "serialized_dashboard": ser, "warehouse_id": wh,
                   "parent_path": APP_FOLDER})
    v = _verify_dashboard(title, d.get("dashboard_id"))
    if not v:
        raise RuntimeError(f"dashboard '{title}' did not verify after the fallback create")
    return v


# ----------------------------------------------------------------------------- spec check / profile / DQ
def _ok_pending(flag: Any, v: dict) -> bool:
    """Planner-validated, or (RAW product only) pending: checked at build, once the pipeline has created the tables."""
    return flag is True or (bool(v.get("pipe")) and flag == "pending")


def _spec_checks(spec: dict, dp: dict, v: dict) -> list[dict]:
    need, design, reuse = v["need"], dp.get("design") or {}, dp.get("reuse") or {}
    requested = list(need.get("outputs") or [])
    mv, ga, db = v["mv"], v["ga"], v["db"]
    checks = [
        _chk("data product v2/v3 (engine genie_code)", dp.get("version") in (2, 3) and dp.get("engine") == "genie_code",
             f"version {dp.get('version')}, engine {dp.get('engine')}"),
        _chk("purpose captured", bool(str(need.get("purpose") or "").strip()), "need.purpose"),
        _chk("outputs asked for are valid", bool(requested) and set(requested) <= set(OUTPUTS),
             f"outputs: {', '.join(requested) or '(none)'}"),
        _chk("design has exactly the requested outputs",
             all((design.get(o) is not None) == (o in requested) for o in OUTPUTS),
             "built only what was asked: " + (", ".join(requested) or "nothing")),
        _chk("the Acme data can answer the ask", reuse.get("buildable") is not False,
             "reuse.buildable = " + str(reuse.get("buildable"))),
        _chk("every listed data source is a metric view in an allowed schema",
             bool(v["listed"]) and all(_fq_ok(s) for s in v["listed"]),
             f"{len(v['listed'])} listed: " + ", ".join(s.split(".")[-1] for s in v["listed"])),
    ]
    if mv:
        tmpl = str(mv.get("sql_template") or "")
        checks += [
            _chk("metric view is named in the dev schema", v["mv_dev"].startswith(f"{CAT}.{DEV}.mv_") and _fq_ok(v["mv_dev"]),
                 v["mv_dev"] or "(none)"),
            _chk("metric view SQL is a schema template for that name",
                 "{schema}" in tmpl and "WITH METRICS" in tmpl.upper() and f"{CAT}.{{schema}}.{v['mv_base']}" in tmpl,
                 "CREATE ... VIEW ... {schema}." + v["mv_base"]),
            _chk("metric view validated by the planner (or at build, for a pipeline)", _ok_pending(mv.get("validated"), v),
                 f"{len(mv.get('measures') or [])} measures; validated = {mv.get('validated')}"),
        ]
    queries: list[tuple[str, str]] = []
    if ga:
        for b in ga.get("benchmarks") or []:
            queries.append((f"benchmark '{b.get('question', '')}'", b.get("sql") or ""))
    for w in v["widgets"]:
        queries.append((f"widget '{w.get('title', '')}'", w.get("sql") or ""))
    bad = []
    for label, sql in queries:
        p = _guard_sql(sql, v, DEV)
        if p:
            bad.append(f"{label}: {p}")
    checks.append(_chk("every query is one read-only SELECT on listed metric views (parsed)",
                       not bad, f"{len(queries)} queries checked" + (" | " + "; ".join(bad[:3]) if bad else "")))
    dq_rules = [r for r in ((dp.get("dq") or {}).get("rules") or []) if r.get("enabled", True)]
    dq_bad = [f"{r.get('rule_name')}: {_dq_rule_ok(str(r.get('table_name') or '').split('.')[-1], r.get('constraint_sql') or '', tuple(v['pipe_tables']))}"
              for r in dq_rules if _dq_rule_ok(str(r.get("table_name") or "").split(".")[-1], r.get("constraint_sql") or "", tuple(v["pipe_tables"]))]
    checks.append(_chk("every DQ rule passes the guard", not dq_bad,
                       f"{len(dq_rules)} rule(s)" + (" | " + "; ".join(dq_bad[:3]) if dq_bad else "")))
    pii = [label for label, sql in queries if re.search(PII_RE, sql or "", re.I)]
    checks.append(_chk("no customer names in any query", not pii, "no customer-name column" if not pii else f"{pii[:3]}"))
    if ga:
        bench = ga.get("benchmarks") or []
        checks += [
            _chk("agent instructions point at the metric views", _instr_points_at_views(
                 ga.get("instructions") or "", [_nfq(s) for s in ga.get("data_sources") or []]),
                 "MEASURE() or the metric view names"),
            _chk("agent sources are listed metric views",
                 bool(ga.get("data_sources")) and all(_nfq(s) in v["sources"] for s in ga.get("data_sources") or []),
                 f"{len(ga.get('data_sources') or [])} data source(s)"),
            _chk("day-one questions captured", bool(v["questions"]), f"{len(v['questions'])} day-one question(s)"),
            _chk("benchmarks have SQL and expected answers",
                 bool(bench) and all((b.get("sql") or "").strip() and (b.get("expected") or "").strip() for b in bench),
                 f"{len(bench)} benchmark(s)"),
        ]
    if db:
        checks += [
            _chk("dashboard has pages with validated widgets (or pending, for a pipeline)",
                 bool(v["widgets"]) and all(_ok_pending(w.get("validated"), v) and w.get("sql") for w in v["widgets"]),
                 f"{len(db.get('pages') or [])} page(s), {len(v['widgets'])} widget(s)"),
            _chk("dashboard excludes customer names", db.get("include_customer_names") is False,
                 "include_customer_names = false"),
        ]
    rules = [r for r in ((dp.get("dq") or {}).get("rules") or []) if r.get("enabled", True)]
    checks.append(_chk("DQ rules enabled with SQL",
                       bool(rules) and all((r.get("constraint_sql") or "").strip() for r in rules),
                       f"{len(rules)} enabled rule(s)"))
    if dp.get("pipeline"):
        pp = v["pipe_problems"]
        checks.append(_chk("pipeline SQL passes the server checks (raw feed only, planned tables, guarded SELECTs)",
                           bool(v["pipe"]) and not pp,
                           "; ".join(pp[:3]) if pp else f"{len(v['pipe']['planned'])} tables, "
                                                        f"{len(v['pipe']['expectations'])} expectations"))
        checks.append(_chk("a pipeline has a metric view over its gold table",
                           bool(v["mv"]) and bool(v["gold"]), f"gold: {', '.join(v['gold']) or '(none)'}"))
    return checks


def _profile(v: dict) -> dict[str, Any]:
    latest = _sql(f"SELECT CAST(MAX(week_start) AS STRING) AS w FROM {SRC}.gold_sales_line_rebate_alloc")[0]["w"]
    sources = []
    for fq in v["sources"]:
        if fq == v["mv_dev"]:
            sources.append({"name": fq, "type": "new (built in this request)"})
            continue
        s, t = fq.split(".")[1], fq.split(".")[2]
        rows = _sql(f"SELECT table_type FROM {CAT}.information_schema.tables "
                    f"WHERE table_schema = '{s}' AND table_name = '{t}'")
        sources.append({"name": fq, "type": rows[0]["table_type"] if rows else "MISSING"})
    missing = [s["name"] for s in sources if s["type"] not in ("METRIC_VIEW", "new (built in this request)")]
    return {"latest_week": latest, "sources": sources, "missing": missing}


def _run_dq(rid: str, dp: dict, pipe_tables: tuple = ()) -> list[dict]:
    step = N.STANDARDS_VALIDATOR
    rules = [r for r in ((dp.get("dq") or {}).get("rules") or []) if r.get("enabled", True)]
    where = SRC + (f" and {CAT}.{DEV} (pipeline tables)" if pipe_tables else "")
    _emit(rid, step, S.IN_PROGRESS, f"Running {len(rules)} DQ rules against {where} (read-only).")
    results = []
    for r in rules:
        t = (r.get("table_name") or "").split(".")[-1]
        full = f"{CAT}.{DEV}.{t}" if t in pipe_tables else f"{SRC}.{t}"
        row = {"table_name": t, "rule_name": r.get("rule_name"), "constraint_sql": r.get("constraint_sql"),
               "severity": r.get("severity", "warn"), "origin": r.get("origin", "existing")}
        refused = _dq_rule_ok(t, r.get("constraint_sql") or "", tuple(pipe_tables))
        if refused:
            row.update(status="error", message=f"not run: {refused}", total_rows=None, violations=None)
            results.append(row)
            continue
        try:
            rr = _sql(f"SELECT COUNT(*) AS total, SUM(CASE WHEN COALESCE(({r['constraint_sql']}), false) "
                      f"THEN 0 ELSE 1 END) AS violations FROM {full}")[0]
            total, viol = int(rr["total"]), int(rr["violations"] or 0)
            row.update(total_rows=total, violations=viol)
            if viol == 0:
                row["status"] = "pass"
            elif row["severity"] == "drop":
                row["status"] = "would_drop"
            else:
                row["status"] = "warn"
        except Exception:  # noqa: BLE001
            row.update(status="error", message="rule could not run on the warehouse", total_rows=None, violations=None)
        results.append(row)
    cnt = {k: sum(1 for x in results if x["status"] == k) for k in ("pass", "would_drop", "warn", "error")}
    _emit(rid, step, S.SUCCEEDED,
          f"DQ: {len(results)} rules run on {where}: {cnt['pass']} pass, {cnt['would_drop']} drop-severity rule(s) "
          f"would drop rows, {cnt['warn']} warning(s), {cnt['error']} could not run. Warnings never fail the build.",
          kind="dq_check", rules=results, counts=cnt)
    return results


# ----------------------------------------------------------------------------- numbers: live reference
_NUM_RE = re.compile(r"(?<![\w.])([-+]?\$?\d[\d,]*(?:\.\d+)?)(\s*%|\s*(?:bps|million|billion|thousand|M|K|B)\b)?",
                     re.I)
_SCALE = {"million": 1e6, "m": 1e6, "thousand": 1e3, "k": 1e3, "billion": 1e9, "b": 1e9}
_PCT_COL = re.compile(r"(pct|percent|rate|share|ratio|%)", re.I)
_DATE_LIKE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _nums(text: str) -> list[tuple[float, bool]]:
    out = []
    for m in _NUM_RE.finditer((text or "").replace("−", "-")):
        try:
            val = float(m.group(1).replace("$", "").replace(",", ""))
        except ValueError:
            continue
        unit = (m.group(2) or "").strip().lower()
        pct = unit.startswith("%")
        out.append((val * _SCALE.get(unit, 1.0), pct))
    return out


def _close(target: float, cand: float) -> bool:
    return abs(cand - target) <= max(abs(target) * 0.02, 0.051)


def _row_cells(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Every numeric cell of a result, with its row index, its column and the row's text label."""
    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        label, nums = "", []
        for col, val in row.items():
            if val is None:
                continue
            s_val = str(val)
            try:
                nums.append((str(col), float(s_val.replace(",", ""))))
            except ValueError:
                if not label and not _DATE_LIKE.match(s_val):
                    label = s_val
        for col, f in nums:
            out.append({"row": i, "col": col, "v": f, "pct": bool(_PCT_COL.search(col)), "label": label})
    return out


def _row_table(rows: list[dict[str, Any]], cap: int = 300) -> list[dict[str, Any]]:
    """Each live row as its text parts (category, branch, segment...) and its numbers."""
    out = []
    for row in rows[:cap]:
        parts, vals = [], []
        for val in row.values():
            if val is None:
                continue
            sv = str(val)
            try:
                vals.append(float(sv.replace(",", "")))
            except ValueError:
                if not _DATE_LIKE.match(sv) and sv.strip().lower() not in ("true", "false"):
                    parts.append(sv.strip())
        out.append({"parts": parts, "vals": vals})
    return out


def _live_ref(question: str, expected: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """The LIVE reference for a benchmark. The expected text says which numbers and names matter; each of its
    numbers is pointed at the matching live cell of the benchmark SQL (re-run now). An expected number the live
    result no longer shows is drift. With no numbers in the expected text, the first row is the reference."""
    cells = _row_cells(rows)
    values: list[dict[str, Any]] = []
    drift: list[float] = []
    used: set[tuple[int, str]] = set()
    for x in _expected_nums(question, expected):
        best, best_d = None, None           # the NEAREST unused cell that matches the expected number
        for c in cells:
            if (c["row"], c["col"]) in used:
                continue
            cands = [c["v"]] + ([c["v"] * 100, c["v"] / 100] if c["pct"] else [])
            close = [abs(cand - x) for cand in cands if _close(x, cand)]
            if close and (best_d is None or min(close) < best_d):
                best, best_d = c, min(close)
        if best is None:
            drift.append(x)
        else:
            used.add((best["row"], best["col"]))
            values.append(best)
    if not _expected_nums(question, expected):
        values = [c for c in cells if c["row"] == 0]
    values = values[:8]
    exp_l = (expected or "").lower()
    labels: list[str] = []
    for v in values:
        lb = v["label"]
        if lb and lb.lower() in exp_l and lb not in labels:
            labels.append(lb)
    if not labels and not _expected_nums(question, expected) and cells:
        labels = [v["label"] for v in values[:1] if v["label"]]
    return {"table": _row_table(rows),
            "values": [{"col": v["col"], "v": v["v"], "pct": v["pct"]} for v in values],
            "labels": labels, "drift": drift, "rows": len(rows)}


def _ref_text(ref: dict) -> str:
    def fmt(x: float) -> str:
        return f"{x:,.0f}" if abs(x) >= 1000 else f"{x:,.2f}"
    parts = [f"{x['col']} {fmt(x['v'])}{'%' if x['pct'] else ''}" for x in ref.get("values") or []]
    return ", ".join(parts) + (f" (rows: {', '.join(ref.get('labels') or [])})" if ref.get("labels") else "")


def _expected_nums(question: str, expected: str) -> list[float]:
    """Numbers in the expected text, minus the asides in brackets and numbers that are in the question.
    The planner writes "Label: Measure: value, Measure: value; ..."; when that shape is present only the VALUES
    (right after a colon) count, so digits inside labels and measure names ("Branch 10", "Last 90 Days",
    "Top 200") are not mistaken for expected numbers."""
    qset = {round(v, 6) for v, _ in _nums(question)}
    exp = re.sub(r"\([^)]*\)", " ", expected or "")
    vals = re.findall(r":\s*(-?\$?[0-9][0-9,]*(?:\.[0-9]+)?\s*[KMB%]?)", exp)
    if vals:
        exp = " ; ".join(vals)
    return [v for v, _ in _nums(exp) if round(v, 6) not in qset]


def _num_match(t: float, cands: list[float], pct: bool = False) -> bool:
    return any(_close(t, c) or ((pct or abs(t) <= 1.5) and (_close(t, c * 100) or _close(t, c / 100)))
               for c in cands)


def _grade_accuracy(table: list[dict[str, Any]], answer: str, cands: list[float]) -> Optional[dict[str, Any]]:
    """Multi-row results: every row the answer NAMES (all of its text parts appear) must carry at least one of
    its live numbers; at least one row must be named. None when the result isn't a multi-row labelled table."""
    labelled = [r for r in table if r["parts"]]
    if len(labelled) < 2:
        return None
    low = (answer or "").lower()
    named = [r for r in labelled
             if all(re.search(r"(?<![a-z0-9])" + re.escape(p.lower()) + r"(?![a-z0-9])", low) for p in r["parts"])]
    right = [r for r in named if any(_num_match(v, cands) for v in r["vals"])]
    wrong = [", ".join(r["parts"]) for r in named if r not in right]
    passed = bool(named) and len(right) >= 0.8 * len(named)
    return {"mode": "accuracy", "rows_named": len(named), "rows_right": len(right), "rows_wrong": len(wrong),
            "numbers_expected": [], "matched": [], "missing": [], "labels_missing": [] if named else ["(no row named)"],
            "pass": passed}


def _grade_ref(ref: dict, answer: str, data: list[str]) -> dict[str, Any]:
    """Grade a Genie Agent answer against the LIVE reference numbers and labels."""
    cands = [v for v, _ in _nums(answer or "")]
    for x in data or []:
        try:
            cands.append(float(str(x).replace(",", "")))
        except ValueError:
            pass
    acc = _grade_accuracy(ref.get("table") or [], answer, cands)
    if acc is not None:
        return acc
    matched, missing = [], []
    for item in ref.get("values") or []:
        t = item["v"]
        # fractions are often spoken as percentages (0.315 -> "31.5%"), flagged or not
        ok = any(_close(t, c) or ((item["pct"] or abs(t) <= 1.5) and (_close(t, c * 100) or _close(t, c / 100)))
                 for c in cands)
        (matched if ok else missing).append(t)
    # yes/no flags (true/false) are not names an answer would say
    ref_labels = [lb for lb in (ref.get("labels") or []) if str(lb).strip().lower() not in ("true", "false", "none", "null", "")]
    ref = dict(ref, labels=ref_labels)
    labels_missing = [lb for lb in ref_labels if lb.lower() not in (answer or "").lower()]
    has_ref = bool(ref.get("values") or ref.get("labels"))
    # Single-number questions must match exactly. Multi-row "which ..." questions pass when the answer carries
    # most of the live numbers (>= 60%) and names at least one of the expected rows: a good answer leads with
    # the top rows, it does not recite the whole table.
    vals = ref.get("values") or []
    labels = ref.get("labels") or []
    if not has_ref:
        passed = None
    elif len(vals) <= 1 and len(labels) <= 1:
        passed = not missing and not labels_missing
    else:
        share = (len(matched) / len(vals)) if vals else 1.0
        passed = share >= 0.6 and (not labels or len(labels_missing) < len(labels))
    return {"numbers_expected": [x["v"] for x in ref.get("values") or []], "matched": matched,
            "missing": missing, "labels_missing": labels_missing, "pass": passed}


def _grade(question: str, expected: str, answer: str, data: list[str]) -> dict[str, Any]:
    """Text-expected grader (kept for ops/bench_check.py)."""
    exp_nums = [v for v in _expected_nums(question, expected)]
    cands = [v for v, _ in _nums(answer)]
    for x in data:
        try:
            cands.append(float(x))
        except ValueError:
            pass
    matched, missing = [], []
    for v in exp_nums:
        if any(_close(v, c) for c in cands):
            matched.append(v)
        else:
            missing.append(v)
    passed = None if not exp_nums else not missing
    return {"numbers_expected": exp_nums, "matched": matched, "missing": missing, "pass": passed}


def _live_checks(rid: str, v: dict) -> dict[str, Any]:
    """Re-run every benchmark SQL and every widget SQL on the live data (dev). Benchmarks whose expected
    text no longer matches the data are flagged ("drift"); grading later uses the live values."""
    step = N.STANDARDS_VALIDATOR
    out: dict[str, Any] = {"benches": [], "widgets": []}
    if v["ga"]:
        for b in v["ga"].get("benchmarks") or []:
            q, exp, sql = b.get("question", ""), b.get("expected", ""), _sub_mv(b.get("sql") or "", v, DEV)
            e: dict[str, Any] = {"question": q, "expected": exp, "sql": sql}
            refused = _guard_sql(b.get("sql") or "", v, DEV)
            if refused:
                e.update(status="refused", error=refused, rows=0, ref={"values": [], "labels": [], "drift": []},
                         live="", drift=[])
                out["benches"].append(e)
                continue
            try:
                rows = _sql(sql)
                ref = _live_ref(q, exp, rows)                       # what the agent answer is graded on
                e.update(status="ok" if rows else "no rows", rows=len(rows),
                         ref={k: v for k, v in ref.items() if k != "table"},   # live rows stay in memory only
                         live=_ref_text(ref), drift=ref["drift"])
            except Exception as ex:  # noqa: BLE001
                e.update(status="error", error=str(ex)[:200], rows=0, ref={"values": [], "labels": [], "drift": []},
                         live="", drift=[])
            out["benches"].append(e)
    for w in v["widgets"]:
        sql = _one_line(w.get("sql") or "")
        e = {"title": w.get("title", ""), "sql": sql}
        refused = _guard_sql(w.get("sql") or "", v, DEV)
        if refused:
            out["widgets"].append(dict(e, rows=0, status="refused", error=refused))
            continue
        try:
            n = len(_sql(sql))
            e.update(rows=n, status="ok" if n else "no rows")
        except Exception as ex:  # noqa: BLE001
            e.update(rows=0, status="error", error=str(ex)[:200])
        out["widgets"].append(e)
    b_ok = sum(1 for b in out["benches"] if b["status"] == "ok")
    drift = sum(1 for b in out["benches"] if b.get("drift"))
    w_ok = sum(1 for w in out["widgets"] if w["status"] == "ok")
    parts = []
    if out["benches"]:
        parts.append(f"{b_ok} of {len(out['benches'])} benchmark SQL return rows ({drift} with drifted expected numbers)")
    if out["widgets"]:
        parts.append(f"{w_ok} of {len(out['widgets'])} dashboard widget queries return rows")
    _emit(rid, step, S.SUCCEEDED, "Live check on the dev data: " + "; ".join(parts) + ".",
          kind="live_check", benches=out["benches"], widgets=out["widgets"])
    return out


# ----------------------------------------------------------------------------- PR + review
def _checklist(v: dict, mv_y: dict, agent: Optional[dict], dash: Optional[dict], dq: list[dict],
               live: dict, pipe_run: Optional[dict] = None) -> list[dict]:
    """Checks only for what was built; a reviewer reads these, the AI reviewer summarises them."""
    items: list[dict] = []
    if pipe_run:
        items.append(_chk("pipeline: SQL passes the server checks (raw feed only, planned tables, guarded SELECTs)",
                          bool(v["pipe"]) and not v["pipe_problems"], "checked before the build"))
        for c in pipe_run.get("checks") or []:
            items.append(_chk("pipeline: " + c["name"], c["pass"], c["detail"]))
        dropping = [e for e in pipe_run.get("expectations") or [] if e.get("action") == "drop"]
        items.append(_chk("pipeline: drop counts read from the event log",
                          all(e.get("dropped") is not None for e in dropping),
                          "; ".join(f"{e['name']} on {e['table']}: {e['dropped']} dropped" for e in dropping)
                          or "no drop rules"))
    if v["mv"]:
        measures = mv_y.get("measures") or []
        names = [m.get("name") for m in measures]
        no_comment = [n for n, m in zip(names, measures) if not str(m.get("comment") or "").strip()]
        planned = list((v["mv"] or {}).get("measures") or [])
        missing = [m for m in planned if m not in names]
        items.append(_chk("every metric-view measure has a comment", bool(measures) and not no_comment,
                          f"{len(measures)} measures in {v['mv_dev']}"
                          + (f"; no comment on: {', '.join(no_comment)}" if no_comment else "")))
        items.append(_chk("the metric view has every measure in the plan", bool(planned) and not missing,
                          f"{len(planned) - len(missing)} of {len(planned)} planned measures"
                          + (f"; missing: {', '.join(missing)}" if missing else "")))
    if v["ga"]:
        bench = v["ga"].get("benchmarks") or []
        bq = [b.get("question", "") for b in bench]
        missing_q = [q for q in v["questions"] if not any(_same_question(q, b) for b in bq)]
        items.append(_chk("every day-one question has a benchmark with an expected answer",
                          not missing_q and all((b.get("expected") or "").strip() for b in bench),
                          f"{len(v['questions'])} day-one questions, {len(bench)} benchmarks."
                          + (" No benchmark for: " + " | ".join(f"'{q}'" for q in missing_q) if missing_q else "")))
        benches = live.get("benches") or []
        drifted = [b for b in benches if b.get("drift")]
        items.append(_chk("benchmark expected numbers match the live data", not drifted,
                          "every expected number is in the live data" if not drifted else
                          "drifted: " + "; ".join(f"'{b['question']}' expects {b['drift']}" for b in drifted)))
        empty = [b["question"] for b in benches if b.get("status") != "ok"]
        items.append(_chk("every benchmark SQL runs and returns rows", bool(benches) and not empty,
                          f"{len(benches) - len(empty)} of {len(benches)} benchmark queries return rows"
                          + (f"; failing: {empty}" if empty else "")))
        items.append(_chk("Genie instructions point the agent at the metric views",
                          bool(agent) and _instr_points_at_views(agent.get("instructions") or "", v["sources"]),
                          "checked the agent instructions text"))
        tables = (agent or {}).get("tables") or []
        items.append(_chk("agent reads metric views only, from the listed data sources",
                          bool(tables) and all(t in v["sources"] for t in tables) and all(".mv_" in t for t in tables),
                          ", ".join(t.split(".")[-1] for t in tables) or "(none read)"))
    if v["db"]:
        dtext = (dash or {}).get("datasets_text") or ""
        wl = live.get("widgets") or []
        bad_w = [w["title"] for w in wl if w.get("status") != "ok"]
        items.append(_chk("every dashboard widget query runs and returns rows", bool(wl) and not bad_w,
                          f"{len(wl) - len(bad_w)} of {len(wl)} widget queries return rows"
                          + (f"; failing: {bad_w}" if bad_w else "")))
        pii = re.search(PII_RE, dtext, re.I)
        items.append(_chk("no customer names in any dashboard dataset", not pii,
                          "no customer-name column in any dataset" if not pii else f"a dataset references '{pii.group(0)}'"))
        raw = re.findall(r"\b(gold|silver|bronze)_[a-z_]+", dtext)
        in_ds = all(s in dtext.lower() for s in v["ref_sources_dash"])
        items.append(_chk("dashboard datasets read metric views, not raw tables", not raw and in_ds,
                          "all datasets query the listed metric views" if not raw and in_ds else
                          (f"reads raw tables: {sorted(set(raw))}" if raw else "a planned metric view is missing")))
        used, total = 0, 0
        for w in v["widgets"]:
            total += 1
            ms = re.findall(r"MEASURE\(`([^`]+)`\)", w.get("sql") or "")
            if all(m in dtext for m in ms) and (ms or any(s in dtext.lower() for s in _refs([w.get("sql") or ""]))):
                used += 1
        items.append(_chk("dashboard uses the validated widget queries", total > 0 and used == total,
                          f"{used} of {total} widget queries found in the dashboard datasets"))
    rules_err = [r["rule_name"] for r in dq if r.get("status") == "error"]
    items.append(_chk("every DQ rule ran", not rules_err,
                      "all rules ran" if not rules_err else f"could not run: {rules_err}"))
    return items


def _llm_review(built: list[str], not_built: list[str], payload: dict) -> Optional[str]:
    try:
        from agent.llm import get_llm
        from langchain_core.messages import HumanMessage, SystemMessage
        sys_msg = (f"You are the AI reviewer on a pull request that adds {', '.join(built)} for a finance data "
                   "product. The checklist results are authoritative. Write 3 to 6 short bullets. Each bullet names "
                   "the exact item (question, measure, widget, dataset or instruction) and says what to change. Do "
                   "not restate passing checks. Plain text, no emojis. End with one line: VERDICT: approve OR "
                   "VERDICT: request_changes. Use request_changes if any checklist item failed. "
                   + (f"These were NOT asked for and are intentionally absent, so never ask for them: "
                      f"{', '.join(not_built)}. " if not_built else "")
                   + "Dev titles carry a '[dev <id>]' suffix on purpose; never flag it. Comment only on failed "
                     "checklist items and on real quality problems in the artefacts.")
        resp = get_llm().invoke([SystemMessage(content=sys_msg),
                                 HumanMessage(content=json.dumps(payload, indent=2, default=str))])
        return (resp.content or "").strip() or None
    except Exception as e:  # noqa: BLE001
        LOG.warning("AI review unavailable: %s", e)
        return None


def _review(v: dict, agent: Optional[dict], dash: Optional[dict], mv_info: Optional[dict], checklist: list[dict],
            dq: list[dict], pipe_run: Optional[dict] = None) -> tuple[str, str, Optional[str]]:
    failed = [c for c in checklist if not c["pass"]]
    built = (["a pipeline (raw to gold, Lakeflow SDP)"] if pipe_run else []) \
        + (["a governed metric view"] if mv_info else []) + (["a Genie Agent"] if agent else []) \
        + (["a dashboard"] if dash else [])
    payload = {
        "checklist": checklist,
        "pipeline": {"name": pipe_run["title"], "tables": [t["name"] for t in pipe_run["tables"]],
                     "expectations": [{"name": e["name"], "table": e["table"], "action": e["action"],
                                       "dropped": e["dropped"]} for e in pipe_run.get("expectations") or []]}
        if pipe_run else None,
        "agent": {"title": agent["title"], "tables": agent["tables"],
                  "sample_questions": agent["sample_questions"]} if agent else None,
        "dashboard": {"title": dash["title"], "pages": dash["pages"], "datasets": dash["dataset_names"]}
        if dash else None,
        "metric_view": {"name": mv_info["name"], "measures": [m["name"] for m in mv_info["measures"]]}
        if mv_info else None,
        "dq": {"rules": len(dq), "would_drop": sum(1 for r in dq if r["status"] == "would_drop"),
               "warn": sum(1 for r in dq if r["status"] == "warn")},
    }
    not_built = (["a metric view (every source is reused)"] if not mv_info else []) \
        + (["a Genie Agent"] if not agent else []) + (["a dashboard"] if not dash else [])
    llm_text = _llm_review(built, not_built, payload)
    llm_verdict = None
    if llm_text:
        m = re.search(r"VERDICT:\s*(approve|request_changes)", llm_text, re.I)
        llm_verdict = m.group(1).lower() if m else None
        llm_text = re.sub(r"\s*VERDICT:.*$", "", llm_text, flags=re.I | re.S).strip()
    verdict = "request_changes" if (failed or llm_verdict == "request_changes") else "approve"
    lines = ["## Automated review", "", "### Checklist"]
    for c in checklist:
        lines.append(f"- [{'pass' if c['pass'] else 'FAIL'}] {c['name']}: {c['detail']}")
    lines += ["", "### Reviewer notes"]
    lines.append(llm_text or "AI reviewer unavailable; the checklist above is the review.")
    lines += ["", f"**Verdict: {'request changes' if verdict == 'request_changes' else 'approve'}**"]
    return verdict, "\n".join(lines), (llm_text if llm_text else None)


# ----------------------------------------------------------------------------- benchmarks (grade)
def _ask(space_id: str, question: str) -> dict[str, Any]:
    w = _ws()
    msg = w.genie.start_conversation_and_wait(space_id, question, timeout=dt.timedelta(seconds=240))
    conv = getattr(msg, "conversation_id", None) or ""
    mid = getattr(msg, "message_id", None) or getattr(msg, "id", None)
    texts, data, sqls = [], [], []
    for a in (msg.attachments or []):
        if a.text and a.text.content:
            texts.append(a.text.content)
        if a.query and a.attachment_id:
            sqls.append(getattr(a.query, "query", "") or "")
            qr = w.genie.get_message_attachment_query_result(space_id, conv, mid, a.attachment_id)
            sr = qr.statement_response
            rows = (sr.result.data_array if (sr and sr.result) else None) or []
            data.extend(str(v) for row in rows for v in row if v is not None)
    return {"text": "\n".join(texts), "data": data, "sql": sqls,
            "status": msg.status.value if getattr(msg, "status", None) else "",
            "conversation_id": conv}


def _run_benchmarks(rid: str, step: StepName, env: str, space_id: str, v: dict, schema: str) -> dict:
    """Ask each benchmark question of the Genie Agent and grade the answer against the LIVE values of the
    benchmark SQL (re-run now, in the environment's schema)."""
    rows = []
    for b in (v["ga"] or {}).get("benchmarks") or []:
        q, exp = b.get("question", ""), b.get("expected", "")
        try:
            refused = _guard_sql(b.get("sql") or "", v, schema)
            if refused:
                raise RuntimeError(f"query refused: {refused}")
            ref = _live_ref(q, exp, _sql(_sub_mv(b.get("sql") or "", v, schema)))
            ans = _ask(space_id, q)
            g = _grade_ref(ref, ans["text"], ans["data"])
            row = {"question": q, "expected": exp, "live": _ref_text(ref),
                   "got": (ans["text"] or "(no text answer)")[:400],
                   "data_preview": ans["data"][:6], "status": ans["status"],
                   "conversation_id": ans["conversation_id"],
                   "conversation_url": _conv_url(ans["conversation_id"]), **g}
        except Exception as e:  # noqa: BLE001
            row = {"question": q, "expected": exp, "live": "", "got": f"error: {str(e)[:200]}", "pass": False,
                   "numbers_expected": [], "matched": [], "missing": [], "labels_missing": [], "data_preview": []}
        rows.append(row)
    graded = [r for r in rows if r.get("pass") is not None]
    passed = sum(1 for r in graded if r["pass"])
    _emit(rid, step, S.SUCCEEDED,
          f"Benchmarks ({env}): {passed} of {len(graded)} benchmarks with live numbers pass against the {env} Genie Agent.",
          kind="benchmark_results", env=env, space_id=space_id, rows=rows, passed=passed, total=len(graded))
    return {"passed": passed, "total": len(graded), "rows": rows}


# ----------------------------------------------------------------------------- dev build
# ----------------------------------------------------------------------------- pipeline: verify, run, report
def _pipeline_url(pid: str) -> str:
    return f"{_host()}/pipelines/{pid}"


def _pipeline_spec(name: str, schema: str, lib_path: str) -> dict:
    return {"name": name, "catalog": CAT, "schema": schema, "serverless": True, "continuous": False,
            "channel": "CURRENT", "development": False, "libraries": [{"file": {"path": lib_path}}]}


def _find_pipeline_by_name(name: str, schema: str) -> Optional[str]:
    """The pipeline with exactly this name whose target is this catalog.schema, or None. Used before any create, so
    a Genie Code retry or the app fallback reuses the pipeline instead of making a second one with the same name."""
    if not re.fullmatch(r"[a-z0-9-]+", name or ""):
        return None
    r = _api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{name}'", "max_results": "50"}) or {}
    for st in r.get("statuses") or []:
        if st.get("name") != name or not st.get("pipeline_id"):
            continue
        p = _api("GET", f"/api/2.0/pipelines/{st['pipeline_id']}") or {}
        spec = p.get("spec") or p
        if spec.get("catalog") == CAT and (spec.get("schema") or spec.get("target")) == schema:
            return st["pipeline_id"]
    return None


def _feed_profile(pipe: dict) -> dict[str, Any]:
    """The raw feed folder is readable and has csv files (the source check before the pipeline runs)."""
    names = []
    for e in _ws().files.list_directory_contents(pipe["path"]):
        if not getattr(e, "is_directory", False) and getattr(e, "name", None):
            names.append(e.name)
    csvs = sorted(n for n in names if n.lower().endswith(".csv"))
    if not csvs:
        raise RuntimeError(f"no csv files in {pipe['path']}")
    return {"path": pipe["path"], "files": len(csvs), "first": csvs[0], "last": csvs[-1]}


def _pipeline_shared(pid: str) -> bool:
    """The app identity can manage the pipeline (CAN_MANAGE, or it owns it)."""
    # The identity running this code (the app SP when deployed; the developer locally) must be able to manage it.
    try:
        me = (_ws().current_user.me().user_name or "").lower()
    except Exception:  # noqa: BLE001
        me = ""
    try:
        acl = (_api("GET", f"/api/2.0/permissions/pipelines/{pid}") or {}).get("access_control_list") or []
    except Exception:  # noqa: BLE001
        return False
    for entry in acl:
        who = {(entry.get(k) or "").lower() for k in ("service_principal_name", "application_id", "user_name")}
        if APP_SP.lower() not in who and (not me or me not in who):
            continue
        levels = {p.get("permission_level") for p in entry.get("all_permissions") or []}
        levels.add(entry.get("permission_level"))
        if levels & {"CAN_MANAGE", "IS_OWNER"}:
            return True
    return False


def _norm_sql_text(t: str) -> str:
    return re.sub(r"\s+", " ", (t or "")).strip().rstrip(";").strip()


def _library_matches(spec: dict, expected_sql: str) -> tuple[bool, str]:
    """Exactly one file library, and its content equals the reviewed SQL (whitespace-normalised)."""
    libs = spec.get("libraries") or []
    files = [((l or {}).get("file") or {}).get("path") for l in libs]
    if len(libs) != 1 or not files[0]:
        return False, f"expected exactly one file library, found {len(libs)}"
    path = files[0]
    api_path = path[len("/Workspace"):] if path.startswith("/Workspace/") else path
    try:
        out = _api("GET", "/api/2.0/workspace/export", query={"path": api_path, "format": "SOURCE"}) or {}
        content = base64.b64decode(out.get("content") or "").decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return False, f"library {path} not readable: {str(e)[:120]}"
    if _norm_sql_text(content) != _norm_sql_text(expected_sql):
        return False, f"library {path} differs from the reviewed SQL"
    return True, path


def _verify_pipeline(name: str, pid: Optional[str], schema: str, tables: list[str], env: str,
                     expected_sql: Optional[str] = None) -> dict:
    """The pipeline with this name exists, targets this schema, runs serverless, its latest update COMPLETED, every
    planned table exists in the schema with rows, and the app identity can manage it. Never raises: a failed check
    comes back as ok=False with reasons (the Genie Code step then retries, or the app creates it)."""
    if not pid:
        return {"ok": False, "reasons": ["Genie Code did not report a pipeline id"]}
    try:
        p = _api("GET", f"/api/2.0/pipelines/{pid}") or {}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "reasons": [f"pipeline {pid} is not readable: {str(e)[:120]}"]}
    spec = p.get("spec") or p
    if (p.get("name") or spec.get("name")) != name:
        return {"ok": False, "reasons": [f"pipeline {pid} is not named {name}"]}
    upd = (p.get("latest_updates") or [{}])[0] or {}
    ustate = upd.get("state") or ""
    types: dict[str, str] = {}
    if tables:
        rows = _sql(f"SELECT table_name, table_type FROM {CAT}.information_schema.tables "
                    f"WHERE table_schema = '{schema}' AND table_name IN ({', '.join(repr(t) for t in tables)})")
        types = {r["table_name"]: r["table_type"] for r in rows}
    tab = []
    for t in tables:
        n = None
        if types.get(t):
            n = int(_sql(f"SELECT COUNT(*) AS n FROM {CAT}.{schema}.{t}")[0]["n"])
        tab.append({"name": t, "type": types.get(t), "rows": n, "url": _mv_url(f"{CAT}.{schema}.{t}")})
    target = spec.get("schema") or spec.get("target")
    lib_ok, lib_detail = (True, "not checked") if expected_sql is None else _library_matches(spec, expected_sql)
    checks = [
        _chk("runs exactly the reviewed SQL", lib_ok, lib_detail),
        _chk("targets the expected schema", spec.get("catalog") == CAT and target == schema,
             f"{spec.get('catalog')}.{target} (expected {CAT}.{schema})"),
        _chk("serverless", bool(spec.get("serverless")), "serverless compute" if spec.get("serverless") else "not serverless"),
        _chk("latest update COMPLETED", ustate == "COMPLETED", f"update {upd.get('update_id') or '(none)'} {ustate or 'not run'}"),
        _chk("every table exists and has rows", bool(tab) and all(t["type"] and t["rows"] for t in tab),
             ", ".join(f"{t['name']} {t['rows']} rows" if t["rows"] is not None else f"{t['name']} missing" for t in tab)),
        _chk("shared with the app identity (CAN_MANAGE)", _pipeline_shared(pid), "app service principal"),
    ]
    reasons = [f"{c['name']}: {c['detail']}" for c in checks if not c["pass"]]
    return {"ok": not reasons, "reasons": reasons, "id": pid, "url": _pipeline_url(pid), "title": name,
            "schema": schema, "env": env, "tables": tab, "update_id": upd.get("update_id") or "",
            "update_state": ustate, "checks": checks, "serverless": bool(spec.get("serverless"))}


def _run_pipeline_update(pid: str) -> tuple[str, str, str]:
    """One full-refresh update, waited on. (update_id, state, cause)."""
    u = _api("POST", f"/api/2.0/pipelines/{pid}/updates", body={"full_refresh": True}) or {}
    uid = u.get("update_id") or ""
    if not uid:
        return "", "NO_UPDATE", "the pipeline did not start an update"
    deadline = time.time() + 2400
    while time.time() < deadline:
        upd = (_api("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}") or {}).get("update") or {}
        st = upd.get("state") or ""
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            return uid, st, str(upd.get("cause") or "")
        time.sleep(15)
    return uid, "TIMEOUT", ""


def _expectation_counts(pid: str, update_id: str, schema: str, expected: list[dict]) -> list[dict]:
    """Each expectation with the rows it dropped in this update (drop rules), read from the pipeline event log."""
    flows: dict[str, int] = {}
    per: dict[tuple[str, str], int] = {}
    err = ""
    try:
        rows = _sql(f"SELECT origin.flow_name AS flow, CAST(details AS STRING) AS d FROM event_log('{pid}') "
                    f"WHERE event_type = 'flow_progress' AND origin.update_id = '{update_id}'")
        for r in rows:
            try:
                dq = (json.loads(r.get("d") or "{}").get("flow_progress") or {}).get("data_quality") or {}
            except Exception:  # noqa: BLE001
                continue
            flow = str(r.get("flow") or "")
            flows[flow] = max(flows.get(flow, 0), int(dq.get("dropped_records") or 0))
            for e in dq.get("expectations") or []:
                key = (flow, str(e.get("name") or ""))
                per[key] = max(per.get(key, 0), int(e.get("failed_records") or 0))
    except Exception as e:  # noqa: BLE001
        err = f"event log not readable: {str(e)[:120]}"
    out = []
    for x in expected:
        flow = f"{CAT}.{schema}.{x['table']}"
        if (flow, x["name"]) in per:
            dropped, src = per[(flow, x["name"])], "event log (this expectation)"
        elif x["action"] == "drop" and flow in flows:
            dropped, src = flows[flow], "event log (table total, no per-expectation count)"
        else:
            dropped, src = None, err or "no data quality row in the event log"
        out.append({**x, "dropped": dropped, "source": src})
    return out


def _pipeline_folder() -> str:
    """Workspace folder for the reviewed pipeline SQL. OPS_PIPELINE_FOLDER if set; otherwise a folder under the
    identity the app runs as (the deployed app: its own home; locally: the user's), created before the upload."""
    explicit = os.environ.get("OPS_PIPELINE_FOLDER", "").strip()
    if explicit:
        folder = explicit
    else:
        me = _ws().current_user.me().user_name
        folder = f"/Workspace/Users/{me}/acme_studio/pipelines"
    _api("POST", "/api/2.0/workspace/mkdirs", body={"path": folder.replace("/Workspace", "", 1)})
    return folder


def _deploy_pipeline_env(rid: str, step: StepName, env: str, schema: str, v: dict, name: str,
                         prev_id: Optional[str], fallback: bool = False) -> dict:
    """Create the pipeline (or update the one from an earlier run) as the APP identity, upload the reviewed SQL as a
    workspace file, run one full-refresh update and verify it. Returns the verified report. Emits no run event;
    the caller reports it with _pipeline_report."""
    pl = v["pipe"]
    # The app identity writes the reviewed SQL under its own home, in a folder it creates itself.
    ws_path = f"{_pipeline_folder()}/{name}.sql"          # /Workspace/... (pipeline library path)
    path = ws_path[len("/Workspace"):] if ws_path.startswith("/Workspace/") else ws_path   # API path
    _api("POST", "/api/2.0/workspace/import", body={
        "path": path, "format": "RAW", "overwrite": True,
        "content": base64.b64encode(pl["sql"].encode()).decode()})
    spec = _pipeline_spec(name, schema, ws_path)
    existing = prev_id or _find_pipeline_by_name(name, schema)   # never create a second pipeline with the same name
    created = not existing
    if existing:
        _api("PUT", f"/api/2.0/pipelines/{existing}", body=dict(spec, id=existing))
        pid = existing
    else:
        pid = (_api("POST", "/api/2.0/pipelines", body=spec) or {}).get("pipeline_id")
    if not pid:
        raise RuntimeError(f"{env} pipeline create returned no id")
    url = _pipeline_url(pid)
    if env == "prod" and not prev_id:
        _record_prod_id(rid, "pipeline", pid, url)
    elif created:
        _emit(rid, step, S.IN_PROGRESS, f"{env} pipeline {name} created: {url}", kind="pipeline_run", env=env,
              state="CREATED", pipeline_id=pid, pipeline_url=url, schema=schema)
    _share("pipeline", pid)
    uid, ustate, cause = _run_pipeline_update(pid)
    if ustate != "COMPLETED":
        raise RuntimeError(f"{env} pipeline update {uid or '(none)'} ended {ustate}: {cause[:200]}")
    rep = _verify_pipeline(name, pid, schema, pl["planned"], env, expected_sql=pl["sql"])
    if not rep["ok"]:
        raise RuntimeError(f"{env} pipeline did not verify: " + "; ".join(rep["reasons"])[:400])
    rep.update(update_id=uid, update_state=ustate, fallback=fallback)
    return rep


def _pipeline_report(rid: str, step: StepName, env: str, rep: dict, v: dict) -> dict:
    """Attach the expectation drop counts from the event log and emit the pipeline_run event the tracker reads."""
    exps = _expectation_counts(rep["id"], rep.get("update_id") or "", rep["schema"], v["pipe"]["expectations"])
    rep = dict(rep, expectations=exps)
    drops = [e for e in exps if e["action"] == "drop"]
    tab = ", ".join(f"{t['name']} {t['rows']} rows" for t in rep["tables"])
    dq = "; ".join(f"{e['name']} dropped {e['dropped'] if e['dropped'] is not None else 'n/a'}" for e in drops) \
        or "no drop rules"
    _emit(rid, step, S.SUCCEEDED,
          f"Pipeline {env} ({rep['schema']}): update {rep['update_state']}. {tab}. Expectations: {dq}.",
          kind="pipeline_run", env=env, state="TERMINATED", pipeline_id=rep["id"], pipeline_url=rep["url"],
          pipeline_name=rep["title"], schema=rep["schema"], update_id=rep.get("update_id"),
          update_state=rep.get("update_state"), tables=rep["tables"], expectations=exps,
          checks=rep["checks"], fallback=bool(rep.get("fallback")), serverless=rep.get("serverless"))
    return rep


def _fill_pending_benchmarks(rid: str, v: dict) -> None:
    """RAW products: a benchmark that could not be checked at intake (validated 'pending', expected 'computed at
    build') gets its expected text from the live dev data now. The text is written by dp_planner._format_expected
    from the rows the query returned, never typed by a model."""
    from agent import dp_planner as dpp
    if not v["ga"]:
        return
    filled, done = [], []
    for b in v["ga"].get("benchmarks") or []:
        pending = b.get("validated") == "pending" or "computed at build" in str(b.get("expected") or "").lower()
        if not pending:
            filled.append(b)
            continue
        refused = _guard_sql(b.get("sql") or "", v, DEV)
        if refused:
            raise RuntimeError(f"benchmark '{b.get('question')}' refused: {refused}")
        _cols, rows = _sql_cols(_sub_mv(b.get("sql") or "", v, DEV))
        measures = re.findall(r"MEASURE\(`([^`]+)`\)", b.get("sql") or "")
        text = dpp._format_expected(rows, measures) if rows else ""
        if not text:
            raise RuntimeError(f"benchmark '{b.get('question')}' returned no rows on the dev data")
        filled.append(dict(b, expected=text, validated=True))
        done.append(b.get("question"))
    v["ga"]["benchmarks"] = filled
    if done:
        _emit(rid, N.STANDARDS_VALIDATOR, S.SUCCEEDED,
              f"Expected answers computed from the dev data for {len(done)} benchmark(s) that could not be "
              f"checked at intake.", kind="benchmarks_computed", questions=done)


def _mock_pr_number(rid: str) -> int:
    return 1000 + int(rid.replace("-", "")[:6], 16) % 9000


def _safe_thread(rid: str, step: StepName, fn: Callable[[], None], errors: list) -> threading.Thread:
    def runner():
        try:
            fn()
        except Exception as e:  # noqa: BLE001
            LOG.exception("parallel step %s failed", step.value)
            errors.append((step, e))
            _fail(rid, step, f"{step.value} failed: {e}")
    return threading.Thread(target=runner, daemon=True, name=f"genie-{step.value}-{rid[:8]}")


def _outputs_label(v: dict) -> str:
    parts = (["pipeline"] if v["pipe"] else []) + (["metric view"] if v["mv"] else []) + (["Genie Agent"] if v["ga"] else []) \
        + (["dashboard"] if v["db"] else [])
    return " + ".join(parts) or "nothing"


def _job_run_as_user() -> Optional[str]:
    """The identity the Genie Code job runs as (its creator / run_as user)."""
    try:
        j = _api("GET", "/api/2.2/jobs/get", query={"job_id": str(JOB_ID)}) or {}
    except Exception:  # noqa: BLE001
        return None
    ra = (j.get("settings") or {}).get("run_as") or j.get("run_as") or {}
    return ra.get("user_name") or j.get("run_as_user_name") or j.get("creator_user_name")


def _stage_reviewed_sql(name: str, sql: str) -> str:
    """Upload the reviewed SQL as <app folder>/<name>.sql (the app owns it) and give the Genie Code run-as user
    CAN_READ on the folder. Returns the /Workspace/... path for the pipeline library."""
    folder = _pipeline_folder()
    ws_path = f"{folder}/{name}.sql"
    api_path = ws_path[len("/Workspace"):] if ws_path.startswith("/Workspace/") else ws_path
    _api("POST", "/api/2.0/workspace/import", body={
        "path": api_path, "format": "RAW", "overwrite": True, "content": base64.b64encode(sql.encode()).decode()})
    user = _job_run_as_user()
    if user:
        try:
            api_folder = folder[len("/Workspace"):] if folder.startswith("/Workspace/") else folder
            st = _api("GET", "/api/2.0/workspace/get-status", query={"path": api_folder}) or {}
            if st.get("object_id"):
                _api("PATCH", f"/api/2.0/permissions/directories/{st['object_id']}", body={
                    "access_control_list": [{"user_name": user, "permission_level": "CAN_READ"}]})
        except Exception as e:  # noqa: BLE001
            LOG.warning("could not share %s with %s: %s", folder, user, e)
    return ws_path


def _build_dev(rid: str, ctx: _Ctx) -> None:
    req = event_store.fetch_request(rid)
    if not req or req.get("final_status") != "in_progress":
        return
    spec, dp = _spec_dp(req)
    v = _view(spec, dp)
    mv, ga, db = v["mv"], v["ga"], v["db"]

    # 1. spec check
    ctx.step = N.SPEC_CRITIC
    checks = _spec_checks(spec, dp, v)
    bad = [c for c in checks if not c["pass"]]
    if bad:
        _fail(rid, ctx.step, "Spec check failed: " + "; ".join(c["name"] for c in bad), checks=checks)
        return
    _emit(rid, ctx.step, S.SUCCEEDED, f"Spec checked: {len(checks)} of {len(checks)} checks pass. Building: "
          f"{_outputs_label(v)}.", kind="spec_check", checks=checks)

    # 2. source profile
    ctx.step = N.SOURCE_PROFILER
    _emit(rid, ctx.step, S.IN_PROGRESS, "Profiling the listed metric views.")
    prof = _profile(v)
    if prof["missing"]:
        _fail(rid, ctx.step, "Source check failed: not a metric view on the warehouse: " + ", ".join(prof["missing"]))
        return
    if v["pipe"]:
        try:
            prof["feed"] = _feed_profile(v["pipe"])
        except Exception as e:  # noqa: BLE001
            _fail(rid, ctx.step, f"Source check failed: the raw feed {v['pipe']['path']} is not readable: {e}")
            return
    _emit(rid, ctx.step, S.SUCCEEDED,
          f"Profiled {len(prof['sources'])} listed source(s). Latest closed week {prof['latest_week']}."
          + (f" Raw feed: {prof['feed']['files']} csv files." if prof.get("feed") else ""),
          kind="source_profile", **prof)

    # 3. pipeline (RAW data only): Genie Code creates and runs it in the dev schema from the reviewed SQL;
    #    the app verifies the tables, row counts and expectation drops; fallback = the app creates it.
    pipe_run = None
    if v["pipe"]:
        ctx.step = N.DEPLOY_INGESTION
        pl = v["pipe"]
        dev_name = f"{pl['name']}-dev-{rid[:8]}"
        lib_path = _stage_reviewed_sql(dev_name, pl["sql"])      # app-owned copy of the reviewed SQL
        pipe_asset = _build_asset(
            rid, ctx.step, "pipeline", "dev", label="Pipeline", title=dev_name,
            prompt="pipeline.txt",
            prompt_vars={"REF": _ref(rid), "PRODUCT": v["product"], "PIPELINE_NAME": dev_name, "CATALOG": CAT,
                         "TARGET_SCHEMA": DEV, "SOURCE_PATH": pl["path"], "PIPELINE_SQL": pl["sql"],
                         "LIBRARY_PATH": lib_path,
                         "TABLES": ", ".join(pl["planned"]), "WAREHOUSE_ID": WAREHOUSE},
            verify=lambda pid: _verify_pipeline(dev_name, pid or _find_pipeline_by_name(dev_name, DEV),
                                                DEV, pl["planned"], "dev", expected_sql=pl["sql"]),
            fallback=lambda: _deploy_pipeline_env(rid, ctx.step, "dev", DEV, v, dev_name, None, fallback=True))
        pipe_run = _pipeline_report(rid, ctx.step, "dev", pipe_asset, v)

    # 4. metric view (Genie Code), only when the grill asked for a new one
    ctx.step = N.PIPELINE_GENERATOR
    if mv:
        _build_asset(
            rid, ctx.step, "metric_view", "dev", label="Metric view", title=v["mv_dev"],
            prompt="metric_view.txt",
            prompt_vars={"REF": _ref(rid), "PRODUCT": v["product"], "CATALOG": CAT, "DEV_SCHEMA": DEV,
                         "WAREHOUSE_ID": WAREHOUSE, "METRIC_VIEW_SQL": _mv_sql(mv, DEV, tuple(v["gold"])),
                         "MV_NAME": v["mv_dev"],
                         "VERIFY_SQL": "SELECT " + ", ".join(f"MEASURE(`{m}`)" for m in _mv_measures(mv, tuple(v["gold"])))
                                       + f" FROM {v['mv_dev']}",
                         "MEASURES": ", ".join(_mv_measures(mv, tuple(v["gold"])))},
            verify=lambda _pid: _verify_mv(v["mv_dev"], mv.get("measures") or []),
            fallback=lambda: _fallback_mv(v["mv_dev"], mv, tuple(v["gold"])))

    # 4. DQ rules (read-only, warnings never fail)
    ctx.step = N.STANDARDS_VALIDATOR
    dq = _run_dq(rid, dp, tuple(v["pipe_tables"]))
    _fill_pending_benchmarks(rid, v)        # RAW: expected answers from the live data (validated 'pending' at intake)
    live = _live_checks(rid, v)

    # 5 + 6. Genie Agent and dashboard in parallel: both only need the metric views
    agent_title = f"{ga['title']} [dev {rid[:8]}]" if ga else ""
    dash_title = f"{db['title']} [dev {rid[:8]}]" if db else ""
    box: dict[str, dict] = {}
    errors: list = []

    def build_agent():
        bench_lines = "\n".join(f"- {b['question']} -> expected: {b['expected']} | check SQL: "
                                f"{_one_line(_sub_mv(b['sql'], v, DEV))}" for b in ga.get("benchmarks") or [])
        box["agent"] = _build_asset(
            rid, N.TEST_GENERATOR, "genie_agent", "dev", label="Genie Agent", title=agent_title,
            prompt="genie_agent.txt",
            prompt_vars={"REF": _ref(rid), "PRODUCT": v["product"], "TITLE": agent_title,
                         "DESCRIPTION": ga.get("description", ""), "WAREHOUSE_ID": WAREHOUSE, "FOLDER": FOLDER,
                         "DATA_SOURCES": ", ".join(_nfq(s) for s in ga.get("data_sources") or []),
                         "INSTRUCTIONS": ga.get("instructions", ""),
                         "SAMPLE_QUESTIONS": "\n".join(f'  - "{q}"' for q in v["questions"]),
                         "BENCHMARKS": bench_lines},
            verify=lambda pid: _verify_space(agent_title, pid,
                                             must=[_nfq(s) for s in ga.get("data_sources") or []]),
            fallback=lambda: _fallback_space(agent_title, ga.get("description", ""),
                                             [_nfq(s) for s in ga.get("data_sources") or []],
                                             ga.get("instructions", ""), v["questions"]))

    def build_dash():
        pages_txt = "\n".join(
            f'- Page "{p.get("name", "")}":\n' + "\n".join(
                f'    - widget "{w.get("title", "")}" ({w.get("type", "table")}): {_one_line(w.get("sql", ""))}'
                for w in (p.get("widgets") or []))
            for p in db.get("pages") or [])
        box["dash"] = _build_asset(
            rid, N.DEPLOY_DEV, "dashboard", "dev", label="Dashboard", title=dash_title,
            prompt="dashboard.txt",
            prompt_vars={"REF": _ref(rid), "PRODUCT": v["product"], "TITLE": dash_title, "FOLDER": FOLDER,
                         "WAREHOUSE_ID": WAREHOUSE, "DATA_SOURCES": ", ".join(v["ref_sources_dash"]),
                         "PAGES": pages_txt, "FILTERS": ", ".join(db.get("filters") or []) or "none",
                         "REFRESH": db.get("refresh", "") or "none"},
            verify=lambda pid: _verify_dashboard(dash_title, pid, must=v["ref_sources_dash"]),
            fallback=lambda: _fallback_dashboard(dash_title, db.get("pages") or [], WAREHOUSE, v))

    threads = []
    if ga:
        threads.append(_safe_thread(rid, N.TEST_GENERATOR, build_agent, errors))
    if db:
        threads.append(_safe_thread(rid, N.DEPLOY_DEV, build_dash, errors))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors or ("agent" not in box and ga) or ("dash" not in box and db):
        return  # the failing step already recorded its FAILED event

    agent = box.get("agent")
    dash = box.get("dash")
    agent_full = (_verify_space(agent_title, agent["id"], must=[_nfq(s) for s in ga.get("data_sources") or []])
                  or agent) if agent else None
    dash_full = (_verify_dashboard(dash_title, dash["id"], must=v["ref_sources_dash"]) or dash) if dash else None

    # 7. PR artefacts (only for what was built)
    ctx.step = N.BRANCH_AND_PR
    _emit(rid, ctx.step, S.IN_PROGRESS, f"Assembling the PR: {_outputs_label(v)}.")
    mv_info, mv_y, show = None, {}, ""
    if mv:
        show = _sql(f"SHOW CREATE TABLE {v['mv_dev']}")[0]["createtab_stmt"]
        mv_y = _mv_yaml(show)
        mv_info = {"name": v["mv_dev"], "url": _mv_url(v["mv_dev"]),
                   "measures": [{"name": m.get("name"), "comment": m.get("comment", "")}
                                for m in (mv_y.get("measures") or [])]}
    checklist = _checklist(v, mv_y, agent_full, dash_full, dq, live, pipe_run)
    failed_items = [c for c in checklist if not c["pass"]]

    pr_no = _mock_pr_number(rid)
    ref = _ref(rid)
    title = f"{ref}: {v['product']} ({_outputs_label(v)}) [dev]"
    branch = f"studio/{v['slug']}-{rid[:8]}"
    agent_summary = None
    if agent_full:
        agent_summary = {"title": agent_full["title"], "space_id": agent_full["id"], "url": agent_full["url"],
                         "description": ga.get("description", ""), "data_sources": agent_full["tables"],
                         "instructions": agent_full["instructions"], "sample_questions": agent_full["sample_questions"],
                         "benchmarks": [{"question": b.get("question"), "sql": _one_line(b.get("sql", "")),
                                         "expected": b.get("expected")} for b in ga.get("benchmarks") or []]}
    dash_info = None
    if dash_full:
        design_pages = db.get("pages") or []
        dash_info = {"title": dash_full["title"], "id": dash_full["id"], "url": dash_full["url"],
                     "pages": [{"page": p, "widgets": [w.get("title", "") for w in
                                                       ((design_pages[i] if i < len(design_pages) else {})
                                                        .get("widgets") or [])]}
                               for i, p in enumerate(dash_full["pages"])],
                     "datasets": dash_full["dataset_names"]}
    dq_doc = {"rules": dq, "checklist": checklist}
    files: dict[str, str] = {}
    if mv:
        files[f"metric_view/{v['mv_base']}.sql"] = show
    if agent_summary:
        files[f"genie/{v['slug']}_agent.json"] = json.dumps(agent_summary, indent=2, default=str)
    if dash_full:
        files[f"dashboard/{v['slug']}_dashboard.json"] = (
            json.dumps(dash_full["serialized"], indent=2, default=str) if dash_full.get("serialized")
            else dash_full.get("serialized_text", ""))
    files["dq/rules_and_checks.json"] = json.dumps(dq_doc, indent=2, default=str)
    if pipe_run:
        files[f"pipeline/{pipe_run['title']}.sql"] = v["pipe"]["sql"]
        files["pipeline/expectations.json"] = json.dumps(pipe_run["expectations"], indent=2, default=str)
        files["pipeline/run.json"] = json.dumps(
            {k: pipe_run[k] for k in ("id", "url", "update_id", "update_state", "tables")}, indent=2, default=str)
    body_lines = [f"{ref} build (dev): {v['product']}.", ""]
    if pipe_run:
        body_lines.append(f"- Pipeline: {pipe_run['title']} ({pipe_run['url']}), tables: "
                          + ", ".join(f"{t['name']} {t['rows']} rows" for t in pipe_run["tables"]))
    if mv_info:
        body_lines.append(f"- Metric view: {v['mv_dev']} ({len(mv_info['measures'])} measures)")
    if agent_full:
        body_lines.append(f"- Genie Agent: {agent_full['title']} ({agent_full['url']})")
    if dash_full:
        body_lines.append(f"- Dashboard: {dash_full['title']} ({dash_full['url']}), pages: {', '.join(dash_full['pages'])}")
    body_lines.append(f"- DQ: {len(dq)} rules checked against {SRC}")
    body_lines.append(f"- Checklist: {len(checklist) - len(failed_items)} of {len(checklist)} pass")
    body = "\n".join(body_lines) + "\n"
    artifacts = {
        "outputs": v["outputs"],
        "metric_view": mv_info,
        "pipeline": ({k: pipe_run[k] for k in ("id", "url", "title", "tables", "expectations", "update_id",
                                               "update_state")} if pipe_run else None),
        "agent": agent_summary,
        "dashboard": dash_info,
        "dq": dq,
        "checklist": checklist,
        "data_sources": v["sources"],
    }
    _emit(rid, ctx.step, S.SUCCEEDED,
          f"PR #{pr_no} opened in the app: {len(files)} artefacts, {len(failed_items)} checklist item(s) to review.",
          kind="mock_pr", pr_number=pr_no, pr_title=title, pr_body=body, branch=branch, files=files,
          file_count=len(files), artifacts=artifacts, pr_url=f"app://pr/{rid}")

    # 8. AI review
    ctx.step = N.PR_REVIEWER
    _emit(rid, ctx.step, S.IN_PROGRESS, "AI reviewer checking the PR against the checklist.")
    verdict, comment_md, llm_notes = _review(v, agent_full, dash_full, mv_info, checklist, dq, pipe_run)
    _emit(rid, ctx.step, S.SUCCEEDED,
          f"AI review: {'request changes' if verdict == 'request_changes' else 'approve'}. "
          f"{len(failed_items)} checklist item(s) need changes.",
          kind="pr_review_comment", round=1, verdict=verdict, comment_md=comment_md, checklist=checklist,
          llm_notes=llm_notes or "")

    # 9. park at the human gate
    ctx.step = N.AWAIT_HUMAN
    event_store.update_request(rid, pr_url=f"app://pr/{rid}", pr_number=pr_no, final_status="pending_review")
    _emit(rid, ctx.step, S.IN_PROGRESS,
          "Waiting for a human decision. Approve to verify, stage and promote to prod, or reject.",
          kind="await_human", ai_verdict=verdict, ai_findings=len(failed_items))


# ----------------------------------------------------------------------------- promote
def _latest_asset(rid: str, asset: str, env: str) -> Optional[dict]:
    return _last(rid, lambda d: d.get("kind") == "genie_code_run" and d.get("asset") == asset
                 and d.get("env") == env and d.get("state") == "TERMINATED" and d.get("asset_id"))


def _record_prod_id(rid: str, asset: str, asset_id: str, url: str) -> None:
    # Written as soon as the object exists, so a re-run reuses it even if verification failed.
    _emit(rid, N.DEPLOY_PROD, S.IN_PROGRESS, f"Prod {asset} created: {url}",
          kind="prod_asset", asset=asset, env="prod", asset_id=asset_id, asset_url=url)


def _share(kind: str, object_id: Optional[str]) -> None:
    """Give the demo owner CAN_MANAGE on a Genie space / dashboard the APP created.

    The app's service principal owns what it creates; without this the analyst can't open the prod
    Genie Agent or dashboard. Best effort: a failure is logged, never fails the promotion."""
    owner = os.environ.get("OPS_PROD_OWNER", "vaishnavi.mohan@databricks.com")
    if not object_id or not owner:
        return
    path = {"genie": f"/api/2.0/permissions/genie/{object_id}",
            "dashboard": f"/api/2.0/permissions/dashboards/{object_id}",
            "pipeline": f"/api/2.0/permissions/pipelines/{object_id}"}[kind]
    try:
        _api("PATCH", path, body={"access_control_list": [{"user_name": owner, "permission_level": "CAN_MANAGE"}]})
    except Exception as e:  # noqa: BLE001
        LOG.warning("could not share %s %s with %s: %s", kind, object_id, owner, e)


def _norm_folder(path: str) -> str:
    path = (path or "").rstrip("/")
    return path[len("/Workspace"):] if path.startswith("/Workspace/") else path


def _in_app_folder(parent_path: Optional[str]) -> bool:
    return bool(parent_path) and _norm_folder(parent_path) == _norm_folder(APP_FOLDER)


def _find_app_space(title: str) -> Optional[str]:
    """The prod Genie Agent with this exact title, ONLY if the app created it (it lives in APP_FOLDER)."""
    sid = _find_space(title)
    if not sid:
        return None
    g = _api("GET", f"/api/2.0/genie/spaces/{sid}") or {}
    return sid if _in_app_folder(g.get("parent_path")) else None


def _find_app_dash(title: str) -> Optional[str]:
    """The prod dashboard with this exact title, ONLY if the app created it (it lives in APP_FOLDER)."""
    did = _find_dash(title)
    if not did:
        return None
    d = _api("GET", f"/api/2.0/lakeview/dashboards/{did}") or {}
    return did if _in_app_folder(d.get("parent_path")) else None


def _create_or_update_space(rid: str, existing: Optional[str], payload: dict, title: str,
                            must: list[str]) -> dict:
    if existing:
        _api("PATCH", f"/api/2.0/genie/spaces/{existing}", body=payload)
        sid = existing
    else:
        body = dict(payload, parent_path=APP_FOLDER)
        sid = (_api("POST", "/api/2.0/genie/spaces", body=body) or {}).get("space_id")
        if sid:
            _record_prod_id(rid, "genie_agent", sid, _genie_url(sid))
    _share("genie", sid)
    v = None
    for wait in (0, 5, 15):          # read-after-create can lag a few seconds
        if wait:
            time.sleep(wait)
        v = _verify_space(title, sid, must=must, search=False)
        if v:
            break
    if not v:
        raise RuntimeError(f"Genie Agent '{title}' ({sid}) did not verify against {', '.join(must)}")
    return v


def _publish_dashboard(rid: str, dev_dash_id: str, title: str, existing: Optional[str], v: dict,
                       must: list[str]) -> dict:
    dev = _api("GET", f"/api/2.0/lakeview/dashboards/{dev_dash_id}")
    ser = dev.get("serialized_dashboard") or ""
    if v["mv_dev"]:
        ser = ser.replace(v["mv_dev"], _env_fq(v, PROD))   # the new metric view moves to its prod name
    body = {"display_name": title, "serialized_dashboard": ser, "warehouse_id": WAREHOUSE}
    if existing:
        _api("PATCH", f"/api/2.0/lakeview/dashboards/{existing}", body=body)
        did = existing
    else:
        did = (_api("POST", "/api/2.0/lakeview/dashboards", body=dict(body, parent_path=APP_FOLDER)) or {}
               ).get("dashboard_id")
        if did:
            _record_prod_id(rid, "dashboard", did, _dash_url(did))
    _share("dashboard", did)
    _ws().lakeview.publish(did, embed_credentials=False, warehouse_id=WAREHOUSE)
    out = None
    for wait in (0, 5, 15):
        if wait:
            time.sleep(wait)
        out = _verify_dashboard(title, did, must=must, search=False)
        if out:
            break
    if not out:
        raise RuntimeError(f"dashboard '{title}' ({did}) did not verify against {', '.join(must)}")
    out["published_url"] = _dash_url(did, published=True)
    return out


def _widget_rerun(v: dict, schema: str) -> list[dict]:
    out = []
    for w in v["widgets"]:
        sql = _sub_mv(w.get("sql") or "", v, schema)
        try:
            refused = _guard_sql(w.get("sql") or "", v, schema)
            if refused:
                raise RuntimeError(f"query refused: {refused}")
            n = len(_sql(sql))
            out.append({"title": w.get("title", ""), "rows": n, "status": "ok" if n else "no rows"})
        except Exception as e:  # noqa: BLE001
            out.append({"title": w.get("title", ""), "rows": 0, "status": "error", "error": str(e)[:200]})
    return out


def _promote(rid: str, ctx: _Ctx) -> None:
    req = event_store.fetch_request(rid)
    if not req or req.get("final_status") != "in_progress":
        return
    spec, dp = _spec_dp(req)
    v = _view(spec, dp)
    ga, db, mv = v["ga"], v["db"], v["mv"]
    review = _last(rid, lambda d: d.get("kind") == "pr_review_comment") or {}
    ai_verdict = review.get("verdict", "unknown")

    ctx.step = N.VERIFY_PR_APPROVED
    override = ("; the AI review requested changes and the human approved anyway"
                if ai_verdict == "request_changes" else "")
    _emit(rid, ctx.step, S.SUCCEEDED,
          f"Verified: approval recorded in the tracker{override}. The PR lives in the app, so there is no GitHub "
          f"approval to check.", kind="verify", ai_verdict=ai_verdict)

    dev_agent = (_latest_asset(rid, "genie_agent", "dev") or {}) if ga else {}
    dev_dash = (_latest_asset(rid, "dashboard", "dev") or {}) if db else {}
    if ga and not dev_agent.get("asset_id"):
        _fail(rid, ctx.step, "dev Genie Agent not found on the request; cannot benchmark")
        return
    if db and not dev_dash.get("asset_id"):
        _fail(rid, ctx.step, "dev dashboard not found on the request; cannot check it")
        return

    # staging: the pipeline (RAW data), the new metric view (if any), the dashboard widgets and the benchmarks
    ctx.step = N.DEPLOY_STAGING
    if v["pipe"]:
        prev = _last(rid, lambda d: d.get("kind") == "pipeline_run" and d.get("env") == "stage" and d.get("pipeline_id"))
        _emit(rid, ctx.step, S.IN_PROGRESS,
              f"Creating and running the pipeline {v['pipe']['name']}-stage in {CAT}.{STAGE} as the app identity.")
        _pipeline_report(rid, ctx.step, "stage",
                         _deploy_pipeline_env(rid, ctx.step, "stage", STAGE, v, f"{v['pipe']['name']}-stage",
                                              (prev or {}).get("pipeline_id")), v)
    stage_mv = _env_fq(v, STAGE) if mv else ""
    if mv:
        _emit(rid, ctx.step, S.IN_PROGRESS, f"Creating the metric view in {stage_mv}.")
        _sql(_mv_sql(mv, STAGE, tuple(v["gold"])))
        _grant_view_to_owner(stage_mv)
        if not _verify_mv(stage_mv, mv.get("measures") or []):
            _fail(rid, ctx.step, f"{stage_mv} is not a METRIC_VIEW with the planned measures after create")
            return
        _emit(rid, ctx.step, S.IN_PROGRESS,
              f"Staging metric view ready: {_mv_url(stage_mv)}. Re-running the dashboard widgets and benchmarks in staging.",
              kind="stage_asset", asset_id=stage_mv, asset_url=_mv_url(stage_mv))
    stage_msgs = []
    if db:
        wl = _widget_rerun(v, STAGE if mv else DEV)
        bad = [w["title"] for w in wl if w["status"] != "ok"]
        if bad:
            _fail(rid, ctx.step, f"Staging: dashboard widget(s) did not return rows: {bad}", kind="stage_check",
                  widgets=wl)
            return
        stage_msgs.append(f"{len(wl)} dashboard widget queries return rows")
        _emit(rid, ctx.step, S.IN_PROGRESS, f"Staging: {stage_msgs[-1]}.", kind="stage_check", widgets=wl)
    if ga:
        bench = _run_benchmarks(rid, ctx.step, "dev", dev_agent["asset_id"], v, DEV)
        stage_msgs.append(f"dev benchmarks {bench['passed']} of {bench['total']} pass")
    if mv:
        _emit(rid, ctx.step, S.SUCCEEDED,
              f"Staged: {stage_mv}. " + "; ".join(stage_msgs) + ".", kind="stage_asset", asset_id=stage_mv,
              asset_url=_mv_url(stage_mv), stage_asset=True)
    else:
        _emit(rid, ctx.step, S.SUCCEEDED, "Staged: no new metric view (every source is reused). "
              + "; ".join(stage_msgs) + ".", kind="stage_check")

    # prod
    ctx.step = N.DEPLOY_PROD
    prev_assets: dict[str, str] = {}
    for k in ("genie_agent", "dashboard"):
        hit = _last(rid, lambda d, k=k: d.get("kind") == "prod_asset" and d.get("asset") == k and d.get("asset_id"))
        if hit:
            prev_assets[k] = hit["asset_id"]
    assets: dict[str, dict] = {}
    prod_bench = None
    if v["pipe"]:
        prev_p = _last(rid, lambda d: d.get("kind") == "prod_asset" and d.get("asset") == "pipeline" and d.get("asset_id"))
        prod_pipe = _pipeline_report(rid, ctx.step, "prod",
                                     _deploy_pipeline_env(rid, ctx.step, "prod", PROD, v, v["pipe"]["name"],
                                                          (prev_p or {}).get("asset_id")), v)
        assets["pipeline"] = {"id": prod_pipe["id"], "url": prod_pipe["url"],
                              "tables": [t["name"] for t in prod_pipe["tables"]]}
    if mv:
        prod_mv = _env_fq(v, PROD)
        _emit(rid, ctx.step, S.IN_PROGRESS, f"Creating the metric view in {prod_mv}.")
        _sql(_mv_sql(mv, PROD, tuple(v["gold"])))
        _grant_view_to_owner(prod_mv)
        if not _verify_mv(prod_mv, mv.get("measures") or []):
            _fail(rid, ctx.step, f"{prod_mv} is not a METRIC_VIEW with the planned measures after create")
            return
        assets["metric_view"] = {"id": prod_mv, "url": _mv_url(prod_mv)}
        _emit(rid, ctx.step, S.IN_PROGRESS, f"Prod metric view ready: {_mv_url(prod_mv)}.",
              kind="prod_asset", asset="metric_view", asset_id=prod_mv, asset_url=_mv_url(prod_mv))
    if ga:
        prod_title = ga["title"]
        prod_tables = [_to_env(s, v, PROD) for s in ga.get("data_sources") or []]
        agent_v = _create_or_update_space(
            rid, _find_app_space(prod_title) or prev_assets.get("genie_agent"),   # a release UPDATES the one prod agent
            _space_payload(prod_title, ga.get("description", ""), prod_tables, ga.get("instructions", ""),
                           v["questions"]),
            prod_title, must=prod_tables)
        assets["genie_agent"] = {"id": agent_v["id"], "url": agent_v["url"], "title": prod_title}
        _emit(rid, ctx.step, S.IN_PROGRESS, f"Prod Genie Agent '{prod_title}' ready: {agent_v['url']}.",
              kind="prod_asset", asset="genie_agent", asset_id=agent_v["id"], asset_url=agent_v["url"])
        prod_bench = _run_benchmarks(rid, ctx.step, "prod", agent_v["id"], v, PROD)
    if db:
        prod_dash_title = db["title"]
        dash_must = [_to_env(s, v, PROD) for s in v["ref_sources_dash"]]
        dash_v = _publish_dashboard(rid, dev_dash["asset_id"], prod_dash_title,
                                    _find_app_dash(prod_dash_title) or prev_assets.get("dashboard"), v, dash_must)
        assets["dashboard"] = {"id": dash_v["id"], "url": dash_v["url"], "published_url": dash_v["published_url"],
                               "title": prod_dash_title}
        _emit(rid, ctx.step, S.IN_PROGRESS, f"Prod dashboard '{prod_dash_title}' published: {dash_v['published_url']}.",
              kind="prod_asset", asset="dashboard", asset_id=dash_v["id"], asset_url=dash_v["published_url"])

    rel_id = "rel-" + uuid.uuid4().hex[:8]  # release record is created once; re-runs reuse prod ids above
    product = v["product"]
    rel_title = f"{_ref(rid)} · {product}"
    built_parts = []
    if "pipeline" in assets:
        built_parts.append(f"pipeline {assets['pipeline']['id']}")
    if "metric_view" in assets:
        built_parts.append(f"metric view {assets['metric_view']['id']}")
    if "genie_agent" in assets:
        built_parts.append(f"Genie Agent '{assets['genie_agent']['title']}'")
    if "dashboard" in assets:
        built_parts.append(f"published dashboard '{assets['dashboard']['title']}'")
    summary = (f"{product}: " + ", ".join(built_parts) + f". Built by Genie Code from the reviewed plan; "
               f"AI review: {ai_verdict}.")
    event_store.create_release(rel_id, rel_title, summary, req.get("requested_by") or "tracker", [rid])
    event_store.update_release(rel_id, status="released")
    bench_msg = (f" Prod benchmarks {prod_bench['passed']} of {prod_bench['total']} pass." if prod_bench else "")
    _emit(rid, ctx.step, S.SUCCEEDED,
          f"Released to prod: {', '.join(built_parts)}.{bench_msg}",
          kind="prod_release", assets=assets, release_id=rel_id,
          benchmark_passed=prod_bench["passed"] if prod_bench else None,
          benchmark_total=prod_bench["total"] if prod_bench else None)
    event_store.update_request(rid, release_status="released", release_id=rel_id, final_status="done")
    _emit(rid, N.DONE, S.SUCCEEDED, "Build complete. Dev, staging and prod objects exist; release recorded.",
          kind="done")


# ----------------------------------------------------------------------------- entry points
def _guard(fn: Callable[[str, _Ctx], None], rid: str) -> None:
    ctx = _Ctx()
    try:
        fn(rid, ctx)
    except Exception as e:  # noqa: BLE001
        LOG.exception("genie engine crashed for %s at %s", rid, ctx.step)
        try:
            _fail(rid, ctx.step, f"Build stopped: {e or type(e).__name__}")
        except Exception:  # noqa: BLE001
            LOG.exception("could not record the failure for %s", rid)


def _start(fn: Callable[[str, _Ctx], None], rid: str) -> None:
    threading.Thread(target=_guard, args=(fn, rid), daemon=True,
                     name=f"genie-{fn.__name__}-{rid[:8]}").start()


def start_build(request_id: str) -> None:
    """Gate 1 (plan approved): build the dev assets and park at pending_review."""
    _start(_build_dev, request_id)


def start_promote(request_id: str) -> None:
    """Gate 2 (approved): verify, stage, promote to prod, release."""
    _start(_promote, request_id)


def plan_for(data_product: dict) -> list[dict]:
    return aj.plan_steps(data_product or {})
