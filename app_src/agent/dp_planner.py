"""Grill-driven data product planner (CONTRACT2, module agent/dp_planner.py).

Pure planning logic, no FastAPI. The LLM proposes; the code verifies against the
live catalog and the warehouse before anything is returned:

  catalog()               live snapshot: metric views, gold tables, Genie Agents, dashboards
  catalog_prompt_block()  compact catalog text for the grill system prompt
  match_needs()           REUSE / EXTEND / GAP / RAW / NO_DATA per need item (RAW = only in a raw feed)
  author_metric_view()    YAML 1.1 metric view for GAP/EXTEND/RAW items; RAW reads a planned gold table
                          and comes back validated="pending" (pipeline_planner builds the table)
  select_dq()             existing repo rules upstream + validated proposed rules (+ pipeline expectations)
  design()                Genie Agent and/or dashboard; benchmarks and widgets are RUN (RAW: guarded only,
                          expected/validated = "pending")
  validate_sql_readonly() single-SELECT guard on the allowed schemas
"""
from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import re
import threading
import time
from typing import Any, Optional

import yaml

from agent import acme_journey as aj

CAT = aj.CATALOG
SRC_SCHEMA = aj.SRC_SCHEMA
DEV_SCHEMA = aj.DEV_SCHEMA
CONTROL_SCHEMA = aj.CONTROL_SCHEMA
# By default the reuse check ignores metric views this app itself released to studio_prod, so every demo
# run of the same ask behaves the same (the gap is built and promoted again). OPS_REUSE_INCLUDE_PROD=1
# makes earlier releases reusable, which is the "real" behaviour.
CATALOG_SCHEMAS = (SRC_SCHEMA, "studio_prod") if os.environ.get("OPS_REUSE_INCLUDE_PROD", "0") == "1" else (SRC_SCHEMA,)
READ_SCHEMAS = {SRC_SCHEMA, DEV_SCHEMA, "studio_prod"}
CATALOG_TTL_S = 600
LINEAGE_TTL_S = 600
LINEAGE_DAYS = 90
DRAFT_PREFIX = "mv_draft_"
WIDGET_TYPES = {"counter", "bar", "line", "table"}
REVIEW_JOIN_ALIASES = {"branch", "rank", "customer"}
FORBIDDEN_TEXT = re.compile(
    r"\b(blackwoods|cfs|colonial|meridian|monday fund pulse|superannuation|cfs_workbench)\b", re.I)

# Static upstream map, unioned with system.access.table_lineage (which may be unavailable).
GOLD_UPSTREAM_STATIC = {
    "gold_fct_sales_line": ["silver_sales_invoice_line"],
    "gold_sales_line_rebate_alloc": ["gold_fct_sales_line", "gold_fct_supplier_rebate"],
    "gold_fct_supplier_rebate": ["bronze_supplier_rebates"],
    "gold_fct_freight": ["bronze_freight_charges"],
    "gold_plan_gross_margin": ["bronze_fpa_plan"],
    "gold_fct_ar_invoice": ["silver_ar_transaction"],
    "gold_customer_rank": ["gold_fct_ar_invoice"],
    "gold_fct_inventory_snapshot": ["silver_inventory_on_hand"],
    "gold_dim_customer": ["silver_customer_hist"],
    "gold_dim_branch": ["bronze_branches"],
    "gold_dim_product": ["bronze_products"],
}

# Read-only guard: single SELECT, fully qualified tables in allowed schemas only.
_RO_BLOCKED = re.compile(
    r"\b(insert|update|delete|drop|alter|create|grant|revoke|merge|call|exec|execute|truncate|replace|"
    r"attach|detach|use|set|copy|optimize|vacuum|refresh|describe|show|with|into|union)\b", re.I)
_TABLE_REF = re.compile(r"\b(?:from|join)\s+([A-Za-z0-9_.]+)", re.I)
_FQ_REF = re.compile(r"\b([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\b")
_MEASURE_REF = re.compile(r"MEASURE\(\s*`([^`]+)`\s*\)", re.I)
_NUM_REF = re.compile(r"(?<![\w.-])\d+(?:\.\d+)?(?![\w.])")
_MONEY = re.compile(
    r"usd|revenue|cogs|exposure|value|leakage|freight|accrual|billed|\bar\b|overdue|past due|\bover\b|"
    r"cost|balance|inventory|contribution", re.I)

_DECISIONS = ("REUSE", "EXTEND", "GAP", "RAW", "NO_DATA")


# ----------------------------------------------------------------------------- utils
def _fq(schema: str, name: str) -> str:
    return f"{CAT}.{schema}.{name}"


def _short(fq: str) -> str:
    return str(fq).split(".")[-1]


def _key(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip().lower()


def _as_num(v: Any) -> Optional[float]:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", ""))
    except ValueError:
        return None


def _clip(s: Any, n: int) -> str:
    t = re.sub(r"\s+", " ", str(s or "")).strip()
    return t if len(t) <= n else t[: n - 3].rstrip() + "..."


def _clean_text(s: Any, n: int) -> str:
    t = re.sub(r"\s+", " ", str(s or "")).strip()
    if not t or FORBIDDEN_TEXT.search(t):
        return ""
    return t[:n]


def _short_err(e: Any) -> str:
    return _clip(str(e), 300)


def _json_obj(s: Any) -> dict:
    if isinstance(s, dict):
        return s
    try:
        v = json.loads(s) if isinstance(s, str) and s.strip() else {}
    except (TypeError, ValueError):
        return {}
    return v if isinstance(v, dict) else {}


def _bt(name: str) -> str:
    return "`" + str(name).replace("`", "") + "`"


def _drop(out: dict, kind: str, item: str, reason: str) -> None:
    out["dropped"].append({"kind": kind, "item": item, "reason": reason})


def _llm_text(system: str, user: str) -> str:
    from langchain_core.messages import HumanMessage, SystemMessage
    from agent.llm import get_llm
    resp = get_llm().invoke([SystemMessage(content=system), HumanMessage(content=user)])
    if getattr(resp, "truncated", False):
        raise RuntimeError("LLM reply was cut off at the output limit")
    content = resp.content
    return content if isinstance(content, str) else str(content)


def _llm_json(system: str, user: str) -> Any:
    raw = _llm_text(system, user)
    fence = re.search(r"```(?:json)?[ \t]*\n?(.*?)```", raw, re.S)
    body = fence.group(1) if fence else raw
    starts = [i for i in (body.find("{"), body.find("[")) if i >= 0]
    if not starts:
        raise ValueError("LLM reply had no JSON")
    obj, _ = json.JSONDecoder().raw_decode(body[min(starts):])
    return obj


def _fq_refs(text: str) -> list[str]:
    out: list[str] = []
    for c, s, t in re.findall(rf"\b({re.escape(CAT)})\.(\w+)\.(\w+)\b", text):
        fq = f"{c}.{s}.{t}"
        if fq not in out:
            out.append(fq)
    return out


def _slug(need: dict) -> str:
    raw = str(need.get("slug") or need.get("purpose") or "data_product").lower()
    s = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    s = re.sub(r"^mv_", "", s)[:40].strip("_")
    return s or "data_product"


# ----------------------------------------------------------------------------- catalog
_CACHE: dict[str, Any] = {"at": 0.0, "value": None}
_CACHE_LOCK = threading.Lock()


def catalog(force: bool = False) -> dict:
    """Live snapshot. Cached for CATALOG_TTL_S seconds."""
    with _CACHE_LOCK:
        if not force and _CACHE["value"] is not None and time.time() - _CACHE["at"] < CATALOG_TTL_S:
            return _CACHE["value"]
    value = _build_catalog()
    with _CACHE_LOCK:
        _CACHE["at"], _CACHE["value"] = time.time(), value
    return value


def _build_catalog() -> dict:
    schemas = ", ".join(aj.lit(s) for s in CATALOG_SCHEMAS)
    mv_rows = aj.sql(f"SELECT table_schema, table_name FROM {CAT}.information_schema.tables "
                     f"WHERE table_schema IN ({schemas}) AND table_type = 'METRIC_VIEW' ORDER BY 1, 2")
    fqs = [_fq(r["table_schema"], r["table_name"]) for r in mv_rows]
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        stmts = list(ex.map(_show_create, fqs))
    metric_views = []
    for fq, stmt in zip(fqs, stmts):
        body = _yaml_body(stmt)
        mv = _mv_from_body(fq, body) if body else None
        if mv:
            metric_views.append(mv)
    col_rows = aj.sql(
        f"SELECT table_name, column_name FROM {CAT}.information_schema.columns "
        f"WHERE table_schema = {aj.lit(SRC_SCHEMA)} AND (LEFT(table_name, 5) = 'gold_' "
        f"OR LEFT(table_name, 7) = 'silver_' OR LEFT(table_name, 7) = 'bronze_') "
        f"ORDER BY table_name, ordinal_position")
    columns: dict[str, list[str]] = {}
    for r in col_rows:
        columns.setdefault(r["table_name"], []).append(r["column_name"])
    mv_names = {m["name"] for m in metric_views}
    latest = None
    try:
        rows = aj.sql(f"SELECT CAST(MAX(week_start) AS STRING) AS w FROM {_fq(SRC_SCHEMA, 'gold_fct_sales_line')}")
        latest = (rows[0].get("w") if rows else None) or None
    except Exception:  # noqa: BLE001
        latest = None
    return {
        "snapshot_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "host": aj.host(),  # resolved once; aj.host() builds a client when DATABRICKS_HOST is unset
        "metric_views": metric_views,
        "gold_tables": [{"name": t, "columns": c} for t, c in columns.items() if t.startswith("gold_")],
        "bronze_tables": sorted(t for t in columns if t.startswith("bronze_")),
        "columns": columns,
        "raw_feeds": _raw_feeds(),
        "genie_spaces": _genie_spaces(mv_names),
        "dashboards": _dashboards(mv_names),
        "latest_week": latest,
    }


RAW_ROOT = f"/Volumes/{CAT}/{CONTROL_SCHEMA}/raw_landing"


def _raw_feeds() -> list[dict]:
    """Landing folders under raw_landing: file count and the columns read_files infers (no tables yet)."""
    try:
        ws = aj._ws()
        folders = [e for e in ws.files.list_directory_contents(RAW_ROOT) if e.is_directory]
    except Exception:  # noqa: BLE001  (volume missing or not readable: no raw feeds)
        return []
    feeds = []
    for e in folders:
        path = e.path.rstrip("/") + "/"
        try:
            files = [f for f in ws.files.list_directory_contents(path.rstrip("/")) if not f.is_directory]
            rows = aj.sql(f"DESCRIBE QUERY SELECT * FROM read_files({aj.lit(path)}, format => 'csv', header => true)")
        except Exception:  # noqa: BLE001
            continue
        cols = [{"name": r["col_name"], "type": r["data_type"]} for r in rows
                if r.get("col_name") and not str(r["col_name"]).startswith("_rescued")]
        if cols:
            feeds.append({"name": e.name, "path": path, "files": len(files), "columns": cols})
    return feeds


def _show_create(fq: str) -> str:
    try:
        rows = aj.sql(f"SHOW CREATE TABLE {fq}")
    except Exception:  # noqa: BLE001
        return ""
    return str(next(iter(rows[0].values()))) if rows else ""


def _yaml_body(stmt: str) -> Optional[str]:
    if not stmt or stmt.count("$$") < 2:
        return None
    return stmt.split("$$", 1)[1].rsplit("$$", 1)[0]


def _mv_from_body(name: str, body: str) -> Optional[dict]:
    try:
        y = yaml.safe_load(body) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(y, dict):
        return None
    measures = [{"name": str(m["name"]), "expr": str(m.get("expr") or ""), "comment": str(m.get("comment") or "")}
                for m in (y.get("measures") or []) if isinstance(m, dict) and m.get("name")]
    dims = [str(d["name"]) for d in (y.get("dimensions") or []) if isinstance(d, dict) and d.get("name")]
    return {"name": name, "comment": str(y.get("comment") or ""), "measures": measures,
            "dimensions": dims, "source_tables": _fq_refs(body)}


def _genie_spaces(mv_names: set) -> list[dict]:
    ws = aj._ws()
    spaces, token = [], None
    for _ in range(20):
        q: dict[str, Any] = {"page_size": 100}
        if token:
            q["page_token"] = token
        r = ws.api_client.do("GET", "/api/2.0/genie/spaces", query=q) or {}
        spaces += r.get("spaces") or []
        token = r.get("next_page_token")
        if not token:
            break

    def detail(s: dict) -> Optional[dict]:
        try:
            g = ws.api_client.do("GET", f"/api/2.0/genie/spaces/{s['space_id']}",
                                 query={"include_serialized_space": "true"}) or {}
        except Exception:  # noqa: BLE001
            return None
        ss = _json_obj(g.get("serialized_space"))
        tables = [t.get("identifier") for t in ((ss.get("data_sources") or {}).get("tables") or [])
                  if isinstance(t, dict)]
        refs = sorted({t for t in tables if t in mv_names})
        if not refs:
            return None
        return {"id": s["space_id"], "title": g.get("title") or s.get("title") or "", "data_sources": refs}

    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        return [x for x in ex.map(detail, spaces) if x]


def _dashboards(mv_names: set) -> list[dict]:
    ws = aj._ws()
    dashes, token = [], None
    for _ in range(20):
        q: dict[str, Any] = {"page_size": 100}
        if token:
            q["page_token"] = token
        r = ws.api_client.do("GET", "/api/2.0/lakeview/dashboards", query=q) or {}
        dashes += r.get("dashboards") or []
        token = r.get("next_page_token")
        if not token:
            break
    dashes = [d for d in dashes if d.get("lifecycle_state", "ACTIVE") == "ACTIVE"]

    def detail(d: dict) -> Optional[dict]:
        try:
            full = ws.api_client.do("GET", f"/api/2.0/lakeview/dashboards/{d['dashboard_id']}") or {}
        except Exception:  # noqa: BLE001
            return None
        sd = _json_obj(full.get("serialized_dashboard"))
        parts = []
        for ds in sd.get("datasets") or []:
            if isinstance(ds, dict):
                ql = ds.get("queryLines") or []
                parts.append("".join(ql) if isinstance(ql, list) else str(ql))
        blob = "\n".join(parts).replace("`", "")
        refs = sorted(m for m in mv_names if m in blob)
        if not refs:
            return None
        return {"id": d["dashboard_id"], "title": full.get("display_name") or d.get("display_name") or "",
                "datasets_sql_refs": refs}

    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        return [x for x in ex.map(detail, dashes) if x]


def _match_block(cat: dict) -> str:
    out = ["METRIC VIEWS (full name; measures; dimensions):"]
    for m in cat["metric_views"]:
        out.append(f"- {m['name']}: " + "; ".join(x["name"] for x in m["measures"])
                   + " | dims: " + ", ".join(m["dimensions"]))
    out.append("GOLD TABLES: " + " | ".join(f"{g['name']}({', '.join(g['columns'])})" for g in cat["gold_tables"]))
    out.append("BRONZE TABLES: " + ", ".join(cat["bronze_tables"]))
    out.append("RAW FEEDS (files only, no table yet): " + " | ".join(
        f"{f['path']} ({f['files']} files; {', '.join(c['name'] for c in f['columns'])})"
        for f in cat.get("raw_feeds") or []))
    return "\n".join(out)


def catalog_prompt_block(cat: Optional[dict] = None) -> str:
    cat = cat or catalog()
    out = ["GOVERNED METRIC VIEWS (only these measures exist; query with MEASURE(`Name`)):"]
    for m in cat["metric_views"]:
        out.append(f"- {m['name']}: {_clip(m['comment'], 140)}")
        ms = [f"{x['name']} ({_clip(x['comment'], 70)})" if x["comment"] else x["name"] for x in m["measures"]]
        out.append("  measures: " + "; ".join(ms))
        out.append("  dimensions: " + ", ".join(m["dimensions"]))
    out.append("GOLD TABLES (raw material for a NEW metric view; cfo_workbench):")
    for g in cat["gold_tables"]:
        out.append(f"- {g['name']}: {', '.join(g['columns'])}")
    out.append("RAW BRONZE TABLES (upstream only): " + ", ".join(cat["bronze_tables"]))
    feeds = cat.get("raw_feeds") or []
    if feeds:
        out.append("RAW FEEDS (CSV files in the landing volume; not modelled yet, a pipeline builds the tables "
                   "from them; the data IS there):")
        for f in feeds:
            out.append(f"- {f['path']} ({f['files']} daily files): " + ", ".join(
                f"{c['name']} {c['type'].lower()}" for c in f["columns"]))
    if cat.get("latest_week"):
        out.append(f"Latest full week in the data: {cat['latest_week']}.")
    out.append("Anything not listed above is NOT in the data. Say so plainly; never assume it exists.")
    return "\n".join(out)


# ----------------------------------------------------------------------------- SQL guard
def _norm_sql(sql: str) -> str:
    """Strip string literals and backticks; used for keyword and table checks only."""
    return re.sub(r"'(?:[^'\\]|\\.)*'", "''", sql).replace("`", "")


def _read_table_ok(fq: str) -> bool:
    c, s_, t = (fq.split(".") + ["", "", ""])[:3]
    if c != CAT.lower():
        return False
    if s_ in {x.lower() for x in READ_SCHEMAS}:
        return True
    return s_ == CONTROL_SCHEMA.lower() and t.startswith(DRAFT_PREFIX.lower())


def _sql_problem(sql: Any) -> Optional[str]:
    if not isinstance(sql, str) or not sql.strip():
        return "empty SQL"
    from agent import sql_guard
    parsed = sql_guard.check(sql, _read_table_ok)
    if parsed:
        return parsed
    if len(sql) > 6000:
        return "SQL too long"
    body = _norm_sql(sql).strip()
    if body.endswith(";"):
        body = body[:-1].rstrip()
    if ";" in body:
        return "more than one statement"
    if "--" in body or "/*" in body:
        return "comments are not allowed"
    if not re.match(r"(?i)select\b", body):
        return "only a single SELECT is allowed"
    m = _RO_BLOCKED.search(body)
    if m:
        return f"keyword not allowed: {m.group(1).upper()}"
    refs = _TABLE_REF.findall(body)
    if not refs:
        return "no table in FROM/JOIN"
    for r in refs:
        if r.count(".") != 2:
            return f"table must be fully qualified: {r}"
    for c, s, t in _FQ_REF.findall(body):
        if c != CAT:
            return f"catalog not allowed: {c}"
        if s in READ_SCHEMAS:
            continue
        if s == CONTROL_SCHEMA and t.startswith(DRAFT_PREFIX):
            continue
        return f"schema not allowed: {s}.{t}"
    return None


def validate_sql_readonly(sql: str) -> bool:
    """True only for a single read-only SELECT over the allowed schemas (see _sql_problem)."""
    return _sql_problem(sql) is None


def _check_query(sql: str, sources: list[str], measure_union: list[str]) -> Optional[str]:
    """Read-only guard plus: FROM targets must be this design's sources, MEASURE names must exist."""
    p = _sql_problem(sql)
    if p:
        return p
    targets = {t for t in _TABLE_REF.findall(_norm_sql(sql).strip())}
    bad = sorted(t for t in targets if t not in set(sources))
    if bad:
        return f"reads {', '.join(bad)}, which is not a source of this design"
    known = {_key(m) for m in measure_union}
    for m in _MEASURE_REF.findall(sql):
        if _key(m) not in known:
            return f"unknown measure `{m}`"
    return None


def _date_literal_problem(sql: str) -> Optional[str]:
    """A date filter is allowed only against Week Start (the latest-week grain every view shares)."""
    for col in re.findall(r"`?([A-Za-z][A-Za-z ]*?)`?\s*=\s*'\d{4}-\d{2}-\d{2}'", sql):
        if _key(col) != "week start":
            return f"date filter on {col.strip()} is not allowed; date filters are only on Week Start"
    return None


def _ungrounded_dims(sql: str, question: str, dims: list[str]) -> list[str]:
    """Dimensions the SELECT groups by that the question never names (then the question is company-wide)."""
    m = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", _norm_sql(sql))
    if not m:
        return []
    clause = re.sub(r"MEASURE\s*\([^)]*\)", " ", m.group(1), flags=re.I)
    q_words = re.findall(r"[a-z0-9]+", question.lower())
    out = []
    for d in dims:
        if not re.search(r"(?<![A-Za-z0-9_])" + re.escape(d) + r"(?![A-Za-z0-9_])", clause, re.I):
            continue
        words = [w for w in re.findall(r"[a-z0-9]+", d.lower()) if len(w) > 2 and w not in {"date", "start", "name", "type"}]
        words = words or re.findall(r"[a-z0-9]+", d.lower())
        # stem so plurals match: category/categories, branch/branches, segment/segments
        stems = [w[:-1] if w.endswith(("y", "s")) and len(w) > 4 else w for w in words]
        if not any(qw.startswith(s_) for s_ in stems for qw in q_words):
            out.append(d)
    return out


def _check_bench(sql: str, question: str, sources: list[str], measures: list[str], dims: list[str]) -> Optional[str]:
    if re.search(r"\b(accounts?|customers?|clients?)\b", question, re.I) and not re.search(r"\bsegments?\b", question, re.I):
        return "per-customer questions get no benchmark (the answer depends on who asks; customer names stay out)"
    p = _check_query(sql, sources, measures) or _date_literal_problem(sql)
    if p:
        return p
    bad = _ungrounded_dims(sql, question, dims)
    if bad:
        return f"groups by {', '.join(bad)}, which the question does not name; a question with no segment, branch or category is company-wide"
    return None


def _pii_problem(sql: str) -> Optional[str]:
    if re.search(r"`(Customer|Customer Name|Customer Full Name)`|\bcustomer_name\b", sql, re.I):
        return "customer names are not on the shared dashboard"
    return None


def _exec_sql(sql: str, metric_view: Optional[dict]) -> str:
    """Rewrite the new view's real name to its validated draft so the query can run before the build."""
    if not metric_view or not metric_view.get("draft") or not metric_view.get("name"):
        return sql
    pat = r"\.".join(r"`?" + re.escape(p) + r"`?" for p in metric_view["name"].split("."))
    return re.sub(pat, metric_view["draft"], sql)


def _run_all(sqls: list[str]) -> list[tuple[Optional[list], Optional[str]]]:
    def one(s: str):
        try:
            return [{k: (_as_num(v) if _as_num(v) is not None else v) for k, v in r.items()}
                    for r in aj.sql(s)], None
        except Exception as e:  # noqa: BLE001
            return None, _short_err(e)
    if not sqls:
        return []
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        return list(ex.map(one, sqls))


# ----------------------------------------------------------------------------- match_needs
def _need_refs(need: dict) -> list[dict]:
    refs: list[dict] = []
    measures = [m for m in (need.get("measures") or []) if isinstance(m, dict) and str(m.get("name") or "").strip()]
    for i, m in enumerate(measures, 1):
        name = str(m["name"]).strip()
        definition = str(m.get("definition") or "").strip()
        refs.append({"ref": f"M{i}", "text": f"Measure {name}" + (f": {definition}" if definition else ""),
                     "measure": name,
                     "dimensions": [str(d).strip() for d in (m.get("dimensions") or []) if str(d).strip()]})
    qs = [str(q).strip() for q in (need.get("questions") or []) if str(q).strip()]
    for i, q in enumerate(qs, 1):
        refs.append({"ref": f"Q{i}", "text": q, "measure": None, "dimensions": []})
    return refs


def _numbers(text: str) -> set[int]:
    """Qualifier numbers in an ask (30 days, top 200). A comparison value ("42-day target") is not one."""
    t = re.sub(r"\d{1,4}\s*-?\s*(?:days?|%|pct)?\s*(?:target|goal|threshold|sla)\b", " ", str(text or ""), flags=re.I)
    return {int(n) for n in re.findall(r"(?<![\d.])\d{1,4}(?![\d.])", t)}


def _measure_text(view: dict, name: str) -> str:
    for m in view["measures"]:
        if m["name"] == name:
            return m["name"] + " " + m["comment"]
    return name


def _resolve_asset(asset: Any, mvs: dict) -> Optional[str]:
    if not asset:
        return None
    a = str(asset).strip().strip("`")
    if a in mvs:
        return a
    hits = [n for n in mvs if _short(n) == _short(a)]
    return hits[0] if len(hits) == 1 else None


def _resolve_name(want: Any, names: list[str]) -> Optional[str]:
    if not want:
        return None
    w = _key(want)
    for n in names:
        if _key(n) == w:
            return n
    return None


_MATCH_SYSTEM = """You map an analyst's data need onto the governed assets that already exist in this workspace.
You see only the catalog below. Never invent a view, measure, dimension or column.

{block}

For each numbered item (ref) choose one decision:
- REUSE: a metric view above already has the measure the item asks for. asset = full view name, measure = exact measure name.
- EXTEND: a metric view covers part of it but lacks the measure. asset = full view name, measure = the measure the item needs. A DRAFT view may only be EXTEND, never REUSE.
- GAP: no view has it, but real columns of a GOLD or BRONZE table above already record exactly what the item measures. columns = those exact column names; holds = what they record, in plain words.
- RAW: no governed table has it, but a RAW FEED listed above has real columns that record exactly what the item measures. asset = the feed path, columns = those exact feed column names, holds = what they record. A pipeline will build the table from the feed.
- NO_DATA: no column records what the item measures. missing = what is missing, in plain words. essential = true if the item cannot be answered without it.

Rules:
- Prefer a governed table over a raw feed. Use RAW only when no table above has the columns.
- Every REUSE or EXTEND names a measure.
- A column that only sounds related is not the same concept. An invoice date, due date, payment date or stock receipt date is NOT a delivery or promise date. A freight cost is NOT an on-time measure. Answer NO_DATA for those.
- A target or threshold the analyst states in the ask (for example a 42-day target) is an input, not data. Never mark it missing; map the measure it is compared with.
- why: at most 12 words.

Reply with JSON only:
{{"items": [{{"ref": "M1", "decision": "REUSE|EXTEND|GAP|NO_DATA", "asset": null, "measure": null,
             "columns": [], "holds": "", "missing": "", "essential": true, "why": ""}}]}}"""


def _raw_item(text: str, want: str, cols: list[str], feeds: list[dict]) -> Optional[dict]:
    """A RAW item when one raw feed (the named one, or any) has every column. None otherwise."""
    if not cols or not feeds:
        return None
    want = want.strip().rstrip("/")
    for f in feeds:
        if want and want not in (f["path"].rstrip("/"), f["name"]):
            continue
        real = {c["name"].lower(): c["name"] for c in f["columns"]}
        if all(c.lower() in real for c in cols):
            return {"need": text, "decision": "RAW", "asset": f["path"], "measure": None,
                    "why": "only in raw files; a pipeline will build it",
                    "source_columns": [real[c.lower()] for c in cols], "feed": f["name"]}
    return None


def match_needs(need: dict, cat: Optional[dict] = None) -> dict:
    """Map each need item to REUSE / EXTEND / GAP / RAW / NO_DATA. Every asset, measure and column is verified."""
    cat = cat or catalog()
    refs = _need_refs(need)
    mvs = {m["name"]: m for m in cat["metric_views"]}
    feeds = cat.get("raw_feeds") or []
    known_cols = {c.lower() for cols in cat["columns"].values() for c in cols}
    for m in mvs.values():
        known_cols |= {_key(x["name"]) for x in m["measures"]} | {_key(d) for d in m["dimensions"]}

    raw_by_ref: dict[str, dict] = {}
    if refs:
        ask = "NEED\nPurpose: {}\nAudience: {}\nDecisions: {}\nOutputs: {}\n\nITEMS\n{}".format(
            need.get("purpose") or "", need.get("audience") or "", "; ".join(need.get("decisions") or []),
            ", ".join(need.get("outputs") or []),
            "\n".join(f"[{r['ref']}] {r['text']}" + (f" (dimensions: {', '.join(r['dimensions'])})"
                                                      if r["dimensions"] else "") for r in refs))
        reply = _llm_json(_MATCH_SYSTEM.format(block=_match_block(cat)), ask)
        items_in = reply.get("items") if isinstance(reply, dict) else reply
        for i in items_in or []:
            if isinstance(i, dict):
                raw_by_ref[str(i.get("ref") or "").strip().upper()] = i

    items: list[dict] = []
    no_data: list[dict] = []
    for ref in refs:
        raw = raw_by_ref.get(ref["ref"], {})
        dec = str(raw.get("decision") or "").strip().upper().replace(" ", "_")
        if dec not in _DECISIONS:
            dec = "GAP"
        if dec == "NO_DATA":
            missing = str(raw.get("missing") or raw.get("why") or "not in the catalog").strip()
            essential = raw.get("essential") is not False
            items.append({"need": ref["text"], "decision": "NO_DATA", "asset": None, "measure": None,
                          "why": missing})
            no_data.append({"need": ref["text"], "missing": missing, "essential": essential})
            continue
        cols_in = [str(c).strip().strip("`").split(".")[-1].strip("`") for c in (raw.get("columns") or [])]
        cols_in = [c for c in cols_in if c]
        real = {c.lower() for cc in cat["columns"].values() for c in cc}
        if dec == "RAW":
            # A RAW item needs the feed's exact columns. If governed tables already carry them, it is a GAP.
            if cols_in and all(c.lower() in real for c in cols_in):
                dec = "GAP"
            else:
                raw_item = _raw_item(ref["text"], str(raw.get("asset") or ""), cols_in, feeds)
                if raw_item:
                    items.append(raw_item)
                    continue
                dec = "GAP"  # not a feed either: the GAP check below answers NO_DATA
        asset = _resolve_asset(raw.get("asset"), mvs) if dec in ("REUSE", "EXTEND") else None
        view = mvs.get(asset) if asset else None
        if view is None:
            # GAP needs real source columns whose stated meaning is the measure; anything else is NO_DATA.
            absent = [c for c in cols_in if c.lower() not in real]
            if cols_in and not absent:
                items.append({"need": ref["text"], "decision": "GAP", "asset": None, "measure": None,
                              "why": str(raw.get("holds") or raw.get("why") or "")[:200],
                              "source_columns": cols_in})
                continue
            raw_item = _raw_item(ref["text"], "", cols_in, feeds)
            if raw_item:
                items.append(raw_item)
                continue
            missing = (f"no column named {', '.join(absent)} in the catalog" if absent
                       else str(raw.get("missing") or "no column in the catalog records this"))
            items.append({"need": ref["text"], "decision": "NO_DATA", "asset": None, "measure": None,
                          "why": missing})
            no_data.append({"need": ref["text"], "missing": missing, "essential": True})
            continue
        cand_measure = raw.get("measure") or ref["measure"]
        measure = _resolve_name(cand_measure, [x["name"] for x in view["measures"]]) if cand_measure else None
        if measure and ref["measure"] and _key(measure) != _key(ref["measure"]):
            # A measure item must be answered by the measure it names; a different measure is partial cover.
            measure_is_named = False
        else:
            measure_is_named = True
        if measure:
            # Numeric qualifiers in the ask (30 days, top 200, 180 days) must appear in the measure's own
            # name or comment. If not, switch only to a UNIQUE catalog measure that carries all of them.
            qual = _numbers(ref["text"])
            if qual and not qual <= _numbers(_measure_text(view, measure)):
                alts = [(v["name"], m["name"]) for v in cat["metric_views"] for m in v["measures"]
                        if qual <= _numbers(m["name"] + " " + m["comment"])]
                if len(alts) == 1:
                    asset, view = alts[0][0], mvs[alts[0][0]]
                    measure = alts[0][1]
                    raw = {**raw, "why": f"Qualifier match: {measure} carries {', '.join(map(str, sorted(qual)))}."}
        missing_dims = [d for d in ref["dimensions"] if _resolve_name(d, view["dimensions"]) is None]
        is_draft = "draft" in (view["name"] + " " + view["comment"]).lower()
        if measure and measure_is_named and not missing_dims and not is_draft:
            # The verified facts decide: measure, dimensions and qualifiers all check out. Drafts are never REUSE.
            out_dec, why = "REUSE", str(raw.get("why") or "")
        else:
            out_dec = "EXTEND"
            gaps = []
            if not measure or not measure_is_named:
                gaps.append("the measure it names is not in this view")
            if is_draft:
                gaps.append("it is a draft, not certified")
            if missing_dims:
                gaps.append(f"no dimension {', '.join(missing_dims)}")
            why = f"{_short(asset)} covers part of this; " + (" and ".join(gaps) or "needs a new measure")
        items.append({"need": ref["text"], "decision": out_dec, "asset": asset, "measure": measure, "why": why})

    n_reuse = sum(1 for i in items if i["decision"] == "REUSE")
    n_extend = sum(1 for i in items if i["decision"] == "EXTEND")
    coverage = int(round(100 * (n_reuse + 0.5 * n_extend) / len(items))) if items else 0
    buildable = (any(i["decision"] in ("REUSE", "EXTEND", "GAP", "RAW") for i in items)
                 and not any(n["essential"] for n in no_data))
    return {"coverage_pct": coverage, "items": items, "related_assets": _related(items, cat),
            "buildable": buildable, "no_data": [{"need": n["need"], "missing": n["missing"]} for n in no_data]}


def _related(items: list[dict], cat: dict) -> list[dict]:
    # RAW items point at a file path, not a metric view or a dashboard, so they are not related assets.
    used = {i["asset"]: i["decision"] for i in items if i["asset"] and i["decision"] != "RAW"}
    host = (cat.get("host") or "").rstrip("/")
    out: list[dict] = []
    for name, dec in used.items():
        c, sch, t = name.split(".")
        out.append({"kind": "metric_view", "id": name, "title": t,
                    "open_url": f"{host}/explore/data/{c}/{sch}/{t}", "decision": dec})
    for kind, ents, key, path in (("genie", cat["genie_spaces"], "data_sources", "genie/rooms/{}"),
                                  ("dashboard", cat["dashboards"], "datasets_sql_refs", "dashboardsv3/{}")):
        for e in ents:
            hit = [used[s] for s in e[key] if s in used]
            if hit:
                out.append({"kind": kind, "id": e["id"], "title": e["title"],
                            "open_url": f"{host}/{path.format(e['id'])}",
                            "decision": "EXTEND" if "EXTEND" in hit else "REUSE"})
    return out


# ----------------------------------------------------------------------------- shared views
def _sources(reuse: dict, metric_view: Optional[dict]) -> list[str]:
    out: list[str] = []
    for i in reuse.get("items", []):
        if i.get("decision") == "REUSE" and i.get("asset") and i["asset"] not in out:
            out.append(i["asset"])
    if metric_view and metric_view.get("validated") and metric_view.get("name") not in out:
        out.append(metric_view["name"])
    return out


def _views(sources: list[str], cat: dict, metric_view: Optional[dict]) -> dict[str, dict]:
    by = {m["name"]: m for m in cat["metric_views"]}
    out: dict[str, dict] = {}
    for s in sources:
        if metric_view and s == metric_view.get("name") and metric_view.get("yaml"):
            v = _mv_from_body(s, metric_view["yaml"])
            if v:
                out[s] = v
        elif s in by:
            out[s] = by[s]
    return out


def _measure_union(views: dict) -> list[str]:
    seen: list[str] = []
    for v in views.values():
        for m in v["measures"]:
            if m["name"] not in seen:
                seen.append(m["name"])
    return seen


def _dim_union(views: dict) -> list[str]:
    seen: list[str] = []
    for v in views.values():
        for d in v["dimensions"]:
            if d not in seen:
                seen.append(d)
    return seen


# ----------------------------------------------------------------------------- author_metric_view
_AUTHOR_SYSTEM = """You write a Unity Catalog METRIC VIEW in YAML 1.1 over the gold tables you are given.
Reply with exactly one ```yaml block and nothing else.
Hard rules, each one a failure we have already hit on this workspace:
1. Top level: version: 1.1, source: (a SQL SELECT in a | block over fully qualified tables from the list you are given), joins: (only when the source has no window functions), dimensions:, measures:.
2. If the source uses window functions (OVER), put every join INSIDE the source SQL and omit joins:.
3. Never name a join branch, rank or customer. Quote the key "on" in every join.
4. Every dimension and every measure has name, expr and a one-line plain-English comment.
5. Measures are aggregates (SUM, COUNT) or MEASURE(`Other Measure`) composition of measures in this same view. Ratios use NULLIF(denominator, 0). Names with spaces go in backticks.
6. The first dimension is the time grain (Week Start) when the data has one.
7. Use only the tables and columns listed. Do not invent columns or tables.
8. A planned table (listed as PLANNED) is built by a pipeline that has not run yet: write the view over its listed columns only; the view is validated when it is built."""


def _extract_yaml(raw: str) -> str:
    m = re.search(r"```(?:yaml|yml)?[ \t]*\n(.*?)```", raw, re.S)
    body = m.group(1) if m else raw
    if body.count("$$") >= 2:
        body = body.split("$$", 1)[1].rsplit("$$", 1)[0]
    return body.replace("$$", "").strip("\n").rstrip()


def _check_mv_yaml(body: str, cat: dict, planned: Optional[dict] = None) -> tuple[Optional[str], Optional[dict]]:
    """planned: {fq name: columns} of gold tables a pipeline will build (RAW items); allowed as sources too."""
    if not body.strip():
        return "the reply had no YAML", None
    if ";" in body or "$$" in body:
        return "YAML must not contain semicolons or $$", None
    try:
        y = yaml.safe_load(body)
    except yaml.YAMLError as e:
        return f"YAML does not parse: {_clip(e, 200)}", None
    if not isinstance(y, dict):
        return "top level must be a mapping", None
    if str(y.get("version")) != "1.1":
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
    for j in y.get("joins") or []:
        if not isinstance(j, dict):
            return "each join must be a mapping", None
        if True in j:
            return 'quote the join key "on" (YAML reads an unquoted on as true)', None
        if str(j.get("name") or "").strip().lower() in REVIEW_JOIN_ALIASES:
            return f"join name '{j.get('name')}' breaks on Azure metric views; rename it", None
    if (y.get("joins") or []) and re.search(r"\bOVER\s*\(", source, re.I):
        return "the source has window functions: move the joins into the source SQL and remove joins:", None
    for ref in re.findall(r"MEASURE\(\s*`([^`]+)`\s*\)", body):
        if ref not in names_by_kind["measure"]:
            return f"MEASURE(`{ref}`) refers to a measure that is not in this view", None
    allowed = {f"{CAT}.{SRC_SCHEMA}.{t}".lower() for t in cat["columns"]}
    allowed |= {k.lower() for k in (planned or {})}
    for c, s, t in re.findall(rf"\b({re.escape(CAT)})\.(\w+)\.(\w+)\b", body):
        if f"{c}.{s}.{t}".lower() not in allowed:
            return f"{s}.{t} is not an allowed source (use the tables listed)", None
    from agent import sql_guard
    allowed_top = {"version", "source", "joins", "dimensions", "measures", "comment", "filter"}
    extra = sorted(str(k) for k in y if k not in allowed_top)
    if extra:
        return f"unsupported YAML keys: {', '.join(extra)}", None
    src_ok = lambda fq: fq.lower() in allowed
    why = sql_guard.check(source, src_ok)
    if why:
        return f"source SQL refused: {why}", None
    guard_ok = lambda fq: fq == "guard_catalog.guard_schema.guard_table"
    for j in y.get("joins") or []:
        if set(map(str, j)) - {"name", "source", "on"}:
            return "joins may only have name, source and on", None
        jsrc = str(j.get("source") or "").strip()
        if not src_ok(jsrc):
            return f"join source not allowed: {jsrc}", None
        on = str(j.get("on") or "")
        why = sql_guard.check(f"SELECT 1 FROM guard_catalog.guard_schema.guard_table WHERE {on}", guard_ok)
        if why or not on.strip():
            return f"join '{j.get('name')}' condition refused: {why or 'empty'}", None
    if y.get("filter") is not None:
        why = sql_guard.check(f"SELECT 1 FROM guard_catalog.guard_schema.guard_table WHERE {y['filter']}", guard_ok)
        if why:
            return f"filter refused: {why}", None
    for kind, items in (("dimension", dims), ("measure", meas)):
        for it in items:
            if set(map(str, it)) - {"name", "expr", "comment", "display_name", "format", "synonyms"}:
                return f"{kind} '{it.get('name')}' has unsupported keys", None
            why = sql_guard.check(f"SELECT {it['expr']} FROM guard_catalog.guard_schema.guard_table", guard_ok)
            if why:
                return f"{kind} '{it['name']}' expr refused: {why}", None
    return None, y


def _create_stmt(fq: str, comment: str, body: str) -> str:
    return (f"CREATE OR REPLACE VIEW {fq} WITH METRICS LANGUAGE YAML COMMENT {aj.lit(comment)} "
            f"AS $$\n{body}\n$$")


def _validate_draft(draft_fq: str, parsed: dict) -> tuple[list[dict], list[str], list[str]]:
    dims = [d["name"] for d in parsed["dimensions"]]
    meas = [m["name"] for m in parsed["measures"]]
    cols = ", ".join(f"MEASURE({_bt(m)}) AS {_bt(m)}" for m in meas)
    rows = aj.sql(f"SELECT {_bt(dims[0])}, {cols} FROM {draft_fq} GROUP BY ALL ORDER BY 1 DESC LIMIT 12")
    sample = [{k: (_as_num(v) if _as_num(v) is not None else v) for k, v in r.items()} for r in rows]
    return sample, meas, dims


def _drop_view(fq: str) -> None:
    for stmt in (f"DROP VIEW IF EXISTS {fq}", f"DROP TABLE IF EXISTS {fq}"):
        try:
            aj.sql(stmt)
            return
        except Exception:  # noqa: BLE001
            continue


def author_metric_view(need: dict, reuse: dict, cat: Optional[dict] = None,
                       pipeline: Optional[dict] = None) -> Optional[dict]:
    """Author a metric view for GAP/EXTEND/RAW items. None when everything is reused.

    RAW items read a gold table that a pipeline (pipeline_planner.author_pipeline) will build in studio_dev.
    That table does not exist yet, so the view is checked against the PLANNED columns and comes back with
    validated="pending"; it is validated for real when the build runs. Without a pipeline, validated=False.
    """
    gaps = [i for i in reuse.get("items", []) if i.get("decision") in ("GAP", "EXTEND", "RAW")]
    if not gaps:
        return None
    cat = cat or catalog()
    slug = _slug(need)
    name = _fq(DEV_SCHEMA, f"mv_{slug}")
    purpose = _clip(need.get("purpose") or "analysis", 140)
    comment = _clean_text(f"{aj.COMPANY} metric view: {purpose}", 300) or f"{aj.COMPANY} metric view"
    pending = any(i["decision"] == "RAW" for i in gaps)

    planned: dict[str, list[str]] = {}
    if pending:
        gold_names = (pipeline or {}).get("tables", {}).get("gold") or []
        pcols = (pipeline or {}).get("planned_columns") or {}
        if not gold_names or gold_names[0] not in pcols:
            return {"name": name, "yaml": "", "sql_template": "", "measures": [], "dimensions": [],
                    "validated": False, "sample": [], "draft": None, "attempts": 0, "history": [],
                    "error": "the pipeline that builds the raw data is not planned yet"}
        planned[_fq(DEV_SCHEMA, gold_names[0])] = pcols[gold_names[0]]

    # Definitions to stay consistent with: measures of the views this need reuses.
    by = {m["name"]: m for m in cat["metric_views"]}
    defs = []
    for i in reuse.get("items", []):
        v = by.get(i.get("asset")) if i.get("asset") else None
        if v:
            defs.append(f"{v['name']}:")
            defs += [f"  {m['name']} = {m['expr']}" for m in v["measures"][:14]]
    gold = "\n".join(f"{g['name']}: {', '.join(g['columns'])}" for g in cat["gold_tables"])
    planned_lines = "".join(f"\nPLANNED {k} (built by the pipeline; not built yet): {', '.join(v)}"
                            for k, v in planned.items())
    needs_lines = "\n".join(f"- {i['need']} [{i['decision']}] {i['why']}" for i in gaps)
    user0 = (f"ANALYST NEED\nPurpose: {need.get('purpose') or ''}\nAudience: {need.get('audience') or ''}\n"
             f"Decisions: {'; '.join(need.get('decisions') or [])}\nView to write: {name}\n\n"
             f"WHAT THE VIEW MUST COVER (gaps from the reuse check):\n{needs_lines}\n"
             "If any line mentions plan, margin or leakage, also carry Gross Margin Pct, Plan Gross Margin Pct "
             "and GM vs Plan Bps, so leakage is read against plan in the same view.\n\n"
             "EXISTING GOVERNED DEFINITIONS (keep the same logic; do not duplicate these views):\n"
             + ("\n".join(defs) or "(none)") + "\n\n"
             f"GOLD TABLES (cfo_workbench, the allowed sources):\n{gold}{planned_lines}\n\n"
             f"EXAMPLE OF A VALID VIEW ON THIS DATA (copy its structure):\n```yaml\n{aj.metric_view_yaml()}\n```\n"
             "Reply with one ```yaml block: the full YAML 1.1 body (version, source, joins, dimensions, measures).")

    body, last_error, attempts, history = "", None, 0, []
    for attempt in range(3):
        attempts = attempt + 1
        user = user0 + (f"\n\nYour previous attempt failed: {last_error}\nFix exactly that and reply with the full YAML."
                        if last_error else "")
        try:
            body = _extract_yaml(_llm_text(_AUTHOR_SYSTEM, user))
        except Exception as e:  # noqa: BLE001
            last_error = f"LLM error: {_short_err(e)}"
            continue
        problem, parsed = _check_mv_yaml(body, cat, planned)
        if problem or parsed is None:
            last_error = problem
            history.append(problem)
            continue
        if pending:
            return {"name": name, "yaml": body,
                    "sql_template": _create_stmt(f"{CAT}.{{schema}}.mv_{slug}", comment, body),
                    "measures": [m["name"] for m in parsed["measures"]],
                    "dimensions": [d["name"] for d in parsed["dimensions"]], "validated": "pending",
                    "sample": [], "draft": None, "error": None, "attempts": attempts, "history": history}
        draft = _fq(CONTROL_SCHEMA, DRAFT_PREFIX + hashlib.sha1(body.encode()).hexdigest()[:8])
        try:
            aj.sql(_create_stmt(draft, comment, body))
        except Exception as e:  # noqa: BLE001
            last_error = f"CREATE VIEW failed: {_short_err(e)}"
            history.append(last_error)
            continue
        try:
            sample, meas, dims = _validate_draft(draft, parsed)
        except Exception as e:  # noqa: BLE001
            _drop_view(draft)
            last_error = f"a measure failed when grouped by {parsed['dimensions'][0]['name']}: {_short_err(e)}"
            history.append(last_error)
            continue
        return {"name": name, "yaml": body, "sql_template": _create_stmt(f"{CAT}.{{schema}}.mv_{slug}", comment, body),
                "measures": meas, "dimensions": dims, "validated": True, "sample": sample,
                "draft": draft, "error": None, "attempts": attempts, "history": history}
    return {"name": name, "yaml": body,
            "sql_template": _create_stmt(f"{CAT}.{{schema}}.mv_{slug}", comment, body or "<none>"),
            "measures": [], "dimensions": [], "validated": False, "sample": [], "draft": None,
            "error": last_error, "attempts": attempts, "history": history}


# ----------------------------------------------------------------------------- select_dq
_LINEAGE: dict[str, Any] = {"at": 0.0, "edges": None}


def _lineage_edges() -> dict[str, set]:
    if _LINEAGE["edges"] is not None and time.time() - _LINEAGE["at"] < LINEAGE_TTL_S:
        return _LINEAGE["edges"]
    edges: dict[str, set] = {k: set(v) for k, v in GOLD_UPSTREAM_STATIC.items()}
    try:
        rows = aj.sql(
            f"SELECT DISTINCT source_table_full_name AS s, target_table_full_name AS t "
            f"FROM system.access.table_lineage "
            f"WHERE target_table_full_name LIKE '{CAT}.{SRC_SCHEMA}.%' "
            f"AND source_table_full_name LIKE '{CAT}.{SRC_SCHEMA}.%' "
            f"AND event_date >= current_date() - INTERVAL {LINEAGE_DAYS} DAYS")
        for r in rows:
            edges.setdefault(_short(r["t"]), set()).add(_short(r["s"]))
    except Exception:  # noqa: BLE001
        pass  # lineage is optional; the static map still applies
    _LINEAGE.update(at=time.time(), edges=edges)
    return edges


def _upstream(base: set[str]) -> set[str]:
    edges = _lineage_edges()
    seen, stack = set(base), list(base)
    while stack:
        for s in edges.get(stack.pop(), ()):
            if s not in seen:
                seen.add(s)
                stack.append(s)
    return seen


_DQ_SYSTEM = """You propose 2 or 3 data-quality rules that protect the metric views described below.
Use ONLY the tables and columns listed. Each constraint_sql is a SQL boolean that must be TRUE for a good row:
simple column comparisons, AND/OR, IS NULL / IS NOT NULL, BETWEEN, COALESCE, ABS, LENGTH, TRIM, LOWER, UPPER.
No subqueries, no joins, no function calls outside that list.
severity is "drop" only when a bad row would make a headline number wrong, otherwise "warn".
Reply with JSON only: {"rules": [{"table_name": "...", "rule_name": "...", "constraint_sql": "...", "severity": "drop|warn", "why": "..."}]}"""


def select_dq(reuse: dict, metric_view: Optional[dict], cat: Optional[dict] = None,
              pipeline: Optional[dict] = None) -> dict:
    """Existing repo rules upstream of the data sources, plus up to 3 validated proposed rules.

    pipeline (RAW products): its silver expectations are added as origin "pipeline" rules; the pipeline
    validated them already (see pipeline_planner)."""
    from agent.genie_engine import DQ_TABLES
    cat = cat or catalog()
    sources = _sources(reuse, metric_view)
    views = _views(sources, cat, metric_view)
    base = {_short(t) for v in views.values() for t in v["source_tables"]}
    upstream = _upstream(base)
    tables = sorted(t for t in upstream if t in cat["columns"])

    rules: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for r in aj.existing_rules():
        if r["table_name"] not in upstream:
            continue
        seen.add((r["table_name"], _key(r["constraint_sql"])))
        rules.append({"table_name": r["table_name"], "rule_name": r["rule_name"],
                      "constraint_sql": r["constraint_sql"],
                      "severity": r["severity"] if r.get("severity") in ("drop", "warn") else "warn",
                      "origin": "existing", "enabled": True, "disabled_reason": "",
                      "why": str(r.get("why") or "")})
    rejected: list[dict] = []

    checkable = [t for t in tables if t in DQ_TABLES]
    candidates: list[dict] = []
    try:
        if checkable and views:
            col_lines = "\n".join(f"{t}: {', '.join(cat['columns'][t])}" for t in checkable)
            view_lines = "\n".join(f"{v['name']} measures: {', '.join(m['name'] for m in v['measures'])}"
                                   for v in views.values())
            reply = _llm_json(_DQ_SYSTEM, f"TABLES AND COLUMNS:\n{col_lines}\n\nMETRIC VIEWS:\n{view_lines}")
            raw = reply.get("rules") if isinstance(reply, dict) else reply
            candidates = [c for c in (raw or []) if isinstance(c, dict)]
    except Exception as e:  # noqa: BLE001
        rejected.append({"text": "proposed rules", "reason": f"planner could not propose rules: {_short_err(e)}"})

    fallback = [{"table_name": p["table_name"], "rule_name": p["rule_name"], "constraint_sql": p["constraint_sql"],
                 "severity": p["severity"], "why": p["why"]} for p in aj.PROPOSED_RULES]

    def verify(batch: list[dict]) -> list[tuple[dict, dict]]:
        work = []
        for c in batch:
            t = str(c.get("table_name") or "").strip()
            sql = str(c.get("constraint_sql") or "").strip()
            if t not in tables or (t, _key(sql)) in seen:
                if t not in tables:
                    rejected.append({"text": f"{t}: {c.get('rule_name')}", "reason": "table is not upstream of these views"})
                continue
            work.append((c, t, sql))
        with cf.ThreadPoolExecutor(max_workers=6) as ex:
            res = list(ex.map(lambda w: aj.check_constraint(w[1], w[2]), work))
        out = []
        for (c, t, sql), r in zip(work, res):
            if r.get("ok") and int(r.get("violations_now") or 0) == 0:
                seen.add((t, _key(sql)))
                out.append((c, {"ok": True, "violations_now": 0, "table": t, "sql": sql}))
            elif r.get("ok"):
                n = int(r.get("violations_now") or 0)
                rejected.append({"text": f"{t}: {c.get('rule_name')} ({sql})",
                                 "reason": f"fails on current data ({n} rows): the rule is wrong for this data "
                                           f"or the data has a real issue; review before adopting"})
            else:
                rejected.append({"text": f"{t}: {c.get('rule_name')} ({sql})", "reason": r.get("reason", "rejected")})
        return out

    accepted: list[dict] = []
    for c, verdict in verify(candidates)[:3]:
        accepted.append({"table_name": verdict["table"], "rule_name": str(c.get("rule_name") or "proposed rule")[:80],
                         "constraint_sql": verdict["sql"],
                         "severity": c.get("severity") if c.get("severity") in ("drop", "warn") else "warn",
                         "origin": "proposed", "enabled": True, "disabled_reason": "",
                         "why": _clip(c.get("why") or "", 200), "violations_now": verdict["violations_now"]})
    if not accepted and views:
        for c, verdict in verify(fallback):
            accepted.append({**c, "table_name": verdict["table"], "constraint_sql": verdict["sql"],
                             "origin": "proposed", "enabled": True, "disabled_reason": "",
                             "violations_now": verdict["violations_now"]})
    rules += accepted
    if pipeline:
        for e in pipeline.get("expectations") or []:
            rules.append({"table_name": e["table"], "rule_name": e["name"], "constraint_sql": e["constraint_sql"],
                          "severity": "drop" if e.get("action") == "drop" else "warn", "origin": "pipeline",
                          "enabled": True, "disabled_reason": "", "violations_now": e.get("feed_rows_failing"),
                          "why": "expectation in the pipeline (validated by the pipeline)"})
        tables = sorted(set(tables) | {e["table"] for e in pipeline.get("expectations") or []})
    return {"tables": tables, "rules": rules, "rejected": rejected, "review_id": "",
            "counts": {"existing": sum(r["origin"] == "existing" for r in rules),
                       "proposed": sum(r["origin"] == "proposed" for r in rules),
                       "pipeline": sum(r["origin"] == "pipeline" for r in rules),
                       "rejected": len(rejected)}}


# ----------------------------------------------------------------------------- design
_AGENT_SYSTEM = """You design a Genie Agent (a natural-language analyst) for a finance team. It answers ONLY from the governed metric views you are given. Reply with JSON only:
{"title": "short plain title", "description": "one sentence",
 "instructions": "4-8 short sentences: what it answers; which measure defines each concept; use the latest full week when asked for latest or this week; company-wide unless a segment is asked; say so when a question is outside these views",
 "benchmarks": [{"ref": "Q1", "answerable": true, "sql": "SELECT ..."}]}
Rules:
- instructions never state a number. The only date allowed is the latest full week given.
- one benchmark per question ref. sql is a single SELECT: SELECT <dimensions>, MEASURE(`<measure>`) ... FROM <full view name> GROUP BY ALL. Add a date filter only when the view has a dimension named Week Start and the question asks about the latest week: WHERE `Week Start` = '<latest week>'. Otherwise no date filter. Use only the listed views, measures and dimensions. Never recompute a measure in SQL.
- A question that names a target or threshold is answerable: answer with the measure; the target is the analyst's input.
- A question that names no segment, branch or category is company-wide: SELECT MEASURE(...) with no grouping dimension.
- answerable=false (and no sql) only when no listed measure can answer the question."""

_DASH_SYSTEM = """You design a Databricks AI/BI dashboard that answers the analyst's questions. Use ONLY the governed metric views you are given. Reply with JSON only:
{"title": "short title", "pages": [{"name": "page name", "widgets": [{"title": "...", "type": "counter|bar|line|table", "sql": "SELECT ..."}]}], "filters": ["dimension names"]}
Rules: at most 2 pages and 6 widgets per page. Each widget answers one question. Add a date filter only when the view has a dimension named Week Start and the question asks about the latest week; otherwise no date filter. sql is a single SELECT: SELECT ... FROM <full view name> ... GROUP BY ALL, measures as MEASURE(`name`). A counter is one row with one measure and no dimension. A trend uses the time dimension. Only listed names. No customer names and no customer-level rows. filters: up to 3 dimension names the analyst would filter by."""


def _design_user(need: dict, views: dict, qrefs: list[dict], latest: Optional[str]) -> str:
    qs = "\n".join(f"{r['ref']}: {r['text']}" for r in qrefs) or "(none)"
    vs = "\n".join(f"{v['name']}\n  measures: {', '.join(m['name'] for m in v['measures'])}\n"
                   f"  dimensions: {', '.join(v['dimensions'])}" for v in views.values())
    return (f"ANALYST NEED\nPurpose: {need.get('purpose') or ''}\nAudience: {need.get('audience') or ''}\n"
            f"Decisions: {'; '.join(need.get('decisions') or [])}\nCadence: {need.get('cadence') or ''}\n"
            f"Latest full week: {latest or 'unknown'}\n\nQUESTIONS\n{qs}\n\nDATA YOU MAY USE\n{vs}")


def _allowed_blob(need: dict, views: dict, latest: Optional[str]) -> str:
    parts = [json.dumps(need, default=str), latest or ""]
    for v in views.values():
        parts += [v["name"], v["comment"]] + [m["name"] + " " + m["comment"] for m in v["measures"]] + v["dimensions"]
    return " ".join(parts)


def _clean_instructions(text: Any, allowed: str, latest: Optional[str], out: dict) -> str:
    t = _clean_text(text, 4000)
    if not t:
        return ""
    kept = []
    for sentence in re.split(r"(?<=[.!?])\s+", t):
        bad = [n for n in _NUM_REF.findall(sentence) if (len(n) > 1 or "." in n) and n not in allowed]
        if bad:
            out["warnings"].append(f"dropped an instruction sentence with unverified number(s) {bad}")
            continue
        kept.append(sentence)
    res = " ".join(kept)
    if latest and re.search(r"latest|this week", res, re.I) and latest not in res:
        res += f" The latest full week is {latest}."
    return res


def _format_expected(rows: list[dict], measures: list[str]) -> str:
    """Expected text computed from the rows the query returned. Never typed by the LLM."""
    def measure_for(col: str) -> Optional[str]:
        hits = [m for m in measures if _key(m) in _key(col)]
        return max(hits, key=len) if hits else None

    def money(v: float) -> str:
        sign = "-" if v < 0 else ""
        a = abs(v)
        if a >= 1e6:
            return f"{sign}${a / 1e6:.2f}M"
        if a >= 1e3:
            return f"{sign}${a / 1e3:.1f}K"
        return f"{sign}${a:,.0f}"

    def fmt(name: str, v: Any) -> str:
        n = _as_num(v)
        if n is None:
            return str(v)
        low = name.lower()
        if "bps" in low:
            return f"{n:+.0f} bps"
        if re.search(r"\bpct\b|percent|share|realis|realiz", low):
            return f"{n * 100:.1f}%"
        if _MONEY.search(low):
            return money(n)
        if "days" in low or "dso" in low:
            return f"{n:.1f} days"
        return f"{n:,.0f}" if float(n).is_integer() else f"{n:,.2f}"

    lines = []
    for r in rows[:6]:
        labels, metrics = [], []
        for col, v in r.items():
            m = measure_for(col)
            if m is not None:
                metrics.append(f"{m}: {fmt(m, v)}")
            elif _as_num(v) is not None and not re.match(r"^\d{4}-", str(v)):
                metrics.append(f"{col}: {fmt(col, v)}")
            else:
                labels.append(str(v))
        line = ", ".join(labels)
        if labels and metrics:
            line += ": "
        lines.append(line + ", ".join(metrics))
    more = " ..." if len(rows) > 6 else ""
    return "; ".join(lines) + more


def design(need: dict, reuse: dict, metric_view: Optional[dict], cat: Optional[dict] = None) -> dict:
    """Genie Agent and/or dashboard, only for the outputs asked. Benchmarks and widgets are run."""
    cat = cat or catalog()
    out: dict[str, Any] = {"genie_agent": None, "dashboard": None, "dropped": [], "warnings": []}
    if not reuse.get("buildable"):
        out["skipped"] = "reuse.buildable is false; nothing to design until the NO_DATA gap is resolved"
        return out
    outputs = {str(o).strip().lower() for o in (need.get("outputs") or [])}
    want_agent, want_dash = "genie_agent" in outputs, "dashboard" in outputs
    if not (want_agent or want_dash):
        out["skipped"] = "no outputs requested"
        return out
    sources = _sources(reuse, metric_view)
    views = _views(sources, cat, metric_view)
    if len(views) != len(sources):
        out["warnings"].append("some data sources could not be read from the catalog")
    # RAW product: the gold table is built by the pipeline, so nothing can run at intake. Guards still apply;
    # benchmarks and widgets are kept with expected/validated = "pending" and run at build.
    pending = any(i.get("decision") == "RAW" for i in reuse.get("items", []))
    qrefs = [r for r in _need_refs(need) if r["ref"].startswith("Q")]
    latest = cat.get("latest_week")
    allowed = _allowed_blob(need, views, latest)
    measures, dims = _measure_union(views), _dim_union(views)
    user = _design_user(need, views, qrefs, latest)

    replies: dict[str, Any] = {}
    with cf.ThreadPoolExecutor(max_workers=2) as ex:
        futs = {}
        if want_agent:
            futs["agent"] = ex.submit(_llm_json, _AGENT_SYSTEM, user)
        if want_dash:
            futs["dash"] = ex.submit(_llm_json, _DASH_SYSTEM, user)
        for k, f in futs.items():
            try:
                r = f.result()
                replies[k] = r if isinstance(r, dict) else {}
            except Exception as e:  # noqa: BLE001
                out["warnings"].append(f"{k} design failed: {_short_err(e)}")

    if "agent" in replies:
        r = replies["agent"]
        qmap = {q["ref"]: q for q in qrefs}
        wanted: dict[str, str] = {}
        for b in r.get("benchmarks") or []:
            if not isinstance(b, dict):
                continue
            ref = str(b.get("ref") or "").strip().upper()
            if ref not in qmap:
                _drop(out, "benchmark", ref or "?", "unknown question ref")
            elif b.get("answerable") is False:
                _drop(out, "benchmark", qmap[ref]["text"], "the listed views cannot answer this")
            else:
                wanted[ref] = str(b.get("sql") or "").strip()

        def evaluate(batch: dict[str, str]):
            ok_refs, jobs, failed = [], [], {}
            for ref, sql in batch.items():
                reason = _check_bench(sql, qmap[ref]["text"], sources, measures, dims)
                if reason:
                    failed[ref] = (sql, reason)
                else:
                    ok_refs.append(ref)
                    jobs.append(_exec_sql(sql, metric_view))
            if pending:  # the tables do not exist yet: the guard passed, the numbers come at build
                return {ref: (batch[ref], None) for ref in ok_refs}, failed
            passed = {}
            for ref, (rows, err) in zip(ok_refs, _run_all(jobs)):
                if err or not rows:
                    failed[ref] = (batch[ref], err or "returned no rows")
                else:
                    passed[ref] = (batch[ref], rows)
            return passed, failed

        passed, failed = evaluate(wanted)
        if failed:
            lines = "\n".join(f"{ref} ({qmap[ref]['text']}): {sql}\n   failed: {why}" for ref, (sql, why) in failed.items())
            try:
                fixed_reply = _llm_json(_AGENT_SYSTEM, user + "\n\nREPAIR. These benchmark SQLs failed. Reply JSON "
                                        "{\"benchmarks\": [{\"ref\": \"Q1\", \"answerable\": true, \"sql\": \"...\"}]} "
                                        "for these refs only:\n" + lines)
                fixes = fixed_reply.get("benchmarks") if isinstance(fixed_reply, dict) else []
                redo = {str(f.get("ref") or "").strip().upper(): str(f.get("sql") or "").strip()
                        for f in fixes or [] if isinstance(f, dict) and str(f.get("ref") or "").strip().upper() in failed
                        and f.get("answerable") is not False}
            except Exception as e:  # noqa: BLE001
                out["warnings"].append(f"benchmark repair failed: {_short_err(e)}")
                redo = {}
            p2, f2 = evaluate(redo)
            passed.update(p2)
            failed = {ref: v for ref, v in failed.items() if ref not in p2}
            failed.update(f2)
        for ref, (sql, why) in failed.items():
            _drop(out, "benchmark", qmap[ref]["text"], why)
        benchmarks = [{"question": qmap[ref]["text"], "sql": sql,
                       "expected": "computed at build" if pending else _format_expected(rows, measures),
                       "validated": "pending" if pending else True}
                      for ref, (sql, rows) in passed.items()]
        benchmarks.sort(key=lambda b: [q["text"] for q in qrefs].index(b["question"]))
        out["genie_agent"] = {
            "title": _clean_text(r.get("title"), 90) or f"{aj.COMPANY} {_clip(need.get('purpose'), 60)}".strip(),
            "description": _clean_text(r.get("description"), 300),
            "data_sources": sources,
            # Fixed closing rule (not left to the model): answer only from the governed metric views, via MEASURE().
            "instructions": (_clean_instructions(r.get("instructions"), allowed, latest, out).rstrip()
                             + " Answer only from these governed metric views, using MEASURE(): "
                             + ", ".join(s.split(".")[-1] for s in sources) + "."),
            "sample_questions": [q["text"] for q in qrefs],
            "benchmarks": benchmarks,
            "access": f"share with the app service principal and {need.get('audience') or 'the requesting team'} (CAN_RUN)",
        }

    if "dash" in replies:
        r = replies["dash"]
        flat: list[tuple[int, str, str, str]] = []
        page_names: list[str] = []
        for p in (r.get("pages") or [])[:2]:
            if not isinstance(p, dict):
                continue
            page_names.append(_clean_text(p.get("name"), 60) or "Overview")
            for w in (p.get("widgets") or [])[:6]:
                if not isinstance(w, dict):
                    continue
                wsql = str(w.get("sql") or "").strip()
                wtitle = _clean_text(w.get("title"), 100) or "Widget"
                wtype = str(w.get("type") or "table").lower()
                if wtype not in WIDGET_TYPES:
                    wtype = "table"
                problem = _check_query(wsql, sources, measures) or _pii_problem(wsql) or _date_literal_problem(wsql)
                if problem:
                    _drop(out, "widget", wtitle, problem)
                    continue
                flat.append((len(page_names) - 1, wtitle, wtype, wsql))
        if pending:  # RAW product: widgets read tables the build creates; kept with validated="pending"
            results = [(None, None)] * len(flat)
        else:
            results = _run_all([_exec_sql(s, metric_view) for _, _, _, s in flat])
        pages = [{"name": n, "widgets": []} for n in page_names]
        for (pi, t, ty, s), (rows, err) in zip(flat, results):
            if not pending and (err or not rows):
                _drop(out, "widget", t, err or "no rows")
                continue
            pages[pi]["widgets"].append({"title": t, "type": ty, "sql": s,
                                         "validated": "pending" if pending else True})
        pages = [p for p in pages if p["widgets"]]
        if not pages:
            out["warnings"].append("dashboard: no widget ran, so no dashboard was designed")
        else:
            filters: list[str] = []
            for f in r.get("filters") or []:
                c = _resolve_name(f, dims)
                if c and c not in filters:
                    filters.append(c)
                elif not c:
                    out["warnings"].append(f"filter {f!r} is not a dimension of the sources; dropped")
            out["dashboard"] = {
                "title": _clean_text(r.get("title"), 90) or f"{aj.COMPANY} {_clip(need.get('purpose'), 60)}".strip(),
                "pages": pages, "filters": filters[:3], "refresh": str(need.get("cadence") or ""),
                "include_customer_names": False,
            }
    return out


# ----------------------------------------------------------------------------- design change (chat / card)
# The analyst asks for a change while the design card is open. The LLM proposes ops limited to the design's
# own sources; every new or changed query gets the same guards as design() (sources, known MEASUREs, sql_guard,
# plural-aware grounding, PII) and is RUN (RAW: validated against the planned columns, "pending").
# Anything the data does not have is refused with the reason and the closest real name, never invented.
_PII_ASK = re.compile(r"customer (names?|full names?)|names? of (the )?customers?|customer-level", re.I)
_PII_REASON = ("customer names are not on the shared dashboard. They are personal and commercially sensitive, and the "
               "dashboard is shared with the whole team. It can show segments and totals; the Genie Agent answers by "
               "name, per user, with their own permissions.")
WIDGET_MAX_ROWS = 500
_CHANGE_OPS = ("add_widget", "replace_widget", "remove_widget", "add_sample_question", "add_benchmark",
               "set_instructions", "refuse")
_SQL_WORDS = {"distinct", "all", "null", "true", "false", "case", "when", "then", "else", "end", "and", "or",
              "not", "is", "in", "between", "like", "desc", "asc"}
_CHANGE_SYSTEM = """You edit the design of a data product for an analyst. The design has a dashboard (pages of widgets; each widget has a title, a type and one SQL query) and optionally a Genie Agent (sample questions, benchmarks with SQL, instructions). You may use ONLY the data you are given. Reply with JSON only:
{"changes": [
  {"op": "add_widget", "page": "<existing page name, or empty>", "title": "...", "type": "counter|bar|line|table", "sql": "SELECT ...", "asked_as": "<words copied exactly from the request that name the new column>"},
  {"op": "replace_widget", "match_title": "<existing widget title>", "title": "...", "type": "...", "sql": "SELECT ...", "asked_as": "..."},
  {"op": "remove_widget", "match_title": "<existing widget title>"},
  {"op": "add_sample_question", "question": "..."},
  {"op": "add_benchmark", "question": "...", "sql": "SELECT ...", "asked_as": "..."},
  {"op": "set_instructions", "text": "..."},
  {"op": "refuse", "ask": "<the part of the request that cannot be done>", "reason": "<the column or measure that is missing, or why it is not allowed>", "closest": "<the closest name the data has, exactly as listed, or empty>"}
]}
Rules:
- To add a dimension or a measure to an existing widget, use replace_widget and send the widget's whole new SQL, keeping its existing columns.
- sql is a single SELECT: SELECT <dimensions>, MEASURE(`<measure>`) ... FROM <full view name> GROUP BY ALL. Measures are always MEASURE(`name`). Use only the listed views, measures and dimensions. Never recompute a measure in SQL. A counter has one row, one measure and no dimension.
- asked_as is copied word for word from the request, the words that name the new dimension or measure (for example "product type" when the request says "product type"). Never invent it.
- Match the analyst's words to the listed names by meaning, not only by spelling. Each measure shows what it computes (its expression), so a count of rows is a count measure whatever it is called. For example a product type is the Category dimension when the data has one, and a sales count is a count measure. Use that listed name in the widget, and copy asked_as from the request. Do not refuse what a listed name already covers.
- Refuse (refuse op) only what no listed name means. Give the reason and the closest listed name; the closest name is never a stand-in for something the data lacks. Never invent a column, a measure, a number or a date. Never put customer names or customer-level rows on the dashboard.
- A table widget should stay readable: group by the columns the analyst asked for, not extra ones.
- Only what the request asks for. Leave the other widgets alone.
- Instructions never state a number or a date."""


def _change_user(text: str, design: dict, views: dict[str, dict], latest: Optional[str], pending: bool) -> str:
    lines: list[str] = []
    dash = design.get("dashboard") or {}
    for p in dash.get("pages") or []:
        lines.append(f"Page {p.get('name')}:")
        for w in p.get("widgets") or []:
            lines.append(f"  widget '{w.get('title')}' ({w.get('type')}): {w.get('sql')}")
    if not dash:
        lines.append("(no dashboard)")
    ga = design.get("genie_agent") or {}
    if ga:
        lines.append("Genie Agent sample questions: " + " | ".join(ga.get("sample_questions") or []))
        lines.append("Genie Agent benchmarks: " + " | ".join(b.get("question", "") for b in ga.get("benchmarks") or []))
        lines.append("Genie Agent instructions: " + (ga.get("instructions") or ""))
    else:
        lines.append("(no Genie Agent)")
    vs = "\n".join(
        f"{v['name']}\n  measures: " + "; ".join(
            m["name"] + (f" ({m['comment']})" if m.get("comment") else "") + (f" = {m['expr']}" if m.get("expr") else "")
            for m in v["measures"])
        + f"\n  dimensions: {', '.join(v['dimensions'])}" for v in views.values())
    note = ("The tables are not built yet (a pipeline builds them): new SQL is checked against the planned columns "
            "and is not run.\n") if pending else ""
    if _PII_ASK.search(text):
        note += ("The app refuses customer names itself and records that refusal. Do not add a refuse op for them; "
                 "carry on with the rest of the request.\n")
    return (f"REQUEST\n{text}\n\nCURRENT DESIGN\n" + "\n".join(lines) + f"\n\nLatest full week: {latest or 'unknown'}\n"
            f"{note}\nDATA YOU MAY USE\n{vs}")


def _select_clause(sql: str) -> str:
    m = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", _norm_sql(str(sql or "")))
    return m.group(1) if m else ""


def _dims_in(sql: str, dims: list[str]) -> list[str]:
    """Dimensions the SELECT list names (MEASURE() arguments do not count)."""
    clause = re.sub(r"(?i)MEASURE\s*\([^)]*\)", " ", _select_clause(sql))
    return [d for d in dims if re.search(r"(?<![A-Za-z0-9_])" + re.escape(d) + r"(?![A-Za-z0-9_])", clause, re.I)]


def _verified_phrase(asked_as: Any, request_text: str) -> str:
    """The analyst's own words for a new column, only when they really appear in the request."""
    p = re.sub(r"\s+", " ", str(asked_as or "")).strip().lower()
    if len(p) < 3:
        return ""
    return p if re.search(r"(?<![a-z0-9])" + re.escape(p) + r"(?![a-z0-9])", request_text.lower()) else ""


def _week_filter(sql: str) -> bool:
    return bool(re.search(r"(?i)Week Start\s*(=|>=|<=|<|>|between)", _norm_sql(str(sql or ""))))


def _ungrounded_new(sql: str, old_sql: str, dims: list[str], ground_text: str, asked: str) -> list[str]:
    """New dimensions (not in the query being replaced) that the request does not name. A verified
    asked_as phrase (the analyst's own words) stands in for the name, e.g. "product type" for Category."""
    if asked:
        return []
    old = set(_dims_in(old_sql, dims))
    new = [d for d in _dims_in(sql, dims) if d not in old]
    return _ungrounded_dims(sql, ground_text, new) if new else []


def _unknown_columns(sql: str, dims: list[str], measures: list[str]) -> list[str]:
    """Bare or backticked names in the SELECT list that are neither a dimension nor a measure of the sources."""
    m = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", str(sql or ""))
    if not m:
        return []
    clause = re.sub(r"(?i)MEASURE\s*\(\s*`[^`]+`\s*\)", " ", m.group(1))
    clause = re.sub(r"(?i)\bAS\s+(`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)", " ", clause)
    names = re.findall(r"`([^`]+)`", clause)
    for piece in re.sub(r"`[^`]+`", " ", clause).split(","):
        p = piece.strip()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) and p.lower() not in _SQL_WORDS:
            names.append(p)
    known = {_key(x) for x in list(dims) + list(measures)}
    out: list[str] = []
    for n in names:
        if _key(n) not in known and n not in out:
            out.append(n)
    return out


def _find_widget(design: dict, title: Any) -> Optional[tuple[dict, dict]]:
    want = _key(title)
    if not want:
        return None
    for p in (design.get("dashboard") or {}).get("pages") or []:
        for w in p.get("widgets") or []:
            if _key(w.get("title")) == want:
                return p, w
    return None


def _change_ops(raw: Any) -> list[dict]:
    ops = raw.get("changes") if isinstance(raw, dict) else raw
    return [o for o in (ops or []) if isinstance(o, dict)][:12]


def design_change(design: dict, request_text: str, need: dict, reuse: dict, metric_view: Optional[dict],
                  cat: Optional[dict] = None) -> dict:
    """Apply an analyst's change request to an open design.

    Returns {design, changes, refused}: design is the updated design (same shape as design()); changes are
    human-readable lines of what was done; refused is [{ask, reason, closest}] for everything not done.
    """
    import copy

    cat = cat or catalog()
    text = " ".join(str(request_text or "").split())
    cur = copy.deepcopy(design or {})
    cur.setdefault("genie_agent", None)
    cur.setdefault("dashboard", None)
    cur.setdefault("skipped", "")
    cur.setdefault("dropped", [])
    cur.setdefault("warnings", [])
    changes: list[str] = []
    refused: list[dict] = []

    def result() -> dict:
        return {"design": cur, "changes": changes, "refused": refused}

    def refuse(ask: str, reason: str, closest: str = "") -> None:
        if _PII_ASK.search(ask or "") and refused and any(r["reason"] == _clip(_PII_REASON, 400) for r in refused):
            return   # the customer-name refusal is recorded once
        refused.append({"ask": _clip(ask or text, 160), "reason": _clip(reason, 400), "closest": closest or ""})

    if not text:
        refuse("", "the change request is empty")
        return result()
    if not reuse.get("buildable"):
        refuse(text, "nothing is buildable yet: the data for this ask is missing, so the design cannot change")
        return result()
    outputs = {str(o).strip().lower() for o in (need.get("outputs") or [])}
    want_agent, want_dash = "genie_agent" in outputs, "dashboard" in outputs
    sources = _sources(reuse, metric_view)
    views = _views(sources, cat, metric_view)
    measures, dims = _measure_union(views), _dim_union(views)
    latest = cat.get("latest_week")
    allowed = _allowed_blob(need, views, latest)
    pending = any(i.get("decision") == "RAW" for i in reuse.get("items", []))
    if _PII_ASK.search(text):
        refuse("customer names", _PII_REASON)

    user = _change_user(text, cur, views, latest, pending)
    try:
        raw = _llm_json(_CHANGE_SYSTEM, user)
    except Exception as e:  # noqa: BLE001
        refuse(text, f"I could not plan that change ({_short_err(e)}). Try again or rephrase it.")
        return result()
    entries = [dict(op, ref=f"C{i}") for i, op in enumerate(_change_ops(raw), 1)]

    def closest_name(c: Any) -> str:
        return _resolve_name(c, measures + dims) or ""

    def prepare(e: dict) -> Optional[dict]:
        """Checks that need no warehouse call. None = refused (already recorded)."""
        op = str(e.get("op") or "").strip().lower()
        ref = e["ref"]
        if op == "refuse":
            refuse(_clean_text(e.get("ask"), 200) or text, _clean_text(e.get("reason"), 300) or "not in the data",
                   closest_name(e.get("closest")))
            return None
        if op not in _CHANGE_OPS:
            refuse(text, f"'{op or 'empty'}' is not a change I can make to the design")
            return None
        if op in ("add_widget", "replace_widget"):
            if not want_dash:
                refuse(text, "the request did not ask for a dashboard, so there is no widget to change")
                return None
            title = _clean_text(e.get("title"), 100) or "Widget"
            sql = str(e.get("sql") or "").strip()
            wtype = str(e.get("type") or "table").lower()
            wtype = wtype if wtype in WIDGET_TYPES else "table"
            old_sql = ""
            if op == "replace_widget":
                hit = _find_widget(cur, e.get("match_title"))
                if hit is None:
                    refuse(title, f"there is no widget titled '{_clip(e.get('match_title'), 80)}' on the dashboard")
                    return None
                old_sql = hit[1].get("sql") or ""
            problem = (_check_query(sql, sources, measures) or _pii_problem(sql) or _date_literal_problem(sql))
            if not problem:
                bad = _unknown_columns(sql, dims, measures)
                if bad:
                    problem = (f"{', '.join(bad)} is not a dimension or measure of the listed data. "
                               "It is not on the dashboard")
            if not problem and old_sql and _week_filter(old_sql) and not _week_filter(sql):
                problem = "it drops the Week Start filter the widget had, so it would show every week; keep that filter"
            if not problem:
                bad = _ungrounded_new(sql, old_sql, dims, text, _verified_phrase(e.get("asked_as"), text))
                if bad:
                    problem = (f"groups by {', '.join(bad)}, which the request does not name")
            if problem:
                refuse(title, problem, closest_name(e.get("closest")))
                return None
            return {"kind": "widget", "op": op, "ref": ref, "label": title, "title": title, "type": wtype,
                    "sql": sql, "match": e.get("match_title"), "page": _clean_text(e.get("page"), 60),
                    "asked": _verified_phrase(e.get("asked_as"), text)}
        if op == "remove_widget":
            if not want_dash:
                refuse(text, "the request did not ask for a dashboard")
                return None
            hit = _find_widget(cur, e.get("match_title"))
            if hit is None:
                refuse(_clip(e.get("match_title"), 100), "there is no widget with that title on the dashboard")
                return None
            return {"kind": "remove", "ref": ref, "label": hit[1].get("title"), "match": e.get("match_title")}
        if op == "add_sample_question":
            if not want_agent:
                refuse(text, "the request did not ask for a Genie Agent, so there are no sample questions to add")
                return None
            q = _clean_text(e.get("question"), 300)
            if not q:
                refuse(text, "the sample question was empty")
                return None
            return {"kind": "question", "ref": ref, "label": q, "question": q}
        if op == "add_benchmark":
            if not want_agent:
                refuse(text, "the request did not ask for a Genie Agent, so there are no benchmarks to add")
                return None
            q = _clean_text(e.get("question"), 300)
            sql = str(e.get("sql") or "").strip()
            if not q or not sql:
                refuse(q or text, "the benchmark needs a question and a query")
                return None
            if re.search(r"\b(accounts?|customers?|clients?)\b", q, re.I) and not re.search(r"\bsegments?\b", q, re.I):
                refuse(q, "per-customer questions get no benchmark (the answer depends on who asks; customer names stay out)")
                return None
            problem = _check_query(sql, sources, measures) or _pii_problem(sql) or _date_literal_problem(sql)
            if not problem:
                bad = _unknown_columns(sql, dims, measures)
                if bad:
                    problem = f"{', '.join(bad)} is not a dimension or measure of the listed data"
            if not problem:
                bad = _ungrounded_new(sql, "", dims, text + " " + q, _verified_phrase(e.get("asked_as"), text))
                if bad:
                    problem = f"groups by {', '.join(bad)}, which the question does not name"
            if problem:
                refuse(q, problem, closest_name(e.get("closest")))
                return None
            return {"kind": "benchmark", "ref": ref, "label": q, "question": q, "sql": sql,
                    "asked": _verified_phrase(e.get("asked_as"), text)}
        if op == "set_instructions":
            if not want_agent:
                refuse(text, "the request did not ask for a Genie Agent, so there are no instructions to change")
                return None
            warn: dict[str, Any] = {"warnings": []}
            body = _clean_instructions(e.get("text"), allowed, latest, warn)
            cur["warnings"] = list(cur.get("warnings") or []) + warn["warnings"]
            if not body:
                refuse(text, "the instructions text was empty or only had numbers that the data does not hold")
                return None
            return {"kind": "instructions", "ref": ref, "label": "Genie Agent instructions", "text": body}
        return None

    def execute(items: list[dict]) -> list[dict]:
        """Run the new widget and benchmark SQL once. Failures get item['error']; RAW items are only guarded."""
        runnable = [it for it in items if it["kind"] in ("widget", "benchmark")]
        if pending:
            for it in runnable:
                it["validated"] = "pending"
            return []
        results = _run_all([_exec_sql(it["sql"], metric_view) for it in runnable])
        failed = []
        for it, (rows, err) in zip(runnable, results):
            if err:
                it["error"] = f"the query did not run on the warehouse: {err}"
                failed.append(it)
            elif not rows:
                it["error"] = "the query returned no rows"
                failed.append(it)
            elif it["kind"] == "widget" and len(rows) > WIDGET_MAX_ROWS:
                it["error"] = (f"the query returns {len(rows)} rows, too many for a dashboard widget (at most "
                               f"{WIDGET_MAX_ROWS}). Group by fewer columns, or filter it")
                failed.append(it)
            else:
                it["rows"], it["validated"] = rows, True
        return failed

    items = [it for it in (prepare(e) for e in entries) if it]
    failed = execute(items)
    if failed:
        # One repair round for queries that failed, as design() does for benchmarks.
        lines = "\n".join(f"{it['ref']} ({it['label']}): {it['sql']}\n   failed: {it['error']}" for it in failed)
        fixes: dict[str, dict] = {}
        try:
            fixed = _llm_json(_CHANGE_SYSTEM, user + "\n\nREPAIR. These changes failed on the warehouse. Reply JSON "
                              "{\"changes\": [...]} with one corrected op for each ref below (same op type, same "
                              "\"ref\"), using only the listed data:\n" + lines)
            fixes = {str(o.get("ref") or ""): o for o in _change_ops(fixed) if o.get("ref")}
        except Exception as e:  # noqa: BLE001
            cur["warnings"].append(f"change repair failed: {_short_err(e)}")
        redo: list[dict] = []
        for it in failed:
            if it["ref"] not in fixes:
                refuse(it["label"], it["error"])
                continue
            again = prepare(dict(fixes[it["ref"]], ref=it["ref"]))
            if again is not None:
                redo.append(again)
        items = [it for it in items if not it.get("error")]
        for it in execute(redo):
            refuse(it["label"], it["error"])
        items += [it for it in redo if not it.get("error")]
    items = [it for it in items if not it.get("error")]
    items.sort(key=lambda it: int(it["ref"][1:]))

    def new_dashboard() -> dict:
        return {"title": f"{aj.COMPANY} {_clip(need.get('purpose'), 60)}".strip(),
                "pages": [{"name": "Overview", "widgets": []}], "filters": [],
                "refresh": str(need.get("cadence") or ""), "include_customer_names": False}

    def status(it: dict) -> str:
        return "validated at build" if it.get("validated") == "pending" else f"returned {len(it['rows'])} rows"

    def closing_rule() -> str:
        return (" Answer only from these governed metric views, using MEASURE(): "
                + ", ".join(s.split(".")[-1] for s in sources) + ".")

    for it in items:
        k = it["kind"]
        if k == "widget" and cur.get("dashboard") is None:
            cur["dashboard"] = new_dashboard()
        if k in ("question", "benchmark", "instructions") and cur.get("genie_agent") is None:
            cur["genie_agent"] = {"title": f"{aj.COMPANY} {_clip(need.get('purpose'), 60)}".strip(),
                                  "description": "", "data_sources": list(sources), "instructions": closing_rule(),
                                  "sample_questions": [], "benchmarks": [],
                                  "access": f"share with the app service principal and {need.get('audience') or 'the requesting team'} (CAN_RUN)"}
        if k == "widget":
            pages = cur["dashboard"]["pages"]
            wid = {"title": it["title"], "type": it["type"], "sql": it["sql"], "validated": it.get("validated", True)}
            if it["op"] == "replace_widget":
                hit = _find_widget(cur, it["match"])
                if hit is None:
                    refuse(it["title"], "the widget to replace is no longer on the dashboard")
                    continue
                hit[1].update(wid)
                changes.append(f"Replaced widget '{it['title']}' ({it['type']}): {status(it)}.")
                continue
            hit = _find_widget(cur, it["title"])
            if hit is not None:
                hit[1].update(wid)
                changes.append(f"Updated widget '{it['title']}' ({it['type']}): {status(it)}.")
                continue
            page = next((p for p in pages if _key(p.get("name")) == _key(it.get("page"))), None) or pages[0]
            if len(page["widgets"]) >= 6:
                room = next((p for p in pages if len(p["widgets"]) < 6), None)
                if room is None:
                    refuse(it["title"], "the dashboard is full: 6 widgets a page, and no page has room")
                    continue
                page = room
            page["widgets"].append(wid)
            changes.append(f"Added widget '{it['title']}' ({it['type']}) to page {page['name']}: {status(it)}.")
        elif k == "remove":
            hit = _find_widget(cur, it["match"])
            if hit is None:
                refuse(it["label"] or "", "the widget is no longer on the dashboard")
                continue
            hit[0]["widgets"].remove(hit[1])
            changes.append(f"Removed widget '{hit[1].get('title')}' from page {hit[0]['name']}.")
        elif k == "question":
            ga = cur["genie_agent"]
            if not any(_key(q) == _key(it["question"]) for q in ga.get("sample_questions") or []):
                ga["sample_questions"] = list(ga.get("sample_questions") or []) + [it["question"]]
                changes.append(f"Added the sample question: {it['question']}")
            else:
                changes.append(f"The sample question '{it['question']}' is already there.")
        elif k == "benchmark":
            ga = cur["genie_agent"]
            expected = "computed at build" if it.get("validated") == "pending" else _format_expected(it["rows"], measures)
            bm = {"question": it["question"], "sql": it["sql"], "expected": expected,
                  "validated": it.get("validated", True)}
            same = [b for b in ga.get("benchmarks") or [] if _key(b.get("question")) == _key(it["question"])]
            if same:
                same[0].update(bm)
                changes.append(f"Updated the benchmark '{it['question']}': expected {expected}.")
            else:
                ga["benchmarks"] = list(ga.get("benchmarks") or []) + [bm]
                changes.append(f"Added the benchmark '{it['question']}': expected {expected}.")
        elif k == "instructions":
            cur["genie_agent"]["instructions"] = it["text"] + closing_rule()
            changes.append("Updated the Genie Agent instructions.")
    # A refusal the same words of the request already got applied (e.g. "product type" refused by the model
    # while a widget using Product was added) is not a refusal: keep the applied change only.
    covered = [it["asked"] for it in items if it.get("asked")]
    refused[:] = [r for r in refused if not any(c and c in str(r["ask"]).lower() for c in covered)]
    if not changes and not refused:
        refuse(text, "I did not find a change to the design in that request")
    return result()


# ----------------------------------------------------------------------------- self-check helpers
def _check_sql_guard_cases() -> list[tuple[str, bool]]:
    """Used by the test: expected results for validate_sql_readonly."""
    return [
        ("SELECT Category, MEASURE(`GM vs Plan Bps`) FROM vmohan_demo_catalog.studio_dev.mv_margin_leakage GROUP BY ALL", True),
        ("SELECT * FROM vmohan_demo_catalog.studio_control.mv_draft_ab12cd34", True),
        ("SELECT 'x; y' AS a FROM vmohan_demo_catalog.cfo_workbench.mv_working_capital", True),
        ("DROP TABLE vmohan_demo_catalog.cfo_workbench.mv_working_capital", False),
        ("SELECT 1; DROP TABLE vmohan_demo_catalog.cfo_workbench.gold_fct_freight", False),
        ("SELECT * FROM vmohan_demo_catalog.bronze.sales", False),
        ("SELECT * FROM vmohan_demo_catalog.studio_control.requests", False),
        ("SELECT * FROM cfo_workbench.mv_working_capital", False),
        ("SELECT * FROM vmohan_demo_catalog.cfo_workbench.mv_working_capital -- note", False),
        ("INSERT INTO vmohan_demo_catalog.studio_dev.x SELECT 1", False),
    ]
