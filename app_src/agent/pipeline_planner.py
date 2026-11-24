"""Pipeline planner for RAW feeds (CONTRACT3, owner P).

When a need can only be answered from raw files (match_needs decision RAW), the plan gains a Lakeflow
Spark Declarative Pipeline: raw -> bronze (streaming table over read_files) -> silver (typed, deduplicated,
DQ expectations) -> gold (weekly aggregates the metric view will sit on).

author_pipeline() is the only entry point:
  1. the LLM drafts the SDP SQL from the feed's REAL columns (dp.catalog() raw_feeds, inferred by read_files)
  2. code checks it: exactly three statements; the bronze reads only the feed path; every table, function and
     column name is on the allowlist for its stage; expectation conditions pass sql_guard.check_expression
  3. it is VALIDATED by a validate-only update of a scratch pipeline `acme-draft-<hash>` (serverless, catalog
     vmohan_demo_catalog, target studio_control). Nothing is materialised. A failed analysis error is fed
     back to the LLM for at most MAX_REPAIRS repairs.

Nothing else is created or deleted here except the acme-draft-* scratch pipelines (and their uploaded
SQL files, which are overwritten by content hash). The real pipeline in studio_dev is built by the engine.
"""
from __future__ import annotations

import hashlib
import io
import os
import re
import time
from typing import Any, Optional

import sqlglot
from sqlglot import exp

from agent import acme_journey as aj
from agent import dp_planner as dp
from agent import sql_guard

MAX_REPAIRS = 2
VALIDATE_TIMEOUT_S = 420
# A DROP ROW expectation may remove only wrong rows. Above this share of the feed it is refused (repair).
DROP_SHARE_LIMIT = 0.01
DRAFT_PREFIX = "acme-draft-"
# Draft pipelines this process created. Other sessions validate through the same prefix; cleanup only touches these.
_OWN_DRAFTS: set[str] = set()
READ_OPTIONS = {"format": {"'csv'"}, "header": {"true"}, "inferColumnTypes": {"true", "false"}}
# sql_guard.SAFE_FUNCTIONS plus what SDP silver/gold steps need (typed casts, timestamps, window helpers).
# Compared with underscores removed: sqlglot names DATE_TRUNC as TIMESTAMP_TRUNC, for example.
PIPE_FUNCTIONS = {f.replace("_", "") for f in set(sql_guard.SAFE_FUNCTIONS) | {
    "try_cast", "try_to_date", "to_timestamp", "try_to_timestamp", "current_timestamp", "make_date",
    "row_number", "trunc", "date_part", "extract", "timestamp_trunc", "date_trunc",
}}


# ----------------------------------------------------------------------------- names and folders
def _names(need: dict) -> dict[str, str]:
    slug = dp._slug(need)
    return {"slug": slug, "pipeline": f"acme-{slug.replace('_', '-')}-pipeline", "bronze": f"bronze_{slug}",
            "silver": f"silver_{slug}", "gold": f"gold_{slug}"}


def _feed_for(reuse: dict, cat: dict) -> Optional[dict]:
    """The raw feed a RAW item points at (asset = the feed path)."""
    feeds = cat.get("raw_feeds") or []
    for item in reuse.get("items", []):
        if item.get("decision") != "RAW":
            continue
        want = str(item.get("asset") or "").rstrip("/")
        for f in feeds:
            if f["path"].rstrip("/") == want:
                return f
    return None


def _workspace_folder(w) -> str:
    folder = os.environ.get("OPS_PIPELINE_FOLDER", "").strip()
    if not folder:
        me = w.current_user.me().user_name
        folder = f"/Workspace/Users/{me}/acme_studio/pipelines"
    return folder.rstrip("/")


# ----------------------------------------------------------------------------- prompts
_PIPE_SYSTEM = """You write a Lakeflow Spark Declarative Pipeline in SQL for ONE raw CSV feed. Reply with exactly one ```sql block and nothing else.

The file has exactly three statements, in this order, each ending with a semicolon:

1. Bronze, the feed as landed (use these exact names and options):
CREATE OR REFRESH STREAMING TABLE <BRONZE> AS SELECT * FROM STREAM read_files('<FEED PATH>', format => 'csv', header => true);

2. Silver, a MATERIALIZED VIEW over <BRONZE>: typed columns, one row per natural key, and data quality expectations:
CREATE OR REFRESH MATERIALIZED VIEW <SILVER> (
  CONSTRAINT <snake_case_name> EXPECT (<condition>) ON VIOLATION DROP ROW,
  CONSTRAINT <snake_case_name> EXPECT (<condition>)
) AS SELECT <explicit typed columns with aliases> FROM <BRONZE> QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <column>) = 1;

3. Gold, a MATERIALIZED VIEW over <SILVER>: weekly aggregates the analyst's measures need, named <GOLD>:
CREATE OR REFRESH MATERIALIZED VIEW <GOLD> AS SELECT <grain columns>, <additive counts and sums with aliases> FROM <SILVER> GROUP BY ALL;

Rules (each one is checked by code; a violation is rejected):
- Use ONLY the feed columns listed in the user message, with their exact names. Never invent a column or table.
- The three names are exactly <BRONZE>, <SILVER>, <GOLD>. No other tables, no joins, no subqueries, no CTEs, no UNION.
- Silver lists every column explicitly (no SELECT *), casts types (CAST or TRY_CAST), and gives every output column an alias if it is an expression.
- Expectation conditions are simple boolean SQL over SILVER output column names: comparisons, AND/OR, IS NULL, IS NOT NULL, BETWEEN, IN, COALESCE, ABS, LENGTH, TRIM, LOWER, UPPER. No subqueries.
- Use ON VIOLATION DROP ROW only for conditions a bad row would make a headline number wrong (missing key, missing branch, impossible date order). Leave the others as warnings (no ON VIOLATION clause).
- A column that is legitimately empty for some rows (for example a delivery date while the shipment is still in transit) must NOT be a DROP ROW condition: those rows are real. Use a warning, and make measures count only rows where the outcome is known (CASE WHEN delivered_date IS NULL THEN NULL ...); never treat an unknown outcome as late.
- Gold carries additive counts (for example shipments, delivered, on_time) and sums, so ratios can be computed as SUM over NULLIF(SUM(...), 0). Gold grain: the week start (DATE_TRUNC('WEEK', date)) plus the dimension columns the analyst asked for.
- Functions allowed: CAST, TRY_CAST, DATE_TRUNC, COALESCE, NULLIF, ROW_NUMBER, SUM, COUNT, COUNT_IF, AVG, MIN, MAX, ROUND, ABS, LOWER, UPPER, TRIM, LENGTH, CASE/WHEN, IF, DATEDIFF, DATE_ADD, DATE_SUB, CURRENT_DATE.
- No comments, no other statements, no semicolons inside the statements.
"""

_REPAIR_TEXT = ("\n\nYour previous draft was rejected: {err}\nFix exactly that. Keep the same three names, the feed path "
                "and the same structure. Reply with the full file in one ```sql block.")


def _extract_sql(raw: str) -> str:
    m = re.search(r"```(?:sql)?[ \t]*\n(.*?)```", raw, re.S | re.I)
    return (m.group(1) if m else raw).strip() + "\n"


def _feed_sample(path: str, limit: int = 3) -> str:
    rows = aj.sql(f"SELECT * FROM read_files({aj.lit(path)}, format => 'csv', header => true) LIMIT {limit}")
    if not rows:
        return "(no rows)"
    cols = list(rows[0].keys())
    lines = [",".join(cols)] + [",".join("" if r[c] is None else str(r[c]) for c in cols) for r in rows]
    return "\n".join(lines)


def _user_prompt(need: dict, feed: dict, names: dict) -> str:
    measures = "\n".join(f"- {m.get('name')}: {m.get('definition') or ''} (dimensions: {', '.join(m.get('dimensions') or [])})"
                         for m in (need.get("measures") or []) if isinstance(m, dict))
    cols = "\n".join(f"- {c['name']} ({c['type']})" for c in feed["columns"])
    return (f"ANALYST NEED\nPurpose: {need.get('purpose') or ''}\nAudience: {need.get('audience') or ''}\n"
            f"Questions:\n" + "\n".join(f"- {q}" for q in (need.get("questions") or [])) + "\n"
            f"Measures:\n{measures or '(none)'}\n\n"
            f"NAMES: <BRONZE>={names['bronze']}, <SILVER>={names['silver']}, <GOLD>={names['gold']}\n"
            f"FEED PATH: {feed['path']}\nFEED FILES: {feed.get('files', 0)} CSV files\n"
            f"FEED COLUMNS (inferred by read_files; the feed is CSV text until silver casts them):\n{cols}\n\n"
            f"SAMPLE ROWS:\n{_feed_sample(feed['path'])}\n")


# ----------------------------------------------------------------------------- checks
def _statements(sql: str) -> list[str]:
    body = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in body.split(";") if s.strip()]


def _top_level_split(s: str) -> list[str]:
    out, depth, cur, quote = [], 0, "", False
    for ch in s:
        if ch == "'":
            quote = not quote
        if not quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                out.append(cur)
                cur = ""
                continue
        cur += ch
    if cur.strip():
        out.append(cur)
    return [x.strip() for x in out if x.strip()]


def _matching_paren(s: str, start: int) -> int:
    depth, quote = 0, False
    for i in range(start, len(s)):
        ch = s[i]
        if ch == "'":
            quote = not quote
        if quote:
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _read_problems(args: str, feed_path: str) -> list[str]:
    m = re.match(r"^\s*'([^']*)'\s*(?:,(.*))?$", args, re.S)
    if not m:
        return ["read_files must start with the feed path as a literal"]
    if m.group(1).rstrip("/") != feed_path.rstrip("/"):
        return [f"read_files may only read {feed_path}"]
    opts_text = m.group(2) or ""
    pairs = re.findall(r"(\w+)\s*=>\s*('[^']*'|\w+)", opts_text)
    out = []
    if len(pairs) != opts_text.count("=>"):
        out.append("read_files options must be name => value pairs")
    for k, v in pairs:
        if k not in READ_OPTIONS or v not in READ_OPTIONS[k]:
            out.append(f"read_files option {k} => {v} is not allowed")
    d = dict(pairs)
    if d.get("format") != "'csv'" or d.get("header") != "true":
        out.append("read_files needs format => 'csv', header => true")
    return out


def _tree_problems(body: str, label: str, source_tables: set[str], source_cols: set[str]) -> tuple[list[str], list[str]]:
    """A single SELECT over the allowed source table, allowed functions only, every column known.
    Returns (problems, output column names)."""
    try:
        tree = sqlglot.parse_one(body, read="databricks")
    except Exception as e:  # noqa: BLE001
        return [f"{label}: SQL does not parse ({str(e).splitlines()[0][:120]})"], []
    problems: list[str] = []
    if not isinstance(tree, exp.Select):
        return [f"{label}: must be a single SELECT"], []
    if tree.find(*sql_guard.FORBIDDEN_NODES) is not None:
        problems.append(f"{label}: statements other than SELECT are not allowed")
    for node_type in (exp.Subquery, exp.Union, exp.CTE, exp.With, exp.Join):
        if tree.find(node_type) is not None:
            problems.append(f"{label}: {node_type.__name__} is not allowed (one table, no joins, no subqueries)")
    for t in tree.find_all(exp.Table):
        if t.catalog or t.db or (t.name or "").lower() not in source_tables:
            problems.append(f"{label}: table {t.sql()} is not allowed; read only {', '.join(sorted(source_tables))}")
    for f in tree.find_all(exp.Func):
        name = (f.name if isinstance(f, exp.Anonymous) else f.sql_name()).lower().replace("_", "")
        if name and name not in PIPE_FUNCTIONS:
            problems.append(f"{label}: function {name} is not allowed")
    if any(isinstance(e, exp.Star) for e in tree.expressions):
        problems.append(f"{label}: list the columns explicitly (no SELECT *)")
    outputs: list[str] = []
    for e in tree.expressions:
        if isinstance(e, exp.Alias):
            outputs.append(e.alias.lower())
        elif isinstance(e, exp.Column):
            outputs.append(e.name.lower())
        else:
            problems.append(f"{label}: an expression output needs an alias")
    aliases = {a.alias.lower() for a in tree.find_all(exp.Alias)}
    used = {c.name.lower() for c in tree.find_all(exp.Column) if c.name}
    unknown = sorted(used - source_cols - aliases)
    if unknown:
        problems.append(f"{label}: unknown column(s) {', '.join(unknown)}; the source has {', '.join(sorted(source_cols))}")
    return problems, outputs


def _check(sql: str, feed: dict, names: dict) -> tuple[list[str], Optional[dict]]:
    """Every name and expression check. Returns (problems, parsed) with parsed=None when there are problems."""
    stmts = _statements(sql)
    if len(stmts) != 3:
        return [f"the file must have exactly 3 statements (bronze, silver, gold); found {len(stmts)}"], None
    b, s, g = stmts
    feed_cols = [c["name"].lower() for c in feed["columns"]]
    problems: list[str] = []

    m = re.match(r"(?is)^CREATE\s+OR\s+REFRESH\s+STREAMING\s+TABLE\s+(\w+)\s+AS\s+SELECT\s+\*\s+FROM\s+STREAM\s+"
                 r"read_files\((.*)\)$", b)
    if not m:
        problems.append("statement 1 must be CREATE OR REFRESH STREAMING TABLE <bronze> AS SELECT * FROM STREAM read_files(...)")
    else:
        if m.group(1).lower() != names["bronze"]:
            problems.append(f"the bronze table must be named {names['bronze']}")
        problems += _read_problems(m.group(2), feed["path"])

    sm = re.match(r"(?is)^CREATE\s+OR\s+REFRESH\s+(?:MATERIALIZED\s+VIEW|STREAMING\s+TABLE)\s+(\w+)\s*(.*)$", s)
    expectations: list[dict] = []
    silver_out: list[str] = []
    if not sm:
        problems.append("statement 2 must be CREATE OR REFRESH MATERIALIZED VIEW <silver> (...) AS SELECT ...")
    else:
        if sm.group(1).lower() != names["silver"]:
            problems.append(f"the silver table must be named {names['silver']}")
        rest = sm.group(2)
        header = ""
        if rest.startswith("("):
            end = _matching_paren(rest, 0)
            if end < 0:
                problems.append("silver: unbalanced parentheses in the expectations list")
                rest = ""
            else:
                header, rest = rest[1:end], rest[end + 1:]
        am = re.match(r"(?is)^\s*AS\s+(SELECT\b.*)$", rest)
        if not am:
            problems.append("silver: after the expectations list, write AS SELECT ...")
        else:
            for item in _top_level_split(header):
                cm = re.match(r"(?is)^CONSTRAINT\s+(\w+)\s+EXPECT\s*\((.*)\)\s*(ON\s+VIOLATION\s+DROP\s+ROW)?\s*$", item)
                if not cm:
                    problems.append(f"silver: expectation must be CONSTRAINT <name> EXPECT (<condition>) "
                                    f"[ON VIOLATION DROP ROW]: {item[:80]}")
                    continue
                expectations.append({"table": names["silver"], "name": cm.group(1).lower(),
                                     "constraint_sql": cm.group(2).strip(),
                                     "action": "drop" if cm.group(3) else "warn"})
            silver_body = am.group(1)
            sp, silver_out = _tree_problems(silver_body, "silver", {names["bronze"]}, set(feed_cols))
            problems += sp
            for e in expectations:
                why = sql_guard.check_expression(e["constraint_sql"])
                if why:
                    problems.append(f"expectation {e['name']} refused: {why}")
                    continue
                cols = {c.name.lower() for c in sqlglot.parse_one(
                    f"SELECT 1 FROM t WHERE {e['constraint_sql']}", read="databricks").find_all(exp.Column)}
                bad = sorted(cols - set(silver_out))
                if bad:
                    problems.append(f"expectation {e['name']} uses {', '.join(bad)}, which silver does not output")
            names_seen = [e["name"] for e in expectations]
            if len(set(names_seen)) != len(names_seen):
                problems.append("expectation names must be unique")

    gm = re.match(r"(?is)^CREATE\s+OR\s+REFRESH\s+MATERIALIZED\s+VIEW\s+(\w+)\s+AS\s+(SELECT\b.*)$", g)
    gold_out: list[str] = []
    if not gm:
        problems.append("statement 3 must be CREATE OR REFRESH MATERIALIZED VIEW <gold> AS SELECT ...")
    else:
        if gm.group(1).lower() != names["gold"]:
            problems.append(f"the gold table must be named {names['gold']}")
        gp, gold_out = _tree_problems(gm.group(2), "gold", {names["silver"]}, set(silver_out))
        problems += gp

    if problems:
        return problems, None
    return [], {"bronze_cols": feed_cols, "silver_cols": silver_out, "gold_cols": gold_out,
                "expectations": expectations, "bronze": names["bronze"], "silver_body": silver_body}


def _feed_counts(parsed: dict, feed: dict) -> tuple[int, list[int]]:
    """Feed rows (after silver's dedupe) and, per expectation, how many it would fail. One warehouse query.
    A row fails an expectation when the condition is not TRUE, which is how SDP treats a NULL condition."""
    body = re.sub(rf"(?i)\bFROM\s+{re.escape(parsed['bronze'])}\b",
                  f"FROM read_files({aj.lit(feed['path'])}, format => 'csv', header => true)",
                  parsed["silver_body"], count=1)
    exps = parsed["expectations"]
    cols = ", ".join(f"SUM(CASE WHEN COALESCE(({e['constraint_sql']}), FALSE) THEN 0 ELSE 1 END) AS c{i}"
                     for i, e in enumerate(exps))
    row = aj.sql(f"WITH s AS ({body}) SELECT COUNT(*) AS n{', ' + cols if cols else ''} FROM s")[0]
    total = int(row["n"] or 0)
    return total, [int(row[f"c{i}"] or 0) for i in range(len(exps))]


def _drop_problems(parsed: dict, total: int, fails: list[int]) -> list[str]:
    """Records the counts on each expectation; returns a problem for each DROP ROW that removes too much."""
    out = []
    for e, n in zip(parsed["expectations"], fails):
        e["feed_rows"], e["feed_rows_failing"] = total, n
        if e["action"] == "drop" and total and n / total > DROP_SHARE_LIMIT:
            out.append(f"expectation {e['name']} would drop {n} of {total} feed rows ({100 * n / total:.1f}%). "
                       "A DROP ROW rule may only remove wrong rows; a column that is legitimately empty for some "
                       "rows (such as a delivery date for a shipment still in transit) must allow that case, for example "
                       "delivered_date IS NULL OR delivered_date >= despatch_date, or be a warning")
    return out


# ----------------------------------------------------------------------------- validation
def _enum(x: Any) -> str:
    return str(getattr(x, "value", x) or "")


def _update_errors(w, pid: str, uid: str) -> str:
    msgs: list[str] = []
    try:
        for e in w.pipelines.list_pipeline_events(pid, filter=f"update_id='{uid}'", max_results=100):
            err = getattr(e, "error", None)
            for x in (getattr(err, "exceptions", None) or []):
                if x.message:
                    msgs.append(x.message)
            if "ERROR" in _enum(getattr(e, "level", None)) and e.message:
                msgs.append(e.message)
    except Exception as ex:  # noqa: BLE001
        msgs.append(f"could not read the pipeline events: {ex}")
    seen: list[str] = []
    for m in msgs:
        if m not in seen:
            seen.append(m)
    return dp._clip(" | ".join(seen) or "validation failed", 700)


def _validate(sql: str) -> dict[str, Any]:
    """Validate-only update of acme-draft-<hash> (serverless, vmohan_demo_catalog.studio_control)."""
    from databricks.sdk.service.pipelines import FileLibrary, PipelineLibrary
    from databricks.sdk.service.workspace import ImportFormat
    w = aj._ws()
    h = hashlib.sha1(sql.encode()).hexdigest()[:8]
    name = f"{DRAFT_PREFIX}{h}"
    folder = _workspace_folder(w)
    path = f"{folder}/{name}.sql"
    w.workspace.mkdirs(folder)
    w.workspace.upload(path, io.BytesIO(sql.encode()), format=ImportFormat.RAW, overwrite=True)
    spec = dict(name=name, catalog=aj.CATALOG, target=aj.CONTROL_SCHEMA, serverless=True, development=True,
                continuous=False, libraries=[PipelineLibrary(file=FileLibrary(path=path))])
    _OWN_DRAFTS.add(name)
    found = [p for p in w.pipelines.list_pipelines(filter=f"name LIKE '{name}'") if p.name == name]
    pid = found[0].pipeline_id if found else w.pipelines.create(**spec).pipeline_id
    if found:
        w.pipelines.update(pid, **spec)
    uid = w.pipelines.start_update(pid, validate_only=True).update_id
    t0 = time.time()
    while True:
        state = _enum(w.pipelines.get_update(pid, uid).update.state)
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        if time.time() - t0 > VALIDATE_TIMEOUT_S:
            return {"draft_pipeline": name, "pipeline_id": pid, "update_id": uid, "state": "TIMEOUT",
                    "error": "validation did not finish in time", "seconds": round(time.time() - t0, 1)}
        time.sleep(4)
    return {"draft_pipeline": name, "pipeline_id": pid, "update_id": uid, "state": state,
            "error": None if state == "COMPLETED" else _update_errors(w, pid, uid),
            "seconds": round(time.time() - t0, 1)}


def drop_draft_pipelines() -> list[str]:
    """Delete the scratch pipelines THIS process created (never another session's acme-draft-*). Returns the names."""
    w = aj._ws()
    gone: list[str] = []
    if not _OWN_DRAFTS:
        return gone
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '{DRAFT_PREFIX}%'"):
        if (p.name or "") in _OWN_DRAFTS:
            w.pipelines.delete(p.pipeline_id)
            gone.append(p.name)
    _OWN_DRAFTS.clear()
    return gone


# ----------------------------------------------------------------------------- public
def author_pipeline(need: dict, reuse: dict, cat: Optional[dict] = None) -> Optional[dict]:
    """Draft, check and validate the SDP pipeline for the RAW item(s) of this need.

    None when no item is RAW. Otherwise the contract's pipeline dict:
      name, source {path, format, columns}, tables {bronze, silver, gold: [gold]}, sql,
      expectations [{table, name, constraint_sql, action: drop|warn}],
      validated (bool), validation {update_id, state, error, draft_pipeline, attempts, history, seconds},
      planned_columns {table: [columns]} (what the metric view can read; the real tables are built later
      in studio_dev under these names).
    validated=False with validation.error when the draft still fails after MAX_REPAIRS repairs.
    """
    if not any(i.get("decision") == "RAW" for i in reuse.get("items", [])):
        return None
    cat = cat or dp.catalog()
    names = _names(need)
    feed = _feed_for(reuse, cat)
    if feed is None:
        return _result(names, None, "", None, False, {"state": "NO_FEED", "error": "no raw feed matches the RAW item",
                                                       "attempts": 0, "history": []})
    user0 = _user_prompt(need, feed, names)
    history: list[str] = []
    last_err: Optional[str] = None
    sql, parsed, validation = "", None, {}
    attempts = 0
    for attempt in range(MAX_REPAIRS + 1):
        attempts = attempt + 1
        user = user0 + (_REPAIR_TEXT.format(err=last_err) if last_err else "")
        try:
            sql = _extract_sql(dp._llm_text(_PIPE_SYSTEM, user))
        except Exception as e:  # noqa: BLE001
            last_err = f"LLM error: {dp._short_err(e)}"
            history.append(last_err)
            continue
        problems, parsed = _check(sql, feed, names)
        counts_error = None
        if not problems:
            try:  # how many feed rows each expectation would fail (warehouse counts; nothing is written)
                total, fails = _feed_counts(parsed, feed)
                problems = _drop_problems(parsed, total, fails)
            except Exception as e:  # noqa: BLE001
                counts_error = dp._short_err(e)
        if problems:
            last_err = "; ".join(problems)[:900]
            history.append(last_err)
            continue
        try:
            validation = _validate(sql)
        except Exception as e:  # noqa: BLE001
            validation = {"state": "ERROR", "error": dp._short_err(e), "update_id": None, "draft_pipeline": None}
        validation = {**validation, "attempts": attempts, "history": history, "counts_error": counts_error}
        if validation.get("state") == "COMPLETED":
            return _result(names, feed, sql, parsed, True, validation)
        last_err = validation.get("error") or validation.get("state") or "validation failed"
        history.append(last_err)
    return _result(names, feed, sql, parsed, False, {**validation, "attempts": attempts, "history": history,
                                                     "error": last_err})


def _result(names: dict, feed: Optional[dict], sql: str, parsed: Optional[dict], ok: bool,
            validation: dict) -> dict:
    cols = (parsed or {})
    planned = {}
    if cols:
        planned = {names["bronze"]: cols["bronze_cols"], names["silver"]: cols["silver_cols"],
                   names["gold"]: cols["gold_cols"]}
    return {
        "name": names["pipeline"],
        "source": {"path": feed["path"] if feed else "", "format": "csv",
                   "columns": feed["columns"] if feed else []},
        "tables": {"bronze": names["bronze"], "silver": names["silver"], "gold": [names["gold"]]},
        "sql": sql,
        "expectations": cols.get("expectations", []) if cols else [],
        "validated": bool(ok),
        "validation": {"update_id": validation.get("update_id"), "state": validation.get("state", ""),
                       "error": validation.get("error"), "draft_pipeline": validation.get("draft_pipeline"),
                       "attempts": validation.get("attempts", 0), "history": validation.get("history", []),
                       "seconds": validation.get("seconds"), "counts_error": validation.get("counts_error")},
        "planned_columns": planned,
    }
