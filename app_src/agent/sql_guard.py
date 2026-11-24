"""Parser-based SQL guard (sqlglot, Databricks dialect) for every LLM- or user-derived SQL the app runs.

Replaces the regex checks flagged by the security review. A statement passes only if:
  - it parses as exactly ONE statement and that statement is a SELECT (CTEs/UNIONs of SELECTs allowed
    only when allow_cte=True),
  - it contains no DDL/DML/command nodes anywhere,
  - every table reference is fully qualified (catalog.schema.table) and accepted by `table_ok`,
    except references to its own CTE names,
  - every function is on the allowlist (MEASURE, aggregates, plain scalar functions). Anything else
    (secret, http_request, ai_*, read_files, reflect, java_method, current_user, ...) is refused.
"""
from __future__ import annotations

from typing import Callable, Optional

import sqlglot
from sqlglot import exp

SAFE_FUNCTIONS = {
    # metric views
    "measure",
    # aggregates
    "sum", "count", "avg", "min", "max", "approx_distinct", "count_if", "stddev", "variance",
    "percentile", "median", "first", "last", "any_value",
    # logic / null handling
    "and", "or", "not", "case", "if", "coalesce", "nullif", "ifnull", "nvl", "greatest", "least",
    # numbers
    "round", "abs", "floor", "ceil", "div", "mod", "power", "sqrt", "ln", "log",
    # casting / strings
    "cast", "trycast", "try_cast", "lower", "upper", "trim", "length", "concat", "substring", "substr",
    "left", "right", "lpad", "rpad", "replace", "startswith", "endswith", "contains",
    # dates
    "current_date", "currentdate", "datediff", "date_diff", "datetrunc", "date_trunc", "timestamptrunc",
    "dateadd", "date_add", "datesub", "date_sub", "ts_or_ds_add", "tsordsadd", "ts_or_ds_to_date",
    "tsordstodate", "add_months", "addmonths", "last_day", "lastday", "year", "month", "quarter", "week",
    "weekofyear", "dayofweek", "day", "to_date", "todate", "timestampadd", "timestamp_add", "timestampdiff",
    "timestamp_diff", "timestamptrunc", "timestamp_trunc", "date_trunc_ts", "str_to_date", "strtodate", "date_format",
    "timetostr", "date",
    # window helpers
    "row_number", "rank", "dense_rank", "lag", "lead", "ntile", "percent_rank", "cume_dist",
}

FORBIDDEN_NODES = tuple(n for n in (
    getattr(exp, name, None) for name in (
        "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "AlterTable", "Command",
        "Grant", "Revoke", "Use", "Set", "Copy", "Transaction", "Commit", "Rollback", "TruncateTable",
        "LoadData", "Describe", "Show", "Pragma", "Cache", "Uncache", "Refresh",
    )) if isinstance(n, type))


def _fn_name(node: exp.Func) -> str:
    if isinstance(node, exp.Anonymous):
        return (node.name or "").lower()
    return node.sql_name().lower()


def check(sql: str, table_ok: Callable[[str], bool], *, allow_cte: bool = False,
          max_len: int = 8000) -> Optional[str]:
    """None if the SQL may run; otherwise the reason it is refused."""
    if not isinstance(sql, str) or not sql.strip():
        return "empty SQL"
    if len(sql) > max_len:
        return "SQL too long"
    try:
        stmts = [s for s in sqlglot.parse(sql, read="databricks") if s is not None]
    except Exception as e:  # noqa: BLE001
        return f"SQL does not parse: {str(e).splitlines()[0][:160]}"
    if len(stmts) != 1:
        return "exactly one statement is allowed"
    tree = stmts[0]
    if not isinstance(tree, (exp.Select, exp.Union)) or (isinstance(tree, exp.Union) and not allow_cte):
        return "only a single SELECT is allowed"
    if tree.find(*FORBIDDEN_NODES) if FORBIDDEN_NODES else None:
        return "statements other than SELECT are not allowed"
    # CTEs are refused outright: resolving CTE scope correctly is subtle (a CTE name could shadow a real
    # table), and none of the app's SQL needs one. allow_cte is kept for signature compatibility only.
    if tree.find(exp.CTE) is not None or tree.find(exp.With) is not None:
        return "WITH clauses are not allowed"
    tables = list(tree.find_all(exp.Table))
    if not tables:
        return "no table referenced"
    for t in tables:
        if not (t.catalog and t.db and t.name):
            return f"table must be fully qualified (catalog.schema.table): {t.sql(dialect='databricks')}"
        fq = f"{t.catalog}.{t.db}.{t.name}".lower()
        if not table_ok(fq):
            return f"table not allowed: {fq}"
    for f in tree.find_all(exp.Func):
        name = _fn_name(f)
        if name and name not in SAFE_FUNCTIONS:
            return f"function not allowed: {name}"
    return None


def check_expression(expr: str, *, max_len: int = 400) -> Optional[str]:
    """A boolean condition (DQ rule): parsed as `SELECT 1 FROM x WHERE <expr>`; no subqueries, no tables,
    only allowlisted functions."""
    if not isinstance(expr, str) or not expr.strip() or len(expr) > max_len:
        return "constraint is empty or too long"
    wrapped = f"SELECT 1 FROM guard_catalog.guard_schema.guard_table WHERE {expr}"
    reason = check(wrapped, lambda fq: fq == "guard_catalog.guard_schema.guard_table")
    if reason:
        return reason
    tree = sqlglot.parse_one(wrapped, read="databricks")
    if len(list(tree.find_all(exp.Select))) != 1 or len(list(tree.find_all(exp.Table))) != 1:
        return "constraint must be a simple column condition (no subqueries)"
    return None
