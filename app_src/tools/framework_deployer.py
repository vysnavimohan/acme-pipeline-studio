"""Deploy a request as a Lakeflow Framework pipeline (the 'full adoption' path).

For FILE / Auto Loader requests (spec.source.kind == "file") the agent does NOT
hand-write SDP Python. Instead it generates the **framework's metadata** — a
pipeline-bundle project of dataflowspec / schema / expectation JSON — and runs
it through the vendored, pre-deployed Lakeflow Framework engine
(framework/ → workspace `.bundle/lakeflow_framework/.../src`).

Rather than the `databricks bundle deploy` CLI (unavailable inside the agent
Job), we replicate it via the SDK: upload the generated project to a workspace
folder and create an SDP pipeline whose single library is the framework's
`dlt_pipeline` notebook, with `bundle.sourcePath` pointed at the uploaded
project. The framework reads the dataflowspecs and builds the tables.

Pipeline shape (one SDP pipeline per request per env, name `ops-{env}-{table}`):
  • Bronze dataflow  — cloudFiles (Auto Loader) from the landing Volume, 1:1,
    every column STRING (faithful landing). Multi-char delimiters are read as
    `text` and split with selectExp; single-char as `csv`.
  • Silver dataflow  — reads the Bronze Delta table, casts to the profiled
    types (JSON / dirty values stay string), applies DQ expectations.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, Callable, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as p

from agent.state import PipelineSpec
from tools.real_deployer import (
    DeployResult, _apply_column_comments, _count_rows, _dbsql_conn,
    _ensure_scheduled_job, _ensure_warehouse_running, _env_tag_from_schema,
    _pipeline_compute, _start_and_wait, _warehouse_id,
)

import os

_WS_ROOT = "/Workspace/Shared/databricks-pipeline-studio/fwpipelines"

# Spark types we will cast to in Silver (others stay string).
_CASTABLE = {"long", "int", "integer", "double", "float", "decimal", "boolean",
             "timestamp", "date"}


def framework_source_path() -> str:
    """Workspace path of the deployed framework engine `src/`.

    Prefers OPS_FRAMEWORK_SOURCE_PATH (set by the one-line deploy). Otherwise
    derives it from the current user + bundle target so the solution is
    portable to any workspace: the framework bundle deploys under the deployer's
    home at `.bundle/lakeflow_framework/<target>/current/files/src`."""
    env = os.environ.get("OPS_FRAMEWORK_SOURCE_PATH", "").strip()
    if env:
        return env.rstrip("/")
    target = os.environ.get("OPS_BUNDLE_TARGET", "dev")
    try:
        me = WorkspaceClient().current_user.me().user_name
    except Exception:
        me = "unknown"
    return (f"/Workspace/Users/{me}/.bundle/lakeflow_framework/"
            f"{target}/current/files/src")


def _pipeline_name(env_tag: str, table: str) -> str:
    return f"ops-{env_tag}-{table}"


def _regex_literal(delim: str) -> str:
    """Backslash-free regex that matches `delim` literally, by wrapping each
    char in its own character class (e.g. '@$!~' -> '[@][$][!][~]'). Survives
    the Python->JSON->Spark-SQL-literal round-trip without escaping issues.
    The few chars that aren't literal inside a class ( ] \\ ^ - ) fall back to
    a backslash escape (rare for delimiters)."""
    parts = []
    for ch in delim:
        parts.append("\\" + ch if ch in "]\\^-" else f"[{ch}]")
    return "".join(parts)


def _struct(fields: list[tuple[str, str]]) -> dict[str, Any]:
    """Spark StructType JSON from (name, type) pairs."""
    return {"type": "struct", "fields": [
        {"name": n, "type": t, "nullable": True, "metadata": {}} for n, t in fields
    ]}


# Bronze audit / operational-metadata columns — the file analogue of the legacy
# `ecf_*` columns. Source file + ingest timestamp + ingest DATE (the Liquid
# Clustering key) + a per-ingest run id. Stamped on every Bronze row by the
# consolidated file-ingest pipeline so lineage + reprocessing stay auditable.
_BRONZE_AUDIT_FIELDS: list[tuple[str, str]] = [
    ("_source_file", "string"), ("_ingested_at", "timestamp"),
    ("_ingest_date", "date"), ("_run_id", "string"),
]
# Liquid Clustering key for every Bronze table — cluster by ingest date (the
# best-practice analogue of the legacy CLUSTER BY on ECF_NSRT_TS).
_BRONZE_CLUSTER_BY: list[str] = ["_ingest_date"]


def _bronze_data_exp(fs, columns: list[dict]) -> list[str]:
    """selectExp for the DATA columns only (1:1 landing, no audit metadata).
    Reused by the deep config test so it validates pure data parsing."""
    if fs.file_format == "text":
        # Multi-char (or custom) delimiter: split the whole line. Build a
        # BACKSLASH-FREE regex by wrapping each delimiter char in its own
        # character class (e.g. "@$!~" -> "[@][$][!][~]"). This matches the
        # delimiter literally AND survives the JSON->SQL round-trip cleanly —
        # backslash escapes (\Q..\E or \$) get mangled across the 4 layers
        # (Python -> JSON file -> framework json.loads -> Spark string literal).
        # try_element_at is 1-based and returns NULL (not an error) for ragged rows.
        delim_re = _regex_literal(fs.delimiter)
        return [f"try_element_at(split(value, '{delim_re}'), {i + 1}) AS `{c['name']}`"
                for i, c in enumerate(columns)]
    # Single-char csv: the header already named the columns. Select + CAST each
    # to STRING (faithful 1:1 landing) by its header (source_name) so the query
    # schema EXACTLY matches the declared all-STRING Bronze target schema — this
    # both forces strings (no cloudFiles type inference) and excludes the
    # cloudFiles `_rescued_data` column, which a bare `*` would leak in and which
    # would make the declared schema "incompatible with the inferred schema".
    return [f"CAST(`{c.get('source_name') or c['name']}` AS STRING) AS `{c['name']}`"
            for c in columns]


def _bronze_meta_exp(run_id: str = "") -> list[str]:
    """selectExp for the audit columns appended to every Bronze row."""
    return [
        "_metadata.file_path AS _source_file",
        "current_timestamp() AS _ingested_at",
        "CAST(current_timestamp() AS DATE) AS _ingest_date",
        f"'{_sql_lit(run_id)}' AS _run_id",
    ]


def _bronze_select_exp(fs, columns: list[dict], run_id: str = "") -> list[str]:
    """selectExp that turns the raw source into the 1:1 Bronze columns + audit."""
    return _bronze_data_exp(fs, columns) + _bronze_meta_exp(run_id)


def _reader_options(fs) -> dict:
    """cloudFiles reader options for the bronze Auto Loader read. `text` for
    multi-char delimiters (split later via selectExp); `csv` for single-char."""
    reader = {"cloudFiles.format": fs.file_format if fs else "text"}
    if fs and fs.file_format == "csv":
        reader["header"] = "true" if fs.has_header else "false"
        # Faithful 1:1 landing: read every column as STRING (no type inference),
        # matching the declared all-STRING Bronze target schema.
        reader["cloudFiles.inferColumnTypes"] = "false"
        if fs.delimiter:
            reader["sep"] = fs.delimiter
    return reader


def _bronze_source_details(fs, table: str, run_id: str = "") -> dict:
    """The dataflowspec sourceDetails for the bronze cloudFiles read — the part
    that depends ONLY on the FileSource. Reused by the deploy, the code preview,
    and the read_files config test so all three always agree."""
    cols = [{"name": c.name, "source_name": getattr(c, "source_name", None)}
            for c in fs.columns] if fs and fs.columns else []
    src: dict[str, Any] = {
        "path": fs.landing_path if fs else "",
        "readerOptions": _reader_options(fs),
        "selectExp": _bronze_select_exp(fs, cols, run_id),
    }
    if fs and fs.file_format == "text":
        # text source is a single `value` string column.
        src["schemaPath"] = "source/" + f"{table}_bronze_schema.json"
        # text/Auto Loader can't skip a header line (a CSV-only option), so drop
        # it explicitly. The framework applies whereClause to the RAW source
        # (the single `value` line) BEFORE selectExp, so match on `value` — the
        # header line starts with "<firstcol><delim>". Use the ORIGINAL header
        # token (source_name), not the possibly-renamed column name, or the
        # rename would let the header row leak into Bronze.
        if fs.has_header and cols:
            header_token = cols[0].get("source_name") or cols[0]["name"]
            header_prefix = f"{header_token}{fs.delimiter}"
            src["whereClause"] = [f"value NOT LIKE '{header_prefix}%'"]
    return src


def _bronze_dataflowspec(spec: PipelineSpec, table: str, bronze_schema: str,
                         run_id: str = "") -> dict:
    return {
        "dataFlowId": f"{table}_bronze",
        "dataFlowGroup": f"{table}_bronze",
        "dataFlowType": "standard",
        "sourceSystem": spec.source.system,
        "sourceType": "cloudFiles",
        "sourceViewName": f"v_{table}_bronze",
        "sourceDetails": _bronze_source_details(spec.source.file, table, run_id),
        "mode": "stream",
        "targetFormat": "delta",
        "targetDetails": {
            "database": bronze_schema,
            "table": table,
            "type": "ST",
            "schemaPath": f"target/{table}_bronze_schema.json",
            "tableProperties": {"delta.enableChangeDataFeed": "true"},
            "clusterByColumns": _BRONZE_CLUSTER_BY,
        },
    }


def _silver_select_exp(columns: list) -> list[str]:
    exps: list[str] = []
    produced: set[str] = set()

    def _add(expr: str, name: str) -> None:
        # Skip a duplicate output name (e.g. a flattened JSON alias colliding
        # with an existing data column) — keep the first occurrence.
        if name in produced:
            return
        produced.add(name)
        exps.append(expr)

    for c in columns:
        t = (c.type or "string").lower()
        json_fields = getattr(c, "json_fields", None) or []
        if c.is_json and json_fields:
            # P1: flatten the embedded JSON into typed Silver columns — the
            # agent-selected, request-relevant fields — then keep the raw JSON
            # blob alongside for fidelity. This is the bulk hand-coding the
            # platform now generates.
            for f in json_fields:
                path = f.get("path") or f.get("alias")
                alias = f.get("alias") or path
                if not path or not alias:
                    continue
                ft = (f.get("type") or "string").lower()
                expr = f"get_json_object(`{c.name}`, '$.{path}')"
                if ft in _CASTABLE:
                    expr = f"CAST({expr} AS {ft})"
                _add(f"{expr} AS `{alias}`", alias)
            _add(f"`{c.name}`", c.name)                  # raw JSON blob retained
        elif t in _CASTABLE and not c.is_json:
            _add(f"CAST(`{c.name}` AS {t}) AS `{c.name}`", c.name)
        else:
            _add(f"`{c.name}`", c.name)
    exps += ["`_source_file`", "`_ingested_at`", "`_ingest_date`", "`_run_id`"]
    return exps


# Fields the eligibility derivation appends to the Silver projection + schema.
_ELIGIBILITY_FIELDS = [("eligible_flag", "boolean"), ("exclusion_reason", "string")]


def _silver_derived_exp(spec: PipelineSpec) -> tuple[list[str], list[tuple[str, str]]]:
    """Derived Silver columns from the request's eligibility business rule.

    The generator authors a full eligibility SELECT (eligible_flag +
    exclusion_reason) but that SQL only ever reached the PR artefact — the
    framework deploy projected a plain typed passthrough, so the deployed Silver
    silently dropped the two derived columns (the pipeline claimed an eligibility
    credit box it did not apply). This closes that gap on the DEPLOY path so
    shown == run: it reads the request's `transform.business_rules`, and when an
    `eligible_flag = <predicate>` rule is present, appends both derived columns
    as `selectExp` entries evaluated against the Bronze source view (columns are
    cast inline since Bronze is landed as strings). Dedup-to-latest is a DQ
    concern here (unique loan_id), not a projection concern, so it is not part of
    this expression.

    FO-scoped by construction: returns ([], []) when no eligibility rule exists,
    so every non-FO request keeps its exact prior 1:1 typed projection.
    """
    rules = list(getattr(spec.transform, "business_rules", None) or [])
    rule = next((r for r in rules if isinstance(r, str)
                 and r.strip().lower().startswith("eligible_flag")), None)
    if not rule:
        return [], []

    # Parse thresholds from the rule; default to the FO credit box (620/0.95/30).
    def _num(pattern: str, default):
        m = re.search(pattern, rule, re.IGNORECASE)
        return m.group(1) if m else str(default)

    score_min = _num(r"credit_score\s*>=\s*([0-9.]+)", 620)
    ltv_max = _num(r"ltv\s*<=\s*([0-9.]+)", 0.95)
    arrears_max = _num(r"arrears_days\s*<=\s*([0-9.]+)", 30)

    score = "CAST(`credit_score` AS INT)"
    ltv = "CAST(`ltv` AS DOUBLE)"
    arrears = "CAST(`arrears_days` AS INT)"
    eligible = (f"({score} >= {score_min} AND {ltv} <= {ltv_max} "
                f"AND {arrears} <= {arrears_max})")
    exclusion = (
        "CASE "
        f"WHEN {eligible} THEN NULL "
        f"WHEN {score} < {score_min} THEN 'low_score' "
        f"WHEN {ltv} > {ltv_max} THEN 'high_ltv' "
        f"WHEN {arrears} > {arrears_max} THEN 'arrears' "
        "ELSE NULL END"
    )
    exps = [f"{eligible} AS `eligible_flag`", f"{exclusion} AS `exclusion_reason`"]
    return exps, list(_ELIGIBILITY_FIELDS)


def sample_json_columns(fs, *, limit: int = 30,
                        log: Optional[Callable[[str], None]] = None) -> dict:
    """Sample a file dataset's JSON columns from the LANDING file (via read_files,
    so it works at generation time before Bronze exists) — gives the agent the
    candidate keys + a sample value per JSON column to pick request-relevant
    fields for Silver flattening. Returns {json_col: {"keys": [...], "sample": ".."}}.
    Best-effort: returns {} on any failure (flatten then degrades to raw string)."""
    import json as _json
    if not fs or getattr(fs, "file_format", "text") != "text":
        return {}
    cols = [{"name": c.name, "source_name": getattr(c, "source_name", None)}
            for c in (fs.columns or [])]
    json_names = [c.name for c in (fs.columns or []) if getattr(c, "is_json", False)]
    glob = (getattr(fs, "landing_path", "") or "").strip()
    if not json_names or not glob:
        return {}
    data_exp = _bronze_data_exp(fs, cols)
    idx = {c["name"]: i for i, c in enumerate(cols)}
    proj = [data_exp[idx[n]] for n in json_names if n in idx and idx[n] < len(data_exp)]
    if not proj:
        return {}
    where = ""
    if fs.has_header and cols:
        header_token = cols[0].get("source_name") or cols[0]["name"]
        where = f" WHERE value NOT LIKE '{_sql_lit(header_token + fs.delimiter)}%'"
    query = (f"SELECT {', '.join(proj)} FROM read_files('{_sql_lit(glob)}', "
             f"format => 'text'){where} LIMIT {int(limit)}")
    try:
        client = WorkspaceClient(); wid = _warehouse_id()
        _ensure_warehouse_running(client, wid, log or (lambda m: None))
        with _dbsql_conn(wid) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                names = [d[0] for d in cur.description]
    except Exception:
        return {}
    out: dict = {}
    for ci, n in enumerate(names):
        keys: set = set()
        sample = ""
        for r in rows:
            v = r[ci] if ci < len(r) else None
            if not v:
                continue
            try:
                j = _json.loads(v)
            except Exception:
                continue
            obj = j[0] if isinstance(j, list) and j and isinstance(j[0], dict) else j
            if isinstance(obj, dict):
                keys.update(str(k) for k in obj.keys())
                if not sample:
                    sample = str(v)[:600]
        out[n] = {"keys": sorted(keys), "sample": sample}
    return out


def _silver_dataflowspec(spec: PipelineSpec, table: str, bronze_schema: str,
                         silver_schema: str, target_table: str,
                         select_exp: Optional[list] = None) -> dict:
    """`select_exp` overrides the projection derived from a file source's declared
    columns — needed for CDC/database sources, which have no `source.file` and so
    must take their projection from the Bronze table's real schema instead."""
    cols = spec.source.file.columns if spec.source.file else []
    base_exp = select_exp if select_exp is not None else _silver_select_exp(cols)
    # Apply the request's eligibility derivation (eligible_flag/exclusion_reason)
    # on the DEPLOY path so the deployed Silver matches the authored transform.
    # Insert the derived columns BEFORE the trailing audit columns so the layout
    # reads data → derived → audit. No-op for non-eligibility requests.
    derived_exp, _ = _silver_derived_exp(spec)
    if derived_exp:
        audit = {"`_source_file`", "`_ingested_at`", "`_ingest_date`", "`_run_id`"}
        head = [e for e in base_exp if e not in audit]
        tail = [e for e in base_exp if e in audit]
        base_exp = head + derived_exp + tail
    return {
        "dataFlowId": f"{target_table}_silver",
        "dataFlowGroup": f"{target_table}_silver",
        "dataFlowType": "standard",
        "sourceSystem": spec.source.system,
        "sourceType": "delta",
        "sourceViewName": f"v_{target_table}_silver",
        "sourceDetails": {
            "database": bronze_schema,
            "table": table,
            "cdfEnabled": False,
            "selectExp": base_exp,
        },
        # Streaming table (not MV): streams from the Bronze table via the
        # source view — matches the Bronze pattern and avoids the MV path's
        # sourceView/sqlPath requirement. Bronze is append-only (Auto Loader),
        # so a streaming Silver is a clean typed 1:1 projection.
        "mode": "stream",
        "targetFormat": "delta",
        "targetDetails": {
            "database": silver_schema,
            "table": target_table,
            "type": "ST",
        },
        "dataQualityExpectationsEnabled": True,
        "dataQualityExpectationsPath": f"{target_table}_silver_dqe.json",
    }


def _expectations(spec: PipelineSpec) -> dict:
    """Silver DQ expectations from the spec's validation rules AND business rules.

    Business rules belong here, not in a code comment. They used to be emitted as
    `#` lines described as "applied informationally", so a pipeline claimed to
    honour a rule the requester had typed and silently did not — the generated
    code misrepresented itself. `expect_or_drop` makes a compiled rule actually
    remove the rows it says it removes, and the drop shows up in the pipeline's
    DQ metrics instead of being invisible.
    """
    from tools.real_deployer import compile_business_rule
    rules = []
    seen = set()
    for col in (spec.validation.not_null or []) + (spec.source.primary_key or []):
        if col and col not in seen:
            seen.add(col)
            rules.append({"name": f"{col} not null", "constraint": f"`{col}` IS NOT NULL",
                          "tag": "Validity"})
    for r in ((spec.transform.business_rules or []) if spec.transform else []):
        pred = compile_business_rule(r)
        if pred and pred not in seen:
            seen.add(pred)
            rules.append({"name": r[:120], "constraint": pred, "tag": "BusinessRule"})
    if not rules:
        rules.append({"name": "row present", "constraint": "TRUE", "tag": "Validity"})
    return {"expect_or_drop": rules}


def render_bronze_files(spec: PipelineSpec, dataset: str, bronze_schema: str,
                        run_id: str = "") -> dict[str, str]:
    """Bronze (Auto Loader / cloudFiles) dataflow for a DATASET, keyed by the
    landing dataset name — so it's a SHARED Bronze streaming table reused by
    every request that ingests this dataset. Lives in the consolidated
    file-ingest pipeline (one per env)."""
    fs = spec.source.file
    cols = fs.columns if fs else []
    bronze = _bronze_dataflowspec(spec, dataset, bronze_schema, run_id)
    bronze_target_fields = [(c.name, "string") for c in cols] + _BRONZE_AUDIT_FIELDS
    g = f"dataflows/{dataset}"
    return {
        f"{g}/dataflowspec/{dataset}_bronze_main.json": json.dumps(bronze, indent=2),
        f"{g}/schemas/source/{dataset}_bronze_schema.json": json.dumps(_struct([("value", "string")]), indent=2),
        f"{g}/schemas/target/{dataset}_bronze_schema.json": json.dumps(_struct(bronze_target_fields), indent=2),
    }


def render_silver_files(spec: PipelineSpec, dataset: str, bronze_schema: str,
                        silver_schema: str, target_table: str) -> dict[str, str]:
    """Silver dataflow for a REQUEST — reads the SHARED dataset Bronze table
    (sourceType=delta) and writes the per-request typed Silver table + DQ."""
    fs = spec.source.file
    cols = fs.columns if fs else []
    silver = _silver_dataflowspec(spec, dataset, bronze_schema, silver_schema, target_table)
    silver_target_fields: list[tuple[str, str]] = []
    _seen: set[str] = set()

    def _sf(name: str, typ: str) -> None:
        if name in _seen:
            return
        _seen.add(name)
        silver_target_fields.append((name, typ))

    for c in cols:                                  # mirror _silver_select_exp order/names
        jf = getattr(c, "json_fields", None) or []
        if c.is_json and jf:
            for f in jf:
                alias = f.get("alias") or f.get("path")
                if not alias:
                    continue
                ft = (f.get("type") or "string").lower()
                _sf(alias, ft if ft in _CASTABLE else "string")
            _sf(c.name, "string")                   # raw JSON blob
        else:
            t = (c.type or "string").lower()
            _sf(c.name, t if (t in _CASTABLE and not c.is_json) else "string")
    # Derived eligibility columns (if the request declares an eligibility rule)
    # must be in the target schema too, or the ST projection and the declared
    # schema disagree. Added before the audit columns to mirror the selectExp.
    _, derived_fields = _silver_derived_exp(spec)
    for name, typ in derived_fields:
        _sf(name, typ)
    silver_target_fields += [("_source_file", "string"), ("_ingested_at", "timestamp"),
                             ("_ingest_date", "date"), ("_run_id", "string")]
    g = f"dataflows/{target_table}"
    return {
        f"{g}/dataflowspec/{target_table}_silver_main.json": json.dumps(silver, indent=2),
        f"{g}/schemas/target/{target_table}_silver_schema.json": json.dumps(_struct(silver_target_fields), indent=2),
        f"{g}/expectations/{target_table}_silver_dqe.json": json.dumps(_expectations(spec), indent=2),
    }


def render_cdc_silver_files(spec: PipelineSpec, bronze_table: str, bronze_schema: str,
                            silver_schema: str, target_table: str,
                            bronze_columns: list) -> dict[str, str]:
    """Silver dataflow for a CDC / database-sourced request, in the SAME framework
    bundle shape (and root) as the file path.

    Why this exists: the CDC path used to deploy its Silver through a separate
    module writing a DIFFERENT workspace root under the SAME pipeline name. When
    the Curate step later merged a Gold MV into "the request's pipeline", it
    repointed that pipeline at the framework root — which held only the Gold
    dataflow — so the Silver definition vanished, the MV could not resolve its
    upstream, and the already-materialised Silver TABLE was dropped with the
    pipeline. One request must mean one pipeline AND one root; this makes both
    source kinds identical so that class of bug cannot recur.

    The projection comes from the Bronze table's real schema rather than declared
    file columns, because a database source has no file spec. Bronze is landed
    faithfully, so a 1:1 typed passthrough is the correct Silver here.
    """
    names = [c for c, _ in bronze_columns
             if c and not c.startswith("_")]           # audit cols re-added below
    # Audit columns mirror the file path's, so both source kinds produce the same
    # Silver shape. `_source_file` is NULL for a database source — there is no
    # file — but the column stays so downstream Gold SQL is source-agnostic.
    select_exp = [f"`{n}` AS `{n}`" for n in names] + [
        "CAST(NULL AS STRING) AS _source_file",
        "current_timestamp() AS _ingested_at",
        "current_date() AS _ingest_date",
        "CAST(NULL AS STRING) AS _run_id",
    ]
    silver = _silver_dataflowspec(spec, bronze_table, bronze_schema, silver_schema,
                                  target_table, select_exp=select_exp)
    fields = [(n, t) for n, t in bronze_columns if n and not n.startswith("_")]
    fields += [("_source_file", "string"), ("_ingested_at", "timestamp"),
               ("_ingest_date", "date"), ("_run_id", "string")]
    g = f"dataflows/{target_table}"
    return {
        f"{g}/dataflowspec/{target_table}_silver_main.json": json.dumps(silver, indent=2),
        f"{g}/schemas/target/{target_table}_silver_schema.json": json.dumps(_struct(fields), indent=2),
        f"{g}/expectations/{target_table}_silver_dqe.json": json.dumps(_expectations(spec), indent=2),
    }


# =====================================================================
# Gold layer (P2) — fact / dimension MATERIALIZED VIEWs from Silver
# =====================================================================

def render_gold_files(fact: str, sql_statement: str, gold_schema: str, *,
                      source_system: str = "udp",
                      cluster_by: Optional[list] = None, comment: str = "",
                      refresh_policy: str = "full",
                      not_null: Optional[list] = None) -> dict[str, str]:
    """A Gold fact/dim as a Lakeflow Framework MATERIALIZED VIEW driven by an
    agent-generated SQL statement (joins / aggregations / derived measures over
    the Silver tables, read by full name). The framework runs
    `spark.sql(sqlStatement)` → MV. Shape per
    framework/src/schemas/spec_materialized_views.json:
    dataFlowType=materialized_view, materializedViews[<fact>]={sqlStatement,
    tableDetails{database, clusterByColumns, comment, tableProperties}, ...}."""
    td: dict[str, Any] = {"database": gold_schema,
                          "tableProperties": {"delta.enableChangeDataFeed": "true"}}
    if cluster_by:
        td["clusterByColumns"] = cluster_by
    if comment:
        td["comment"] = comment
    mv_def: dict[str, Any] = {"sqlStatement": sql_statement, "tableDetails": td,
                              "refreshPolicy": refresh_policy}
    files: dict[str, str] = {}
    rules = [{"name": f"{c} not null", "constraint": f"`{c}` IS NOT NULL", "tag": "Validity"}
             for c in (not_null or []) if c]
    if rules:
        mv_def["dataQualityExpectationsEnabled"] = True
        mv_def["dataQualityExpectationsPath"] = f"{fact}_gold_dqe.json"
        files[f"dataflows/gold_{fact}/expectations/{fact}_gold_dqe.json"] = \
            json.dumps({"expect_or_drop": rules}, indent=2)
    mv = {"dataFlowId": f"{fact}_gold", "dataFlowGroup": f"{fact}_gold",
          "dataFlowType": "materialized_view", "sourceSystem": source_system,
          "materializedViews": {fact: mv_def}}
    files[f"dataflows/gold_{fact}/dataflowspec/{fact}_gold_main.json"] = json.dumps(mv, indent=2)
    return files


def render_pipeline_bundle(spec: PipelineSpec, request_id: str, *, env_tag: str,
                           bronze_schema: str, silver_schema: str) -> dict[str, str]:
    """Full project (Bronze + Silver) for a request — used for the PR ARTIFACTS
    so the reviewer sees the whole dataset → Bronze → Silver shape. The live
    deploy SPLITS these: Bronze runs in the consolidated `ops-<env>-file-ingest`
    pipeline (shared per dataset); Silver runs in the per-request pipeline."""
    fs = spec.source.file
    dataset = fs.dataset if fs else spec.target.table
    target_table = spec.target.table
    files = {**render_bronze_files(spec, dataset, bronze_schema),
             **render_silver_files(spec, dataset, bronze_schema, silver_schema, target_table)}
    files["pipeline_configs/ENV_substitutions.json"] = json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)
    files["README.md"] = (
        f"# Generated pipeline bundle — {target_table}\n\nRequest `{request_id}`. "
        f"Lakeflow Framework metadata.\n\n"
        f"- **Bronze** (shared, Auto Loader 1:1): `{bronze_schema}.{dataset}` from landing "
        f"`{fs.landing_path if fs else '?'}` — materialised once in the consolidated "
        f"`ops-{env_tag}-file-ingest` pipeline and reused across requests.\n"
        f"- **Silver** (per-request, typed + DQ): `{silver_schema}.{target_table}` reads the "
        f"shared Bronze.\n")
    return files


def _upload_project(client: WorkspaceClient, files: dict[str, str], root: str,
                    log: Callable[[str], None]) -> None:
    from databricks.sdk.service.workspace import ImportFormat
    try:
        client.workspace.delete(root, recursive=True)
    except Exception:
        pass
    for rel, content in files.items():
        full = f"{root}/{rel}"
        parent = full.rsplit("/", 1)[0]
        try:
            client.workspace.mkdirs(parent)
        except Exception:
            pass
        client.workspace.upload(path=full, content=content.encode("utf-8"),
                                format=ImportFormat.AUTO, overwrite=True)
    log(f"Uploaded framework pipeline bundle ({len(files)} files) to `{root}`.")


def _upload_files_additive(client: WorkspaceClient, files: dict[str, str], root: str,
                           log: Callable[[str], None]) -> None:
    """Upload files into `root` WITHOUT deleting what's already there — used for
    the consolidated file-ingest project so each new dataset's Bronze dataflow
    is added ALONGSIDE the existing ones (the framework reads every dataflow in
    the project)."""
    from databricks.sdk.service.workspace import ImportFormat
    for rel, content in files.items():
        full = f"{root}/{rel}"
        parent = full.rsplit("/", 1)[0]
        try:
            client.workspace.mkdirs(parent)
        except Exception:
            pass
        client.workspace.upload(path=full, content=content.encode("utf-8"),
                                format=ImportFormat.AUTO, overwrite=True)
    log(f"Merged {len(files)} metadata file(s) into the consolidated project `{root}`.")


def _ensure_pipeline(client: WorkspaceClient, name: str, catalog: str,
                     default_schema: str, bundle_src: str, fw_src: str,
                     log: Callable[[str], None], direct_publish: bool = False,
                     will_own: Optional[list] = None) -> str:
    """Create/update the SDP pipeline that runs the framework against our bundle.

    direct_publish=True uses `schema=` (UC direct-publishing mode) so ONE pipeline
    can write to MULTIPLE schemas in the catalog — needed for the per-request
    pipeline that owns both Silver (silver_schema) and Gold (gold_schema). Legacy
    `target=` binds a pipeline to a single schema (fine for the bronze ingest).
    A mode switch can't be done in-place, so an existing single-schema pipeline of
    the same name is recreated when direct_publish is requested.

    `will_own`: fully-qualified tables the bundle at `bundle_src` declares. If a
    mode switch forces a re-create, anything the OLD pipeline owns that is not in
    this list would be destroyed — so the delete is refused. Pass it whenever the
    caller knows its targets; omitting it makes the guard maximally strict (any
    owned table blocks the delete), which is the safe default."""
    host = client.config.host.rstrip("/")
    cfg = {
        "bundle.sourcePath": bundle_src,
        "framework.sourcePath": fw_src,
        "workspace.host": host,
    }
    libraries = [p.PipelineLibrary(notebook=p.NotebookLibrary(path=f"{fw_src}/dlt_pipeline"))]
    schema_kw = {"schema": default_schema} if direct_publish else {"target": default_schema}
    existing = None
    legacy_mode = False
    for pipe in client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if (pipe.name or "") == name:
            existing = pipe.pipeline_id
            try:                      # detect a single-schema (legacy `target`) pipeline
                spec = client.pipelines.get(existing).spec
                legacy_mode = bool(getattr(spec, "target", None)) and not getattr(spec, "schema", None)
            except Exception:         # noqa: BLE001
                legacy_mode = False
            break
    if existing and direct_publish and legacy_mode:
        log(f"Recreating `{name}` in direct-publishing mode (was bound to one schema).")
        # GUARDED: this delete destroyed a materialised Silver table once, because
        # the replacement pipeline was built from a DIFFERENT bundle root and so
        # never re-declared it. `will_own` is what the replacement will re-create.
        _delete_pipeline_guarded(client, existing,
                                 reason=f"switching `{name}` to direct-publishing mode",
                                 log=log, recreated_by=will_own)
        existing = None
    if existing:
        log(f"Updating framework SDP pipeline `{name}` ({existing[:8]}…)")
        client.pipelines.update(pipeline_id=existing, name=name, catalog=catalog,
                                **_pipeline_compute(), channel="CURRENT",
                                libraries=libraries, configuration=cfg, **schema_kw)
        return existing
    log(f"Creating framework SDP pipeline `{name}`" + (" (direct-publishing)" if direct_publish else ""))
    created = client.pipelines.create(name=name, catalog=catalog,
                                      **_pipeline_compute(), channel="CURRENT",
                                      libraries=libraries, configuration=cfg, **schema_kw)
    return created.pipeline_id


class PipelineOwnsTablesError(RuntimeError):
    """Raised instead of deleting a pipeline that still owns materialised tables."""


def _tables_owned_by(client: WorkspaceClient, pipeline_id: str) -> list[str]:
    """Materialised tables currently owned by `pipeline_id`, by asking UC.

    Best-effort BY DESIGN, but note which way it fails: an API hiccup returns []
    and the guard then allows a delete. That is the pragmatic choice — a hard
    failure here would block every legitimate re-create — so the guard is a
    backstop against the known destructive path, not a proof of safety.
    """
    owned: list[str] = []
    try:
        spec = client.pipelines.get(pipeline_id).spec
        schemas = [s for s in (getattr(spec, "schema", None),
                               getattr(spec, "target", None)) if s]
        catalog = getattr(spec, "catalog", None)
        if not catalog or not schemas:
            return []
        for sch in schemas:
            for t in client.tables.list(catalog_name=catalog, schema_name=sch):
                if getattr(t, "pipeline_id", None) == pipeline_id:
                    owned.append(t.full_name)
    except Exception:  # noqa: BLE001 — see docstring
        return []
    return owned


def _delete_pipeline_guarded(client: WorkspaceClient, pipeline_id: str, *,
                             reason: str, log: Callable[[str], None],
                             recreated_by: Optional[list] = None) -> None:
    """Delete a pipeline ONLY when doing so cannot orphan materialised data.

    This guard exists because of a real data loss: a Gold deploy repointed a
    request's pipeline at a different source root and, because the mode differed,
    deleted and recreated it — dropping a Silver table that had just materialised
    1,384 rows. The Gold MV then could not resolve its own upstream. Nothing in
    the code said "this delete destroys tables"; it read as a harmless re-create.

    `recreated_by` is the caller's promise: the tables it will re-declare in the
    replacement pipeline. Anything owned but NOT in that list would be orphaned,
    so we refuse and explain rather than proceed. Failing loud is the point —
    silently dropping a user's data is the worst possible outcome here.
    """
    owned = _tables_owned_by(client, pipeline_id)
    promised = {t.lower() for t in (recreated_by or [])}
    orphaned = [t for t in owned if t.lower() not in promised]
    if orphaned:
        raise PipelineOwnsTablesError(
            f"refusing to delete pipeline {pipeline_id[:8]}… ({reason}): it still owns "
            f"{len(orphaned)} materialised table(s) that the replacement would not "
            f"re-create, so deleting it would DESTROY them — "
            f"{', '.join(orphaned[:5])}{' …' if len(orphaned) > 5 else ''}. "
            "Deploy the Silver/Gold definitions into the SAME bundle root so one "
            "pipeline owns the whole request, or drop the tables deliberately first.")
    if owned:
        log(f"  (deleting {pipeline_id[:8]}… — its {len(owned)} table(s) are re-created "
            f"by the replacement)")
    client.pipelines.delete(pipeline_id=pipeline_id)


def _release_conflicting_tables(client: WorkspaceClient, target_fqns: list,
                                keep_pipeline_id: str, wid: str,
                                log: Callable[[str], None]) -> None:
    """A UC table can only be owned by ONE pipeline. Before the request pipeline
    claims its Silver/Gold tables, free any target that is currently owned by a
    DIFFERENT pipeline — a stale per-table pipeline from a pre-consolidation build,
    the old shared gold pipeline, or another request. We delete that owner pipeline
    and drop the (now-unmanaged) table so the request pipeline can recreate it from
    Bronze in the same update. On a fresh workspace the tables don't exist yet, so
    this is a no-op — it only matters for migration / re-runs."""
    for fq in target_fqns:
        try:
            owner = getattr(client.tables.get(fq), "pipeline_id", None)
        except Exception:                       # noqa: BLE001 — table doesn't exist yet
            continue
        if not owner or owner == keep_pipeline_id:
            continue
        log(f"Releasing `{fq}` from pipeline {owner[:8]}… so the request pipeline can own it.")
        try:
            client.pipelines.delete(pipeline_id=owner)
        except Exception as e:                   # noqa: BLE001
            log(f"  (could not delete old owner pipeline {owner[:8]}: {str(e)[:120]})")
        try:
            with _dbsql_conn(wid) as c:
                c.cursor().execute(f"DROP TABLE IF EXISTS {fq}")
        except Exception as e:                   # noqa: BLE001
            log(f"  (could not drop {fq}: {str(e)[:120]})")


_INGEST_ROOT = "/Workspace/Shared/databricks-pipeline-studio/fwingest"


def _ingest_pipeline_name(env_tag: str) -> str:
    """ONE consolidated Auto Loader Bronze-ingestion pipeline per env (the file
    analogue of the CDC `ops-<env>-source-ingest`). It owns one Bronze streaming
    table PER landing dataset, shared/reused across every request that uses the
    dataset — decoupling ingestion from the per-request Silver pipelines."""
    return f"ops-{env_tag}-file-ingest"


def ingest_file_to_bronze(spec: PipelineSpec, *, catalog: str, bronze_schema: str,
                          env_tag: str, log: Callable[[str], None],
                          force: bool = False) -> dict:
    """Ensure the dataset's Bronze streaming table is materialised in the
    consolidated `ops-<env>-file-ingest` pipeline (Auto Loader / cloudFiles),
    REUSING it when already populated. Two requests on the same dataset share
    one ingestion. Returns {ingestion_pipeline_name, bronze_table, bronze_rows,
    ran, reused, dataset, ...}."""
    t0 = time.time()
    fs = spec.source.file
    dataset = fs.dataset
    client = WorkspaceClient()
    name = _ingest_pipeline_name(env_tag)
    bronze_fq = f"{catalog}.{bronze_schema}.{dataset}"
    host = client.config.host.rstrip("/")
    wid = _warehouse_id()

    if not force:
        rows = -1
        try:
            _ensure_warehouse_running(client, wid, log)
            rows = _count_rows(bronze_fq, wid)
        except Exception:
            rows = -1
        if rows > 0:
            log(f"Bronze `{bronze_fq}` already populated ({rows:,} rows) — reusing; no re-ingest.")
            return {"ingestion_pipeline_name": name, "bronze_table": bronze_fq,
                    "bronze_rows": rows, "ran": False, "reused": True, "dataset": dataset}

    run_id = uuid.uuid4().hex
    files = render_bronze_files(spec, dataset, bronze_schema, run_id=run_id)
    files["pipeline_configs/ENV_substitutions.json"] = json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)
    bundle_src = f"{_INGEST_ROOT}/{env_tag}/src"
    _upload_files_additive(client, files, bundle_src, log)

    fw_src = framework_source_path()
    pipeline_id = _ensure_pipeline(client, name, catalog, bronze_schema, bundle_src, fw_src, log)
    log(f"Consolidated file-ingest pipeline `{name}` — materialising Bronze for dataset `{dataset}`.")
    update_id = _start_and_wait(client, pipeline_id, streaming=False, log=log)

    _ensure_warehouse_running(client, wid, log)
    bronze_rows = _count_rows(bronze_fq, wid)
    log(f"Bronze `{bronze_fq}` = {bronze_rows:,} rows (consolidated `{name}`, {time.time() - t0:.0f}s).")
    return {"ingestion_pipeline_id": pipeline_id, "ingestion_pipeline_name": name,
            "ingestion_pipeline_url": f"{host}/pipelines/{pipeline_id}",
            "bronze_table": bronze_fq, "bronze_rows": bronze_rows,
            "ran": True, "reused": False, "dataset": dataset, "update_id": update_id,
            "run_id": run_id}


def ingest_files_to_bronze(specs: list, *, catalog: str, bronze_schema: str,
                           env_tag: str, log: Callable[[str], None],
                           force: bool = False) -> dict:
    """Ingest MULTIPLE datasets to Bronze in ONE consolidated-pipeline update.

    Renders + additively merges every dataset's Bronze dataflowspec into the
    shared `ops-<env>-file-ingest` project, then runs the pipeline ONCE (a single
    update materialises every dataset's Bronze table). Datasets already populated
    are reused (skipped). Returns {datasets:[...per-dataset dicts...], ran, ...}.
    Each spec must be a single-dataset PipelineSpec (spec.source.file set)."""
    t0 = time.time()
    client = WorkspaceClient()
    name = _ingest_pipeline_name(env_tag)
    wid = _warehouse_id()
    host = client.config.host.rstrip("/")
    bundle_src = f"{_INGEST_ROOT}/{env_tag}/src"
    try:
        _ensure_warehouse_running(client, wid, log)
    except Exception:
        pass

    results: list[dict] = []
    to_build: list[tuple] = []
    pipeline_id = None
    for spec in specs:
        ds = spec.source.file.dataset
        bronze_fq = f"{catalog}.{bronze_schema}.{ds}"
        rows = -1
        if not force:
            try:
                rows = _count_rows(bronze_fq, wid)
            except Exception:
                rows = -1
        if rows > 0:
            log(f"Bronze `{bronze_fq}` already populated ({rows:,} rows) — reusing.")
            results.append({"ingestion_pipeline_name": name, "bronze_table": bronze_fq,
                            "bronze_rows": rows, "ran": False, "reused": True, "dataset": ds})
            continue
        run_id = uuid.uuid4().hex
        files = render_bronze_files(spec, ds, bronze_schema, run_id=run_id)
        _upload_files_additive(client, files, bundle_src, log)
        to_build.append((ds, bronze_fq, run_id))

    if to_build:
        _upload_files_additive(
            client, {"pipeline_configs/ENV_substitutions.json":
                     json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)},
            bundle_src, log)
        fw_src = framework_source_path()
        pipeline_id = _ensure_pipeline(client, name, catalog, bronze_schema, bundle_src, fw_src, log)
        log(f"Consolidated file-ingest `{name}` — materialising Bronze for "
            f"{len(to_build)} dataset(s): {', '.join(d for d, _, _ in to_build)} (ONE update).")
        update_id = _start_and_wait(client, pipeline_id, streaming=False, log=log)
        try:
            _ensure_warehouse_running(client, wid, log)
        except Exception:
            pass
        for ds, bronze_fq, run_id in to_build:
            try:
                rows = _count_rows(bronze_fq, wid)
            except Exception:
                rows = -1
            log(f"Bronze `{bronze_fq}` = {rows:,} rows.")
            results.append({"ingestion_pipeline_id": pipeline_id, "ingestion_pipeline_name": name,
                            "ingestion_pipeline_url": f"{host}/pipelines/{pipeline_id}",
                            "bronze_table": bronze_fq, "bronze_rows": rows, "ran": True,
                            "reused": False, "dataset": ds, "update_id": update_id, "run_id": run_id})

    # Resolve the consolidated ingest pipeline id even when every dataset was
    # reused (nothing built this run) — it always exists by name — so the UI can
    # always deep-link to the ingestion pipeline, not just on the "ran" path.
    if pipeline_id is None:
        try:
            for pipe in client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
                if (pipe.name or "") == name:
                    pipeline_id = pipe.pipeline_id
                    break
        except Exception:
            pipeline_id = None

    out = {"datasets": results, "ingestion_pipeline_name": name,
           "ran": bool(to_build), "duration_seconds": time.time() - t0}
    if pipeline_id:
        out["ingestion_pipeline_id"] = pipeline_id
        out["ingestion_pipeline_url"] = f"{host}/pipelines/{pipeline_id}"
        # Backfill into reused entries so every dataset row is linkable too.
        for r in results:
            r.setdefault("ingestion_pipeline_id", pipeline_id)
            r.setdefault("ingestion_pipeline_url", f"{host}/pipelines/{pipeline_id}")
    return out


def deploy_silver_via_framework(spec: PipelineSpec, *, catalog: str, bronze_schema: str,
                                silver_schema: str, request_id: str,
                                log: Callable[[str], None],
                                create_schedule: bool = False) -> DeployResult:
    """Per-request Silver pipeline `ops-<env>-<target>` — reads the SHARED dataset
    Bronze (delta) and writes the typed Silver table (+ DQ + optional scheduled
    Job). Bronze must already be ingested (ingest_file_to_bronze)."""
    t0 = time.time()
    env_tag = _env_tag_from_schema(silver_schema)
    fs = spec.source.file
    dataset = fs.dataset
    target_table = spec.target.table
    name = _pipeline_name(env_tag, target_table)
    client = WorkspaceClient()

    log(f"Framework Silver deploy ({env_tag}) — `{silver_schema}.{target_table}` from "
        f"shared Bronze `{bronze_schema}.{dataset}` (typed + DQ expectations).")
    files = render_silver_files(spec, dataset, bronze_schema, silver_schema, target_table)
    files["pipeline_configs/ENV_substitutions.json"] = json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)
    files["README.md"] = (f"# Silver pipeline — {target_table}\n\nRequest `{request_id}`. "
                          f"Reads the shared Bronze `{bronze_schema}.{dataset}` → typed Silver "
                          f"(`{silver_schema}.{target_table}`) with DQ expectations.\n")
    root = f"{_WS_ROOT}/{env_tag}/{target_table}"
    bundle_src = f"{root}/src"
    _upload_project(client, files, bundle_src, log)

    fw_src = framework_source_path()
    pipeline_id = _ensure_pipeline(client, name, catalog, silver_schema, bundle_src, fw_src, log)
    host = client.config.host.rstrip("/")
    update_id = _start_and_wait(client, pipeline_id, streaming=False, log=log)

    wid = _warehouse_id()
    _ensure_warehouse_running(client, wid, log)
    bronze_fq = f"{catalog}.{bronze_schema}.{dataset}"
    silver_fq = f"{catalog}.{silver_schema}.{target_table}"
    try:
        bronze_rows = _count_rows(bronze_fq, wid)
    except Exception:
        bronze_rows = -1
    silver_rows = _count_rows(silver_fq, wid)
    log(f"Silver `{silver_fq}` = {silver_rows:,} rows (from shared Bronze `{bronze_fq}` = {bronze_rows:,}).")

    job_id = job_name = cron = ""
    if create_schedule:
        job_id, job_name, cron = _ensure_scheduled_job(
            client, spec, pipeline_id, name, catalog, silver_schema, log)

    return DeployResult(
        catalog=catalog,
        bronze_schema_fq=f"{catalog}.{bronze_schema}",
        silver_schema_fq=f"{catalog}.{silver_schema}",
        bronze_tables=[bronze_fq], bronze_counts={bronze_fq: bronze_rows},
        silver_table=silver_fq, silver_row_count=silver_rows,
        pipeline_id=pipeline_id, pipeline_name=name, update_id=update_id,
        pipeline_url=f"{host}/pipelines/{pipeline_id}", sdp_workspace_path=bundle_src,
        job_id=job_id, job_name=job_name, job_url=(f"{host}/jobs/{job_id}" if job_id else ""),
        job_schedule_cron=cron, bronze_strategy="lakeflow_framework_autoloader_consolidated",
        duration_seconds=time.time() - t0,
    )


def deploy_via_framework(spec: PipelineSpec, *, catalog: str, bronze_schema: str,
                         silver_schema: str, request_id: str,
                         log: Callable[[str], None],
                         create_schedule: bool = False) -> DeployResult:
    """Convenience wrapper: ensure the SHARED Bronze (consolidated ingest) then
    deploy the per-request Silver. The agent nodes call the two steps separately
    for clearer tracker events; kept for any single-call caller."""
    env_tag = _env_tag_from_schema(silver_schema)
    ingest_file_to_bronze(spec, catalog=catalog, bronze_schema=bronze_schema,
                          env_tag=env_tag, log=log)
    return deploy_silver_via_framework(spec, catalog=catalog, bronze_schema=bronze_schema,
                                       silver_schema=silver_schema, request_id=request_id,
                                       log=log, create_schedule=create_schedule)


def _apply_uc_comments(fq_table: str, spec, column_comments: dict | None,
                       table_comment: str | None, wid: str, log) -> None:
    """Document a framework-built table in Unity Catalog, AFTER materialisation.

    Why this exists: there are two deploy paths, and only one documented itself.
    The CDC/warehouse path renders a Python SDP file that attaches comments with
    `F.col(c).alias(c, metadata={"comment": …})`, and those DO reach UC —
    `sales_summary` has 11 of 11 columns commented. The FILE/Auto-Loader path
    goes through the vendored Lakeflow Framework's YAML dataflows and never touches
    that template, so the generator's column comments (78 of them on the four-feed
    example) were emitted into the PR artefact and then **dropped on the floor**.
    Confirmed by direct SQL: 0 of 38 columns commented, and no table comment either.

    WHY `ALTER TABLE` RATHER THAN THE FRAMEWORK'S NATIVE SUPPORT. The framework CAN
    carry per-column comments: `targets/base.py:add_columns()` accepts StructFields
    and `delta_materialized_view` passes them on as `schema=`, and a comment in
    StructField metadata does survive. But passing `schema=` makes the schema
    ENFORCING, and this projection is derived at RUNTIME — collision-prefixed
    names, join keys dropped from the right side, JSON fields promoted by an LLM
    decision at build time. A declared schema off by one column FAILS the pipeline
    instead of documenting it, turning a cosmetic gap into a dead demo. Prefer the
    mechanism whose worst case is a missing nice-to-have.

    **If the projection ever becomes deterministic, switch to the native
    `add_columns` route — it is cleaner, and the only reason it is wrong here is
    the runtime derivation.**

    Deliberately post-materialisation and deliberately swallowing failures: a
    comment cannot then affect whether the table gets built at all, which is the
    strongest fail-open available. A missing comment is cosmetic; a failed
    materialisation is not.
    """
    try:
        applied = _apply_column_comments(fq_table, column_comments, wid, log)
        if not applied and column_comments:
            log(f"  (no column comments applied to `{fq_table}` — none matched)")
    except Exception as e:                                   # noqa: BLE001
        log(f"  (column comments skipped for `{fq_table}`: {str(e)[:140]})")

    # The table-level comment is missing on this path too — not the same defect as
    # the columns, so it is set explicitly rather than assumed to come along.
    comment = (table_comment or getattr(spec, "intent", "") or "").strip()
    if not comment:
        return
    try:
        with _dbsql_conn(wid) as conn:
            with conn.cursor() as cur:
                cur.execute(f"COMMENT ON TABLE {fq_table} IS '{comment.replace(chr(39), chr(39) * 2)}'")
        log(f"Set the table comment on `{fq_table}`.")
    except Exception as e:                                   # noqa: BLE001
        log(f"  (table comment skipped for `{fq_table}`: {str(e)[:140]})")


def deploy_request_silvers(specs: list, *, catalog: str, bronze_schema: str,
                           silver_schema: str, request_id: str, primary_table: str,
                           log: Callable[[str], None],
                           create_schedule: bool = False,
                           column_comments: dict | None = None,
                           table_comment: str | None = None) -> list:
    """Render ALL of a request's per-dataset Silver dataflowspecs into ONE
    per-request pipeline (`ops-<env>-<primary_table>`, direct-publishing) and run
    ONE update — every Silver table materialises together. The request's Gold
    fact/dim MVs are merged into this SAME pipeline by the Curate step
    (deploy_gold_via_framework), so a request == ONE pipeline that owns its whole
    Silver→Gold medallion (reading the shared Bronze). Returns a list[DeployResult]
    (one per Silver), all sharing the request pipeline id — the node reshapes."""
    t0 = time.time()
    env_tag = _env_tag_from_schema(silver_schema)
    client = WorkspaceClient()
    name = _pipeline_name(env_tag, primary_table)
    bundle_src = f"{_WS_ROOT}/{env_tag}/{primary_table}/src"

    files: dict[str, str] = {}
    targets: list[tuple] = []                  # (target_table, dataset, spec)
    for sub in specs:
        ds = sub.source.file.dataset if sub.source.file else sub.target.table
        tgt = sub.target.table
        files.update(render_silver_files(sub, ds, bronze_schema, silver_schema, tgt))
        targets.append((tgt, ds, sub))
    files["pipeline_configs/ENV_substitutions.json"] = json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)
    files["README.md"] = (
        f"# Request pipeline — {primary_table}\n\nRequest `{request_id}`.\n\n"
        f"ONE Spark Declarative Pipeline that owns this request's whole medallion "
        f"beyond Bronze: the typed **Silver** tables (now) and the **Gold** fact/dim "
        f"MVs (merged in by Curate). Reads the shared Bronze in `{bronze_schema}`; "
        f"writes Silver to `{silver_schema}` and Gold to its gold schema "
        f"(multi-schema direct-publishing).\n\nSilver tables: "
        + ", ".join(t for t, _, _ in targets) + "\n")

    log(f"Request pipeline `{name}` — building {len(targets)} Silver table(s) in ONE "
        f"pipeline (direct-publishing): {', '.join(t for t, _, _ in targets)}.")
    _upload_project(client, files, bundle_src, log)          # clean replace of the request bundle
    fw_src = framework_source_path()
    # will_own: the Silver tables this bundle declares, so a forced mode-switch
    # re-create is allowed to drop exactly those (it rebuilds them) and refuses
    # if the old pipeline owns anything else — e.g. a Gold MV from a prior run.
    pipeline_id = _ensure_pipeline(client, name, catalog, silver_schema, bundle_src, fw_src, log,
                                   direct_publish=True,
                                   will_own=[f"{catalog}.{silver_schema}.{t}"
                                             for t, _, _ in targets])
    host = client.config.host.rstrip("/")
    wid = _warehouse_id()
    # Free any Silver target still owned by a stale per-table pipeline (migration)
    # or another request, so this ONE request pipeline can own them all.
    _release_conflicting_tables(
        client, [f"{catalog}.{silver_schema}.{t}" for t, _, _ in targets],
        pipeline_id, wid, log)
    update_id = _start_and_wait(client, pipeline_id, streaming=False, log=log)

    wid = _warehouse_id()
    _ensure_warehouse_running(client, wid, log)
    results: list = []
    for tgt, ds, sub in targets:
        bronze_fq = f"{catalog}.{bronze_schema}.{ds}"
        silver_fq = f"{catalog}.{silver_schema}.{tgt}"
        try:
            bronze_rows = _count_rows(bronze_fq, wid)
        except Exception:                                    # noqa: BLE001
            bronze_rows = -1
        silver_rows = _count_rows(silver_fq, wid)
        log(f"Silver `{silver_fq}` = {silver_rows:,} rows "
            f"(from shared Bronze `{bronze_fq}` = {bronze_rows:,}).")
        # Document it in UC now the table demonstrably exists. See _apply_uc_comments.
        _apply_uc_comments(silver_fq, sub, column_comments, table_comment, wid, log)
        results.append(DeployResult(
            catalog=catalog,
            bronze_schema_fq=f"{catalog}.{bronze_schema}",
            silver_schema_fq=f"{catalog}.{silver_schema}",
            bronze_tables=[bronze_fq], bronze_counts={bronze_fq: bronze_rows},
            silver_table=silver_fq, silver_row_count=silver_rows,
            pipeline_id=pipeline_id, pipeline_name=name, update_id=update_id,
            pipeline_url=f"{host}/pipelines/{pipeline_id}", sdp_workspace_path=bundle_src,
            bronze_strategy="lakeflow_framework_autoloader_consolidated",
            duration_seconds=time.time() - t0))

    # The scheduled Job (staging/prod) triggers the WHOLE request pipeline (Silver+Gold).
    if create_schedule and results:
        job_id, job_name, cron = _ensure_scheduled_job(
            client, targets[0][2], pipeline_id, name, catalog, silver_schema, log)
        for r in results:
            r.job_id, r.job_name, r.job_schedule_cron = job_id, job_name, cron
            r.job_url = f"{host}/jobs/{job_id}" if job_id else ""
    return results


def deploy_cdc_silver_via_framework(spec: PipelineSpec, *, catalog: str,
                                    bronze_schema: str, silver_schema: str,
                                    request_id: str, log: Callable[[str], None],
                                    create_schedule: bool = False) -> list:
    """Silver for a CDC / database-sourced request, through the framework and into
    the SAME bundle root as its Gold — so one request == one pipeline == one root,
    identical to the file/Auto-Loader path.

    Bronze is already materialised by the consolidated Lakeflow Connect ingest, so
    the projection is read from the Bronze table's real schema.
    """
    t0 = time.time()
    env_tag = _env_tag_from_schema(silver_schema)
    client = WorkspaceClient()
    target_table = spec.target.table
    name = _pipeline_name(env_tag, target_table)
    bundle_src = f"{_WS_ROOT}/{env_tag}/{target_table}/src"
    wid = _warehouse_id()
    _ensure_warehouse_running(client, wid, log)

    # The Bronze table this Silver reads. Lakeflow Connect lands one Bronze table
    # per source table, named <system>__<table>.
    src_tbl = (spec.source.tables or [target_table])[0]
    bronze_table = src_tbl.replace(".", "__")
    bronze_fq = f"{catalog}.{bronze_schema}.{bronze_table}"
    with _dbsql_conn(wid) as c:
        cur = c.cursor(); cur.execute(f"DESCRIBE {bronze_fq}")
        bronze_columns = [(r[0], r[1]) for r in cur.fetchall()
                          if r[0] and not r[0].startswith("#") and r[0].strip()]
    log(f"Framework Silver deploy ({env_tag}) — `{silver_schema}.{target_table}` from "
        f"Bronze `{bronze_fq}` ({len(bronze_columns)} columns).")

    files = render_cdc_silver_files(spec, bronze_table, bronze_schema, silver_schema,
                                    target_table, bronze_columns)
    files["pipeline_configs/ENV_substitutions.json"] = json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)
    files["README.md"] = (
        f"# Request pipeline — {target_table}\n\nRequest `{request_id}`.\n\n"
        f"ONE Spark Declarative Pipeline owning this request's medallion beyond "
        f"Bronze: typed **Silver** (`{silver_schema}.{target_table}`) now, and the "
        f"**Gold** fact/dim MVs merged in by Curate — same bundle, same pipeline.\n")
    _upload_project(client, files, bundle_src, log)
    fw_src = framework_source_path()
    silver_fq = f"{catalog}.{silver_schema}.{target_table}"
    pipeline_id = _ensure_pipeline(client, name, catalog, silver_schema, bundle_src, fw_src,
                                   log, direct_publish=True, will_own=[silver_fq])
    _release_conflicting_tables(client, [silver_fq], pipeline_id, wid, log)
    update_id = _start_and_wait(client, pipeline_id, streaming=False, log=log)
    silver_rows = _count_rows(silver_fq, wid)
    bronze_rows = _count_rows(bronze_fq, wid)
    host = client.config.host.rstrip("/")
    log(f"Silver `{silver_fq}` = {silver_rows:,} rows (from Bronze `{bronze_fq}` = {bronze_rows:,}).")
    result = DeployResult(
        catalog=catalog,
        bronze_schema_fq=f"{catalog}.{bronze_schema}",
        silver_schema_fq=f"{catalog}.{silver_schema}",
        bronze_tables=[bronze_fq], bronze_counts={bronze_fq: bronze_rows},
        silver_table=silver_fq, silver_row_count=silver_rows,
        pipeline_id=pipeline_id, pipeline_name=name, update_id=update_id,
        pipeline_url=f"{host}/pipelines/{pipeline_id}", sdp_workspace_path=bundle_src,
        bronze_strategy="lakeflow_connect_consolidated",
        duration_seconds=time.time() - t0)
    if create_schedule:
        job_id, job_name, cron = _ensure_scheduled_job(
            client, spec, pipeline_id, name, catalog, silver_schema, log)
        result.job_id, result.job_name, result.job_schedule_cron = job_id, job_name, cron
        result.job_url = f"{host}/jobs/{job_id}" if job_id else ""
    return [result]


def deploy_gold_via_framework(fact: str, sql_statement: str, *, catalog: str,
                              gold_schema: str, env_tag: str, primary_table: str,
                              silver_schema: str,
                              log: Callable[[str], None],
                              cluster_by: Optional[list] = None, comment: str = "",
                              not_null: Optional[list] = None,
                              refresh_policy: str = "full") -> dict:
    """Build a Gold fact/dim MATERIALIZED VIEW (agent-generated SQL over Silver) by
    MERGING it into the request's OWN pipeline (`ops-<env>-<primary_table>`, the
    same one that built this request's Silver) and running it. So one request ==
    one pipeline owning Silver→Gold. The Gold MV writes to `gold_schema` while the
    Silver tables live in `silver_schema` — the pipeline is in direct-publishing
    mode, so one pipeline spans both. Returns {gold_table, rows, pipeline_*, fact}."""
    t0 = time.time()
    client = WorkspaceClient()
    name = _pipeline_name(env_tag, primary_table)
    files = render_gold_files(fact, sql_statement, gold_schema, cluster_by=cluster_by,
                              comment=comment, not_null=not_null, refresh_policy=refresh_policy)
    files["pipeline_configs/ENV_substitutions.json"] = json.dumps({"tokens": {}, "prefix_suffix": {}}, indent=2)
    bundle_src = f"{_WS_ROOT}/{env_tag}/{primary_table}/src"
    _upload_files_additive(client, files, bundle_src, log)        # merge Gold into the request bundle
    fw_src = framework_source_path()
    # default schema stays silver_schema (same as the Silver step) so we don't flap
    # the pipeline config; per-flow targetDetails.database routes Gold to gold_schema.
    #
    # NO will_own here, deliberately: the Gold upload is ADDITIVE into this root,
    # so if a mode switch were needed the replacement would only ever declare
    # what is already in this bundle. Passing nothing makes the guard maximally
    # strict — any table the old pipeline owns blocks the delete. That is exactly
    # right: this call site is the one that dropped a live Silver table, and the
    # correct outcome is to refuse and tell the operator, not to re-create.
    pipeline_id = _ensure_pipeline(client, name, catalog, silver_schema, bundle_src, fw_src, log,
                                   direct_publish=True)
    gold_fq = f"{catalog}.{gold_schema}.{fact}"
    wid = _warehouse_id()
    # Free the Gold table if it's still owned by the OLD shared gold pipeline (or
    # another request) so this request pipeline can own it.
    _release_conflicting_tables(client, [gold_fq], pipeline_id, wid, log)
    log(f"Request pipeline `{name}` — merging Gold MV `{fact}` (→ `{gold_schema}`) "
        f"alongside the request's Silver; running update.")
    update_id = _start_and_wait(client, pipeline_id, streaming=False, log=log)
    _ensure_warehouse_running(client, wid, log)
    rows = _count_rows(gold_fq, wid)
    host = client.config.host.rstrip("/")
    log(f"Gold `{gold_fq}` = {rows:,} rows (request pipeline `{name}`, {time.time() - t0:.0f}s).")
    return {"gold_table": gold_fq, "rows": rows, "pipeline_id": pipeline_id,
            "pipeline_name": name, "pipeline_url": f"{host}/pipelines/{pipeline_id}",
            "update_id": update_id, "fact": fact}


# =====================================================================
# Onboarding helpers — code preview + config test (used by the Connectors UI)
# =====================================================================

def render_auto_loader_code_preview(fs, *, bronze_table: str = "bronze") -> dict[str, str]:
    """Show the raw Auto Loader code a saved config translates to — both the
    equivalent PySpark `cloudFiles` read AND the framework dataflowspec
    sourceDetails that actually gets deployed. Reuses the same builders as the
    deploy so the preview always matches what runs."""
    # Bronze + Silver both reference the SHARED dataset Bronze streaming table.
    bronze_table = getattr(fs, "dataset", None) or bronze_table
    src = _bronze_source_details(fs, bronze_table)
    reader = src["readerOptions"]
    bronze_exp = src["selectExp"]
    silver_exp = _silver_select_exp(fs.columns if fs else [])
    glob = (fs.landing_path if fs else "") or "<landing glob>"

    lines = [
        "# === Bronze — Auto Loader (cloudFiles), faithful 1:1 landing ===",
        "bronze_df = (",
        '    spark.readStream.format("cloudFiles")',
    ]
    lines += [f'        .option("{k}", "{v}")' for k, v in reader.items()]
    lines += [f'        .load("{glob}")', ")"]
    for w in src.get("whereClause", []):
        lines.append(f'bronze_df = bronze_df.where("{w}")  # drop the header line')
    lines.append("bronze_df = bronze_df.selectExpr(")
    lines += [f'    "{e}",' for e in bronze_exp]
    lines += [
        ")",
        "",
        "# === Silver — typed projection (JSON / dirty values stay string) + DQ ===",
        f'silver_df = spark.readStream.table("{bronze_table}").selectExpr(',
    ]
    lines += [f'    "{e}",' for e in silver_exp]
    lines.append(")")

    dataflowspec = json.dumps({
        "sourceType": "cloudFiles",
        "sourceDetails": src,
        "mode": "stream",
        "targetDetails": {"type": "ST", "table": bronze_table},
    }, indent=2)
    return {"pyspark": "\n".join(lines), "dataflowspec": dataflowspec}


def _sql_lit(s: str) -> str:
    return str(s).replace("'", "''")


def test_config_via_read_files(fs, *, limit: int = 50,
                               log: Optional[Callable[[str], None]] = None) -> dict:
    """Deep config test: run a bounded `read_files()` query on the SQL warehouse
    that mirrors the saved Auto Loader config. `read_files` is the batch sibling
    of `cloudFiles` and shares the inference engine, so a pass here is a faithful
    proof the streaming bronze read will parse. No pipeline/table is created.

    Returns {ok, rows_sampled, columns, sample_rows, query, message, error?}."""
    def _log(m):
        if log:
            log(m)

    glob = (getattr(fs, "landing_path", "") or "").strip()
    if not glob:
        return {"ok": False, "error": "no landing path configured for this dataset"}

    cols = [{"name": c.name, "source_name": getattr(c, "source_name", None)}
            for c in fs.columns] if getattr(fs, "columns", None) else []
    if fs.file_format == "csv":
        opts = ["format => 'csv'", f"header => {'true' if fs.has_header else 'false'}"]
        if fs.delimiter:
            opts.append(f"sep => '{_sql_lit(fs.delimiter)}'")
        opts.append("mode => 'PERMISSIVE'")
        query = (f"SELECT * FROM read_files('{_sql_lit(glob)}', {', '.join(opts)}) "
                 f"LIMIT {int(limit)}")
    else:
        # text: read raw lines, drop the header, split via the SAME selectExp the
        # framework deploys (minus the _source_file/_ingested_at meta columns).
        proj = _bronze_data_exp(fs, cols)
        where = ""
        if fs.has_header and cols:
            header_token = cols[0].get("source_name") or cols[0]["name"]
            prefix = f"{header_token}{fs.delimiter}"
            where = f" WHERE value NOT LIKE '{_sql_lit(prefix)}%'"
        query = (f"SELECT {', '.join(proj)} FROM read_files('{_sql_lit(glob)}', "
                 f"format => 'text'){where} LIMIT {int(limit)}")

    _log(f"Deep test (read_files): {query}")
    client = WorkspaceClient()
    try:
        wid = _warehouse_id()
        _ensure_warehouse_running(client, wid, _log)
        with _dbsql_conn(wid) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                colnames = [d[0] for d in cur.description]
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e)[:500], "query": query}

    sample = [[(str(v)[:200] if v is not None else None) for v in r] for r in rows[:10]]
    n = len(rows)
    ok = n > 0
    return {
        "ok": ok,
        "rows_sampled": n,
        "columns": colnames,
        "sample_rows": sample,
        "query": query,
        "message": (f"Parsed {n} sample row(s); {len(colnames)} column(s) resolved via read_files."
                    if ok else "Query ran but returned 0 rows — check the path/glob, "
                    "delimiter and header setting."),
    }
