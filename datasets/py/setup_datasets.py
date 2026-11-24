#!/usr/bin/env python3
"""Create every dataset the Acme Corp Pipeline Studio demo uses, in any Databricks workspace.

All values come from ../../config.env (CATALOG, PROFILE, WAREHOUSE_ID, ...). Each sub-command is
idempotent, so you can re-run any step.

  schemas         catalog schemas + landing volumes
  erp-data        generate the synthetic ERP CSVs and upload them to <catalog>.cfo_workbench.raw_erp
  erp-pipeline    upload + create/update + run (full refresh) the bronze/silver/gold SDP pipeline
  metric-views    governed metric views (mv_revenue_margin, mv_working_capital, mv_inventory_ageing, draft)
  dq-rules        the shared DQ rules repo table (cfo_workbench.dq_rules_repo, 10 rules)
  carrier-feed    synthetic carrier shipment CSVs into <catalog>.studio_control.raw_landing (raw-layer demo)
  verify          row counts + the planted story numbers (Fasteners -140 bps vs plan, DSO ~47, on-time ~91%)
  all             every step above, in order
"""
from __future__ import annotations

import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines
from databricks.sdk.service.workspace import ImportFormat

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parents[1]                       # package root (holds config.env)
SCHEMAS = ["cfo_workbench", "studio_control", "studio_dev", "studio_stage", "studio_prod"]
VOLUMES = [("cfo_workbench", "raw_erp"), ("studio_control", "raw_landing")]



# --------------------------------------------------------------------------- config / clients
def load_config() -> dict[str, str]:
    path = pathlib.Path(os.environ.get("ACME_CONFIG", ROOT / "config.env"))
    if not path.exists():
        sys.exit(f"config file not found: {path} (copy config.env.example to config.env and fill it in)")
    cfg: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip().strip('"').strip("'")
    cfg.update({k: v for k, v in os.environ.items() if k in cfg and v})  # env overrides file
    for key in ("CATALOG", "PROFILE", "WAREHOUSE_ID"):
        if not cfg.get(key):
            sys.exit(f"config.env: {key} is required")
    return cfg


CFG = load_config()
CAT = CFG["CATALOG"]
PIPELINE_NAME = f"acme-erp-pipeline-{CAT}"
W = WorkspaceClient(profile=CFG["PROFILE"])


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sql(stmt: str, quiet: bool = False) -> list[list]:
    r = W.statement_execution.execute_statement(warehouse_id=CFG["WAREHOUSE_ID"], statement=stmt, wait_timeout="50s")
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(2)
        r = W.statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED":
        msg = r.status.error.message if r.status.error else r.status.state.value
        raise RuntimeError(f"SQL failed: {msg}\n--- statement ---\n{stmt[:800]}")
    rows = (r.result.data_array if r.result and r.result.data_array else []) or []
    if not quiet and rows:
        for row in rows:
            print("   ", " | ".join("" if v is None else str(v) for v in row))
    return rows


def lit(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


def me() -> str:
    return W.current_user.me().user_name


# --------------------------------------------------------------------------- steps
def step_schemas() -> None:
    log(f"catalog {CAT}")
    loc = CFG.get("CATALOG_MANAGED_LOCATION", "")
    try:
        if not sql(f"SHOW CATALOGS LIKE '{CAT}'", quiet=True):
            sql(f"CREATE CATALOG IF NOT EXISTS `{CAT}`" + (f" MANAGED LOCATION '{loc}'" if loc else ""), quiet=True)
    except RuntimeError as e:  # Default-Storage workspaces and non-admins cannot create catalogs via SQL
        rows = sql(f"SHOW CATALOGS LIKE '{CAT}'", quiet=True)
        if not rows:
            sys.exit(f"catalog {CAT} does not exist and could not be created ({e}). Create it in Catalog Explorer "
                     "(or set CATALOG_MANAGED_LOCATION in config.env), then re-run.")
    for s in SCHEMAS:
        log(f"schema {CAT}.{s}")
        sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.`{s}` COMMENT 'Acme Corp Pipeline Studio demo (synthetic data)'", quiet=True)
    for s, v in VOLUMES:
        log(f"volume {CAT}.{s}.{v}")
        sql(f"CREATE VOLUME IF NOT EXISTS `{CAT}`.`{s}`.`{v}`", quiet=True)


def _upload_dir(local: pathlib.Path, remote: str) -> int:
    n = 0
    for p in sorted(local.rglob("*")):
        if p.is_file():
            dest = f"{remote}/{p.relative_to(local).as_posix()}"
            W.files.upload(dest, io.BytesIO(p.read_bytes()), overwrite=True)
            n += 1
    return n


def step_erp_data() -> None:
    out = pathlib.Path(tempfile.mkdtemp(prefix="acme_erp_"))
    log(f"generating synthetic ERP CSVs into {out} (deterministic seed, ~1 min)")
    subprocess.run([sys.executable, str(HERE / "gen_synthetic_erp.py"), str(out)], check=True, stdout=subprocess.DEVNULL)
    manifest = json.loads((out / "manifest.json").read_text())
    remote = f"/Volumes/{CAT}/cfo_workbench/raw_erp"
    log(f"uploading to {remote}")
    n = _upload_dir(out, remote)
    log(f"uploaded {n} files. Planted values: Fasteners GM vs plan {manifest['fasteners_gm_vs_plan_bps']['achieved']} bps, "
        f"DSO {manifest['dso_days']['achieved']} days")


def step_erp_pipeline() -> None:
    user = me()
    folder = f"/Workspace/Users/{user}/acme_pipeline_studio_{CAT}/pipeline"
    src = f"{folder}/erp_pipeline.py"
    W.workspace.mkdirs(folder)
    W.workspace.upload(src, io.BytesIO((HERE / "erp_pipeline.py").read_bytes()), format=ImportFormat.AUTO, overwrite=True)
    log(f"uploaded pipeline source {src}")
    spec = dict(name=PIPELINE_NAME, serverless=True, catalog=CAT, schema="cfo_workbench", channel="CURRENT",
                configuration={"landing_root": f"/Volumes/{CAT}/cfo_workbench/raw_erp"},
                libraries=[pipelines.PipelineLibrary(file=pipelines.FileLibrary(path=src))],
                continuous=False, development=True, tags={"app": "acme-pipeline-studio"})
    existing = [p for p in W.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")
                if p.name == PIPELINE_NAME]
    if existing:
        pid = existing[0].pipeline_id
        W.pipelines.update(pipeline_id=pid, **spec)
        log(f"updated pipeline {pid}")
    else:
        pid = W.pipelines.create(**spec).pipeline_id
        log(f"created pipeline {pid}")
    upd = W.pipelines.start_update(pipeline_id=pid, full_refresh=True)
    log(f"full refresh started (update {upd.update_id}); serverless start + run takes ~5-10 min")
    while True:
        st = W.pipelines.get_update(pipeline_id=pid, update_id=upd.update_id).update.state.value
        log(f"  pipeline state {st}")
        if st in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(30)
    if st != "COMPLETED":
        for ev in W.pipelines.list_pipeline_events(pipeline_id=pid, max_results=20, filter="level='ERROR'"):
            print("   ERROR:", (ev.message or "")[:400])
        sys.exit(f"pipeline {pid} ended {st}")
    log(f"pipeline {pid} COMPLETED")


def _run_sql_file(name: str) -> None:
    text = (HERE / "sql" / name).read_text().replace("{{CATALOG}}", CAT)
    stmts = [s.strip() for s in text.split("-- @@STATEMENT") if s.strip()]
    for st in stmts:
        head = next((l for l in st.splitlines() if l.upper().startswith("CREATE")), st.splitlines()[0])
        log(f"  {head[:110]}")
        sql(st, quiet=True)


def step_metric_views() -> None:
    log("metric views (metric_views.sql)")
    _run_sql_file("metric_views.sql")
    log("draft view + draft metric view (metric_views_draft.sql)")
    _run_sql_file("metric_views_draft.sql")


def step_dq_rules() -> None:
    t = f"`{CAT}`.cfo_workbench.dq_rules_repo"
    sql(f"""CREATE TABLE IF NOT EXISTS {t} (
      table_name STRING, rule_name STRING, constraint_sql STRING, severity STRING,
      origin STRING, enabled BOOLEAN, why STRING, owner STRING, updated_by STRING,
      updated_at TIMESTAMP) USING DELTA
      COMMENT 'Shared data-quality rules repo for the silver tables (Pipeline Studio DQ step)'""", quiet=True)
    rules = json.loads((HERE.parent / "data" / "dq_rules_repo.json").read_text())
    sql(f"DELETE FROM {t} WHERE origin = 'existing'", quiet=True)
    values = ",\n".join(
        f"({lit(r['table_name'])}, {lit(r['rule_name'])}, {lit(r['constraint_sql'])}, {lit(r['severity'])}, "
        f"'existing', {lit(str(r['enabled']).lower() == 'true')}, {lit(r['why'])}, {lit(r['owner'] or 'data-engineering')}, "
        f"'seed', current_timestamp())" for r in rules)
    sql(f"INSERT INTO {t} VALUES\n{values}", quiet=True)
    log(f"dq_rules_repo seeded with {len(rules)} rules")


def step_carrier_feed() -> None:
    env = {**os.environ, "CATALOG": CAT, "DATABRICKS_CONFIG_PROFILE": CFG["PROFILE"]}
    log("generating + uploading carrier shipment CSVs (182 daily files, ~49.6k rows)")
    subprocess.run([sys.executable, str(HERE / "gen_carrier_shipments.py"), "--upload", "--stats"], check=True, env=env)


def step_verify() -> None:
    ok = True
    log("gold row counts")
    for tbl in ["gold_fct_sales_line", "gold_fct_ar_invoice", "gold_fct_inventory_snapshot", "gold_dim_customer",
                "gold_dim_product", "gold_dim_branch", "gold_fct_freight", "gold_fct_supplier_rebate",
                "gold_plan_gross_margin", "gold_customer_rank"]:
        n = int(sql(f"SELECT count(*) FROM `{CAT}`.cfo_workbench.{tbl}", quiet=True)[0][0])
        print(f"    {tbl:32s} {n:>9,}")
        ok &= n > 0
    log("planted story: GM vs plan by category, week of 2026-11-09 (expect Fasteners about -140 bps)")
    rows = sql(f"SELECT Category, ROUND(MEASURE(`GM vs Plan Bps`), 0) FROM `{CAT}`.cfo_workbench.mv_revenue_margin "
               f"WHERE `Week Start` = DATE'2026-11-09' GROUP BY ALL ORDER BY 2")
    fast = [float(r[1]) for r in rows if r[0] == "Fasteners"]
    ok &= bool(fast) and -160 <= fast[0] <= -120
    log("planted story: DSO (expect about 47 days)")
    dso = sql(f"SELECT ROUND(MEASURE(`DSO Days`), 1) FROM `{CAT}`.cfo_workbench.mv_working_capital")
    ok &= bool(dso) and 44 <= float(dso[0][0]) <= 50
    log("DQ rules repo")
    n = int(sql(f"SELECT count(*) FROM `{CAT}`.cfo_workbench.dq_rules_repo WHERE enabled", quiet=True)[0][0])
    print(f"    enabled rules: {n}")
    ok &= n == 10
    log("carrier feed (expect 49,653 rows, on-time about 91%)")
    rows = sql(f"""SELECT count(*), ROUND(100 * avg(CASE WHEN delivered_date <= promised_date THEN 1 ELSE 0 END), 2)
                   FROM read_files('/Volumes/{CAT}/studio_control/raw_landing/carrier_shipments/', format => 'csv', header => true)
                   WHERE status = 'delivered'""", quiet=True)
    total = int(sql(f"SELECT count(*) FROM read_files('/Volumes/{CAT}/studio_control/raw_landing/carrier_shipments/', "
                    f"format => 'csv', header => true)", quiet=True)[0][0])
    print(f"    rows {total:,}; on-time (delivered rows) {rows[0][1]}%")
    ok &= total == 49653
    log("ALL CHECKS PASSED" if ok else "SOME CHECKS FAILED (see above)")
    if not ok:
        sys.exit(1)


STEPS = {"schemas": step_schemas, "erp-data": step_erp_data, "erp-pipeline": step_erp_pipeline,
         "metric-views": step_metric_views, "dq-rules": step_dq_rules, "carrier-feed": step_carrier_feed,
         "verify": step_verify}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in [*STEPS, "all"]:
        sys.exit(__doc__)
    names = list(STEPS) if sys.argv[1] == "all" else [sys.argv[1]]
    log(f"workspace {W.config.host}  profile {CFG['PROFILE']}  catalog {CAT}")
    for name in names:
        log(f"=== {name} ===")
        STEPS[name]()
    log("done")
