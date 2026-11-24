# Acme Corp Pipeline Studio: install package

A Databricks App where a business analyst asks for a data product in plain English (for example "a Genie Agent
and dashboard showing where we're leaking margin and what's happening with cash"). The app:

1. questions the analyst one step at a time,
2. checks what already exists in Unity Catalog,
3. proposes data-quality rules taken from the shared rules repo,
4. has Genie Code build the missing pieces (metric view, SDP pipeline from raw files, Genie Agent, AI/BI dashboard),
5. promotes them dev → stage → prod behind a review and approval gate.

All data is **synthetic** (a fictional distributor, "Acme Corp"). No customer data is included.

---

## What's in the package

```
acme-pipeline-studio/
├── install.sh                  one-shot install (datasets + app)
├── config.env.example          the settings you fill in (copy to config.env)
├── installer-requirements.txt  Python deps for the installer only
├── bin/                        the executables, one per step
│   ├── 00_setup_python.sh        local venv for the installer
│   ├── 01_create_schemas.sh      schemas + landing volumes
│   ├── 02_load_erp_data.sh       synthetic ERP extracts → volume
│   ├── 03_build_erp_pipeline.sh  bronze/silver/gold SDP pipeline (create + full refresh)
│   ├── 04_create_metric_views.sh governed metric views
│   ├── 05_seed_dq_rules.sh       shared DQ rules repo
│   ├── 06_load_carrier_feed.sh   raw carrier-shipment files (delivery-times scenario)
│   ├── 07_verify_datasets.sh     row counts + the planted demo numbers
│   ├── create_datasets.sh        runs 01 → 07
│   └── install_app.sh            skills, Genie Code job, app, seed (sub-steps below)
├── datasets/
│   ├── py/setup_datasets.py      the dataset logic behind bin/01-07
│   ├── py/gen_synthetic_erp.py   ERP generator (fixed seed → same numbers every time)
│   ├── py/gen_carrier_shipments.py carrier feed generator (fixed seed)
│   ├── py/erp_pipeline.py        the SDP pipeline source
│   ├── py/sql/*.sql              metric view DDL ({{CATALOG}} filled in at run time)
│   └── data/dq_rules_repo.json   the 10 existing DQ rules
├── platform/
│   ├── install_app.py            app install logic behind bin/install_app.sh
│   └── skills/                   11 Genie Code skills the build prompts use
└── app_src/                      the app source (FastAPI + Jinja); DEMO_SCRIPT.md = copy-paste demo answers
```

---

## Prerequisites

| Need | Detail |
|---|---|
| Workspace | Unity Catalog, serverless compute (SQL warehouse, serverless pipelines, Databricks Apps) |
| Model endpoint | A chat foundation model endpoint, default `databricks-claude-sonnet-4-6` (set `LLM_ENDPOINT` to change) |
| Genie Code | Genie Code enabled, plus the **Genie Code task type in Lakeflow Jobs** (Beta) |
| Your rights | Create schemas/volumes in the target catalog (or create the catalog), create pipelines, jobs and apps, grant on the catalog |
| Laptop | Databricks CLI (recent v0.2xx+), authenticated: `databricks auth login --host https://<workspace> --profile <name>`; Python 3.10+ |

---

## Quick start

```bash
unzip acme-pipeline-studio.zip && cd acme-pipeline-studio
cp config.env.example config.env      # set PROFILE, CATALOG, WAREHOUSE_ID (APP_NAME optional)
./install.sh                          # ~25 min: datasets (~15 min) + app build/deploy (~8 min)
```

`install.sh` stops once, at the Genie Code job step, to print a 1-minute UI step (see step 3 below). Do that step,
then run `bin/install_app.sh all` to finish. Every step can be re-run safely.

When it finishes, `bin/install_app.sh status` prints the app URL. Open it → **New request**, and follow
`app_src/DEMO_SCRIPT.md`.

---

## Step by step

### 1. Configure
```bash
cp config.env.example config.env
bin/00_setup_python.sh
```
| Key | Meaning |
|---|---|
| `PROFILE` | Databricks CLI profile for the target workspace |
| `CATALOG` | Catalog for all demo schemas. Created if it doesn't exist and you have the rights. On metastores with no default storage root, create it in Catalog Explorer first, or set `CATALOG_MANAGED_LOCATION` |
| `WAREHOUSE_ID` | SQL warehouse id (serverless recommended) |
| `APP_NAME` | App to create/update (default `acme-pipeline-studio`) |
| `LLM_ENDPOINT` | Foundation model endpoint for the intake chat and planner |
| `GENIE_CODE_CONFIG_ID` | Optional, see step 3 |
| `SKILLS_DIR` | Where skills go. Workspace-wide `/Workspace/.assistant/skills` needs admin; falls back to `/Workspace/Users/<you>/.assistant/skills` |

### 2. Create the datasets: `bin/create_datasets.sh` (or run 01-07 one by one)

| Step | Creates (in `<CATALOG>`) |
|---|---|
| 01 | Schemas `cfo_workbench` (source data), `studio_control` (app state), `studio_dev`, `studio_stage`, `studio_prod` (build targets); volumes `cfo_workbench.raw_erp`, `studio_control.raw_landing` |
| 02 | 104 weeks of ERP-shaped CSV extracts (sales invoice lines, AR, inventory, customers with an SCD2 change feed, products, branches, freight, supplier rebates, FP&A plan) in `raw_erp/<feed>/` |
| 03 | Serverless SDP pipeline `acme-erp-pipeline-<CATALOG>`: Auto Loader bronze → silver (SCD2 customers, DQ expectations) → gold facts/dims (`gold_fct_sales_line`, `gold_fct_ar_invoice`, `gold_fct_inventory_snapshot`, `gold_dim_*`, `gold_plan_gross_margin`, ...) |
| 04 | Metric views `mv_revenue_margin`, `mv_working_capital`, `mv_inventory_ageing`, plus `gold_sales_line_rebate_alloc` and `mv_revenue_margin_draft` |
| 05 | `cfo_workbench.dq_rules_repo`: the 10 "existing" DQ rules the app shows in its DQ step (7 drop, 3 warn) |
| 06 | 182 daily carrier-shipment CSVs (49,653 rows) in `studio_control/raw_landing/carrier_shipments/` for the "build a pipeline from raw files" scenario. Dirty rows are included on purpose: 25 duplicate ids, 20 null branch, 15 delivered before despatch |
| 07 | Checks: gold tables non-empty, **Fasteners about −140 bps vs plan** (week of 2026-11-09), **DSO about 47 days**, 10 DQ rules, 49,653 shipment rows |

The generators use fixed seeds, so every install has the same story numbers: GM 31.8% vs plan 32.4% (−60 bps),
Fasteners −140 bps, DSO 47 days, $6.2M overdue for the top-200 accounts, $4.1M of stock older than 180 days;
on-time delivery about 91%, FastFreight late about 18% in the East region. The demo calendar is anchored to
Monday 2026-11-16.

### 3. Genie Code job: `bin/install_app.sh genie-job`
The app builds through a Lakeflow Job with a **Genie Code** task and a `prompt` parameter. The installer creates
the job shell (`<APP_NAME>-genie-code`). There is no public API to create a new Genie Code task, so add it once in the UI:

1. Open the job link the installer prints.
2. **Tasks → Add task →** type **Genie Code**. Task name `genie_code_step`.
3. Prompt: `{{prompt}}` (the app fills the `prompt` job parameter on every run).
4. Save, then re-run `bin/install_app.sh genie-job`. It confirms the task is there.

If you already have a Genie Code insight id (`genie_code/users/<uid>/scheduled_insights/<id>` from an existing
Genie Code job's JSON), put it in `GENIE_CODE_CONFIG_ID` and the installer attaches it with no UI step.

The job **runs as you** (Genie Code tasks run as the job creator). Assets Genie Code builds are owned by you and
shared with the app's service principal.

### 4. App: `bin/install_app.sh app`
- Creates the app `APP_NAME` if needed, and adds resources: SQL warehouse (CAN_USE), model endpoint (CAN_QUERY),
  Genie Code job (CAN_MANAGE_RUN).
- Grants the app service principal: `USE CATALOG`; `USE SCHEMA, SELECT, MODIFY, CREATE TABLE` on `cfo_workbench`
  and `studio_control`; `+ CREATE MATERIALIZED VIEW` on `studio_dev/stage/prod`; `READ VOLUME` on both volumes;
  CAN_MANAGE on `/Workspace/Users/<you>/acme_studio` (Genie Code saves dev dashboards there) and on the MLflow
  experiment `/Shared/acme-pipeline-studio-agent`.
- Stages a copy of `app_src/` with this workspace's values (catalog, your user, the app SP, warehouse, job id, host),
  syncs it to `/Workspace/Users/<you>/<APP_NAME>/app` and deploys.

### 5. Skills: `bin/install_app.sh skills`
Uploads the 11 skills the Genie Code prompts @-mention (`metric-view-author`, `medallion-sdp`,
`genie-agent-author`, `aibi-dashboard-standards`, `governance-by-construction`, `business-glossary`, `dq-standards`,
`reuse-first`, `platform-context`, `intake-grill`, `monday-margin-pulse`), with your catalog name filled in.
(`all` runs this first.)

### 6. Seed: `bin/install_app.sh seed`
Calls the app's admin API to reset the tracker, add 5 view-only example requests (2 Deployed, 2 Blocked, 1 Awaiting
approval) so the KPI strip isn't empty, and register the 6 Acme ERP feeds in the New Request Data Browser.
To reset before a demo, use the About page → **Reset**, or run `bin/install_app.sh seed` again.

---

## Running the demo
`app_src/DEMO_SCRIPT.md` has copy-paste answers for every scenario:

- **Flagship**: Genie Agent + dashboard for margin and cash (reuse, new metric view, DQ, build, review, promote).
- **A** vague ask; **B** dashboard only; **C** Genie Agent only.
- **D** pipeline from raw files (delivery times): Genie Code builds an SDP pipeline raw → bronze → silver (DQ) → gold.
- **E** data that doesn't exist: the app refuses honestly.

A full build (approve the plan → prod release) takes about 10-15 minutes, mostly Genie Code runs.

## Run the app locally (optional)
```bash
cd <staged or app_src folder>
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
export DATABRICKS_CONFIG_PROFILE=<profile>
# export every env value from app.yaml (catalog, warehouse, job id ...), plus:
export DATABRICKS_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/<WAREHOUSE_ID>
.venv/bin/uvicorn app.app:app --port 8770
```
Locally the app runs as you, not the service principal.

## Troubleshooting
| Symptom | Fix |
|---|---|
| `UC_METASTORE_STORAGE_ROOT_URL_NOT_FOUND...` at step 01 | Create the catalog in Catalog Explorer, or set `CATALOG_MANAGED_LOCATION` |
| Pipeline step FAILED | The script prints the pipeline's error events. Re-run `bin/03_build_erp_pipeline.sh` (it does a full refresh) |
| Builds fail with "Genie Code job has no task" | Finish step 3 |
| Genie Code build can't read data | The job runs as its creator; that user needs SELECT on `<CATALOG>` |
| App shows PERMISSION_DENIED on a schema | Re-run `bin/install_app.sh app` (re-applies grants) |
| `seed` returns 401/403 | Your CLI login must be able to open the app (you are the app owner by default) |
| Skills upload falls back to your user folder | Expected without workspace admin; Genie Code runs as you, so your user skills are used |

## Known limits
- The 5 seeded example requests are view-only snapshots. Links inside them point to the workspace they were
  recorded in.
- Pull requests run in **mock mode** (reviewed inside the app). `GITHUB_PIPELINES_REPO` is only a display label.
- If the app restarts mid-build, the build does not resume. Retry it from the tracker.
- The intake LLM varies a little run to run (for example, how much it reuses). `DEMO_SCRIPT.md` covers the variants.

## Uninstall
Delete the app (`databricks apps delete <APP_NAME>`), the job `<APP_NAME>-genie-code`, the pipeline
`acme-erp-pipeline-<CATALOG>` and any pipelines/dashboards/Genie spaces the demo built (named after the request),
then `DROP SCHEMA <CATALOG>.<schema> CASCADE` for the 5 schemas (or drop the catalog).
