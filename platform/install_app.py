#!/usr/bin/env python3
"""Install the Acme Corp Pipeline Studio app into a Databricks workspace (after the datasets exist).

Values come from ../config.env. Each sub-command is idempotent.

  skills       upload the Genie Code skills the build prompts @-mention (catalog name rewritten)
  genie-job    create the Lakeflow Job with the Genie Code task the app triggers (see README, step 3)
  app          create the app (if needed), stage the source for this workspace, add resources,
               grant the app service principal, sync and deploy
  seed         reset the tracker with the view-only example requests + register the ERP feeds
  status       print the app URL and deployment state
  all          skills, genie-job, app, seed
"""
from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "datasets" / "py"))
from setup_datasets import CFG, CAT, W, log, me, sql  # noqa: E402  (shared config + client)

from databricks.sdk.service import apps, iam, jobs, workspace  # noqa: E402

APP_NAME = CFG.get("APP_NAME") or "acme-pipeline-studio"
LLM = CFG.get("LLM_ENDPOINT") or "databricks-claude-sonnet-4-6"
JOB_NAME = f"{APP_NAME}-genie-code"
SKILLS = ROOT / "platform" / "skills"
APP_SRC = ROOT / "app_src"

# Values baked into the packaged source -> replaced with this workspace's values when staging.
ORIG = {
    "catalog": "vmohan_demo_catalog",
    "owner": "vaishnavi.mohan@databricks.com",
    "sp": "709e095d-db7e-4d9f-a243-a51ccbcb5764",
    "warehouse": "7464666eb7d50c27",
    "job": "28927436286061",
    "host": "adb-7405604635797033.13.azuredatabricks.net",
    "org": "7405604635797033",
    "llm": "databricks-claude-sonnet-4-6",
    "host2": "fevm-pipeline-studio.cloud.databricks.com",   # tracker.js fallback host
}


def _state_path() -> pathlib.Path:
    return ROOT / ".install_state.json"


def state() -> dict:
    p = _state_path()
    return json.loads(p.read_text()) if p.exists() else {}


def save_state(**kv) -> None:
    s = state()
    s.update(kv)
    _state_path().write_text(json.dumps(s, indent=2))


# --------------------------------------------------------------------------- skills
def step_skills() -> None:
    targets = [CFG.get("SKILLS_DIR") or "/Workspace/.assistant/skills", f"/Workspace/Users/{me()}/.assistant/skills"]
    for base in targets:
        try:
            W.workspace.mkdirs(base)
            n = 0
            for f in sorted(SKILLS.rglob("*")):
                if not f.is_file():
                    continue
                rel = f.relative_to(SKILLS).as_posix()
                dest = f"{base}/{rel}"
                W.workspace.mkdirs(dest.rsplit("/", 1)[0])
                text = f.read_text().replace(ORIG["catalog"], CAT)
                W.workspace.import_(dest, content=__import__("base64").b64encode(text.encode()).decode(),
                                    format=workspace.ImportFormat.AUTO, overwrite=True)
                n += 1
            log(f"uploaded {n} skill files ({len(list(SKILLS.iterdir()))} skills) to {base}")
            save_state(skills_dir=base)
            return
        except Exception as e:  # noqa: BLE001  (workspace-level folder may need admin)
            log(f"could not write {base} ({str(e)[:120]}); trying the next location")
    sys.exit("could not upload skills anywhere")


# --------------------------------------------------------------------------- Genie Code job
def _find_job() -> int | None:
    for j in W.jobs.list(name=JOB_NAME):
        return j.job_id
    return None


def step_genie_job() -> None:
    job_id = _find_job()
    if job_id is None:
        job_id = W.jobs.create(
            name=JOB_NAME, max_concurrent_runs=3,
            description="Acme Corp Pipeline Studio: Genie Code build step. The app calls run_now with a prompt.",
            parameters=[jobs.JobParameterDefinition(name="prompt", default="")],
            tags={"app": "acme-pipeline-studio"}).job_id
        log(f"created job {JOB_NAME} ({job_id})")
    else:
        log(f"job {JOB_NAME} exists ({job_id})")
    save_state(genie_job_id=job_id)
    tasks = W.jobs.get(job_id).settings.tasks or []
    if tasks:
        log(f"job already has task '{tasks[0].task_key}'; nothing to add")
        return
    cfg_id = CFG.get("GENIE_CODE_CONFIG_ID", "")
    if cfg_id:
        body = {"job_id": job_id, "new_settings": {"tasks": [
            {"task_key": "genie_code_step", "timeout_seconds": 3600, "genie_task": {"configuration_id": cfg_id}}]}}
        W.api_client.do("POST", "/api/2.2/jobs/update", body=body)
        log(f"attached Genie Code task (configuration_id {cfg_id})")
        return
    host = W.config.host.rstrip("/")
    print(f"""
  ACTION NEEDED (one time, ~1 minute): Genie Code tasks can only be created in the UI.
    1. Open {host}/jobs/{job_id}
    2. Tasks -> Add task -> type "Genie Code". Task name: genie_code_step
    3. Prompt: {{{{prompt}}}}   (references the job's 'prompt' parameter, which the app fills on every run)
    4. Save, then re-run:  bin/install_app.sh genie-job   (it will see the task and continue)
""")


# --------------------------------------------------------------------------- app
def _ensure_app() -> apps.App:
    try:
        app = W.apps.get(APP_NAME)
        log(f"app {APP_NAME} exists")
    except Exception:  # noqa: BLE001
        log(f"creating app {APP_NAME} (compute start ~2-4 min)")
        app = W.apps.create_and_wait(app=apps.App(name=APP_NAME, description="Acme Corp Pipeline Studio (demo)"))
    return app


def _stage_source(sp_id: str, job_id: int) -> pathlib.Path:
    host = W.config.host.rstrip("/").replace("https://", "")
    org = str(W.get_workspace_id())
    new = {"catalog": CAT, "owner": me(), "sp": sp_id, "warehouse": CFG["WAREHOUSE_ID"], "job": str(job_id),
           "host": host, "org": org, "llm": LLM, "host2": host}
    out = pathlib.Path(tempfile.mkdtemp(prefix="acme_app_")) / "app"
    shutil.copytree(APP_SRC, out, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".DS_Store"))
    changed = 0
    for f in out.rglob("*"):
        if not f.is_file() or f.suffix not in {".py", ".yaml", ".yml", ".json", ".html", ".js", ".txt", ".md", ".sql", ".sh", ".css"}:
            continue
        text = f.read_text()
        orig = text
        for k in ("host", "host2", "sp", "job", "warehouse", "owner", "catalog", "llm"):   # host before org (org is inside host)
            text = text.replace(ORIG[k], new[k])
        text = re.sub(rf"(?<![0-9]){ORIG['org']}(?![0-9])", new["org"], text)
        if text != orig:
            f.write_text(text)
            changed += 1
    log(f"staged source in {out} ({changed} files rewritten for this workspace)")
    return out


def _grant(securable: str, name: str, principal: str, privs: list[str]) -> None:
    sql(f"GRANT {', '.join(privs)} ON {securable} {name} TO `{principal}`", quiet=True)


def _grants(sp_id: str, job_id: int) -> None:
    log(f"granting the app service principal {sp_id}")
    _grant("CATALOG", f"`{CAT}`", sp_id, ["USE CATALOG"])
    _grant("SCHEMA", f"`{CAT}`.cfo_workbench", sp_id, ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"])
    _grant("SCHEMA", f"`{CAT}`.studio_control", sp_id, ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"])
    for s in ("studio_dev", "studio_stage", "studio_prod"):
        _grant("SCHEMA", f"`{CAT}`.{s}", sp_id, ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE", "CREATE MATERIALIZED VIEW"])
    _grant("VOLUME", f"`{CAT}`.studio_control.raw_landing", sp_id, ["READ VOLUME"])
    _grant("VOLUME", f"`{CAT}`.cfo_workbench.raw_erp", sp_id, ["READ VOLUME"])
    # Folder where Genie Code (running as you) saves dev dashboards; the app reads/updates them.
    folder = f"/Workspace/Users/{me()}/acme_studio"
    W.workspace.mkdirs(folder)
    st = W.workspace.get_status(folder)
    W.permissions.update("directories", str(st.object_id), access_control_list=[
        iam.AccessControlRequest(service_principal_name=sp_id, permission_level=iam.PermissionLevel.CAN_MANAGE)])
    # MLflow experiment for intake-agent traces.
    exp_path = "/Shared/acme-pipeline-studio-agent"
    exp = W.experiments.get_by_name(exp_path) if _exists_exp(exp_path) else None
    exp_id = exp.experiment.experiment_id if exp else W.experiments.create_experiment(name=exp_path).experiment_id
    W.permissions.update("experiments", exp_id, access_control_list=[
        iam.AccessControlRequest(service_principal_name=sp_id, permission_level=iam.PermissionLevel.CAN_MANAGE)])
    log("grants done")


def _exists_exp(path: str) -> bool:
    try:
        W.experiments.get_by_name(path)
        return True
    except Exception:  # noqa: BLE001
        return False


def step_app() -> None:
    job_id = state().get("genie_job_id") or _find_job()
    if not job_id:
        sys.exit("run `bin/install_app.sh genie-job` first")
    if not (W.jobs.get(job_id).settings.tasks or []):
        log("WARNING: the Genie Code job has no task yet; builds will fail until you add it (see genie-job step)")
    app = _ensure_app()
    sp_id = app.service_principal_client_id
    save_state(app_sp_id=sp_id)
    W.apps.update(APP_NAME, app=apps.App(name=APP_NAME, description=app.description, resources=[
        apps.AppResource(name="warehouse", sql_warehouse=apps.AppResourceSqlWarehouse(
            id=CFG["WAREHOUSE_ID"], permission=apps.AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE)),
        apps.AppResource(name="llm", serving_endpoint=apps.AppResourceServingEndpoint(
            name=LLM, permission=apps.AppResourceServingEndpointServingEndpointPermission.CAN_QUERY)),
        apps.AppResource(name="genie_code_job", job=apps.AppResourceJob(
            id=str(job_id), permission=apps.AppResourceJobJobPermission.CAN_MANAGE_RUN)),
    ]))
    log("app resources set: warehouse (CAN_USE), llm (CAN_QUERY), genie_code_job (CAN_MANAGE_RUN)")
    _grants(sp_id, job_id)
    src = _stage_source(sp_id, job_id)
    remote = f"/Workspace/Users/{me()}/{APP_NAME}/app"
    log(f"syncing source to {remote}")
    subprocess.run(["databricks", "sync", "--full", str(src), remote, "-p", CFG["PROFILE"]], check=True,
                   stdout=subprocess.DEVNULL)
    log("deploying (build + start, typically 3-6 min)")
    dep = W.apps.deploy_and_wait(APP_NAME, apps.AppDeployment(source_code_path=remote),
                                 timeout=__import__("datetime").timedelta(minutes=20))
    log(f"deployment {dep.deployment_id}: {dep.status.state.value} {dep.status.message or ''}")
    step_status()


# --------------------------------------------------------------------------- seed + status
def _call_app(method: str, path: str) -> dict:
    url = W.apps.get(APP_NAME).url.rstrip("/") + path
    headers = W.config.authenticate()
    for attempt in range(16):  # a freshly deployed app answers 502/503 for a minute or two while it warms up
        req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=300) as r:
                return json.loads(r.read().decode() or "{}")
        except urllib.error.HTTPError as e:
            if e.code not in (502, 503, 504) or attempt == 15:
                raise
            log(f"  app not ready yet (HTTP {e.code}); retrying in 15 s")
            time.sleep(15)
            headers = W.config.authenticate()


def step_seed() -> None:
    log("reset tracker + seed the view-only example requests")
    out = _call_app("POST", "/api/admin/reset?close_prs=false&seed=true")
    print("   ", json.dumps(out)[:300])
    log("register the 6 Acme ERP feeds in the Data Browser")
    out = _call_app("POST", "/api/admin/seed-cfo-datasets")
    print("   ", json.dumps(out)[:300])


def step_status() -> None:
    a = W.apps.get(APP_NAME)
    dep = a.active_deployment
    print(json.dumps({"app": a.name, "url": a.url, "app_state": a.app_status.state.value if a.app_status else None,
                      "compute": a.compute_status.state.value if a.compute_status else None,
                      "deployment": dep.deployment_id if dep else None,
                      "deployment_state": dep.status.state.value if dep and dep.status else None,
                      "service_principal": a.service_principal_client_id}, indent=2))


STEPS = {"skills": step_skills, "genie-job": step_genie_job, "app": step_app, "seed": step_seed, "status": step_status}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in [*STEPS, "all"]:
        sys.exit(__doc__)
    names = ["skills", "genie-job", "app", "seed"] if sys.argv[1] == "all" else [sys.argv[1]]
    log(f"workspace {W.config.host}  profile {CFG['PROFILE']}  catalog {CAT}  app {APP_NAME}")
    for n in names:
        log(f"=== {n} ===")
        STEPS[n]()
    log("done")
