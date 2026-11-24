"""Deploy a generated Databricks App from emitted source files.

The app_generator LLM writes a manifest; app_emitter stamps Streamlit source
into artifacts/{relative_paths}. THIS module takes those emitted files and deploys
them as a live Databricks App via `databricks apps deploy --source-code-path`.

Responsibilities:
  1. Create/ensure the app exists (`databricks apps create`)
  2. Upload emitted source to a workspace-local path (`workspace import`)
  3. Deploy from that path (`databricks apps deploy --source-code-path`)
  4. Grant the app's service principal the required UC + warehouse permissions
  5. Return the app's public URL + deployment metadata

Gated for safety: designed so `deploy_for_real()` only fires when explicitly
called by the orchestrator with `allow_live_deploy=True`. Test/code-only runs
pass `allow_live_deploy=False` (the default) or don't set the env var.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from typing import Any, Callable, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import apps as apps_service


def _allow_live_deploy() -> bool:
    """Check if live deploy is enabled. Default False (safe for tests)."""
    env_val = os.environ.get("ALLOW_LIVE_APP_DEPLOY", "0").lower()
    return env_val in ("1", "true", "yes")


def _get_cli_profile() -> str | None:
    """Get the Databricks CLI profile to use."""
    return os.environ.get("DATABRICKS_CONFIG_PROFILE") or None


def _sanitize_app_name(name: str) -> str:
    """Databricks App names must be lowercase, 2–30 chars, [a-z0-9-] only, and
    start/end alphanumeric. The generator can emit longer, descriptive names
    (e.g. a 40-character descriptive name) which the Apps API
    rejects. Normalise + truncate to a valid, stable name so create/get/deploy
    all agree on the same string."""
    import re as _re
    s = _re.sub(r"[^a-z0-9-]+", "-", (name or "").lower()).strip("-")
    s = _re.sub(r"-{2,}", "-", s)
    if len(s) > 30:
        s = s[:30].rstrip("-")
    if len(s) < 2:
        s = (s + "-app")[:30]
    return s


def _app_sp(w: WorkspaceClient, app_name: str) -> str | None:
    """Resolve the app's service principal ID (works on any workspace)."""
    try:
        app = w.apps.get(name=app_name)
        return (getattr(app, "service_principal_client_id", None)
                or getattr(app, "service_principal_id", None))
    except Exception as e:
        return None


def _grant_app_permissions(
    app_name: str,
    gold_schema_fq: str,
    warehouse_id: str,
    profile: str | None = None,
    log: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Grant the app service principal the required UC permissions:
      - SELECT on the Gold schema (reads pool_allocation, etc)
      - CAN_USE on the warehouse
      - EXECUTE on ai_gov.default.claude_governed (if using Genie/governed LLM)
      - USE_CATALOG + USE_SCHEMA on ai_gov (for Genie + governance LLM)

    Returns a result dict with the grants applied."""
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(f"  {msg}")

    from databricks.sdk.service import catalog as _cat
    from databricks.sdk.service import sql as _sql

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    sp = _app_sp(w, app_name)
    if not sp:
        _log(f"! could not resolve app SP for {app_name}")
        return {"error": f"app SP not resolved"}

    _log(f"App SP: {sp}")

    # Grant UC privileges via the SDK (works on serverless — no `databricks`
    # CLI binary present there, so we must NOT shell out).
    def grant(securable_type: str, full_name: str, privs: list[str]) -> bool:
        try:
            w.grants.update(
                securable_type=securable_type,
                full_name=full_name,
                changes=[_cat.PermissionsChange(
                    principal=sp,
                    add=[_cat.Privilege[p] for p in privs],
                )],
            )
            _log(f"✓ {securable_type} {full_name}: {privs}")
            return True
        except Exception as e:  # noqa: BLE001
            _log(f"! {securable_type} {full_name}: {str(e)[:160]}")
            return False

    # Grant on Gold schema
    if "." in gold_schema_fq and gold_schema_fq.count(".") >= 1:
        if not grant("SCHEMA", gold_schema_fq, ["USE_SCHEMA", "SELECT"]):
            _log(f"warn: failed to grant SELECT on {gold_schema_fq}")
    else:
        _log(f"warn: invalid gold schema FQ: {gold_schema_fq}")

    # Grant on warehouse (permissions API, not UC grants).
    if warehouse_id:
        try:
            w.warehouses.set_permissions(
                warehouse_id=warehouse_id,
                access_control_list=[_sql.WarehouseAccessControlRequest(
                    service_principal_name=sp,
                    permission_level=_sql.WarehousePermissionLevel.CAN_USE,
                )],
            )
            _log(f"✓ warehouse {warehouse_id}: [CAN_USE]")
        except Exception as e:  # noqa: BLE001
            _log(f"! warehouse {warehouse_id}: {str(e)[:160]}")

    # Grant on ai_gov catalog/schema + EXECUTE on the governed function
    # (for Genie/governed LLM). Best-effort; skip without failing the deploy.
    try:
        grant("CATALOG", "ai_gov", ["USE_CATALOG"])
        grant("SCHEMA", "ai_gov.default", ["USE_SCHEMA", "EXECUTE"])
    except Exception as e:  # noqa: BLE001
        _log(f"warn: could not grant ai_gov permissions: {str(e)[:160]}")

    return {"granted": True, "app_sp": sp}


def _write_app_files_to_workspace(
    emitted_files: dict[str, str],
    app_name: str,
    ws_path: str = "/Workspace/Shared",
    profile: str | None = None,
    log: Optional[Callable[[str], None]] = None,
) -> str | None:
    """Write emitted app files to a workspace directory and return the
    workspace path where they live (e.g. `/Workspace/Shared/app-name-2026-08-09/src`).

    Uses workspace import (`/api/2.0/workspace/import`) to batch-upload files.
    Returns the workspace path to the source root (where app.py, requirements.txt, etc live).
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(f"  {msg}")

    from databricks.sdk.service.workspace import ImportFormat as _ImportFormat
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    # The emitted set contains BOTH the pipeline bundle (pipelines/…) and the
    # Databricks App bundle (app/app.yaml, app/app.py, …). Only the app files go
    # to the App, and Databricks Apps expects app.yaml at the SOURCE ROOT — so
    # strip the leading `app/` prefix and deploy from that root (previously we
    # uploaded everything under `.../src` with the prefix intact, so app.yaml
    # sat at src/app/app.yaml and the deploy failed with "no command to run").
    app_files = {k[len("app/"):]: v for k, v in emitted_files.items()
                 if k.startswith("app/")}
    if not app_files:
        # Fallback: no app/ prefix — take files that look like an app bundle.
        app_files = {k: v for k, v in emitted_files.items()
                     if not k.startswith("pipelines/")}
    if "app.yaml" not in app_files and "app.yml" not in app_files:
        _log("! no app.yaml found among emitted files — cannot deploy app")
        return None

    ts = int(time.time())
    ws_app_root = f"{ws_path.rstrip('/')}/app-{app_name}-{ts}/src"
    _log(f"uploading {len(app_files)} app file(s) to {ws_app_root}...")

    made_dirs: set[str] = set()
    for rel_path, content in app_files.items():
        ws_file_path = f"{ws_app_root}/{rel_path.lstrip('/')}"
        # workspace.upload does NOT create parent folders — mkdirs first.
        parent = ws_file_path.rsplit("/", 1)[0]
        if parent not in made_dirs:
            try:
                w.workspace.mkdirs(parent)
            except Exception:  # noqa: BLE001 — idempotent; ignore "exists"
                pass
            made_dirs.add(parent)
        try:
            # WorkspaceExt.upload takes `content` (bytes | BinaryIO) +
            # `overwrite` (not `contents`/`is_overwrite`).
            w.workspace.upload(
                path=ws_file_path,
                content=io.BytesIO(content.encode("utf-8")),
                overwrite=True,
                format=_ImportFormat.AUTO,
            )
        except Exception as e:
            _log(f"! failed to upload {ws_file_path}: {str(e)[:160]}")
            return None

    _log(f"✓ app source uploaded to {ws_app_root}")
    return ws_app_root


def _ensure_app_exists(
    app_name: str,
    profile: str | None = None,
    log: Optional[Callable[[str], None]] = None,
) -> bool:
    """Ensure the Databricks App exists. If not, create it (STOPPED).
    Returns True if it exists or was created."""
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(f"  {msg}")

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    try:
        app = w.apps.get(name=app_name)
        _log(f"app {app_name} already exists")
        return True
    except Exception:
        pass

    # Create the app (starts in STOPPED state). apps.create takes an App object
    # (not a name= kwarg) in this SDK version.
    try:
        w.apps.create_and_wait(app=apps_service.App(name=app_name))
        _log(f"✓ created app {app_name}")
        return True
    except Exception as e:
        _log(f"! failed to create app {app_name}: {str(e)[:160]}")
        return False


def _wait_for_app_compute(
    app_name: str,
    max_wait_sec: int = 240,
    profile: str | None = None,
    log: Optional[Callable[[str], None]] = None,
) -> bool:
    """Poll the app's compute status until RUNNING/ACTIVE or timeout."""
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(f"  {msg}")

    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    start = time.time()
    while time.time() - start < max_wait_sec:
        try:
            app = w.apps.get(name=app_name)
            state = (getattr(app.compute_status, "state", None)
                    or getattr(app.app_status, "state", None)
                    or getattr(app, "state", None))
            # state is a ComputeState enum — normalise to its string name/value
            # (comparing the enum object against string literals never matches,
            # which is why this timed out even when compute was ACTIVE).
            state_s = getattr(state, "value", None) or getattr(state, "name", None) or str(state)
            if state_s in ("RUNNING", "ACTIVE"):
                _log(f"compute state: {state_s}")
                return True
            _log(f"compute state: {state_s} — waiting…")
        except Exception as e:
            _log(f"! error checking status: {str(e)[:160]}")
        time.sleep(6)

    _log(f"! app compute did not start within {max_wait_sec}s")
    return False


def _deploy_app_source(
    app_name: str,
    source_code_path: str,
    profile: str | None = None,
    log: Optional[Callable[[str], None]] = None,
) -> Optional[dict[str, Any]]:
    """Run `databricks apps deploy --source-code-path` via CLI.
    Returns the app's public URL on success, None on failure."""
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(f"  {msg}")

    from databricks.sdk.service import apps as apps_svc

    _log(f"deploying app {app_name} from {source_code_path}…")
    w = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    try:
        # SNAPSHOT mode = deploy the source as-is (no live git sync). Works on
        # serverless (SDK call, not the `databricks` CLI which isn't present).
        deployment = apps_svc.AppDeployment(
            source_code_path=source_code_path,
            mode=apps_svc.AppDeploymentMode.SNAPSHOT,
        )
        w.apps.deploy_and_wait(app_name=app_name, app_deployment=deployment)
        _log("✓ app deployed")
    except Exception as e:  # noqa: BLE001
        _log(f"! deploy failed: {str(e)[:300]}")
        return None

    # Fetch the app's public URL
    try:
        app = w.apps.get(name=app_name)
        url = getattr(app, "url", "") or ""
        if url:
            _log(f"app URL: {url}")
            return {"url": url, "name": app_name}
    except Exception as e:  # noqa: BLE001
        _log(f"! could not fetch app URL: {str(e)[:160]}")

    return {"name": app_name}


def deploy_for_real(
    emitted_files: dict[str, str],
    app_name: str,
    gold_schema_fq: str,
    warehouse_id: str,
    profile: str | None = None,
    log: Optional[Callable[[str], None]] = None,
) -> dict[str, Any]:
    """Deploy a generated app end-to-end: create, upload source, grant SP,
    deploy, and return metadata.

    GATED: only fires when `allow_live_deploy=True` is set in the environment.
    Otherwise returns a dry-run result for testing/validation.

    Args:
        emitted_files: {relative_path: file_contents} from app_emitter.emit_app()
        app_name: app name (e.g. "margin-cash-tracker")
        gold_schema_fq: fully-qualified gold schema (e.g. "catalog.schema")
        warehouse_id: warehouse ID the app will query
        profile: Databricks CLI profile (defaults to env or ~/.databrickscfg)
        log: optional logging callback(msg) for recording progress

    Returns:
        {"app_name": str, "url": str, "app_sp": str, ...} on success
        {"error": str, ...} on failure
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(f"  {msg}")

    # Databricks App names are constrained (lowercase, 2–30 chars). The
    # generator's descriptive name can exceed that, so normalise once here and
    # use the sanitized name for every downstream API call.
    raw_name = app_name
    app_name = _sanitize_app_name(app_name)

    if not _allow_live_deploy():
        _log("(dry-run: ALLOW_LIVE_APP_DEPLOY not set — would deploy here)")
        return {
            "dry_run": True,
            "app_name": app_name,
            "message": "deploy gated — ALLOW_LIVE_APP_DEPLOY env not set",
        }

    if app_name != raw_name:
        _log(f"app name normalised: `{raw_name}` → `{app_name}` (Apps API limit 30 chars)")
    _log(f"deploying app {app_name} (live mode)…")

    # 1. Ensure app exists
    if not _ensure_app_exists(app_name, profile=profile, log=_log):
        return {"error": f"failed to create app {app_name}"}

    # 2. Upload source files
    ws_source_path = _write_app_files_to_workspace(
        emitted_files, app_name, profile=profile, log=_log
    )
    if not ws_source_path:
        return {"error": "failed to upload app source files"}

    # 3. Grant app SP permissions
    grant_result = _grant_app_permissions(
        app_name, gold_schema_fq, warehouse_id, profile=profile, log=_log
    )
    if "error" in grant_result:
        _log(f"warn: {grant_result['error']}")

    app_sp = grant_result.get("app_sp", "(unknown)")

    # 4. Start compute (if not already running) and wait. A freshly-created app
    # is STOPPED, so we must start it before deploying source. start_and_wait is
    # idempotent — if it's already RUNNING this returns quickly.
    _log("starting app compute…")
    w_start = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    try:
        w_start.apps.start_and_wait(name=app_name)
        _log("✓ app compute running")
    except Exception as e:  # noqa: BLE001
        # Some SDK/app states reject start when already active — fall back to a
        # status poll rather than aborting the deploy.
        _log(f"start returned: {str(e)[:160]} — checking status…")
        if not _wait_for_app_compute(app_name, max_wait_sec=240, profile=profile, log=_log):
            return {"error": "app compute did not start in time"}

    # 5. Deploy the source code
    deploy_result = _deploy_app_source(
        app_name, ws_source_path, profile=profile, log=_log
    )
    if not deploy_result:
        return {"error": "failed to deploy app source"}

    # 6. Return deployment metadata
    result = {
        "app_name": app_name,
        "app_sp": app_sp,
        "gold_schema": gold_schema_fq,
        "warehouse_id": warehouse_id,
        "source_path": ws_source_path,
    }
    result.update(deploy_result)
    _log(f"✓ deployment complete: {app_name} at {result.get('url', '(URL pending)')}")
    return result
