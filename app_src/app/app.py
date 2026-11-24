"""Databricks Pipeline Studio — FastAPI app.

Serves a multi-page UI (Jinja2 + Tailwind via CDN + Alpine.js + Cytoscape)
plus a JSON API. The LangGraph agent runs as a background thread inside
this process per request (no separate Job needed for the PoC).
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# Make absolute imports resolve in both deployed (src/ = cwd) and local-dev modes.
_HERE = Path(__file__).resolve().parent              # .../src/app
_SRC = _HERE.parent                                   # .../src
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(_HERE))                        # so `from server...` works
sys.path.insert(0, str(_SRC.parent))                  # repo root for tests

from server.routes import requests as req_routes  # noqa: E402
from server.routes import admin as admin_routes   # noqa: E402
from server.routes import guidelines as guidelines_routes  # noqa: E402
from server.routes import intake as intake_routes  # noqa: E402
from server.routes import observability as obs_routes  # noqa: E402
from server.routes import sources as sources_routes  # noqa: E402
from server.routes import releases as releases_routes  # noqa: E402
from server.routes import datasets as datasets_routes  # noqa: E402
from server.routes import connectors as connectors_routes  # noqa: E402
from server.routes import governance as governance_routes  # noqa: E402
from tools import github_pr  # noqa: E402

LOG = logging.getLogger("ops")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
)

BASE = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE / "templates"))

app = FastAPI(title="Acme Corp Pipeline Studio", version="0.3.0")


# Create the app's control tables in the background on boot. They are otherwise
# created only on the first write, so a fresh deploy 500s on the Tracker, Releases
# and Connectors reads. Idempotent (CREATE ... IF NOT EXISTS); never blocks startup.
def _ensure_control_tables_bg() -> None:
    try:
        from agent import event_store
        event_store.ensure_tables()
    except Exception as _e:  # noqa: BLE001
        LOG.warning("control tables not ensured at startup: %s", _e)


import threading as _threading  # noqa: E402
_threading.Thread(target=_ensure_control_tables_bg, daemon=True).start()

# Say so LOUDLY if the deploy-time config rewrite never ran: otherwise the app
# boots fine and every endpoint returns TABLE_OR_VIEW_NOT_FOUND against a
# placeholder catalog, which reads like data loss. See app/config_check.py.
try:
    from app.config_check import banner as _config_banner  # noqa: E402
    from app.config_check import log_at_startup as _config_log_at_startup  # noqa: E402
except ImportError:  # deployed layout puts `src/` on the path, not its parent
    from config_check import banner as _config_banner  # noqa: E402
    from config_check import log_at_startup as _config_log_at_startup  # noqa: E402
_config_log_at_startup()

try:
    from app.branding import brand as _brand  # noqa: E402
except ImportError:  # same deployed-layout fallback as above
    from branding import brand as _brand  # noqa: E402


class _RevalidateStatic(StaticFiles):
    """Serve static assets with ``Cache-Control: no-cache`` so a redeploy's
    updated JS/CSS is picked up immediately instead of a browser serving a
    stale heuristically-cached copy. ETag / Last-Modified still produce cheap
    304s — this just forces the conditional revalidation on every load."""

    async def get_response(self, path, scope):
        resp = await super().get_response(path, scope)
        resp.headers["Cache-Control"] = "no-cache"
        return resp


app.mount("/static", _RevalidateStatic(directory=str(BASE / "static")), name="static")

# OpenTelemetry — must run BEFORE FastAPI processes its first request.
# Traces persist to a Delta control table (app_spans) for the in-app
# /observability page.
try:
    from agent.otel_setup import init_otel  # noqa: E402
    init_otel(app)
except Exception as _e:
    LOG.warning("OTel init skipped: %s", _e)

# API sub-routers
app.include_router(req_routes.router, prefix="/api/requests", tags=["requests"])
app.include_router(admin_routes.router, prefix="/api/admin", tags=["admin"])
app.include_router(guidelines_routes.router, prefix="/api/agent-config", tags=["agent-config"])
# Back-compat alias for the previous URL
app.include_router(guidelines_routes.router, prefix="/api/guidelines", tags=["agent-config-alias"])
app.include_router(intake_routes.router, prefix="/api/intake", tags=["intake"])
app.include_router(obs_routes.router, prefix="/api/observability", tags=["observability"])
app.include_router(sources_routes.router, prefix="/api/sources", tags=["sources"])
app.include_router(releases_routes.router, prefix="/api/releases", tags=["releases"])
app.include_router(datasets_routes.router, prefix="/api/datasets", tags=["datasets"])
app.include_router(connectors_routes.router, prefix="/api/connectors", tags=["connectors"])
app.include_router(governance_routes.router, prefix="/api/governance", tags=["governance"])


# --- HTML pages -------------------------------------------------------

def _lakebase_enabled() -> bool:
    """Non-throwing wrapper: a page render must not fail over a source probe."""
    try:
        from agent.scenarios import lakebase_available
        return lakebase_available()
    except Exception:  # noqa: BLE001
        return False


def _workspace_label() -> str:
    """Short name of the host workspace, from DATABRICKS_HOST.

    `https://<workspace>.cloud.databricks.com` → `<workspace>`.
    Falls back to a neutral string rather than guessing: a wrong-but-plausible
    workspace name is worse than none, since it invites someone to go looking in
    the wrong place.
    """
    host = (os.environ.get("DATABRICKS_HOST") or "").strip()
    host = host.split("://", 1)[-1].split("/", 1)[0]
    if not host:
        return "this workspace"
    # Keep only the leading label: `adb-123.4.azuredatabricks.net` → `adb-123`.
    return host.split(".", 1)[0] or "this workspace"


def _ctx(request: Request, active: str, **extra) -> dict:
    return {
        "request": request,
        "active": active,
        # Deploy-time UI branding. Defaults ARE the Databricks brand; a deployment
        # can skin itself with OPS_BRAND_* env vars without a code change or a
        # long-lived themed branch. See app/branding.py.
        "brand": _brand(),
        # The workspace this is running in. Was hardcoded in home.html to one
        # workspace name, so every other deployment displayed a hostname it had
        # nothing to do with.
        "workspace_label": _workspace_label(),
        "dev_catalog": os.environ.get("DEV_CATALOG", "vmohan_demo_catalog"),
        "prod_catalog": os.environ.get("PROD_CATALOG", "vmohan_demo_catalog"),
        "github_repo": github_pr.pipelines_repo(),
        # Non-empty when the deploy-time config rewrite did not run: shown as a
        # banner so the cause is visible in the UI, not only in the app logs.
        "config_banner": _config_banner(),
        # Confluence agent-config integration + live Git/PR are disabled for the
        # PoC (100% Databricks-native; PR review is an in-app mock). Flags let
        # the UI hide those affordances without code changes.
        "confluence_enabled": os.environ.get("OPS_CONFLUENCE_ENABLED", "0").strip() == "1",
        "pr_mode": os.environ.get("OPS_PR_MODE", "mock").strip(),
        # Whether the external Postgres (Lakebase) sources are live, so the UI stops
        # calling them "disconnected" on a deployment where they are seeded and
        # their prefills build for real. Same probe the scenario list uses.
        "lakebase_enabled": _lakebase_enabled(),
        **extra,
    }


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(request, "home.html", _ctx(request, active="home"))


@app.get("/request/new", response_class=HTMLResponse)
def new_request(request: Request):
    from agent.scenarios import list_scenarios
    return templates.TemplateResponse(request, "new_request.html", _ctx(request, active="new", scenarios=list_scenarios()),
    )


@app.get("/tracker", response_class=HTMLResponse)
def tracker(request: Request):
    return templates.TemplateResponse(request, "tracker.html", _ctx(request, active="tracker"))


@app.get("/agent-config", response_class=HTMLResponse)
def agent_config_page(request: Request):
    return templates.TemplateResponse(request, "agent_config.html", _ctx(request, active="agent_config"))


@app.get("/releases", response_class=HTMLResponse)
def releases_page(request: Request):
    return templates.TemplateResponse(request, "releases.html", _ctx(request, active="releases"))


@app.get("/governance", response_class=HTMLResponse)
def governance_page(request: Request):
    return templates.TemplateResponse(request, "governance.html", _ctx(request, active="governance"))


@app.get("/connectors", response_class=HTMLResponse)
def connectors_page(request: Request):
    return templates.TemplateResponse(request, "connectors.html", _ctx(request, active="connectors"))


# Redirect the old URL for backwards compatibility
from fastapi.responses import RedirectResponse  # noqa: E402


# Datasets was folded into Connectors (Auto Loader sources are managed there now).
@app.get("/datasets")
def _datasets_redirect():
    return RedirectResponse("/connectors", status_code=308)

@app.get("/guidelines")
def _guidelines_redirect():
    return RedirectResponse("/agent-config", status_code=308)


# The Observability page was replaced by Release Management; keep the API
# router mounted but redirect the old page URL to the new tab.
@app.get("/observability")
def _observability_redirect():
    return RedirectResponse("/releases", status_code=308)


@app.get("/about", response_class=HTMLResponse)
def about(request: Request):
    return templates.TemplateResponse(request, "about.html", _ctx(request, active="about"))


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.exception_handler(Exception)
async def unhandled_exception(request: Request, exc: Exception):
    LOG.exception("unhandled error", exc_info=exc)
    return JSONResponse(status_code=500, content={"detail": str(exc)})
