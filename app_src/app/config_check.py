"""Detect config the deploy was supposed to resolve and did not.

`src/app.yaml` is committed with PLACEHOLDER values — `pipeline_studio_catalog`,
a warehouse id, an agent job id — and `deploy.sh` rewrites them to the real
per-workspace values at deploy time, then restores the file so the repo stays
clean. That works perfectly on the blessed path and is a booby trap off it:
anyone who runs `databricks sync` + `apps deploy` by hand, copies an app.yaml
between workspaces, or interrupts a deploy part-way ships the placeholders
verbatim.

The failure mode is what makes it expensive rather than merely wrong. The app
starts happily, then EVERY endpoint returns

    [TABLE_OR_VIEW_NOT_FOUND] … `pipeline_studio_catalog`.`…dev_control`.`requests`

so the Tracker shows no requests, Connectors shows no sources, and it reads like
data loss. It is not: the data is in the real catalog, and the app is looking in
one that does not exist on this workspace. (This happened during development —
see KNOWN_ISSUES 7.)

So the app now says so at startup, once, naming the unresolved value and the
command that fixes it. Deliberately a loud WARNING plus a banner rather than a
hard exit: a refusal to boot would take away the very page that explains the
problem, and some values are legitimately absent in local dev and unit tests.
"""

from __future__ import annotations

import logging
import os

LOG = logging.getLogger("ops.config")

# The committed placeholders. A DEPLOYED app reading any of these means the
# deploy-time rewrite never ran for it.
PLACEHOLDERS = {
    "OPS_CONTROL_CATALOG": "pipeline_studio_catalog",
    "DEV_CATALOG": "pipeline_studio_catalog",
    "DATABRICKS_WAREHOUSE_ID": "142b3629c3ace494",
}

# What each value should be, so the message tells the reader what to look for
# rather than making them guess from the variable name.
_MEANS = {
    "OPS_CONTROL_CATALOG": "the UC catalog holding this deployment's control tables",
    "DEV_CATALOG": "the UC catalog holding the dev bronze/silver/gold schemas",
    "DATABRICKS_WAREHOUSE_ID": "the id of a SQL warehouse in THIS workspace",
}

_FIX = ("Re-deploy with the supported path, which resolves these for the target "
        "workspace:\n      ./deploy.sh --profile <your-cli-profile>\n"
        "    A bare `databricks sync` + `apps deploy` ships the committed "
        "placeholders as-is.")


def _deployed() -> bool:
    """True when running as a Databricks App (rather than local dev / pytest).

    Apps always set DATABRICKS_APP_NAME; local runs and the test suite do not,
    and there the placeholders are harmless defaults rather than a misconfig.
    """
    return bool(os.environ.get("DATABRICKS_APP_NAME")
                or os.environ.get("DATABRICKS_APP_URL"))


def problems() -> list[str]:
    """Human-readable config problems, empty when the config resolved.

    Each entry names the variable, what it currently holds, why that is wrong,
    and what to run — written for an FE who has just deployed and has no idea
    what a placeholder catalog is.
    """
    if not _deployed():
        return []
    found: list[str] = []
    for var, placeholder in PLACEHOLDERS.items():
        actual = (os.environ.get(var) or "").strip()
        if actual == placeholder:
            found.append(
                f"{var} is still the committed placeholder `{placeholder}` — it "
                f"should be {_MEANS.get(var, 'this workspace’s own value')}.")
    if not found:
        return []
    return found + [_FIX]


def unresolved() -> list[str]:
    """Just the NAMES of the variables still holding a placeholder."""
    if not _deployed():
        return []
    return [var for var, ph in PLACEHOLDERS.items()
            if (os.environ.get(var) or "").strip() == ph]


def banner() -> str:
    """One-line summary for the UI, or '' when the config is fine.

    NAMES the offending variables rather than only counting them. The count alone —
    *"3 value(s) still at their committed placeholders"* — was true, loud and
    correctly placed, and it still sent the reader hunting through app.yaml to work
    out WHICH three, because "3 unresolved" could equally be one real problem and
    two harmless defaults. Failing open and loud is the necessary part; being
    ACTIONABLE is what makes it worth reading.
    """
    names = unresolved()
    if not names:
        return ""
    return (f"Configuration was not resolved for this workspace — "
            f"{len(names)} value(s) still at their committed placeholders: "
            f"{', '.join(names)}. The app is reading a catalog that does not exist "
            f"here. Your data is safe — re-deploy with "
            f"`./deploy.sh --profile <profile>`.")


def log_at_startup() -> list[str]:
    """Log any problems once, loudly. Returns them for the caller/tests."""
    probs = problems()
    if not probs:
        return []
    LOG.error("=" * 78)
    LOG.error("PIPELINE STUDIO CONFIG NOT RESOLVED — the app will not find its tables")
    for p in probs:
        LOG.error("  • %s", p)
    LOG.error("Every API call will fail with TABLE_OR_VIEW_NOT_FOUND until this is "
              "fixed. No data has been lost: it lives in the real catalog, and this "
              "app is pointed at one that does not exist on this workspace.")
    LOG.error("=" * 78)
    return probs
