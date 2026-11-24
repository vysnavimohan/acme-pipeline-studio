"""Admin endpoints — demo reset, etc.

The reset endpoint:
  - Truncates the control tables (or wipes local JSON files).
  - Optionally closes any open PRs in the pipelines repo.
"""

from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException

from agent import event_store

router = APIRouter()


def _close_open_prs() -> dict:
    """Close any open PRs in the pipelines repo (best-effort).

    Uses the same GITHUB_TOKEN the agent uses. Returns a small report.
    """
    import urllib.error
    import urllib.request
    import json

    from tools import github_pr
    repo = github_pr.pipelines_repo()
    tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not tok:
        return {"closed": 0, "note": "no GITHUB_TOKEN; skipping PR close"}

    def _api(method: str, path: str, body: dict | None = None) -> dict:
        url = f"https://api.github.com{path}"
        data = json.dumps(body).encode() if body else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {tok}")
        req.add_header("Accept", "application/vnd.github+json")
        if body:
            req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())

    try:
        prs = _api("GET", f"/repos/{repo}/pulls?state=open&per_page=50")
    except Exception as e:
        return {"closed": 0, "note": f"could not list PRs: {e}"}

    closed_numbers = []
    for pr in prs if isinstance(prs, list) else []:
        try:
            _api("PATCH", f"/repos/{repo}/pulls/{pr['number']}", body={"state": "closed"})
            closed_numbers.append(pr["number"])
        except urllib.error.HTTPError as e:
            # ignore individual failures
            closed_numbers.append({"pr": pr["number"], "error": str(e)})
    return {"closed": len(closed_numbers), "numbers": closed_numbers}


@router.post("/seed-cfo-datasets")
def seed_cfo_datasets():
    """Seed ONLY the Acme Corp ERP feeds as ready Auto Loader
    datasets (sales_invoice_lines, ar_transactions, inventory_on_hand, ...) so they show in the
    New Request Data Browser. Deterministic + idempotent; does NOT touch requests
    / releases / other fixtures — safe to call without a full reset."""
    from agent import autoloader_store
    return autoloader_store.seed_cfo_datasets()


@router.post("/reset")
def reset(close_prs: bool = True, seed: bool = True):
    """Wipe request state and (by default) reseed with 1-2 pre-built
    completed example requests so the tracker is never empty.

    Use `seed=false` for a fully-empty reset; `close_prs=false` to skip
    closing any open PRs in the pipelines repo.
    """
    cleared = event_store.reset_all()
    closed = _close_open_prs() if close_prs else {"closed": 0, "note": "skipped (close_prs=false)"}
    seeded = event_store.seed_demo() if seed else {"count": 0, "note": "skipped (seed=false)"}
    return {"cleared": cleared, "closed_prs": closed, "seeded": seeded}
