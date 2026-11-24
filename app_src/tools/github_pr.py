"""Branch + commit + PR via plain `git` + GitHub REST API.

Authentication: requires GITHUB_TOKEN env var (a PAT or app token with
`repo` scope). On a Databricks Job, the token is sourced from a workspace
secret (see the DAB resource binding).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional


class GitHubPRError(RuntimeError):
    pass


GITHUB_API = "https://api.github.com"

# The repo generated pipeline bundles are raised against. Kept here, in the one
# module that owns GitHub, because the fallback used to be copy-pasted into ten
# call sites: when the target repo moved, `GITHUB_PIPELINES_REPO` was updated in
# the deployed config but every one of those literals still named the old repo,
# so any code path reached without the env var set (a script, a test, a job whose
# env did not carry it) silently pushed at the wrong place. One definition means
# moving repo is a one-line change.
DEFAULT_PIPELINES_REPO = "acme-corp/finance-data-products"


def pipelines_repo() -> str:
    """The `owner/name` of the repo PRs are opened against."""
    return os.environ.get("GITHUB_PIPELINES_REPO") or DEFAULT_PIPELINES_REPO


def _gh_token() -> str:
    tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not tok:
        raise GitHubPRError("GITHUB_TOKEN env var not set")
    return tok


def _run(cmd: list[str], cwd: Path, env: Optional[dict] = None) -> tuple[int, str, str]:
    full_env = os.environ.copy()
    if env:
        full_env.update(env)
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, env=full_env)
    return proc.returncode, proc.stdout, proc.stderr


def _api_request(method: str, path: str, body: Optional[dict] = None) -> dict:
    url = f"{GITHUB_API}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {_gh_token()}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if body:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8", errors="replace")
        raise GitHubPRError(f"GitHub API {method} {path} → {e.code}: {msg}") from e


def _default_branch(repo: str) -> str:
    data = _api_request("GET", f"/repos/{repo}")
    return data.get("default_branch", "main")


def _ensure_repo_initialized(repo_dir: Path, base_branch: str) -> None:
    """If repo is empty (no HEAD), create an initial commit."""
    rc, _, _ = _run(["git", "rev-parse", "--verify", "HEAD"], cwd=repo_dir)
    if rc == 0:
        return
    readme = repo_dir / "README.md"
    if not readme.exists():
        readme.write_text("# Generated pipelines\n")
    for cmd in (
        ["git", "checkout", "-b", base_branch],
        ["git", "add", "README.md"],
        ["git", "-c", "user.email=ops-agent@databricks.com", "-c", "user.name=Databricks Pipeline Studio Agent",
         "commit", "-m", "Initial commit"],
        ["git", "push", "-u", "origin", base_branch],
    ):
        rc, out, err = _run(cmd, cwd=repo_dir)
        if rc != 0:
            raise GitHubPRError(f"init repo step {cmd} failed: {err or out}")


def create_branch_with_files(
    repo: str,
    branch_name: str,
    files: dict[str, str],
    commit_message: str,
    pr_title: str,
    pr_body: str,
    base_branch: Optional[str] = None,
    work_dir: Optional[Path] = None,
    cleanup: bool = True,
) -> tuple[str, int, Path]:
    """Clone repo, create branch, write files, commit, push, open PR.

    Returns (pr_url, pr_number, repo_dir).
    """
    work = work_dir or Path(tempfile.mkdtemp(prefix="ops_repo_"))
    repo_dir = work / "repo"

    base = base_branch or _default_branch(repo)
    clone_url = f"https://x-access-token:{_gh_token()}@github.com/{repo}.git"

    # clone (empty repos are OK)
    rc, out, err = _run(["git", "clone", clone_url, str(repo_dir)], cwd=work)
    if rc != 0:
        raise GitHubPRError(f"git clone failed: {err or out}")

    _ensure_repo_initialized(repo_dir, base)

    # Fetch all branches, then decide: branch already exists on origin? → check
    # it out (so subsequent iterations push a new commit on top); else create
    # a new one off base.
    for cmd in (
        ["git", "fetch", "origin"],
        ["git", "checkout", base],
        ["git", "pull", "origin", base],
    ):
        rc, out, err = _run(cmd, cwd=repo_dir)
        if rc != 0:
            raise GitHubPRError(f"git step {cmd} failed: {err or out}")
    # Does the branch exist on origin?
    rc, out, _ = _run(
        ["git", "ls-remote", "--heads", "origin", branch_name], cwd=repo_dir
    )
    branch_exists_on_origin = rc == 0 and branch_name in (out or "")
    if branch_exists_on_origin:
        rc, out, err = _run(
            ["git", "checkout", "-B", branch_name, f"origin/{branch_name}"], cwd=repo_dir
        )
        if rc != 0:
            raise GitHubPRError(f"checkout existing branch failed: {err or out}")
    else:
        rc, out, err = _run(["git", "checkout", "-b", branch_name], cwd=repo_dir)
        if rc != 0 and "already exists" not in (err or ""):
            raise GitHubPRError(f"create branch failed: {err or out}")

    # write files
    for rel_path, contents in files.items():
        full = repo_dir / rel_path
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(contents)

    # commit + push
    for cmd in (
        ["git", "add", "."],
        ["git", "-c", "user.email=ops-agent@databricks.com", "-c", "user.name=Databricks Pipeline Studio Agent",
         "commit", "-m", commit_message],
        ["git", "push", "-u", "origin", branch_name],
    ):
        rc, out, err = _run(cmd, cwd=repo_dir)
        if rc != 0 and "nothing to commit" not in ((err or "") + (out or "")):
            raise GitHubPRError(f"git step {cmd} failed: {err or out}")

    # open PR via REST API
    try:
        pr = _api_request("POST", f"/repos/{repo}/pulls", body={
            "title": pr_title,
            "body": pr_body,
            "head": branch_name,
            "base": base,
        })
    except GitHubPRError as e:
        # If PR already exists, look it up
        if "pull request" in str(e).lower() and "already exists" in str(e).lower():
            data = _api_request("GET", f"/repos/{repo}/pulls?head={repo.split('/')[0]}:{branch_name}&state=open")
            if data:
                pr = data[0]
            else:
                raise
        else:
            raise

    pr_url = pr["html_url"]
    pr_number = int(pr["number"])

    if cleanup:
        shutil.rmtree(work, ignore_errors=True)

    return pr_url, pr_number, repo_dir


def add_pr_comment(repo: str, pr_number: int, body: str) -> None:
    _api_request("POST", f"/repos/{repo}/issues/{pr_number}/comments", body={"body": body})


def merge_pr(repo: str, pr_number: int, method: str = "squash") -> None:
    _api_request("PUT", f"/repos/{repo}/pulls/{pr_number}/merge", body={"merge_method": method})


def get_pr_status(repo: str, pr_number: int) -> dict:
    """Return a small dict describing the PR + check-rollup."""
    pr = _api_request("GET", f"/repos/{repo}/pulls/{pr_number}")
    sha = pr["head"]["sha"]
    try:
        checks = _api_request("GET", f"/repos/{repo}/commits/{sha}/check-runs")
        check_states = [r.get("conclusion") or r.get("status") for r in checks.get("check_runs", [])]
    except Exception:
        check_states = []
    return {
        "state": pr.get("state"),
        "mergeable": pr.get("mergeable"),
        "statusCheckRollup": [{"conclusion": s} for s in check_states],
        "url": pr.get("html_url"),
    }


def get_pr_review_state(repo: str, pr_number: int) -> dict:
    """Check whether a PR is approved.

    Returns:
        {
          "approved": bool,
          "pr_state": "open" | "closed",
          "merged": bool,
          "latest_reviews": [{"user": ..., "state": ..., "submitted_at": ...}],
          "changes_requested_by": [usernames],
          "approved_by": [usernames],
          "reason": str,  # human-readable explanation
        }

    Approval rule (PoC):
      - At least one reviewer's *latest* non-COMMENTED review is APPROVED, AND
      - No reviewer's latest non-COMMENTED review is CHANGES_REQUESTED.

    A PR that is already merged is treated as approved (the human merged it).
    A closed-but-not-merged PR is treated as rejected.
    """
    pr = _api_request("GET", f"/repos/{repo}/pulls/{pr_number}")
    state = pr.get("state")
    merged = bool(pr.get("merged"))
    if merged:
        return {
            "approved": True,
            "pr_state": state,
            "merged": True,
            "latest_reviews": [],
            "changes_requested_by": [],
            "approved_by": [],
            "reason": "PR is already merged.",
        }
    if state != "open":
        return {
            "approved": False,
            "pr_state": state,
            "merged": False,
            "latest_reviews": [],
            "changes_requested_by": [],
            "approved_by": [],
            "reason": f"PR is `{state}` (not open, not merged).",
        }

    # Page through reviews
    reviews: list[dict] = []
    page = 1
    while True:
        chunk = _api_request("GET", f"/repos/{repo}/pulls/{pr_number}/reviews?per_page=100&page={page}")
        if not isinstance(chunk, list) or not chunk:
            break
        reviews.extend(chunk)
        if len(chunk) < 100:
            break
        page += 1

    # Collapse to latest non-COMMENTED review per user
    latest_per_user: dict[str, dict] = {}
    for r in reviews:
        user = (r.get("user") or {}).get("login") or "unknown"
        r_state = r.get("state", "").upper()
        if r_state == "COMMENTED":
            # comment-only reviews don't change approval state
            continue
        if r_state == "DISMISSED":
            # explicitly dismissed by an admin — treat as removed
            continue
        # Keep the most recent (reviews returned in chronological order)
        latest_per_user[user] = {
            "user": user,
            "state": r_state,
            "submitted_at": r.get("submitted_at"),
        }

    approved_by = [u for u, r in latest_per_user.items() if r["state"] == "APPROVED"]
    changes_requested_by = [u for u, r in latest_per_user.items() if r["state"] == "CHANGES_REQUESTED"]

    if changes_requested_by:
        reason = f"Changes requested by: {', '.join(changes_requested_by)}. Resolve and re-approve."
        approved = False
    elif not approved_by:
        reason = "No approving review yet. Approve the PR on GitHub, then click Re-check."
        approved = False
    else:
        reason = f"Approved by: {', '.join(approved_by)}."
        approved = True

    return {
        "approved": approved,
        "pr_state": state,
        "merged": False,
        "latest_reviews": list(latest_per_user.values()),
        "changes_requested_by": changes_requested_by,
        "approved_by": approved_by,
        "reason": reason,
    }
