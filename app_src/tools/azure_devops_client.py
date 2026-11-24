"""Azure DevOps (Repos + Pull Requests) integration — SCAFFOLD, DISABLED.

Databricks uses Azure DevOps for source control. For the PoC, live external Git /
DevOps integration is DE-SCOPED (100% Databricks-native; the PR + AI review is
an in-app mock — see agent/nodes.py `_pr_mode()` / branch_and_pr). This module
is a commented-out skeleton so the Databricks team can wire real Azure DevOps later:
it mirrors the surface the agent already uses for GitHub (tools/github_pr.py) —
create a branch with files, open a PR, comment, get review state, merge.

To activate (future work, Databricks side):
  1. Provision an Azure DevOps PAT (scopes: Code Read & Write, Pull Request
     Contribute) and store it in a Databricks secret scope.
  2. Set OPS_PR_MODE=azure_devops and the env below (org/project/repo/secret).
  3. Implement the methods using the Azure DevOps REST API (api-version 7.1)
     and route agent/nodes.py PR steps to this client when OPS_PR_MODE==azure_devops.
  4. Add automated tests + a dry-run against a sandbox project.

Azure DevOps REST reference: https://learn.microsoft.com/en-us/rest/api/azure/devops/

NOTE: Everything below is intentionally commented out — importing this module
has no side effects and adds no dependencies. It is documentation-as-code.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# import base64
# import json
# import os
# import urllib.request
# from typing import Optional
#
# _API_VERSION = "7.1"
#
#
# def _cfg() -> dict:
#     """Resolve Azure DevOps config from env (set by the deploy)."""
#     return {
#         "org": os.environ["AZDO_ORG"],            # e.g. "databricks"
#         "project": os.environ["AZDO_PROJECT"],    # e.g. "UDP"
#         "repo": os.environ["AZDO_REPO"],          # e.g. "pipeline-studio-pipelines"
#         "pat": os.environ["AZDO_PAT"],            # from a Databricks secret
#     }
#
#
# def _auth_header(pat: str) -> dict:
#     # Azure DevOps Basic auth: ":" + PAT, base64-encoded.
#     token = base64.b64encode(f":{pat}".encode()).decode()
#     return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}
#
#
# def _base_url(cfg: dict) -> str:
#     return (f"https://dev.azure.com/{cfg['org']}/{cfg['project']}"
#             f"/_apis/git/repositories/{cfg['repo']}")
#
#
# def _api(method: str, url: str, headers: dict, body: dict | None = None) -> dict:
#     data = json.dumps(body).encode() if body is not None else None
#     req = urllib.request.Request(url, data=data, method=method, headers=headers)
#     with urllib.request.urlopen(req, timeout=60) as resp:
#         return json.loads(resp.read().decode())
#
#
# def create_branch_with_files(branch_name: str, files: dict[str, str],
#                              commit_message: str, pr_title: str,
#                              pr_body: str) -> tuple[str, int, str]:
#     """Create a branch off the default branch with `files`, push a commit, and
#     open a PR. Returns (pr_web_url, pr_id, branch_name).
#
#     Steps (Azure DevOps REST):
#       1. GET refs?filter=heads/main          -> base objectId (old commit)
#       2. POST pushes with refUpdates[{name: refs/heads/<branch>, oldObjectId}]
#          + commits[{comment, changes:[{changeType: add/edit, item.path, newContent}]}]
#       3. POST pullrequests {sourceRefName, targetRefName, title, description}
#     """
#     raise NotImplementedError("Azure DevOps integration is a scaffold for the PoC.")
#
#
# def add_pr_comment(pr_id: int, comment_md: str) -> None:
#     """POST pullrequests/{pr_id}/threads {comments:[{content}], status:'active'}."""
#     raise NotImplementedError
#
#
# def get_pr_review_state(pr_id: int) -> dict:
#     """GET pullrequests/{pr_id}/reviewers -> map votes (10=approved,
#     -10=rejected) to {'approved': bool, 'reason': str}."""
#     raise NotImplementedError
#
#
# def merge_pr(pr_id: int) -> None:
#     """PATCH pullrequests/{pr_id} {status:'completed', completionOptions:
#     {mergeStrategy:'squash'}, lastMergeSourceCommit:{...}}."""
#     raise NotImplementedError
