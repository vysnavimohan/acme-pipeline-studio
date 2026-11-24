"""Job entry point — every agent execution is one run of the
`databricks-pipeline-studio-agent` job, launched by the App via the Jobs API
(see agent/runner.py).

Parameters (wired through jobs.yml job parameters):
  --request-id   the request to execute
  --mode         full | gate | from_step
  --start-step   StepName.value, required for mode=from_step
  --config-json  env config rendered by the DAB at deploy time (catalogs,
                 schemas, warehouse, repo, model URI, secret scope)

Execution path: load the registered agent model FROM Unity Catalog
(models:/<...>@champion) and run its predict(). The model artifact ships
its own copy of the agent/tools/common packages (models-from-code), so
the registered version's code is what executes. If the model can't be
loaded, fall back to the bundle's in-repo code.

Serverless quirks this file works around:
  * spark_python_task exec()s the script WITHOUT __file__;
  * the /Workspace FUSE mount is unreliable for module imports that
    happen mid-run — so the bundle src tree is copied to LOCAL disk once
    at startup and imports resolve from there, never from FUSE;
  * we import NOTHING from the agent package before the UC model loads,
    so the model's code copy (not the bundle copy) binds `agent` in
    sys.modules — real model-version provenance.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile

LOG = logging.getLogger("ops.run")

BUNDLE_SRC = ("/Workspace/Users/vaishnavi.mohan@databricks.com/.bundle/"
              "databricks-pipeline-studio/dev/files/src")


def _bootstrap_env(config_json: str) -> None:
    """Apply DAB-rendered config to the environment (existing env wins)."""
    try:
        cfg = json.loads(config_json) if config_json else {}
    except Exception as e:
        print(f"[ops] WARNING: bad --config-json ({e}) — continuing with ambient env",
              flush=True)
        cfg = {}
    for k, v in cfg.items():
        if v is not None and str(v) != "":
            os.environ.setdefault(k, str(v))


def _secret(scope: str, key: str) -> str | None:
    try:
        import base64
        from databricks.sdk import WorkspaceClient
        resp = WorkspaceClient().secrets.get_secret(scope=scope, key=key)
        return base64.b64decode(resp.value).decode("utf-8")
    except Exception:
        return None


def _ensure_secrets() -> None:
    """In a Job there's no App resource injection — pull credentials from
    the workspace secret scope via the SDK. All best-effort."""
    scope = os.environ.get("GITHUB_TOKEN_SCOPE", "databricks-pipeline-studio")
    if not os.environ.get("GITHUB_TOKEN"):
        tok = _secret(scope, os.environ.get("GITHUB_TOKEN_KEY", "github-token"))
        if tok:
            os.environ["GITHUB_TOKEN"] = tok
            print(f"[ops] GITHUB_TOKEN loaded from secret scope {scope}", flush=True)
        else:
            print("[ops] WARNING: could not load GitHub token from secrets", flush=True)
    # Confluence (optional — only used when an agent's guidelines are linked
    # to a Confluence page).
    if not os.environ.get("CONFLUENCE_API_TOKEN"):
        email = _secret(scope, "confluence-email")
        token = _secret(scope, "confluence-api-token")
        if email and token:
            os.environ.setdefault("CONFLUENCE_EMAIL", email)
            os.environ["CONFLUENCE_API_TOKEN"] = token
            print("[ops] Confluence credentials loaded from secrets", flush=True)


def _locate_bundle_src() -> str | None:
    """Find the bundle's src directory (the one containing agent/orchestrate.py)."""
    candidates = [BUNDLE_SRC]
    try:
        candidates.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    except NameError:
        pass  # serverless exec()s without __file__
    cwd = os.getcwd()
    candidates += [cwd, os.path.dirname(cwd)]
    candidates += [p for p in sys.path if p]
    candidates += [os.path.dirname(p) for p in sys.path if p]
    for c in candidates:
        try:
            if os.path.exists(os.path.join(c, "agent", "orchestrate.py")):
                return c
        except OSError:
            continue
    return None


def _stage_src_locally() -> str | None:
    """Copy the bundle src tree from /Workspace FUSE to local disk so module
    imports never touch FUSE mid-run. Returns the local path (added LAST on
    sys.path so the UC model's code dir keeps precedence)."""
    src = _locate_bundle_src()
    if src is None:
        print(f"[ops] WARNING: could not locate bundle src (cwd={os.getcwd()})", flush=True)
        return None
    if not src.startswith("/Workspace"):
        # Local dev — import in place.
        if src not in sys.path:
            sys.path.append(src)
        print(f"[ops] using src in place: {src}", flush=True)
        return src
    local = os.path.join(tempfile.gettempdir(), "ops_agent_src")
    try:
        shutil.rmtree(local, ignore_errors=True)
        shutil.copytree(
            src, local,
            ignore=shutil.ignore_patterns("__pycache__", "app", "*.yaml"),
        )
        if local not in sys.path:
            sys.path.append(local)
        print(f"[ops] staged bundle src to local disk: {local}", flush=True)
        return local
    except Exception as e:
        print(f"[ops] WARNING: local staging failed ({e}) — importing from FUSE", flush=True)
        if src not in sys.path:
            sys.path.append(src)
        return src


def _hash_code_tree(src_dir: str) -> str:
    """sha256 over the .py files of agent/, tools/ and common/ under `src_dir`.

    MUST stay byte-identical to `scripts/register_model.code_fingerprint`, which
    stamps the same value on the model version — the two are compared. Duplicated
    rather than imported because this runs BEFORE the UC model binds `agent` on
    sys.path, and importing anything from the repo here would defeat the provenance
    the model load depends on.
    """
    import hashlib
    h = hashlib.sha256()
    for rel in ("agent", "tools", "common"):
        base = os.path.join(src_dir, rel)
        for dirpath, dirnames, filenames in sorted(os.walk(base)):
            dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
            for name in sorted(filenames):
                if not name.endswith(".py"):
                    continue
                full = os.path.join(dirpath, name)
                # `src/<rel>/...` — the same relative form register_model hashes.
                relpath = os.path.relpath(full, src_dir)
                h.update(("src/" + relpath.replace(os.sep, "/")).encode())
                try:
                    with open(full, "rb") as fh:
                        h.update(fh.read())
                except Exception:
                    return ""
    return h.hexdigest()[:16]


def _warn_if_model_is_stale(model_uri: str) -> str | None:
    """Compare the loaded model's code fingerprint against the code sitting on
    disk beside this run, and shout if they differ.

    This exists because the previous defences did not fire. `check_model_current.py`
    only runs when a human runs it, and on 2026-08-05 it printed
    "check skipped (no control catalog resolved)" and then "skipped (No module
    named 'mlflow')" — a check that skips silently is decoration. Meanwhile the
    job happily executed a model registered 82 minutes BEFORE the fix it was
    supposed to be running, and the resulting PR was reviewed twice against
    pre-fix output while every step reported success (KNOWN_ISSUES 1c).

    Returns a warning string (also printed) or None when current/undeterminable.
    A mismatch is NOT fatal: refusing to run would strand a request over a
    diagnostic, and the bundle-synced copy is not always meant to match. Being
    LOUD and specific is the goal — silence is what cost the time.
    """
    # COMPUTE the fingerprint of the code deployed beside this run, rather than
    # reading a committed `_code_fingerprint.txt`.
    #
    # That file was the first design and it was wrong: it is a SNAPSHOT, so every
    # commit touching src/agent|tools|common without re-running register_model.py
    # left it behind, and the guard then reported a stale MODEL when the truth was a
    # stale FILE. Four commits did exactly that, and the first clean-workspace deploy
    # warned "STALE AGENT MODEL" about a model registered ninety seconds earlier
    # from the very code it was comparing against. A check whose reference value
    # ages is a check that cries wolf — and a guard that cries wolf gets ignored,
    # which is how the real staleness ships silently.
    #
    # Hashing at check time cannot go stale by construction.
    src = _locate_bundle_src()
    if not src:
        return None
    expected = _hash_code_tree(src)
    if not expected:
        return None
    try:
        if "@" not in model_uri:
            return None
        name, alias = model_uri.split("models:/", 1)[-1].rsplit("@", 1)
        from mlflow import MlflowClient
        mv = MlflowClient().get_model_version_by_alias(name, alias)
        live = (mv.tags or {}).get("code_fingerprint", "")
    except Exception:
        return None
    if not live:
        msg = (f"the registered model (v{mv.version}) carries NO code_fingerprint, so "
               f"it predates this check — it may be running old agent code. "
               f"Re-run scripts/register_model.py to be sure.")
    elif live != expected:
        msg = (f"STALE AGENT MODEL: the job loaded @{alias} v{mv.version} whose code "
               f"fingerprint is {live}, but the agent code deployed alongside it is "
               f"{expected}. THIS RUN IS EXECUTING OLD CODE — any recent fix to "
               f"src/agent, src/tools or src/common is NOT in effect. "
               f"Fix: run scripts/register_model.py (or deploy.sh without --skip-model).")
    else:
        print(f"[ops] agent code fingerprint {live} matches the registered model",
              flush=True)
        return None
    print(f"[ops] WARNING: {msg}", flush=True)
    return msg


def _describe_model_version(model_uri: str) -> str:
    """Resolve `models:/name@alias` to the concrete version + its git_ref tag.

    `@champion` is a moving alias, so the job log alone never says WHICH code
    snapshot ran. That cost real debugging time: a fix was committed and the
    bundle re-synced, but the agent job kept executing an hour-old snapshot,
    because the model carries its own copy of `agent/` in code_paths and that
    copy wins on sys.path. The symptom is the worst kind — the fix is present
    in the repo AND in /Workspace, and the run still behaves as if it is not.
    Printing version + git_ref makes a stale @champion obvious in the log
    instead of something you have to go and infer from timestamps.

    Best-effort by design: this is diagnostics, so a registry hiccup must not
    stop the run that was about to work.
    """
    try:
        if "@" not in model_uri:
            return model_uri
        name, alias = model_uri.split("models:/", 1)[-1].rsplit("@", 1)
        from mlflow import MlflowClient
        mv = MlflowClient().get_model_version_by_alias(name, alias)
        git_ref = (mv.tags or {}).get("git_ref", "unknown")
        return f"{name} v{mv.version} (git_ref={git_ref})"
    except Exception as e:  # noqa: BLE001
        return f"{model_uri} (version lookup failed: {e})"


def _execute_via_uc_model(request_id: str, mode: str, start_step: str | None):
    """Load the registered agent from Unity Catalog and run it.
    Returns the result dict, or None if the model path is unavailable.

    The model's code_paths dir lands at sys.path[0] when loaded, so the
    `agent` package binds to the MODEL'S copy — nothing here may import
    agent.* beforehand."""
    model_uri = (os.environ.get("AGENT_MODEL_URI") or "").strip()
    if not model_uri:
        return None
    try:
        import mlflow
        mlflow.set_registry_uri("databricks-uc")
        print(f"[ops] loading agent model from UC: {model_uri}", flush=True)
        print(f"[ops] resolved to {_describe_model_version(model_uri)}", flush=True)
        stale_warning = _warn_if_model_is_stale(model_uri)
        model = mlflow.pyfunc.load_model(model_uri)
    except Exception as e:
        print(f"[ops] WARNING: UC model load failed ({e}) — "
              "falling back to in-repo agent code", flush=True)
        return None
    import pandas as pd
    df = pd.DataFrame([{
        "request_id": request_id, "mode": mode, "start_step": start_step or "",
    }])
    if stale_warning:
        # Into the request's OWN event log, not just the job log. The job log is
        # somewhere you go looking only once you already suspect a problem; the
        # tracker is what a person is actually watching.
        _record_stale_model_warning(request_id, stale_warning)
    raw = model.predict(df)
    result = json.loads(raw[0]) if raw else {}
    result["executed_via"] = model_uri
    if stale_warning:
        result["stale_model_warning"] = stale_warning
    return result


def _record_stale_model_warning(request_id: str, message: str) -> None:
    """Best-effort: a diagnostic must never be the thing that fails a run."""
    try:
        from agent.event_store import append_event
        from agent.state import StepEvent, StepName, StepStatus
        append_event(request_id, StepEvent(
            step=StepName.SPEC_CRITIC, status=StepStatus.IN_PROGRESS,
            message=f"⚠ {message}", details={"kind": "stale_agent_model"}))
    except Exception as e:  # noqa: BLE001
        print(f"[ops] could not record the stale-model warning: {e}", flush=True)


def _record_entrypoint_failure(request_id: str, mode: str, exc: Exception) -> None:
    try:
        from agent.event_store import append_event, update_request
        from agent.state import StepEvent, StepName, StepStatus
        append_event(request_id, StepEvent(
            step=StepName.DONE, status=StepStatus.FAILED,
            message=f"Job entrypoint crashed before the agent graph ran ({mode}): {exc}",
        ))
        update_request(request_id, final_status="failed")
    except Exception as e2:
        print(f"[ops] WARNING: could not record entrypoint failure: {e2}", flush=True)


def main() -> int:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(name)s :: %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--mode", default="full",
                        choices=["full", "gate", "promote", "from_step", "gold"])
    parser.add_argument("--start-step", default="")
    parser.add_argument("--config-json", default="")
    args = parser.parse_args()

    _bootstrap_env(args.config_json)
    _ensure_secrets()
    _stage_src_locally()

    start_step = args.start_step or None
    try:
        result = _execute_via_uc_model(args.request_id, args.mode, start_step)
        if result is None:
            from agent.orchestrate import execute
            result = execute(args.request_id, mode=args.mode, start_step=start_step)
            result["executed_via"] = "in-repo code (UC model unavailable)"
    except Exception as e:
        # orchestrate.execute records graph crashes itself; this catches
        # entrypoint-level failures (model load, env, import) so the request
        # never sticks in `in_progress` with no explanation.
        _record_entrypoint_failure(args.request_id, args.mode, e)
        raise
    finally:
        # Flush whichever agent package ended up bound (model copy or local).
        try:
            from agent.otel_setup import flush as otel_flush
            otel_flush()
        except Exception:
            pass

    print(json.dumps(result, default=str), flush=True)

    # A crashed agent must not leave a GREEN job run. `orchestrate.execute`
    # catches node crashes, records final_status=failed, and RETURNS normally —
    # so nothing propagates, main() returns 0, and the Jobs UI lists SUCCESS. On
    # 2026-08-06 that made an ImportError that killed a run indistinguishable
    # from a clean build in `jobs list-runs`: twelve consecutive SUCCESS rows,
    # one of which had died. The request row was right; the job was reassuring.
    #
    # Raising here (rather than sys.exit, which the IPython runner turns into a
    # spurious failure even for code 0) marks the task FAILED so the run list
    # tells the truth.
    if str((result or {}).get("status", "")).lower() in ("failed", "rejected"):
        raise RuntimeError(
            f"agent run finished with status={result.get('status')}: "
            f"{result.get('error') or 'see the request event log'}")
    return 0


if __name__ == "__main__":
    # NOTE: no sys.exit() — serverless runs this via an IPython shell where
    # SystemExit (even code 0) marks the task FAILED.
    main()
