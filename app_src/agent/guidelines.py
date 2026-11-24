"""Per-agent guidelines (system prompts) — stored in a UC Delta table so
the Databricks team can edit them via the Guidelines page in the app.

At runtime each agent calls `get(agent_name)` which returns the latest
prompt from `control.agent_guidelines` if an override exists, otherwise
the code default from `agent.prompts`.

Schema:
  agent_name        STRING NOT NULL  PRIMARY KEY
  prompt            STRING NOT NULL
  is_override       BOOLEAN          (false ⇒ row was auto-seeded from default)
  version           BIGINT
  updated_at        TIMESTAMP
  updated_by        STRING
  description       STRING            (short blurb shown on the Guidelines page)

The defaults are seeded on first read. After that, both `is_override = true`
edits and `is_override = false` defaults coexist in the table — when a user
hits "Reset to default" the row goes back to `is_override = false` with
the code-default text.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional

from agent import prompts as default_prompts
from agent.event_store import _can_use_dbsql, _conn, _CATALOG, _CONTROL_SCHEMA

# ---- Catalogue of known agents ----------------------------------------

@dataclass(frozen=True)
class AgentInfo:
    name: str             # canonical key
    label: str            # display name
    description: str      # one-line blurb for the Guidelines list
    default: str          # default prompt content
    # Optional grilling-loop config — None means this agent doesn't loop.
    default_max_iterations: int = 1
    default_loop_action: str = "proceed"  # "proceed" | "requeue" (spec_critic only)
    loop_settings_label: str = ""         # blurb shown above settings panel
    supports_iterations: bool = False


AGENT_CATALOG: list[AgentInfo] = [
    AgentInfo(
        name="spec_critic",
        label="Spec Critic",
        description="Grills the submitted spec for ambiguities before any code is generated.",
        default=default_prompts.SPEC_CRITIC,
        supports_iterations=True,
        default_max_iterations=2,
        default_loop_action="proceed",
        loop_settings_label="Grilling loop — number of rounds the Spec Critic spends "
                            "self-refining the spec before locking it in. "
                            "If still unresolved after the last round, the agent can either "
                            "auto-proceed or send the request back to the queue.",
    ),
    AgentInfo(
        name="source_profiler",
        label="Source Profiler",
        description="Inspects the Lakebase source schema + row counts and notes any quirks.",
        default=default_prompts.SOURCE_PROFILER,
    ),
    AgentInfo(
        name="pipeline_generator",
        label="Pipeline Generator",
        description="Emits Lakeflow Connect + SDP code as a DAB. Knows LDP best practice + Databricks medallion conventions.",
        default=default_prompts.PIPELINE_GENERATOR,
    ),
    AgentInfo(
        name="standards_validator",
        label="Standards Validator",
        description="Enforces Databricks engineering standards on the generated artefacts.",
        default=default_prompts.STANDARDS_VALIDATOR,
    ),
    AgentInfo(
        name="test_generator",
        label="Test Generator",
        description="Adds business-rule SQL tests on top of the deterministic checks declared in the spec.",
        default=default_prompts.TEST_GENERATOR,
    ),
    AgentInfo(
        name="pr_reviewer",
        label="PR Reviewer",
        description="Writes the human-readable PR review comment after dev deploy succeeds.",
        default=default_prompts.PR_REVIEWER,
        supports_iterations=True,
        default_max_iterations=2,
        loop_settings_label="Coder ↔ Reviewer loop — number of review rounds. "
                            "Each iteration appends a comment to the PR; if the reviewer "
                            "says 'request_changes' and iterations remain, the pipeline "
                            "generator regenerates with the reviewer's feedback as context, "
                            "then the dev table is rebuilt and re-reviewed. Default is 2 "
                            "to showcase the back-and-forth. Set to 1 for a single review pass.",
    ),
    AgentInfo(
        name="app_generator",
        label="App Generator",
        description="Designs a governed Databricks App on the Gold layer — authors the app manifest (KPIs, charts, tenancy, Genie) the emitter renders.",
        default=default_prompts.APP_GENERATOR,
    ),
    AgentInfo(
        name="app_standards_validator",
        label="App Standards Validator",
        description="Enforces the platform's App standards (Gold-only reads, single-tenant scoping, OBO auth) on the generated app manifest.",
        default=default_prompts.APP_STANDARDS_VALIDATOR,
    ),
    AgentInfo(
        name="deploy_agent",
        label="Deploy Agent (SOPs)",
        description="Standard operating procedure for the deploy_dev + deploy_prod steps.",
        default=default_prompts.DEPLOY_AGENT,
    ),
]

_BY_NAME = {a.name: a for a in AGENT_CATALOG}
_lock = threading.Lock()
# Tiny in-process cache so we don't hit DBSQL on every LLM call within a request.
_cache: dict[str, str] = {}
_cfg_cache: dict[str, dict[str, Any]] = {}
# Confluence page links: agent_name -> page_id ("" = known unlinked).
_link_cache: dict[str, str] = {}
# Whole-catalog list cache — the Agent Config page hits /api/agent-config on
# every visit; a short TTL keeps it snappy without staleness.
_LIST_TTL_SECONDS = 20.0
_list_cache: dict[str, Any] = {"at": 0.0, "data": None}
_seeded = False


def _invalidate_list_cache() -> None:
    with _lock:
        _list_cache["at"] = 0.0
        _list_cache["data"] = None


# ---- DB plumbing -----------------------------------------------------

def _table() -> str:
    return f"{_CATALOG}.{_CONTROL_SCHEMA}.agent_guidelines"


def _ensure_table() -> None:
    if not _can_use_dbsql():
        return
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_CATALOG}.{_CONTROL_SCHEMA}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {_table()} (
                    agent_name STRING NOT NULL,
                    prompt STRING NOT NULL,
                    is_override BOOLEAN NOT NULL,
                    version BIGINT NOT NULL,
                    updated_at TIMESTAMP NOT NULL,
                    updated_by STRING,
                    description STRING,
                    max_iterations INT,
                    loop_action STRING,
                    confluence_page_id STRING,
                    confluence_url STRING
                ) USING DELTA
            """)
            # Best-effort ADD COLUMN for tables that pre-date newer fields.
            for ddl in (
                f"ALTER TABLE {_table()} ADD COLUMNS (max_iterations INT)",
                f"ALTER TABLE {_table()} ADD COLUMNS (loop_action STRING)",
                f"ALTER TABLE {_table()} ADD COLUMNS (confluence_page_id STRING)",
                f"ALTER TABLE {_table()} ADD COLUMNS (confluence_url STRING)",
            ):
                try:
                    cur.execute(ddl)
                except Exception:
                    pass  # already added


def _seed_defaults() -> None:
    """Insert default rows for any agent not already in the table.
    Also runs one-shot migrations for stale defaults (e.g. pr_reviewer
    max_iterations bumped from 1 → 2)."""
    if not _can_use_dbsql():
        return
    _ensure_table()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT agent_name FROM {_table()}")
            existing = {r[0] for r in cur.fetchall()}
            for a in AGENT_CATALOG:
                if a.name in existing:
                    continue
                cur.execute(
                    f"""INSERT INTO {_table()}
                        (agent_name, prompt, is_override, version, updated_at, updated_by, description,
                         max_iterations, loop_action)
                        VALUES (?, ?, FALSE, 1, current_timestamp(), 'system', ?, ?, ?)""",
                    (a.name, a.default, a.description,
                     a.default_max_iterations, a.default_loop_action),
                )
            # One-shot: bump pr_reviewer.max_iterations from 1 → 2 on rows
            # where it's still 1 AND the row is still the auto-seeded default
            # (don't clobber user overrides).
            try:
                cur.execute(
                    f"""UPDATE {_table()}
                        SET max_iterations = 2, updated_at = current_timestamp(),
                            updated_by = 'system-migration-pr-reviewer-2x'
                        WHERE agent_name = 'pr_reviewer'
                          AND COALESCE(max_iterations, 1) < 2
                          AND is_override = FALSE""",
                )
            except Exception:
                pass
            # For every is_override=FALSE row, refresh `prompt` from the
            # current code default — this propagates prompt changes (e.g.
            # PIPELINE_GENERATOR metadata-diligence additions) to existing
            # seeded rows without clobbering user overrides.
            for a in AGENT_CATALOG:
                try:
                    cur.execute(
                        f"""UPDATE {_table()}
                            SET prompt = ?, updated_at = current_timestamp(),
                                updated_by = 'system-refresh-default'
                            WHERE agent_name = ?
                              AND is_override = FALSE
                              AND prompt <> ?""",
                        (a.default, a.name, a.default),
                    )
                except Exception:
                    pass


# ---- Confluence link plumbing ----------------------------------------

def get_confluence_link(agent_name: str) -> Optional[dict[str, str]]:
    """Return {'page_id', 'url'} if this agent's guidelines live on a
    Confluence page, else None. Cached per process ('' = known unlinked)."""
    cached = _link_cache.get(agent_name)
    if cached is not None:
        return json.loads(cached) if cached else None
    link: Optional[dict[str, str]] = None
    if _can_use_dbsql():
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT confluence_page_id, confluence_url FROM {_table()}
                            WHERE agent_name = ? LIMIT 1""",
                        (agent_name,),
                    )
                    row = cur.fetchone()
            if row and row[0]:
                link = {"page_id": row[0], "url": row[1] or ""}
        except Exception:
            return None  # don't cache transient DB failures
    with _lock:
        _link_cache[agent_name] = json.dumps(link) if link else ""
    return link


def confluence_enabled() -> bool:
    """Confluence agent-config integration is OFF by default for the PoC (100%
    Databricks-native; prompts managed in-app / Delta). Set OPS_CONFLUENCE_ENABLED=1
    to re-enable."""
    import os
    return (os.environ.get("OPS_CONFLUENCE_ENABLED", "0").strip() == "1")


def _resolve_confluence_prompt(agent_name: str) -> Optional[str]:
    """If the agent is linked to a Confluence page, return the page's text.
    None on any failure — callers fall back to the stored prompt. Disabled
    entirely unless OPS_CONFLUENCE_ENABLED=1."""
    if not confluence_enabled():
        return None
    link = get_confluence_link(agent_name)
    if not link:
        return None
    try:
        from tools import confluence_client
        if not confluence_client.configured():
            return None
        page = confluence_client.get_page_cached(link["page_id"])
        return page.get("text") or None
    except Exception:
        return None


# ---- Public API ------------------------------------------------------

def get(agent_name: str) -> str:
    """Return the current prompt for `agent_name`.

    Resolution order:
      1. linked Confluence page (the data team's living document),
      2. the Delta-stored prompt (override or seeded default),
      3. the code default.
    """
    confluence_text = _resolve_confluence_prompt(agent_name)
    if confluence_text:
        return confluence_text
    cached = _cache.get(agent_name)
    if cached is not None:
        return cached
    info = _BY_NAME.get(agent_name)
    fallback = info.default if info else ""
    if not _can_use_dbsql():
        return fallback
    try:
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT prompt FROM {_table()} WHERE agent_name = ? ORDER BY version DESC LIMIT 1",
                    (agent_name,),
                )
                row = cur.fetchone()
        if row:
            with _lock:
                _cache[agent_name] = row[0]
            return row[0]
    except Exception:
        pass
    return fallback


def list_all(force_refresh: bool = False) -> list[dict[str, Any]]:
    """List every agent with its current prompt + edit metadata.

    If the DB row is missing for an agent, we synthesise a 'default' entry
    so the Agent Config page always shows the full catalog. The result is
    TTL-cached per process; edits invalidate the cache.
    """
    if not force_refresh:
        with _lock:
            if (_list_cache["data"] is not None
                    and time.time() - _list_cache["at"] < _LIST_TTL_SECONDS):
                return _list_cache["data"]
    global _seeded
    if not _seeded:
        _seed_defaults()
        _seeded = True
    rows_by_name: dict[str, dict[str, Any]] = {}
    if _can_use_dbsql():
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT agent_name, prompt, is_override, version, updated_at, updated_by,
                                   description, max_iterations, loop_action,
                                   confluence_page_id, confluence_url
                            FROM {_table()}"""
                    )
                    cols = [d[0] for d in cur.description]
                    for r in cur.fetchall():
                        d = dict(zip(cols, r))
                        rows_by_name[d["agent_name"]] = d
        except Exception:
            pass
    out = []
    for info in AGENT_CATALOG:
        existing = rows_by_name.get(info.name)
        cached_prompt = _cache.get(info.name)
        cached_cfg = _cfg_cache.get(info.name) or {}
        common_meta = {
            "label": info.label,
            "default_prompt": info.default,
            "supports_iterations": info.supports_iterations,
            "default_max_iterations": info.default_max_iterations,
            "default_loop_action": info.default_loop_action,
            "loop_settings_label": info.loop_settings_label,
        }
        if existing:
            existing.update(common_meta)
            existing["diverges_from_default"] = (existing["prompt"] != info.default)
            # Fill missing config from defaults
            if existing.get("max_iterations") is None:
                existing["max_iterations"] = info.default_max_iterations
            if existing.get("loop_action") is None:
                existing["loop_action"] = info.default_loop_action
            out.append(existing)
        else:
            prompt = cached_prompt if cached_prompt is not None else info.default
            out.append({
                "agent_name": info.name,
                "description": info.description,
                "prompt": prompt,
                "is_override": cached_prompt is not None,
                "version": 1 if cached_prompt is not None else 0,
                "updated_at": None,
                "updated_by": "memory" if cached_prompt is not None else None,
                "diverges_from_default": (cached_prompt is not None and cached_prompt != info.default),
                "max_iterations": cached_cfg.get("max_iterations", info.default_max_iterations),
                "loop_action": cached_cfg.get("loop_action", info.default_loop_action),
                "confluence_page_id": None,
                "confluence_url": None,
                **common_meta,
            })
    with _lock:
        _list_cache["at"] = time.time()
        _list_cache["data"] = out
    return out


def fetch_one(agent_name: str) -> dict[str, Any] | None:
    info = _BY_NAME.get(agent_name)
    if not info:
        return None
    for d in list_all():
        if d["agent_name"] == agent_name:
            return d
    return None


def update(agent_name: str, prompt: str, updated_by: str,
           max_iterations: int | None = None,
           loop_action: str | None = None) -> dict[str, Any]:
    """Persist a user-edited prompt + loop settings as an override."""
    info = _BY_NAME.get(agent_name)
    if not info:
        raise ValueError(f"unknown agent: {agent_name}")
    # Resolve loop settings (None = keep existing/default)
    mi = max_iterations if max_iterations is not None else info.default_max_iterations
    la = loop_action if loop_action is not None else info.default_loop_action
    if not _can_use_dbsql():
        with _lock:
            _cache[agent_name] = prompt
            _cfg_cache[agent_name] = {"max_iterations": mi, "loop_action": la}
        _invalidate_list_cache()
        return {"agent_name": agent_name, "version": 1, "stored": "memory",
                "max_iterations": mi, "loop_action": la}
    _ensure_table()
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT COALESCE(MAX(version), 0),
                           MAX(confluence_page_id), MAX(confluence_url)
                    FROM {_table()} WHERE agent_name = ?""",
                (agent_name,),
            )
            current_version, conf_page_id, conf_url = cur.fetchone()
            new_version = current_version + 1
            cur.execute(f"DELETE FROM {_table()} WHERE agent_name = ?", (agent_name,))
            cur.execute(
                f"""INSERT INTO {_table()}
                    (agent_name, prompt, is_override, version, updated_at, updated_by, description,
                     max_iterations, loop_action, confluence_page_id, confluence_url)
                    VALUES (?, ?, TRUE, ?, current_timestamp(), ?, ?, ?, ?, ?, ?)""",
                (agent_name, prompt, new_version, updated_by, info.description, mi, la,
                 conf_page_id, conf_url),
            )
    with _lock:
        _cache[agent_name] = prompt
        _cfg_cache[agent_name] = {"max_iterations": mi, "loop_action": la}
    _invalidate_list_cache()
    return {"agent_name": agent_name, "version": new_version, "stored": "delta",
            "max_iterations": mi, "loop_action": la}


def set_confluence_link(agent_name: str, page_ref: str, updated_by: str) -> dict[str, Any]:
    """Link an agent's guidelines to a Confluence page (id or URL). The
    page's content becomes the agent's live prompt; the stored prompt
    remains as fallback. Verifies the page is reachable before saving."""
    info = _BY_NAME.get(agent_name)
    if not info:
        raise ValueError(f"unknown agent: {agent_name}")
    from tools import confluence_client
    page_id = confluence_client.extract_page_id(page_ref)
    page = confluence_client.get_page_cached(page_id, force_refresh=True)
    url = page["url"]
    if _can_use_dbsql():
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {_table()}
                        SET confluence_page_id = ?, confluence_url = ?,
                            updated_at = current_timestamp(), updated_by = ?
                        WHERE agent_name = ?""",
                    (page_id, url, updated_by, agent_name),
                )
    with _lock:
        _link_cache[agent_name] = json.dumps({"page_id": page_id, "url": url})
    _invalidate_list_cache()
    return {"agent_name": agent_name, "confluence_page_id": page_id,
            "confluence_url": url, "title": page.get("title")}


def remove_confluence_link(agent_name: str, updated_by: str) -> dict[str, Any]:
    """Unlink — the agent falls back to its Delta-stored prompt."""
    info = _BY_NAME.get(agent_name)
    if not info:
        raise ValueError(f"unknown agent: {agent_name}")
    if _can_use_dbsql():
        _ensure_table()
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE {_table()}
                        SET confluence_page_id = NULL, confluence_url = NULL,
                            updated_at = current_timestamp(), updated_by = ?
                        WHERE agent_name = ?""",
                    (updated_by, agent_name),
                )
    with _lock:
        _link_cache[agent_name] = ""
    _invalidate_list_cache()
    return {"agent_name": agent_name, "confluence_page_id": None}


def get_config(agent_name: str) -> dict[str, Any]:
    """Return loop config for `agent_name` (max_iterations, loop_action).

    Falls back to AgentInfo defaults if no override row is present.
    """
    info = _BY_NAME.get(agent_name)
    if not info:
        return {"max_iterations": 1, "loop_action": "proceed"}
    cached = _cfg_cache.get(agent_name)
    if cached:
        return cached
    if _can_use_dbsql():
        try:
            _ensure_table()
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""SELECT max_iterations, loop_action FROM {_table()}
                            WHERE agent_name = ? ORDER BY version DESC LIMIT 1""",
                        (agent_name,),
                    )
                    row = cur.fetchone()
                    if row:
                        cfg = {"max_iterations": row[0] or info.default_max_iterations,
                               "loop_action": row[1] or info.default_loop_action}
                        with _lock:
                            _cfg_cache[agent_name] = cfg
                        return cfg
        except Exception:
            pass
    return {"max_iterations": info.default_max_iterations,
            "loop_action": info.default_loop_action}


def clear_cache_for(agent_name: str) -> None:
    with _lock:
        _cache.pop(agent_name, None)
        _cfg_cache.pop(agent_name, None)
        _link_cache.pop(agent_name, None)
    _invalidate_list_cache()


def reset(agent_name: str, updated_by: str) -> dict[str, Any]:
    """Revert an override back to the code default."""
    info = _BY_NAME.get(agent_name)
    if not info:
        raise ValueError(f"unknown agent: {agent_name}")
    return update(agent_name, info.default, updated_by,
                  max_iterations=info.default_max_iterations,
                  loop_action=info.default_loop_action) | {"reset": True}


def clear_cache() -> None:
    """Invalidate the in-process cache (called after an edit)."""
    with _lock:
        _cache.clear()
        _cfg_cache.clear()
        _link_cache.clear()
    _invalidate_list_cache()
