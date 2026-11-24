"""Acme Corp intake: the LLM grill plus the review cards, derived from the grill's answers.

One session walks one journey:

    grill -> reuse -> pipeline -> metric_view -> dq -> design -> governance -> ready

  grill        the LLM asks what the product is for, the outputs (dashboard, Genie Agent
               or both), the audience and decisions, the day-one questions, the measures
               and the cadence. Nothing is assumed. It sets next_step "consume" when the
               need is complete.
  reuse        agent/dp_planner.match_needs(): what already exists (REUSE / EXTEND / GAP /
               RAW / NO_DATA). NO_DATA that blocks the build is an honest stop with "Log this
               request"; submit stays locked.
  pipeline     only when an item is RAW (only in a raw feed): agent/pipeline_planner.author_pipeline()
               drafts the SDP SQL (raw -> bronze -> silver -> gold) and validates it with a
               validate-only pipeline update (1 to 3 minutes). Skipped otherwise.
  metric_view  dp_planner.author_metric_view(): skipped when everything is reused, otherwise
               the new metric view, validated on the warehouse (or "pending" when it reads
               a planned gold table, validated at build).
  dq           dp_planner.select_dq(): existing rules for the upstream tables, proposed gap
               rules, plain-English rules (checked against the real columns), then save.
  design       dp_planner.design(): only the outputs that were asked for; benchmarks with
               their computed answers; dashboard widgets with their validation status.
               Until Confirm design, a change request (chat or the card) runs dp_planner.design_change():
               it adds, replaces or removes widgets and benchmarks, and refuses what the data lacks.
  governance   the LLM confirms governance; one "confirm" from the analyst is enough.
  ready        the analyst clicks Submit (POST /api/requests, data_product v2).

Every response carries `journey` (the full card state) and `spec_snapshot` (the spec, with
the `data_product` v2 block inside it). Long steps run as background jobs; the page polls
GET /journey/{session_id} while `journey.jobs` shows one running.

Sessions live in-process (single-replica Databricks App). GC after 1h idle.
"""

from __future__ import annotations

import copy
import datetime as dt
import json
import logging
import os
import re
import threading
import time
import uuid
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel

from agent import acme_journey as acme
from agent import dp_planner as planner
from agent.intake_prompt import build_intake_prompt
from agent.llm import get_llm

LOG = logging.getLogger("ops.intake")

router = APIRouter()

STAGES = ["grill", "reuse", "pipeline", "metric_view", "dq", "design", "governance", "ready"]
# Stages where a card is open and waiting for the analyst. The LLM does not answer chat here: the
# server points at the open card and never claims the spec is ready (ready_to_submit stays false).
_CARD_STAGES = ("reuse", "pipeline", "metric_view", "dq", "design")
_GRILL_STEPS = {"purpose", "outputs", "audience", "questions", "measures", "cadence", "consume", "no_data"}
OUTPUTS = ("dashboard", "genie_agent")
_EXISTS_TERMS = ("already exist", "what exists", "what's already", "what is already", "reuse")
_SESSION_TTL_SECONDS = 3600
_JOB_TIMEOUT_S = 240
_PIPELINE_TIMEOUT_S = 900   # draft + validate-only update (1 to 3 min) + up to two repairs
_CONFIRM_WORDS = {"confirm", "confirmed", "yes", "y", "yes confirm", "yes confirmed", "ok", "okay",
                  "approve", "approved", "looks good", "go ahead", "agreed", "correct", "that's right",
                  "thats right", "sounds good", "confirm governance"}
# A chat message in the dq stage that reads like a rule (a constraint, not a question).
_RULE_HINT = re.compile(r"(\bmust\b|\bshould\b|\bcannot\b|can't|\bnever\b|\bat least\b|\bat most\b|"
                        r"\bunder\b|\bover\b|\babove\b|\bbelow\b|\bgreater\b|\bless\b|\bmore than\b|"
                        r"\bfewer\b|\bbetween\b|\bpositive\b|\bnegative\b|non-?negative|\bnot null\b|"
                        r"\bno more than\b|[<>]=?|=)", re.I)
_RULE_KEYS = ("table_name", "rule_name", "constraint_sql", "severity", "origin", "enabled",
              "disabled_reason", "why")
_STOP = {"a", "an", "the", "for", "of", "to", "and", "or", "with", "by", "on", "in", "at", "from", "that",
         "this", "we", "i", "need", "want", "show", "showing", "so", "they", "their", "them", "is", "are",
         "be", "it", "can", "new", "create", "make", "build", "please", "dashboard", "agent", "genie"}

# Pushbacks: the refusals the grill always applies, recorded once each for the data product.
PB_RAW = ("I won't point the Genie Agent or the dashboard at the raw or gold tables. They read the governed "
          "metric views, so every team sees the same numbers. A raw feed is modelled first by a pipeline, and the "
          "metric view sits on that. I can also log the request.")
PB_PBI = ("No. If Power BI or Excel recalculates a measure, it will disagree with the Genie Agent the first "
          "time someone filters differently. Each measure is defined once, in a metric view, and every "
          "consumer reads it from there.")
PB_PII = ("Not on the shared dashboard. Customer names are personal and commercially sensitive, and the "
          "dashboard is shared with the whole team. It can show segments and totals; anyone who needs names "
          "asks the Genie Agent, which applies their own permissions.")


# =====================================================================
# Session store (in-process, single-replica only)
# =====================================================================

class _IntakeSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.lock = threading.RLock()
        self.history: list[dict] = []          # [{role, content}] for the LLM, alternating
        self.spec_so_far: dict = {}
        self.step_index: str = "purpose"
        self.stage: str = "grill"
        self.pending_messages: list[str] = []  # cards that finished in the background; shown on the next poll
        self.jobs: dict[str, dict] = {}        # name -> {status, message, error, started, abandoned}
        self.cat: Optional[dict] = None        # catalog snapshot taken at the reuse check
        self.catalog_snapshot_at: str = ""
        # Reuse card
        self.reuse: Optional[dict] = None           # planner.match_needs() result, normalised
        self.reuse_decision: Optional[str] = None   # "agree" once the analyst agrees to build
        self.log_request: Optional[dict] = None     # {request_id, logged_at} once logged
        # Pipeline card (RAW items only): the SDP pipeline drafted from the raw feed
        self.pipeline: Optional[dict] = None        # normalised pipeline_planner.author_pipeline() result
        self.pipeline_added: bool = False
        # Metric view card
        self.metric_view: Optional[dict] = None     # planner.author_metric_view() result
        self.metric_view_added: bool = False
        self.mv_skipped: bool = False
        # DQ card
        self.dq_loaded: bool = False
        self.dq_existing: list[dict] = []           # live rules for the upstream tables
        self.dq_proposed: list[dict] = []           # gap rules, each with accepted flag
        self.dq_custom: list[dict] = []             # analyst's accepted plain-English rules
        self.dq_rejected: list[dict] = []           # [{text, reason, ...}]
        self.dq_review_id: str = ""
        # Design card
        self.design: Optional[dict] = None          # {genie_agent, dashboard, dropped}
        self.design_confirmed: bool = False
        self.governance_confirmed: bool = False
        # Recorded pushbacks (what the analyst asked for that we refused)
        self.pushbacks: list[dict] = []
        self.last_active = time.time()   # epoch seconds


_sessions: dict[str, _IntakeSession] = {}
_lock = threading.Lock()


def _gc_locked() -> None:
    cutoff_ts = time.time() - _SESSION_TTL_SECONDS
    dead = [sid for sid, s in _sessions.items() if s.last_active < cutoff_ts]
    for sid in dead:
        _sessions.pop(sid, None)


def _get_session(session_id: str) -> _IntakeSession:
    with _lock:
        _gc_locked()
        sess = _sessions.get(session_id)
        if sess is None:
            sess = _IntakeSession(session_id)
            _sessions[session_id] = sess
        sess.last_active = time.time()
        return sess


def _require_session(session_id: str) -> _IntakeSession:
    with _lock:
        _gc_locked()
        sess = _sessions.get(session_id)
        if sess is None:
            raise HTTPException(404, "Intake session not found (sessions expire after an hour idle). "
                                     "Start again from the ask.")
        sess.last_active = time.time()
        return sess


def _deep_merge(base: dict, patch: dict) -> dict:
    """Recursively merge patch into base. Patch values overwrite base (lists are replaced)."""
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _add_turn(sess: _IntakeSession, role: str, content: str) -> None:
    """Append a turn to the LLM history. Merges into the previous turn of the same role, so the
    history always alternates user / assistant (some endpoints reject non-alternating roles)."""
    if not content:
        return
    if sess.history and sess.history[-1]["role"] == role:
        sess.history[-1]["content"] += "\n\n" + content
    else:
        sess.history.append({"role": role, "content": content})


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _s(v: Any) -> str:
    return str(v).strip() if v is not None and not isinstance(v, (list, dict)) else ""


def _list_str(v: Any) -> list[str]:
    if isinstance(v, str):
        v = [v]
    return [str(x).strip() for x in (v or []) if str(x).strip()]


def _norm_outputs(v: Any) -> list[str]:
    """Map whatever the LLM wrote onto ["dashboard", "genie_agent"] (ordered, unique)."""
    if isinstance(v, str):
        v = [v]
    want: set[str] = set()
    for x in v or []:
        t = str(x).lower()
        if "both" in t:
            want |= {"dashboard", "genie_agent"}
        if "dash" in t or "aibi" in t or "lakeview" in t:
            want.add("dashboard")
        if "genie" in t or "agent" in t:
            want.add("genie_agent")
    return [o for o in OUTPUTS if o in want]


def _norm_measures(v: Any) -> list[dict]:
    out = []
    for m in v or []:
        if isinstance(m, str):
            if m.strip():
                out.append({"name": m.strip(), "definition": "", "dimensions": []})
        elif isinstance(m, dict) and str(m.get("name") or "").strip():
            out.append({"name": str(m.get("name")).strip(),
                        "definition": _s(m.get("definition")),
                        "dimensions": _list_str(m.get("dimensions"))})
    return out


def _slugify(text: str) -> str:
    words = re.findall(r"[a-z0-9]+", (text or "").lower())
    keep = [w for w in words if w not in _STOP][:4]
    slug = "_".join(keep)[:40].strip("_")
    if slug and slug[0].isdigit():
        slug = "p_" + slug
    return slug


def _title(slug: str) -> str:
    return " ".join(w.capitalize() for w in (slug or "data product").split("_") if w) or "Data product"


def _is_fq(name: Any) -> bool:
    return isinstance(name, str) and name.count(".") == 2


def _cadence_sla(cadence: str) -> tuple[int, str]:
    """(freshness minutes, cron). Weekly Monday 07:00 unless the analyst asked for daily."""
    if "daily" in (cadence or "").lower():
        return 1440, "0 7 * * *"
    return 10080, "0 7 * * 1"


def _base_spec() -> dict:
    """The flat spec every intake session starts from. Nothing business-specific: the intent
    and the source/target fields are filled from the grill and the cards, never pre-set."""
    return {
        "intent": "",
        "source_kind": "file",
        "source_system": "acme_erp",
        "source_tables": [],
        "source_incremental_column": "",
        "source_primary_key": [],
        "target_catalog": acme.CATALOG,
        "target_schema": acme.DEV_SCHEMA,
        "target_table": "",
        "target_write_mode": "overwrite",
        "transform_description": "",
        "transform_business_rules": [],
        "validation_not_null": [],
        "validation_unique": [],
        "validation_row_count_tolerance_pct": 5.0,
        "governance_data_owner": "vaishnavi.mohan@databricks.com",
        "governance_business_owner": "",
        "governance_domain": "finance",
        "governance_classification": "confidential",
        "sla_mode": "batch",
        "sla_freshness_minutes": 10080,
        "sla_schedule_cron": "0 7 * * 1",
        "required_approvers": ["finance-data-owner", "data-platform-engineering"],
    }


# =====================================================================
# The need: captured by the grill, the input to every planner call
# =====================================================================

def _need_view(sess: _IntakeSession) -> dict[str, Any]:
    s = sess.spec_so_far
    purpose = _s(s.get("need_purpose"))
    # Name the product after WHAT it measures (first two measures), not after the wording of the ask.
    _meas = _norm_measures(s.get("need_measures"))
    _mslug = _slugify(" ".join(str((m or {}).get("name") or "") for m in _meas[:2]))
    slug = _s(s.get("need_slug")) or _mslug or _slugify(purpose) or ("data_product" if purpose else "")
    return {
        "purpose": purpose,
        "audience": _s(s.get("need_audience")),
        "decisions": _list_str(s.get("need_decisions")),
        "outputs": _norm_outputs(s.get("need_outputs")),
        "questions": _list_str(s.get("need_questions")),
        "measures": _norm_measures(s.get("need_measures")),
        "cadence": _s(s.get("need_cadence")),
        "slug": slug,
    }


_MISSING_CHECKS = [
    ("purpose", "what it is for", "purpose"),
    ("outputs", "the output (dashboard, Genie Agent or both)", "outputs"),
    ("audience", "who uses it and the decisions it drives", "audience"),
    ("questions", "the day-one questions", "questions"),
    ("cadence", "how often it refreshes", "cadence"),
]


def _missing(sess: _IntakeSession) -> list[dict[str, str]]:
    need = _need_view(sess)
    out = []
    for key, label, step in _MISSING_CHECKS:
        if key == "audience":
            ok = bool(need["audience"])
        else:
            ok = bool(need[key])
        if not ok:
            out.append({"field": key, "label": label, "step": step})
    return out


def _sync(sess: _IntakeSession) -> None:
    """Derive the flat spec fields from the need and the cards, then write the data_product v2 block."""
    s = sess.spec_so_far
    need = _need_view(sess)
    if need["purpose"]:
        s["intent"] = need["purpose"]
    # The slug is derived on every read (from the measures once they are known); don't freeze an early
    # purpose-based slug into need_slug.
    if need["audience"]:
        s["app_audience"] = need["audience"]
    s["transform_business_rules"] = ["Q: " + q for q in need["questions"]]
    if need["cadence"]:
        s["sla_mode"] = "batch"
        s["sla_freshness_minutes"], s["sla_schedule_cron"] = _cadence_sla(need["cadence"])
    dp = _data_product(sess)
    s["data_product"] = dp
    s["source_tables"] = list(dp["data_sources"])
    s["target_table"] = (dp["metric_view"] or {}).get("name", "").split(".")[-1] if dp["metric_view"] else ""


def _data_product(sess: _IntakeSession) -> dict[str, Any]:
    need = _need_view(sess)
    reuse = sess.reuse or {}
    items = reuse.get("items") or []
    mv_out = None
    if sess.metric_view and sess.metric_view_added:
        mv = sess.metric_view
        mv_out = {k: mv.get(k) for k in ("name", "yaml", "sql_template", "measures", "dimensions",
                                         "validated", "sample")}
    data_sources: list[str] = []
    for it in items:
        a = it.get("asset")
        if _is_fq(a) and a not in data_sources:
            data_sources.append(a)
    if mv_out and mv_out["name"] not in data_sources:
        data_sources.append(mv_out["name"])
    rules = [_rule_out(r) for r in _active_rules(sess)] if sess.dq_loaded else []
    design = sess.design or {}
    return {
        "version": 3,
        "engine": "genie_code",
        "need": need,
        "catalog_snapshot_at": sess.catalog_snapshot_at,
        "reuse": {
            "coverage_pct": reuse.get("coverage_pct", 0),
            "items": items,
            "related_assets": reuse.get("related_assets") or [],
            "buildable": bool(reuse.get("buildable", False)),
            "no_data": reuse.get("no_data") or [],
        },
        "pipeline": copy.deepcopy(sess.pipeline) if (sess.pipeline and sess.pipeline_added) else None,
        "metric_view": mv_out,
        "data_sources": data_sources,
        "dq": {
            "tables": sorted({r["table_name"] for r in rules}),
            "rules": rules,
            "rejected": sess.dq_rejected,
            "review_id": sess.dq_review_id,
        },
        "design": {
            "genie_agent": design.get("genie_agent") if "genie_agent" in need["outputs"] else None,
            "dashboard": design.get("dashboard") if "dashboard" in need["outputs"] else None,
        },
        "pushbacks": sess.pushbacks,
    }


# =====================================================================
# Background jobs (planner calls that take seconds to minutes)
# =====================================================================

_JOB_TEXT = {
    "match": "Checking what already exists in the catalog…",
    "pipeline": "Drafting the pipeline SQL from the raw feed's real columns…",
    "metric_view": "Writing the metric view and checking it on the warehouse. This takes 30 to 60 seconds.",
    "dq": "Reading the data-quality rules for these tables…",
    "design": "Designing the Genie Agent and dashboard, and running each query once. This can take a minute.",
    "design_change": "Checking the change against the data and running each new query…",
}
# Progress for long steps: (seconds elapsed, message). The last phase reached is shown.
_JOB_PHASES = {
    "pipeline": [
        (0, "Drafting the pipeline SQL from the raw feed's real columns…"),
        (25, "Validating the pipeline on the warehouse with a validate-only update. This takes one to three minutes."),
        (150, "Still validating. Pipeline validation can take up to three minutes; a repair adds about a minute."),
    ],
    "design_change": [
        (0, "Checking the change against the data and running each new query…"),
        (30, "Running the new queries on the warehouse. Each one takes a few seconds."),
        (120, "Still running. A query that fails gets one repair round."),
    ],
}
CHANGE_STARTED = ("Checking the change against the catalog and running each new query. The design card updates when it "
                  "is done, about a minute. Nothing is locked until you confirm the design.")


def _busy(sess: _IntakeSession) -> bool:
    return any(_job_view(sess, n)["status"] == "running" for n in list(sess.jobs))


def _job_view(sess: _IntakeSession, name: str) -> dict[str, Any]:
    job = sess.jobs.get(name)
    if not job:
        return {"status": "", "message": "", "error": "", "elapsed_s": 0}
    elapsed = int(time.time() - job["started"])
    limit = job.get("timeout_s", _JOB_TIMEOUT_S)
    if job["status"] == "running" and elapsed > limit:
        job["status"] = "error"
        job["error"] = f"This took longer than {limit // 60} minutes. Try again."
        job["abandoned"] = True
    message = job["message"]
    for at, text in job.get("phases") or []:
        if elapsed >= at:
            message = text
    return {"status": job["status"], "message": message, "error": job["error"], "elapsed_s": elapsed}


def _job_status(sess: _IntakeSession, name: str) -> str:
    return _job_view(sess, name)["status"]


def _start_job(sess: _IntakeSession, name: str, work: Callable[[], Any],
               done: Callable[[_IntakeSession, Any], None]) -> None:
    job = {"status": "running", "message": _JOB_TEXT[name], "error": "", "started": time.time(),
           "abandoned": False, "timeout_s": _PIPELINE_TIMEOUT_S if name == "pipeline" else _JOB_TIMEOUT_S,
           "phases": _JOB_PHASES.get(name)}
    with sess.lock:
        sess.jobs[name] = job

    def run() -> None:
        try:
            out = work()
        except Exception as e:  # noqa: BLE001
            LOG.exception("intake job %s failed", name)
            with sess.lock:
                if not job["abandoned"]:
                    job["status"] = "error"
                    job["error"] = str(e)[:300] or e.__class__.__name__
            return
        with sess.lock:
            if job["abandoned"]:
                return
            try:
                done(sess, out)
                job["status"] = "done"
            except Exception as e:  # noqa: BLE001
                LOG.exception("intake job %s could not be applied", name)
                job["status"] = "error"
                job["error"] = str(e)[:300] or e.__class__.__name__

    threading.Thread(target=run, daemon=True, name=f"intake-{name}-{sess.session_id[:8]}").start()


def _guard(sess: _IntakeSession, *stages: str) -> None:
    if sess.stage not in stages:
        raise HTTPException(409, f"This step is not open yet (the journey is at '{sess.stage}').")
    if _busy(sess):
        raise HTTPException(409, "Still working on the last step. Wait for it to finish.")


def _catalog_block() -> str:
    try:
        return planner.catalog_prompt_block(planner.catalog())
    except Exception as e:  # noqa: BLE001
        LOG.warning("catalog unavailable for the grill prompt: %s", e)
        return ""


def _related_key(a: dict) -> tuple:
    """Metric views first, then Genie Agents and dashboards that are not per-run [dev] or rehearsal copies."""
    title = str(a.get("title") or "")
    noisy = ("[dev" in title) or ("rehearsal" in title.lower()) or bool(re.search(r"\d{4}-\d{2}-\d{2}", title))
    return (a.get("kind") != "metric_view", noisy, title.lower())


def _norm_reuse(res: dict) -> dict[str, Any]:
    res = dict(res or {})
    return {
        "coverage_pct": int(res.get("coverage_pct") or 0),
        "items": list(res.get("items") or []),
        "related_assets": sorted(res.get("related_assets") or [], key=_related_key),
        "buildable": bool(res.get("buildable", False)),
        "no_data": list(res.get("no_data") or []),
    }


def _open_reuse(sess: _IntakeSession) -> None:
    """Consume beat: the need is complete, so check what exists."""
    sess.stage = "reuse"
    sess.reuse = None
    sess.reuse_decision = None
    sess.log_request = None
    need = _need_view(sess)

    def work() -> tuple[dict, dict]:
        cat = planner.catalog()
        return cat, planner.match_needs(need, cat)

    def done(s: _IntakeSession, out: tuple[dict, dict]) -> None:
        cat, res = out
        s.cat = cat
        s.catalog_snapshot_at = str(cat.get("snapshot_at") or _now_iso())
        s.reuse = _norm_reuse(res)
        r = s.reuse
        if not r["buildable"]:
            gaps = "; ".join(str(x.get("missing") or "").rstrip(".") for x in r["no_data"] if x.get("missing")) or \
                "the data for this ask is not in the catalog"
            s.pending_messages.append((f"I can't build this from the data I can see. Missing: {gaps}. "
                                 "I haven't built anything. Log the request if you want the data team to look at it."))
        else:
            extra = ""
            if r["no_data"]:
                extra = " Some of it is not in the data and is left out: " + \
                    "; ".join(x.get("need", "") for x in r["no_data"]) + "."
            raw = _raw_items(r)
            if raw:
                extra += (f" {len(raw)} of the needs are only in raw files. A pipeline builds them from the feed "
                          "first, and the metric view reads that pipeline's gold table.")
                s.pending_messages.append(("Here is what the catalog already has, and what the raw feeds add. "
                                     "Each need is marked REUSE, EXTEND, GAP, RAW or NO_DATA. Agree to build the plan, "
                                     "and only what is missing gets built." + extra))
            else:
                s.pending_messages.append((f"Here is what already exists: {r['coverage_pct']}% of the ask is covered. "
                                     "Each need is marked REUSE, EXTEND, GAP, RAW or NO_DATA. Agree to build the plan, "
                                     "and only the gap gets built." + extra))

    sess.stage = "reuse"
    _start_job(sess, "match", work, done)


def _raw_items(reuse: dict) -> list[dict]:
    return [i for i in (reuse or {}).get("items") or [] if i.get("decision") == "RAW"]


def _pipeline_needed(sess: _IntakeSession) -> bool:
    return bool(sess.reuse and sess.reuse.get("buildable") and _raw_items(sess.reuse))


def _planner_reuse(sess: _IntakeSession) -> dict:
    return copy.deepcopy(sess.reuse) or {}


def _planned_pipeline(sess: _IntakeSession) -> Optional[dict]:
    """The pipeline the planner reads its planned gold table (and expectations) from: only once it is in the spec."""
    return copy.deepcopy(sess.pipeline) if (sess.pipeline and sess.pipeline_added) else None


def _norm_pipeline(p: dict) -> dict[str, Any]:
    """The pipeline card's shape. Columns are shown as 'name TYPE' strings; expectations are flat rows."""
    p = dict(p or {})
    src = dict(p.get("source") or {})
    cols = []
    for c in src.get("columns") or []:
        if isinstance(c, dict):
            text = f"{_s(c.get('name'))} {_s(c.get('type'))}".strip()
        else:
            text = _s(c)
        if text:
            cols.append(text)
    tables = dict(p.get("tables") or {})
    gold = tables.get("gold")
    gold = [gold] if isinstance(gold, str) else list(gold or [])
    val = dict(p.get("validation") or {})
    exps = []
    for e in p.get("expectations") or []:
        if isinstance(e, dict):
            exps.append({"table": _s(e.get("table")), "name": _s(e.get("name")),
                         "constraint_sql": _s(e.get("constraint_sql")),
                         "action": _s(e.get("action")) or "warn"})
    return {
        "name": _s(p.get("name")),
        "source": {"path": _s(src.get("path")), "format": _s(src.get("format")) or "csv",
                   "files": src.get("files"), "columns": cols},
        "tables": {"bronze": _s(tables.get("bronze")), "silver": _s(tables.get("silver")),
                   "gold": [_s(g) for g in gold if _s(g)]},
        "sql": str(p.get("sql") or ""),
        "expectations": exps,
        "validated": p.get("validated") is True,
        "validation": {"update_id": _s(val.get("update_id")), "state": _s(val.get("state")),
                       "error": _s(val.get("error")), "seconds": val.get("seconds")},
        # The planned columns of bronze, silver and gold: what the metric view may read at build time.
        "planned_columns": {_s(t): [_s(c) for c in cols or [] if _s(c)]
                            for t, cols in dict(p.get("planned_columns") or {}).items()},
    }


def _start_pipeline(sess: _IntakeSession) -> None:
    """RAW items: draft the SDP pipeline from the feed and validate it (background, 1 to 3 minutes)."""
    need = _need_view(sess)
    reuse = copy.deepcopy(sess.reuse)
    cat = sess.cat
    sess.stage = "pipeline"
    sess.pipeline = None
    sess.pipeline_added = False

    def work() -> Any:
        try:
            from agent import pipeline_planner  # the pipeline workstream's module
        except ImportError as e:
            raise RuntimeError("The pipeline planner is not available in this build yet.") from e
        out = pipeline_planner.author_pipeline(need, reuse, cat)
        if not out:
            raise RuntimeError("The planner could not draft a pipeline for the raw feed. Try again.")
        return out

    def done(s: _IntakeSession, out: Any) -> None:
        s.pipeline = _norm_pipeline(out)
        s.stage = "pipeline"
        p = s.pipeline
        if p["validated"]:
            s.pending_messages.append((f"The pipeline {p['name']} validates on the warehouse: bronze, silver and "
                                 f"gold, with {len(p['expectations'])} data-quality expectations. Look at the plan "
                                 "and the SQL, then add it to the spec."))
        else:
            err = p["validation"]["error"] or "unknown error"
            s.pending_messages.append((f"The pipeline did not validate: {err}. Try again, or change the ask in chat."))

    _start_job(sess, "pipeline", work, done)


def _mv_ok(mv: Optional[dict]) -> bool:
    """A metric view can be added when it validated now, or reads a planned gold table (validated at build)."""
    return bool(mv) and mv.get("validated") in (True, "pending")


def _start_mv(sess: _IntakeSession) -> None:
    need = _need_view(sess)
    reuse = _planner_reuse(sess)
    cat = sess.cat
    sess.stage = "metric_view"
    sess.metric_view = None
    sess.metric_view_added = False

    pipeline = _planned_pipeline(sess)

    def work() -> Any:
        return planner.author_metric_view(need, reuse, cat, pipeline=pipeline)

    def done(s: _IntakeSession, out: Any) -> None:
        if out is None:
            s.mv_skipped = True
            s.metric_view = None
            s.pending_messages.append(("Everything you asked for is already reused, so no metric view is needed. "
                                 "Next is data quality."))
            _start_dq(s)
            return
        mv = dict(out)
        mv.setdefault("validated", False)
        s.metric_view = mv
        s.mv_skipped = False
        s.stage = "metric_view"
        if mv["validated"] == "pending":
            s.pending_messages.append(("The metric view reads the planned gold table, so it is checked on the "
                                 "warehouse at build time, after the pipeline has run. Look at the measures and the "
                                 "YAML, then add it to the spec."))
        elif mv["validated"]:
            s.pending_messages.append(("The metric view is written and checked on the warehouse. Look at the measures, "
                                 "the sample rows and the SQL, then add it to the spec."))
        else:
            s.pending_messages.append((f"The metric view did not validate: {mv.get('error') or 'unknown error'}. "
                                 "Try again, or change the measures in chat."))

    _start_job(sess, "metric_view", work, done)


def _start_dq(sess: _IntakeSession) -> None:
    reuse = _planner_reuse(sess)
    mv = copy.deepcopy(sess.metric_view) if (sess.metric_view and sess.metric_view_added) else None
    cat = sess.cat
    sess.stage = "dq"

    pipeline = _planned_pipeline(sess)

    def work() -> Any:
        return planner.select_dq(reuse, mv, cat, pipeline=pipeline)

    def done(s: _IntakeSession, res: Any) -> None:
        _load_dq_from_planner(s, res or {})
        c = _dq_counts(s)
        s.stage = "dq"
        s.pending_messages.append((f"Data quality: {c['kept']} of {c['existing_total']} existing rules kept, "
                             f"{len(s.dq_proposed)} gap rules proposed. Switch any rule off with a reason, "
                             "accept the gap rules you want, or add your own in plain English. Then save."))

    _start_job(sess, "dq", work, done)


def _start_design(sess: _IntakeSession) -> None:
    need = _need_view(sess)
    reuse = _planner_reuse(sess)
    mv = copy.deepcopy(sess.metric_view) if (sess.metric_view and sess.metric_view_added) else None
    cat = sess.cat
    sess.stage = "design"
    sess.design = None
    sess.design_confirmed = False

    def work() -> Any:
        return planner.design(need, reuse, mv, cat)

    def done(s: _IntakeSession, res: Any) -> None:
        s.design = _norm_design(res or {}, _need_view(s))
        s.design_confirmed = False
        s.stage = "design"
        if s.design.get("skipped"):
            s.pending_messages.append("Nothing to design: " + s.design["skipped"])
            return
        dropped = s.design.get("dropped") or []
        note = (f" {len(dropped)} dashboard widget(s) failed their query and were left out." if dropped else "")
        s.pending_messages.append(("The Genie Agent and dashboard design is ready. Check the benchmarks and the "
                             "widgets, then confirm the design. Customer names stay off the shared dashboard." + note))

    _start_job(sess, "design", work, done)


def _change_reply(changes: list[str], refused: list[dict]) -> str:
    """What changed and what was refused, in the chat and on the card."""
    parts = []
    if changes:
        parts.append("Changed the design:\n" + "\n".join("- " + c for c in changes))
    if refused:
        lines = []
        for r in refused:
            line = f"- {r.get('ask') or 'Request'}: {r.get('reason') or 'not in the data'}"
            if r.get("closest"):
                line += f" Closest in the data: {r['closest']}."
            lines.append(line)
        parts.append("Not added:\n" + "\n".join(lines))
    if not changes and not refused:
        parts.append("I didn't change the design.")
    parts.append("Confirm the design on the card when it reads right. Nothing is locked until you confirm.")
    return "\n\n".join(parts)


def _start_design_change(sess: _IntakeSession, text: str) -> str:
    """Run planner.design_change() as a background job. The reply (what changed, what was refused) is
    delivered on the next poll; the design card refreshes with it. ready_to_submit stays false."""
    need = _need_view(sess)
    reuse = _planner_reuse(sess)
    mv = copy.deepcopy(sess.metric_view) if (sess.metric_view and sess.metric_view_added) else None
    cur = copy.deepcopy(sess.design)
    cat = sess.cat

    def work() -> Any:
        return planner.design_change(cur, text, need, reuse, mv, cat)

    def done(s: _IntakeSession, res: Any) -> None:
        res = res or {}
        s.design = _norm_design(res.get("design") or {}, _need_view(s))
        s.design_confirmed = False
        s.stage = "design"
        s.pending_messages.append(_change_reply(res.get("changes") or [], res.get("refused") or []))

    _start_job(sess, "design_change", work, done)
    return CHANGE_STARTED


# =====================================================================
# DQ helpers
# =====================================================================

def _load_dq_from_planner(sess: _IntakeSession, res: dict) -> None:
    """Split the planner's rule list into the card's existing / proposed lists."""
    existing, proposed = [], []
    for r in res.get("rules") or []:
        row = {
            "table_name": r.get("table_name", ""), "rule_name": r.get("rule_name", ""),
            "constraint_sql": r.get("constraint_sql", ""), "severity": r.get("severity", "warn"),
            "why": r.get("why") or "", "owner": r.get("owner") or "",
            "origin": r.get("origin", "existing"), "enabled": bool(r.get("enabled", True)),
            "disabled_reason": r.get("disabled_reason") or "",
        }
        if row["origin"] == "proposed":
            row["accepted"] = bool(r.get("accepted", False))
            row["violations_now"] = r.get("violations_now")
            proposed.append(row)
        else:
            existing.append(row)
    sess.dq_existing = existing
    sess.dq_proposed = proposed
    sess.dq_custom = []
    sess.dq_rejected = [{"text": x.get("text", ""), "reason": x.get("reason", "")} for x in res.get("rejected") or []]
    sess.dq_loaded = True


def _active_rules(sess: _IntakeSession) -> list[dict]:
    """The working rule set: existing + accepted gap rules + accepted custom rules."""
    return sess.dq_existing + [p for p in sess.dq_proposed if p.get("accepted")] + sess.dq_custom


def _find_rule(sess: _IntakeSession, table: str, rule: str) -> Optional[dict]:
    for r in _active_rules(sess):
        if r["table_name"] == table and r["rule_name"] == rule:
            return r
    return None


def _rule_out(r: dict) -> dict:
    out = {k: r.get(k, "") for k in _RULE_KEYS}
    out["enabled"] = bool(r.get("enabled", True))
    return out


def _dq_counts(sess: _IntakeSession) -> dict[str, Any]:
    on = [r for r in sess.dq_existing if r["enabled"]]
    added = [p for p in sess.dq_proposed if p.get("accepted")] + sess.dq_custom
    enabled_all = [r for r in _active_rules(sess) if r["enabled"]]
    return {
        "existing_total": len(sess.dq_existing),
        "kept": len(on),
        "disabled": len(sess.dq_existing) - len(on),
        "added": len(added),
        "rejected": len(sess.dq_rejected),
        "enabled_total": len(enabled_all),
        "drop": sum(1 for r in enabled_all if r.get("severity") == "drop"),
        "warn": sum(1 for r in enabled_all if r.get("severity") == "warn"),
        "tables": len({r["table_name"] for r in enabled_all}),
    }


def _dq_payload(sess: _IntakeSession) -> dict[str, Any]:
    return {
        "existing": sess.dq_existing,
        "proposed": sess.dq_proposed,
        "custom": sess.dq_custom,
        "rejected": sess.dq_rejected,
        "counts": _dq_counts(sess),
        "review_id": sess.dq_review_id,
    }


def _nl_rule(text: str, cat: Optional[dict]) -> dict[str, Any]:
    """Plain-English rule -> {table_name, rule_name, constraint_sql, severity, why} via the LLM.
    The table list is the catalog's gold tables (with their columns), so the rule can name any of them."""
    from langchain_core.messages import HumanMessage as HM, SystemMessage as SM
    tables = {g["name"]: g.get("columns", []) for g in ((cat or {}).get("gold_tables") or [])}
    sys_msg = ("Turn a data-quality rule written in plain English into JSON with keys table_name, rule_name, "
               "constraint_sql (a SQL boolean expression that must be TRUE for good rows), severity "
               "('drop' or 'warn'), why. Use ONLY these tables and use the column names the user says, "
               "even if they are not in the list (the app checks them): " + json.dumps(tables) +
               ". Reply with the JSON only.")
    resp = get_llm().invoke([SM(content=sys_msg), HM(content=text)])
    raw = resp.content if isinstance(resp.content, str) else str(resp.content)
    m = re.search(r"\{.*\}", raw, re.S)
    rule = json.loads(m.group(0)) if m else {}
    rule.setdefault("severity", "warn")
    return rule


def _record_rejected(sess: _IntakeSession, text: str, reason: str, rule: Optional[dict] = None) -> None:
    if any(r["text"] == text and r["reason"] == reason for r in sess.dq_rejected):
        return
    entry = {"text": text, "reason": reason}
    if rule:
        entry["table_name"] = rule.get("table_name", "")
        entry["constraint_sql"] = rule.get("constraint_sql", "")
    sess.dq_rejected.append(entry)


def _add_custom_rule(sess: _IntakeSession, text: str) -> dict[str, Any]:
    """Plain-English rule -> SQL (LLM) -> checked against the real table's columns (acme.check_constraint).
    Accepted rules join the working set (origin custom); rejected ones are recorded with the reason."""
    def _rejected(reason: str, rule: Optional[dict] = None) -> dict[str, Any]:
        _record_rejected(sess, text, reason, rule)
        return {"status": "rejected", "text": text, "reason": reason, "rule": None}

    try:
        rule = _nl_rule(text, sess.cat)
    except Exception as e:  # noqa: BLE001
        return _rejected(f"I couldn't turn that into a rule ({str(e)[:120]}). Name the table and the column.")
    table = (rule.get("table_name") or "").strip()
    constraint = (rule.get("constraint_sql") or "").strip()
    if not table or not constraint:
        return _rejected("I couldn't tell which table or column this is about. Name the table and the column.")
    from agent.genie_engine import DQ_TABLES
    if table not in DQ_TABLES:
        return _rejected(f"rules can only be added to the source tables this product reads; {table} is not one of them")
    # Deterministic: every column the analyst NAMED (snake_case word) must exist on the table and must be
    # the one the rule uses. The LLM may not swap in a different column.
    cols = {c.lower() for c in acme._table_columns(table)}
    named = {w.lower() for w in re.findall(r"\b[a-z][a-z0-9]*_[a-z0-9_]+\b", text, re.I)} - {table.lower()}
    missing = sorted(n for n in named if n not in cols)
    if missing:
        return _rejected(f"column(s) {', '.join(missing)} don't exist on {table}. Available: {', '.join(sorted(cols))}",
                         {"table_name": table, "constraint_sql": constraint})
    swapped = sorted(n for n in named if n not in constraint.lower())
    if swapped:
        return _rejected(f"the rule doesn't use the column you named ({', '.join(swapped)}); name the column again",
                         {"table_name": table, "constraint_sql": constraint})
    chk = acme.check_constraint(table, constraint)
    if not chk["ok"]:
        return _rejected(chk["reason"], {"table_name": table, "constraint_sql": constraint})
    new = {
        "table_name": table,
        "rule_name": (rule.get("rule_name") or text)[:120],
        "constraint_sql": constraint,
        "severity": rule.get("severity") if rule.get("severity") in ("drop", "warn") else "warn",
        "why": rule.get("why") or text,
        "owner": "",
        "origin": "custom",
        "enabled": True,
        "disabled_reason": "",
        "violations_now": chk["violations_now"],
    }
    sess.dq_custom.append(new)
    return {"status": "accepted", "text": text, "reason": "", "rule": _rule_out(new),
            "violations_now": chk["violations_now"]}


# =====================================================================
# Design helpers
# =====================================================================

def _valid_state(v: Any) -> Any:
    """True / False, or "pending" for a widget on a planned table (validated at build)."""
    return "pending" if v == "pending" else bool(v)


def _norm_design(res: dict, need: dict) -> dict[str, Any]:
    """Only the outputs that were asked for. Benchmarks get stable ids so edits map back to their SQL."""
    outs = need["outputs"]
    title = _title(need["slug"])
    out: dict[str, Any] = {"genie_agent": None, "dashboard": None,
                           "skipped": str(res.get("skipped") or ""),
                           "dropped": list(res.get("dropped") or []),
                           "warnings": [str(w) for w in (res.get("warnings") or [])]}
    ga = res.get("genie_agent") if "genie_agent" in outs else None
    if ga:
        g = dict(ga)
        g["title"] = _s(g.get("title")) or title
        g["description"] = _s(g.get("description"))
        g["instructions"] = _s(g.get("instructions"))
        g["sample_questions"] = _list_str(g.get("sample_questions")) or list(need["questions"])
        g["data_sources"] = _list_str(g.get("data_sources"))
        g["access"] = _s(g.get("access"))
        g["benchmarks"] = [{"id": i, "question": _s(b.get("question")), "sql": b.get("sql") or "",
                            "expected": _s(b.get("expected"))}
                           for i, b in enumerate(g.get("benchmarks") or []) if _s(b.get("question"))]
        out["genie_agent"] = g
    db = res.get("dashboard") if "dashboard" in outs else None
    if db:
        d = dict(db)
        d["title"] = _s(d.get("title")) or title
        d["refresh"] = _s(d.get("refresh")) or (need["cadence"] or "weekly, Monday 07:00")
        d["filters"] = _list_str(d.get("filters"))
        d["pages"] = [{"name": _s(p.get("name")) or "Main",
                       "widgets": [{"title": _s(w.get("title")), "type": _s(w.get("type")) or "table",
                                    "sql": w.get("sql") or "", "validated": _valid_state(w.get("validated", True))}
                                   for w in (p.get("widgets") or []) if _s(w.get("title"))]}
                      for p in (d.get("pages") or [])]
        d["include_customer_names"] = False
        out["dashboard"] = d
    return out


def _merge_design(sess: _IntakeSession, edits: dict) -> dict[str, Any]:
    """Apply the analyst's edits. Benchmark SQL and widget SQL are never taken from the client:
    they come from the planner's output (matched by id, or by widget title)."""
    cur = copy.deepcopy(sess.design or {"genie_agent": None, "dashboard": None, "dropped": []})
    e = edits or {}
    g_in = e.get("genie_agent") if isinstance(e.get("genie_agent"), dict) else None
    if cur.get("genie_agent") and g_in is not None:
        orig = cur["genie_agent"]
        orig["title"] = _s(g_in.get("title")) or orig["title"]
        if "instructions" in g_in:
            orig["instructions"] = _s(g_in.get("instructions"))
        by_id = {b["id"]: b for b in orig["benchmarks"]}
        kept = []
        for b in g_in.get("benchmarks") or []:
            ob = by_id.get(b.get("id"))
            if ob is None:
                continue
            kept.append({"id": ob["id"], "question": _s(b.get("question")) or ob["question"],
                         "expected": _s(b.get("expected")) or ob["expected"], "sql": ob["sql"]})
        orig["benchmarks"] = kept
    d_in = e.get("dashboard") if isinstance(e.get("dashboard"), dict) else None
    if cur.get("dashboard") and d_in is not None:
        orig = cur["dashboard"]
        orig["title"] = _s(d_in.get("title")) or orig["title"]
        orig["refresh"] = _s(d_in.get("refresh")) or orig["refresh"]
        widgets = {w["title"]: w for p in orig["pages"] for w in p["widgets"]}
        pages_by_name = {p["name"]: p for p in orig["pages"]}
        new_pages = []
        for p in d_in.get("pages") or []:
            base = pages_by_name.get(_s(p.get("name")))
            if base is None:
                continue
            ws = [widgets[w_title] for w_title in
                  (_s(w.get("title")) for w in (p.get("widgets") or [])) if w_title in widgets]
            new_pages.append({"name": base["name"], "widgets": ws})
        if new_pages:
            orig["pages"] = new_pages
    return cur


def _pushback(sess: _IntakeSession, ask: str, answer: str) -> None:
    if not any(p["ask"] == ask for p in sess.pushbacks):
        sess.pushbacks.append({"ask": ask, "answer": answer})


_PUSHBACK_TRIGGERS = [
    (re.compile(r"raw tables?|gold tables? directly|directly on (the )?(tables|gold)|on the (raw|gold) tables?", re.I),
     "Build it on the raw or gold tables", PB_RAW),
    (re.compile(r"(power ?bi|excel)\b.*\b(calculat|comput|recalc)|\b(calculat|comput|recalc)\w*\b.*\b(power ?bi|excel)\b",
                re.I),
     "Let Power BI or Excel calculate the measure", PB_PBI),
    (re.compile(r"customer names?.*\b(dashboard|tracker|shared|everyone|team)\b|\b(dashboard|tracker)\b.*customer names?",
                re.I),
     "Put customer names on the shared dashboard", PB_PII),
]


def _note_pushbacks(sess: _IntakeSession, msg: str) -> None:
    for rx, ask, answer in _PUSHBACK_TRIGGERS:
        if msg and rx.search(msg):
            _pushback(sess, ask, answer)


# =====================================================================
# Journey state, the spec's data_product block, and the journey note
# =====================================================================

def _journey(sess: _IntakeSession) -> dict[str, Any]:
    cards = []
    if sess.reuse or _job_status(sess, "match") in ("running", "error"):
        cards.append("reuse")
    if sess.pipeline or _job_status(sess, "pipeline") in ("running", "error"):
        cards.append("pipeline")
    if sess.metric_view or _job_status(sess, "metric_view") in ("running", "error"):
        cards.append("metric_view")
    if sess.dq_loaded or _job_status(sess, "dq") in ("running", "error"):
        cards.append("dq")
    if sess.design or _job_status(sess, "design") in ("running", "error"):
        cards.append("design")
    reuse = None
    if sess.reuse:
        reuse = dict(sess.reuse, decided=sess.reuse_decision, log_request=sess.log_request)
    mv = None
    if sess.metric_view:
        mv = dict(sess.metric_view, added=sess.metric_view_added, skipped=sess.mv_skipped)
    pipeline = None
    if sess.pipeline:
        pipeline = dict(sess.pipeline, added=sess.pipeline_added)
    return {
        "stage": sess.stage,
        "stages": STAGES,
        "planner": "dp_planner",
        "need": _need_view(sess),
        "need_missing": _missing(sess),
        "cards": cards,
        "jobs": {n: _job_view(sess, n) for n in sess.jobs},
        "reuse": reuse,
        "reuse_decision": sess.reuse_decision,
        "log_request": sess.log_request,
        "pipeline_needed": _pipeline_needed(sess),
        "pipeline": pipeline,
        "metric_view": mv,
        "mv_skipped": sess.mv_skipped,
        "dq": _dq_payload(sess) if sess.dq_loaded else None,
        "design": dict(sess.design, confirmed=sess.design_confirmed) if sess.design else None,
        "design_confirmed": sess.design_confirmed,
        "governance_confirmed": sess.governance_confirmed,
        "pushbacks": sess.pushbacks,
    }


def _journey_note(sess: _IntakeSession) -> str:
    """Live state of the grill and the cards, appended to the LLM system prompt each turn."""
    need = _need_view(sess)
    missing = _missing(sess)
    lines = ["# Journey note (live state, set by the app)", f"Stage: {sess.stage}."]
    if sess.stage == "grill":
        if missing:
            lines.append("Captured so far is incomplete. Ask the next missing item: "
                         + missing[0]["label"] + ". Still missing: "
                         + ", ".join(m["label"] for m in missing) + ".")
        else:
            lines.append("The need is complete. Confirm it in one line, then set next_step to consume.")
    else:
        lines.append("The need is LOCKED: the cards are built from it. If the analyst wants to change it, "
                     "tell them to start a new request.")
    if need["outputs"]:
        lines.append("Outputs asked for: " + " and ".join(
            {"dashboard": "a dashboard", "genie_agent": "a Genie Agent"}[o] for o in need["outputs"]) + ".")
    if sess.reuse:
        r = sess.reuse
        if not r["buildable"]:
            lines.append("Reuse check: NOT BUILDABLE. Missing data: "
                         + "; ".join(f"{x.get('need', '')} ({x.get('missing', '')})" for x in r["no_data"])
                         + ". Say so plainly, offer to log the request, and do not propose a different build. "
                           "Do not mention any other subject.")
        elif sess.reuse_decision == "agree":
            lines.append(f"Reuse: DONE. The analyst agreed to build the plan (coverage {r['coverage_pct']}%).")
        else:
            lines.append(f"Reuse: SHOWN, waiting for the analyst (coverage {r['coverage_pct']}%). Keep your reply to "
                         "one line that points at the card.")
    elif _job_status(sess, "match") == "running":
        lines.append("Reuse check: running. Keep your reply to one line.")
    if sess.pipeline:
        p = sess.pipeline
        if sess.pipeline_added:
            lines.append(f"Pipeline: DONE. {p['name']} (validated) is in the spec.")
        else:
            state = "validated" if p["validated"] else "did NOT validate"
            lines.append(f"Pipeline: SHOWN ({p['name']}, {state}), not added yet.")
    if sess.mv_skipped:
        lines.append("Metric view: NOT NEEDED. Everything asked for is reused.")
    elif sess.metric_view:
        mv = sess.metric_view
        state = {True: "validated on the warehouse", "pending": "validated at build time (it reads the planned "
                 "gold table)"}.get(mv.get("validated"), "did NOT validate")
        if sess.metric_view_added:
            lines.append(f"Metric view: DONE. {mv['name']} ({state}) is in the spec.")
        else:
            lines.append(f"Metric view: SHOWN ({mv.get('name')}, {state}), not added yet.")
    if sess.dq_loaded:
        c = _dq_counts(sess)
        state = "DONE" if sess.dq_review_id else "IN PROGRESS"
        saved = (f" Review saved ({sess.dq_review_id})." if sess.dq_review_id else " Review not saved yet.")
        lines.append(f"Data quality: {state}. {c['kept']} of {c['existing_total']} existing rules kept "
                     f"({c['disabled']} switched off with a reason), {c['added']} rules added, "
                     f"{c['rejected']} rejected.{saved}")
        if sess.dq_rejected:
            lines.append("DQ rejected (the app checked the real columns; quote this if the analyst raises it "
                         "again): " + "; ".join(f"'{r['text']}': {r['reason'][:140]}" for r in sess.dq_rejected) + ".")
    if sess.design:
        parts = []
        if sess.design.get("genie_agent"):
            parts.append(f"Genie Agent '{sess.design['genie_agent']['title']}'")
        if sess.design.get("dashboard"):
            parts.append(f"dashboard '{sess.design['dashboard']['title']}'")
        state = "DONE" if sess.design_confirmed else "IN PROGRESS"
        lines.append(f"Design: {state}. " + " and ".join(parts) + ".")
    if sess.pushbacks:
        lines.append("Pushbacks already answered (do not re-open): " + "; ".join(p["ask"] for p in sess.pushbacks) + ".")
    if sess.stage == "governance":
        lines.append("Governance: NEXT. Confirm the data owner, domain, classification and approvers with the analyst. "
                     "One yes or confirm is enough. Then set next_step to confirm, ready_to_submit to true, and recap "
                     "in 4 to 6 bullets.")
    elif sess.stage == "ready":
        lines.append("Governance: CONFIRMED. The spec is ready. Tell the analyst to review the form and click Submit. "
                     "Keep this reply to one line.")
    else:
        lines.append("ready_to_submit must stay false until the stage is governance and the analyst has confirmed.")
    lines.append("Never invent asset IDs, rule text, SQL or numbers that are not in these notes.")
    return "\n".join(lines)


def _ready(sess: _IntakeSession) -> bool:
    return sess.stage == "ready" and bool(sess.reuse and sess.reuse.get("buildable"))


def _state(sess: _IntakeSession, **extra: Any) -> dict[str, Any]:
    _sync(sess)
    text = extra.get("assistant_message")
    if not text and sess.pending_messages:
        # Background cards finished: deliver their messages once, and keep them in the LLM history.
        text = "\n\n".join(sess.pending_messages)
        sess.pending_messages = []
        _add_turn(sess, "assistant", text)
    out: dict[str, Any] = {
        "session_id": sess.session_id,
        "stage": sess.stage,
        "next_step": sess.step_index,
        "ready_to_submit": _ready(sess),
        "assistant_message": text or "",
        "journey": _journey(sess),
        "spec_snapshot": sess.spec_so_far,
    }
    extra.pop("assistant_message", None)
    out.update(extra)
    return out


# =====================================================================
# LLM driver
# =====================================================================

def _parse_json_response(raw: str) -> dict:
    """Tolerant JSON parser: strips code fences and leading prose."""
    text = raw.strip()
    if text.startswith("```"):
        first_nl = text.find("\n")
        if first_nl != -1:
            text = text[first_nl + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start:end + 1]
    # strict=False: the LLM puts literal newlines inside assistant_message strings.
    return json.loads(text, strict=False)


_TRACING = {"tried": False, "ready": False}


def _tracing_ready() -> bool:
    if _TRACING["tried"]:
        return _TRACING["ready"]
    _TRACING["tried"] = True
    try:
        import mlflow
        if not os.environ.get("MLFLOW_TRACKING_URI"):
            mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(os.environ.get(
            "MLFLOW_EXPERIMENT_PATH", "/Shared/databricks-pipeline-studio-agent"))
        _TRACING["ready"] = True
        LOG.info("intake tracing active")
    except Exception as e:  # noqa: BLE001
        LOG.warning("intake tracing unavailable: %s", e)
    return _TRACING["ready"]


def _llm_turn(sess: _IntakeSession, user_message: str) -> dict:
    """One grill turn. Returns the parsed assistant JSON envelope."""
    if _tracing_ready():
        import mlflow
        with mlflow.start_span(name="intake_turn", span_type="AGENT") as span:
            span.set_inputs({"session": sess.session_id[:8], "stage": sess.stage,
                             "user_message": user_message})
            parsed = _llm_turn_inner(sess, user_message)
            span.set_outputs({
                "next_step": parsed.get("next_step"),
                "ready_to_submit": parsed.get("ready_to_submit"),
                "assistant_message": (parsed.get("assistant_message") or "")[:500],
                "spec_patch_keys": sorted((parsed.get("spec_patch") or {}).keys()),
            })
            return parsed
    return _llm_turn_inner(sess, user_message)


def _llm_turn_inner(sess: _IntakeSession, user_message: str) -> dict:
    llm = get_llm()
    spec_view = {k: v for k, v in sess.spec_so_far.items()
                 if k not in ("data_product", "transform_business_rules")}
    system_prompt = "\n".join([
        build_intake_prompt(_catalog_block()),
        "",
        "# Current spec_so_far (captured so far)",
        json.dumps(spec_view, indent=2, default=str),
        "",
        _journey_note(sess),
    ])
    # One system message, then alternating user / assistant turns.
    msgs = [SystemMessage(content=system_prompt)]
    for h in sess.history:
        msgs.append(HumanMessage(content=h["content"]) if h["role"] == "user"
                    else AIMessage(content=h["content"]))
    msgs.append(HumanMessage(content=user_message or "<start the intake conversation>"))

    resp = llm.invoke(msgs)
    raw = resp.content if isinstance(resp.content, str) else str(resp.content)
    try:
        parsed = _parse_json_response(raw)
    except Exception as e:  # noqa: BLE001
        # The model sometimes answers in prose. Retry once with a JSON-only nudge.
        LOG.warning("intake LLM returned unparseable JSON, retrying: %s\n---\n%s", e, raw[:400])
        try:
            repair_msgs = msgs + [
                AIMessage(content=raw),
                HumanMessage(content=(
                    "Your last reply was not valid JSON. Reply again with ONLY the JSON object described in "
                    "the system prompt: no prose, no code fences. Keep advancing the conversation; do not ask "
                    "me to repeat myself.")),
            ]
            resp2 = llm.invoke(repair_msgs)
            raw2 = resp2.content if isinstance(resp2.content, str) else str(resp2.content)
            parsed = _parse_json_response(raw2)
        except Exception as e2:  # noqa: BLE001
            LOG.warning("intake LLM JSON repair failed: %s", e2)
            parsed = {
                "assistant_message": "Sorry, I got confused. Could you rephrase your last answer?",
                "spec_patch": {},
                "fields_revealed": [],
                "next_step": sess.step_index,
                "ready_to_submit": False,
            }
    parsed.setdefault("assistant_message", "")
    parsed.setdefault("spec_patch", {})
    parsed.setdefault("fields_revealed", [])
    parsed.setdefault("next_step", sess.step_index)
    parsed.setdefault("ready_to_submit", False)
    return parsed


def _filter_patch(patch: dict, locked: bool) -> dict:
    """Keep only what the grill may write: need_* (until the cards are shown) and governance_*."""
    out = {}
    for k, v in (patch or {}).items():
        if k.startswith("need_"):
            if not locked:
                out[k] = v
        elif k.startswith("governance_") or k == "required_approvers":
            out[k] = v
    return out


def _is_confirm(msg: str) -> bool:
    t = re.sub(r"[^a-z' ]", "", (msg or "").lower()).strip()
    return t in _CONFIRM_WORDS


def _card_pointer(sess: _IntakeSession, msg: str) -> str:
    """Reply to a chat turn while a card is open. Deterministic, no LLM: the cards hold the decisions,
    so chat must never say the spec is ready or tell the analyst to submit. Any pushback the message
    raised is answered first."""
    st = sess.stage
    pushback = " ".join(answer for rx, _ask, answer in _PUSHBACK_TRIGGERS if msg and rx.search(msg))
    if st == "reuse":
        if _job_status(sess, "match") == "running":
            where = "I'm still checking what already exists. The reuse card will show here when that finishes."
        elif sess.reuse and not sess.reuse.get("buildable"):
            where = ("The reuse card above is the stop: the data for this ask is not in the catalog. Use "
                     "Log this request there, or change the ask. I can't change the cards from chat.")
        else:
            where = ("The reuse card above is open. Agree to build the plan, or use Build on the raw tables "
                     "instead. I can't move the journey forward from chat.")
    elif st == "pipeline":
        if _job_status(sess, "pipeline") == "running":
            where = ("The pipeline is being drafted and validated on the warehouse. That takes one to three "
                     "minutes. The pipeline card will show here when it is done.")
        else:
            where = ("The pipeline card above is open. Add it to the spec once it validates, or try again. "
                     "I can't move the journey forward from chat.")
    elif st == "metric_view":
        where = ("The metric view card above is open. Add it to the spec, or try again. "
                 "I can't move the journey forward from chat.")
    elif st == "dq":
        where = ("The data quality card above is open. Switch a rule off with a reason, accept the gap rules you "
                 "want, add a rule in plain English, then save the DQ review. I can't change the rules from chat, "
                 "except a plain-English rule.")
    elif st == "design":
        if _job_status(sess, "design_change") == "running":
            where = "I'm checking the change against the data. The design card updates when that is done."
        elif sess.design is None or _job_status(sess, "design") == "running":
            where = "The design is still being built. The design card will show it when it is ready."
        else:
            where = ("The design card above is open. Ask for a change here or on the card, add, replace or remove "
                     "what the design shows, then confirm the design.")
    else:
        where = "The card above is open."
    return f"{pushback}\n\n{where}".strip() if pushback else where


# =====================================================================
# API: chat (the LLM grill and governance)
# =====================================================================

class ChatBody(BaseModel):
    session_id: Optional[str] = None    # omit on the first turn; the server mints one
    user_message: str = ""


@router.post("/chat")
def chat(body: ChatBody):
    sid = body.session_id or uuid.uuid4().hex
    sess = _get_session(sid)
    if not sess.spec_so_far:
        sess.spec_so_far = _base_spec()

    msg = (body.user_message or "").strip()
    _note_pushbacks(sess, msg)

    if sess.stage == "dq" and msg and _RULE_HINT.search(msg) and sess.dq_loaded and not _busy(sess):
        # A rule typed in chat gets the same check as the DQ card. The grill LLM never sees it,
        # so it cannot assert that a column exists or does not.
        result = _add_custom_rule(sess, msg)
        if result["status"] == "accepted":
            r = result["rule"]
            text = (f"Added the rule '{r['rule_name']}' on {r['table_name']}: {r['constraint_sql']}. "
                    f"{result['violations_now']} rows fail it today.")
        else:
            text = f"I didn't add that rule. {result['reason']}"
        _add_turn(sess, "user", msg)
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text, dq_result=result, spec_patch={}, fields_revealed=[])

    if sess.stage == "design" and msg and sess.design is not None and not sess.design_confirmed and not _busy(sess):
        # The design is open: a chat message is a change request, routed to the planner (no free-form reply).
        _add_turn(sess, "user", msg)
        if _is_confirm(msg):
            text = "Click Confirm design on the design card to lock it. I don't confirm the design from chat."
        else:
            text = _start_design_change(sess, msg)
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text, spec_patch={}, fields_revealed=[])

    if sess.stage in _CARD_STAGES:
        # A card is open. The LLM does not answer here: it has no say over the cards, and it used to tell
        # the analyst the spec was ready. The reply points at the card; ready_to_submit stays false.
        text = _card_pointer(sess, msg)
        if msg:
            _add_turn(sess, "user", msg)
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text, spec_patch={}, fields_revealed=[])

    if sess.stage == "governance" and msg and _is_confirm(msg):
        # One confirm is enough: the spec is ready without a second round-trip through the LLM.
        sess.governance_confirmed = True
        sess.stage = "ready"
        text = "Governance confirmed. The spec is ready. Review the form on the right, then click Submit."
        _add_turn(sess, "user", msg)
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text, spec_patch={}, fields_revealed=[])

    try:
        parsed = _llm_turn(sess, msg)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        LOG.exception("intake turn failed")
        raise HTTPException(502, f"The intake agent couldn't answer just now ({str(e)[:160]}). Try again.")

    reply = parsed.get("assistant_message") or ""
    next_step = parsed.get("next_step") or sess.step_index
    patch = _filter_patch(parsed.get("spec_patch") or {}, locked=sess.stage != "grill")
    _deep_merge(sess.spec_so_far, patch)
    _add_turn(sess, "user", msg)

    # The grill only moves through its own steps. Anything else ("done", a step name from the
    # wrong stage) is pulled back to the first missing item, or to consume when nothing is missing.
    if sess.stage == "grill" and next_step not in _GRILL_STEPS:
        left = _missing(sess)
        next_step = left[0]["step"] if left else "consume"
    # Honest no-data path: the grill says the core of the ask is not in the data. Run the check now with
    # whatever was captured (the analyst's own words if nothing else); it reports NO_DATA, shows the stop
    # card with "Log this request", and keeps submit locked.
    if sess.stage == "grill" and next_step == "no_data":
        if not (sess.spec_so_far.get("need_purpose") or "").strip():
            first_user = next((h["content"] for h in sess.history if h.get("role") == "user"), msg)
            sess.spec_so_far["need_purpose"] = first_user
        if not sess.spec_so_far.get("need_questions"):
            sess.spec_so_far["need_questions"] = [h["content"] for h in sess.history if h.get("role") == "user"][:3]
        _open_reuse(sess)
        next_step = "consume"
    # The grill hands over to the cards only when the need is complete.
    asked = any(t in msg.lower() for t in _EXISTS_TERMS)
    if sess.stage == "grill" and (next_step == "consume" or asked):
        missing = _missing(sess)
        if missing:
            next_step = missing[0]["step"]
            reply = (reply + "\n\nStill needed before I can check what exists: "
                     + ", ".join(m["label"] for m in missing) + ".").strip()
        else:
            _open_reuse(sess)
            next_step = "consume"

    # Governance: the LLM may also confirm; the deterministic path above covers the one-word confirm.
    if sess.stage == "governance" and bool(parsed.get("ready_to_submit")) and next_step in ("confirm", "done"):
        sess.governance_confirmed = True
        sess.stage = "ready"

    sess.step_index = next_step
    _add_turn(sess, "assistant", reply)
    return _state(sess, assistant_message=reply, spec_patch=patch,
                  fields_revealed=parsed.get("fields_revealed") or [])


@router.delete("/session/{session_id}")
def drop_session(session_id: str):
    with _lock:
        _sessions.pop(session_id, None)
    return {"ok": True}


# =====================================================================
# API: journey cards (all keyed by session_id)
# =====================================================================

class SessionBody(BaseModel):
    session_id: str


class ReuseBody(SessionBody):
    decision: str   # "agree" | "raw_tables"


class RetryBody(SessionBody):
    job: str        # "match" | "metric_view" | "dq" | "design"


class DqToggleBody(SessionBody):
    table_name: str
    rule_name: str
    enabled: bool
    reason: str = ""


class DqAcceptBody(SessionBody):
    table_name: str
    rule_name: str
    accepted: bool = True


class DqCustomBody(SessionBody):
    text: str


class DesignBody(SessionBody):
    design: dict[str, Any] = {}   # edits: {"genie_agent": {...}, "dashboard": {...}}


class DesignChangeBody(SessionBody):
    text: str = ""                # a change request in plain English, e.g. "add product type and sales count"


@router.get("/journey/{session_id}")
def journey_get(session_id: str):
    """Full card state. The page polls this while a background step runs, and reloads with it."""
    return _state(_require_session(session_id))


@router.post("/journey/reuse")
def journey_reuse(body: ReuseBody):
    """The analyst's reuse decision. 'agree' starts the build plan (metric view for the gap, then
    data quality). 'raw_tables' is the refused pushback; the card stays."""
    sess = _require_session(body.session_id)
    with sess.lock:
        if body.decision == "agree":
            if sess.reuse_decision == "agree":
                return _state(sess)
            _guard(sess, "reuse")
            if not (sess.reuse and sess.reuse.get("buildable")):
                raise HTTPException(409, "Nothing to build: the reuse check found no buildable plan.")
            _add_turn(sess, "user", "[Analyst] Agreed: build the plan.")
            sess.reuse_decision = "agree"
            if _pipeline_needed(sess):
                _start_pipeline(sess)
                text = ("Agreed. The raw feed has the data, so I'm drafting a pipeline from it (raw, bronze, silver, "
                        "gold) and validating it on the warehouse. That takes one to three minutes. The metric view "
                        "comes after the pipeline.")
            else:
                _start_mv(sess)
                text = ("Agreed. I'm writing the metric view for the gap, if there is one, and checking it on the "
                        "warehouse. That takes 30 to 60 seconds.")
            _add_turn(sess, "assistant", text)
            return _state(sess, assistant_message=text)
        if body.decision == "raw_tables":
            _guard(sess, "reuse")
            _pushback(sess, "Build it on the raw or gold tables", PB_RAW)
            _add_turn(sess, "user", "[Analyst] Build it on the raw tables instead.")
            _add_turn(sess, "assistant", PB_RAW)
            return _state(sess, assistant_message=PB_RAW, refused=True)
    raise HTTPException(400, "decision must be 'agree' or 'raw_tables'")


@router.post("/journey/log-request")
def journey_log_request(body: SessionBody):
    """Write the blocked ask to studio_control.request_backlog. Nothing is built; submit stays locked."""
    sess = _require_session(body.session_id)
    with sess.lock:
        if not (sess.reuse and sess.reuse.get("no_data")):
            raise HTTPException(409, "There is nothing missing to log for this request.")
        if sess.log_request:
            return _state(sess)
        need = _need_view(sess)
        rid = str(uuid.uuid4())
        table = f"{acme.CATALOG}.{acme.CONTROL_SCHEMA}.request_backlog"
        ask = (need["purpose"] or "").replace("\n", " ")
        missing = json.dumps(sess.reuse["no_data"], default=str)
        try:
            acme.sql(f"CREATE TABLE IF NOT EXISTS {table} (request_id STRING, session_id STRING, ask STRING, "
                     "outputs STRING, need_json STRING, no_data_json STRING, status STRING, created_at TIMESTAMP)")
            acme.sql(f"INSERT INTO {table} VALUES ({acme.lit(rid)}, {acme.lit(sess.session_id)}, {acme.lit(ask)}, "
                     f"{acme.lit(','.join(need['outputs']))}, {acme.lit(json.dumps(need, default=str))}, "
                     f"{acme.lit(missing)}, 'logged', current_timestamp())")
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, f"Couldn't write the request to the backlog: {str(e)[:200]}")
        sess.log_request = {"request_id": rid, "logged_at": _now_iso(), "table": table}
        text = (f"Logged as request {rid[:8]} on the backlog. Nothing was built, and submit stays locked. "
                "The data team will see the missing data when they plan the next release.")
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text)


@router.post("/journey/retry")
def journey_retry(body: RetryBody):
    """Re-run a background step that failed (or a metric view that did not validate)."""
    sess = _require_session(body.session_id)
    with sess.lock:
        if _busy(sess):
            raise HTTPException(409, "Still working on the last step. Wait for it to finish.")
        name = body.job
        if name == "match" and sess.stage == "reuse" and sess.reuse is None:
            _open_reuse(sess)
        elif name == "pipeline" and sess.stage == "pipeline" and not sess.pipeline_added \
                and (_job_status(sess, "pipeline") == "error" or (sess.pipeline and not sess.pipeline["validated"])):
            _start_pipeline(sess)
        elif name == "metric_view" and sess.stage == "metric_view" and not sess.metric_view_added \
                and (_job_status(sess, "metric_view") == "error" or
                     (sess.metric_view and not sess.metric_view.get("validated"))):
            _start_mv(sess)
        elif name == "dq" and sess.stage == "dq" and not sess.dq_loaded:
            _start_dq(sess)
        elif name == "design" and sess.stage == "design" and sess.design is None:
            _start_design(sess)
        else:
            raise HTTPException(409, f"Nothing to retry for '{name}' at stage '{sess.stage}'.")
    return _state(sess)


@router.post("/journey/pipeline/add")
def journey_pipeline_add(body: SessionBody):
    """Add the validated pipeline to the spec, then open the metric view (it reads the planned gold table)."""
    sess = _require_session(body.session_id)
    with sess.lock:
        if sess.pipeline_added:
            return _state(sess)
        _guard(sess, "pipeline")
        if not (sess.pipeline and sess.pipeline["validated"]):
            raise HTTPException(409, "This pipeline has not validated yet, so it can't be added. Try again.")
        sess.pipeline_added = True
        name = sess.pipeline["name"]
        _add_turn(sess, "user", "[Analyst] Add the pipeline to the spec.")
        _start_mv(sess)
        text = (f"Added the pipeline {name} to the spec. Next is the metric view, which reads the pipeline's gold "
                "table.")
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text)


@router.post("/journey/metric-view/add")
def journey_metric_view_add(body: SessionBody):
    """Add the metric view to the spec, then open data quality (existing + proposed rules). A view that reads a
    planned gold table ("pending") is added now and validated at build."""
    sess = _require_session(body.session_id)
    with sess.lock:
        if sess.metric_view_added:
            return _state(sess)
        _guard(sess, "metric_view")
        if not _mv_ok(sess.metric_view):
            raise HTTPException(409, "This metric view has not validated on the warehouse yet, so it can't be added.")
        sess.metric_view_added = True
        _add_turn(sess, "user", "[Analyst] Add the metric view to the spec.")
        _start_dq(sess)
        text = (f"Added {sess.metric_view['name']} to the spec. Next is data quality: the existing rules for the "
                "tables it reads, plus gap rules I propose.")
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text)


@router.post("/journey/dq/toggle")
def journey_dq_toggle(body: DqToggleBody):
    """Enable or disable a rule. Switching a rule off needs a non-empty reason."""
    sess = _require_session(body.session_id)
    with sess.lock:
        _guard(sess, "dq")
        rule = _find_rule(sess, body.table_name, body.rule_name)
        if rule is None:
            raise HTTPException(404, f"Rule '{body.rule_name}' on {body.table_name} is not in the working set.")
        if body.enabled:
            rule["enabled"] = True
            rule["disabled_reason"] = ""
        else:
            reason = (body.reason or "").strip()
            if not reason:
                raise HTTPException(400, f"Switching off '{body.rule_name}' needs a reason. Say why it should "
                                         "not run (for example, the source feed is not live yet). The reason "
                                         "is saved with the DQ review.")
            rule["enabled"] = False
            rule["disabled_reason"] = reason
        return _state(sess)


@router.post("/journey/dq/accept")
def journey_dq_accept(body: DqAcceptBody):
    """Accept or un-accept a proposed gap rule. Accepting checks it against the real table."""
    sess = _require_session(body.session_id)
    with sess.lock:
        _guard(sess, "dq")
        p = next((x for x in sess.dq_proposed
                  if x["table_name"] == body.table_name and x["rule_name"] == body.rule_name), None)
        if p is None:
            raise HTTPException(404, f"Proposed rule '{body.rule_name}' not found.")
        if not body.accepted:
            p["accepted"] = False
            return _state(sess)
        chk = acme.check_constraint(p["table_name"], p["constraint_sql"])
        if not chk["ok"]:
            _record_rejected(sess, p["rule_name"], chk["reason"], p)
            return _state(sess, dq_result={"status": "rejected", "text": p["rule_name"],
                                           "reason": chk["reason"], "rule": None})
        p["accepted"] = True
        p["violations_now"] = chk["violations_now"]
        return _state(sess, dq_result={"status": "accepted", "text": p["rule_name"], "reason": "",
                                       "rule": _rule_out(p), "violations_now": chk["violations_now"]})


@router.post("/journey/dq/custom")
def journey_dq_custom(body: DqCustomBody):
    """A rule in plain English. The LLM turns it into SQL; the app checks the columns exist on the real table."""
    sess = _require_session(body.session_id)
    with sess.lock:
        _guard(sess, "dq")
        text = (body.text or "").strip()
        if not text:
            raise HTTPException(400, "Type the rule in plain English first.")
        result = _add_custom_rule(sess, text)
        return _state(sess, dq_result=result)


@router.post("/journey/dq/save")
def journey_dq_save(body: SessionBody):
    """Save the DQ review to studio_control.dq_review; starts the design (Genie Agent and dashboard)."""
    sess = _require_session(body.session_id)
    with sess.lock:
        _guard(sess, "dq")
        review = {
            "session_id": sess.session_id,
            "need": _need_view(sess),
            "rules": [_rule_out(r) for r in _active_rules(sess)],
            "rejected": sess.dq_rejected,
            "not_accepted": [p["rule_name"] for p in sess.dq_proposed if not p.get("accepted")],
            "counts": _dq_counts(sess),
        }
        try:
            sess.dq_review_id = acme.save_dq_review(sess.session_id, review)
        except Exception as e:  # noqa: BLE001
            raise HTTPException(502, f"Couldn't save the DQ review: {str(e)[:200]}")
        c = _dq_counts(sess)
        text = (f"DQ review saved: {c['kept']} existing rules kept, {c['added']} added, {c['rejected']} rejected. "
                "Next is the design: "
                + " and ".join({"dashboard": "the dashboard", "genie_agent": "the Genie Agent"}[o]
                               for o in _need_view(sess)["outputs"]) + ".")
        _add_turn(sess, "user", "[Analyst] Save the DQ review.")
        _add_turn(sess, "assistant", text)
        _start_design(sess)
        return _state(sess, assistant_message=text, review_id=sess.dq_review_id)


@router.post("/journey/design/change")
def journey_design_change(body: DesignChangeBody):
    """A change to the open design (the card's request box). Same handler as a chat message in this stage."""
    sess = _require_session(body.session_id)
    with sess.lock:
        _guard(sess, "design")
        text = (body.text or "").strip()
        if not text:
            raise HTTPException(400, "Type the change first, for example 'add product type and sales count'.")
        if sess.design is None:
            raise HTTPException(409, "The design is not ready yet.")
        if sess.design_confirmed:
            raise HTTPException(409, "The design is already confirmed.")
        _add_turn(sess, "user", "[Analyst] " + text)
        reply = _start_design_change(sess, text)
        _add_turn(sess, "assistant", reply)
        return _state(sess, assistant_message=reply)


@router.post("/journey/design/confirm")
def journey_design_confirm(body: DesignBody):
    """Confirm the design. Edits are merged in (SQL is never taken from the client). Customer names
    on the shared dashboard are refused: the flag is set back to false and the pushback is recorded."""
    sess = _require_session(body.session_id)
    with sess.lock:
        _guard(sess, "design")
        if sess.design is None:
            raise HTTPException(409, "The design is not ready yet.")
        merged = _merge_design(sess, body.design or {})
        d_in = (body.design or {}).get("dashboard") if isinstance((body.design or {}).get("dashboard"), dict) else {}
        refused = bool(d_in.get("include_customer_names"))
        if merged.get("dashboard"):
            merged["dashboard"]["include_customer_names"] = False
        sess.design = merged
        if refused:
            _pushback(sess, "Put customer names on the shared dashboard", PB_PII)
            _add_turn(sess, "user", "[Analyst] Include customer names on the shared dashboard.")
            _add_turn(sess, "assistant", PB_PII)
            return _state(sess, assistant_message=PB_PII, refused=True)
        sess.design_confirmed = True
        sess.stage = "governance"
        text = ("The cards are done: what already exists, the metric view (if any), data quality, and the design. "
                "Next is governance: data owner, domain, classification and approvers. Reply 'confirm' to accept "
                "the defaults, or change them.")
        _add_turn(sess, "user", "[Analyst] Design confirmed.")
        _add_turn(sess, "assistant", text)
        return _state(sess, assistant_message=text, refused=False)
