"""Release Management — the platform/ops view.

The data-engineering team works requests in the tracker up to a STAGING
deploy. The platform team then groups staged requests into a **release** and
promotes them to PROD together. Each release carries an AI-written summary so
the release owner understands what's going out without reading every request.

Endpoints:
  GET  /api/releases               — previous + in-flight releases (with summaries)
  GET  /api/releases/candidates    — staged requests awaiting promotion
  POST /api/releases/summary       — AI summary preview for a set of requests
  POST /api/releases               — create a release + launch prod promotion
  GET  /api/releases/{id}          — release detail + member request states
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from agent import event_store, runner

router = APIRouter()


# --- helpers ----------------------------------------------------------

def _spec_of(req: dict) -> dict:
    try:
        return json.loads(req.get("spec_json") or "{}")
    except Exception:
        return {}


def _request_card(req: dict) -> dict:
    """Compact view of a request for release UIs."""
    spec = _spec_of(req)
    src = (spec.get("source") or {})
    tgt = (spec.get("target") or {})
    return {
        "request_id": req.get("request_id"),
        "intent": req.get("intent") or spec.get("intent"),
        "target_table": tgt.get("table"),
        "source_system": src.get("system"),
        "source_tables": src.get("tables") or [],
        "assignee": req.get("assignee"),
        "requested_by": req.get("requested_by"),
        "final_status": req.get("final_status"),
        "release_status": req.get("release_status"),
        "release_id": req.get("release_id"),
    }


def _candidates() -> list[dict]:
    """Requests deployed to staging and not yet released."""
    out = []
    for req in event_store.list_requests(limit=200):
        if (req.get("final_status") == "deployed"
                and (req.get("release_status") or "staged") in ("staged",)):
            out.append(_request_card(req))
    return out


def _derive_status(member_reqs: list[dict], stored: Optional[str]) -> str:
    """Live release status derived from member request states."""
    if not member_reqs:
        return stored or "in_progress"
    statuses = {(r.get("release_status") or "") for r in member_reqs}
    if statuses == {"released"}:
        return "released"
    if any(s == "failed" for s in statuses):
        return "attention"
    return "in_progress"


def _members_for(release: dict, all_by_id: dict[str, dict]) -> list[dict]:
    try:
        ids = json.loads(release.get("request_ids") or "[]")
    except Exception:
        ids = []
    return [all_by_id[i] for i in ids if i in all_by_id]


_SUMMARY_PROMPT = (
    "You are a release manager for a telco data platform. Given a set of data "
    "pipeline requests that have been built and deployed to STAGING and are now "
    "queued for promotion to PRODUCTION, write a concise release summary (3-5 "
    "sentences, plain prose, no preamble) for the platform/ops team. Cover: what "
    "business capabilities these pipelines deliver, the source systems and target "
    "tables involved, and any noteworthy scope (e.g. cross-system joins, large "
    "volumes). Be specific and factual; do not invent details not in the input."
)


def _ai_summary(cards: list[dict]) -> str:
    """LLM-written release summary; deterministic fallback on any error."""
    payload = [
        {"intent": c.get("intent"), "target_table": c.get("target_table"),
         "source_system": c.get("source_system"), "source_tables": c.get("source_tables")}
        for c in cards
    ]
    try:
        from agent.llm import get_llm
        from langchain_core.messages import HumanMessage, SystemMessage
        resp = get_llm().invoke([
            SystemMessage(content=_SUMMARY_PROMPT),
            HumanMessage(content=json.dumps(payload, indent=2)),
        ])
        text = (resp.content or "").strip()
        if text:
            return text
    except Exception:
        pass
    # Fallback: deterministic summary.
    tables = ", ".join(sorted({c.get("target_table") or "?" for c in cards}))
    systems = ", ".join(sorted({(c.get("source_system") or "?").upper() for c in cards}))
    return (f"Promotes {len(cards)} pipeline(s) to production: {tables}. "
            f"Source systems: {systems}.")


# --- routes -----------------------------------------------------------

@router.get("")
def list_all(limit: int = 50):
    releases = event_store.list_releases(limit=limit)
    all_by_id = {r["request_id"]: _request_card(r)
                 for r in event_store.list_requests(limit=200)}
    out = []
    for rel in releases:
        members = _members_for(rel, all_by_id)
        out.append({
            "release_id": rel.get("release_id"),
            "title": rel.get("title"),
            "summary": rel.get("summary"),
            "created_by": rel.get("created_by"),
            "created_at": str(rel.get("created_at")),
            "status": _derive_status(members, rel.get("status")),
            "request_count": len(members),
            "members": members,
        })
    return {"releases": out}


@router.get("/candidates")
def candidates():
    return {"candidates": _candidates()}


class SummaryBody(BaseModel):
    request_ids: list[str]


@router.post("/summary")
def summary_preview(body: SummaryBody):
    all_by_id = {r["request_id"]: _request_card(r)
                 for r in event_store.list_requests(limit=200)}
    cards = [all_by_id[i] for i in body.request_ids if i in all_by_id]
    if not cards:
        raise HTTPException(status_code=400, detail="no valid requests")
    return {"summary": _ai_summary(cards), "request_count": len(cards)}


class CreateReleaseBody(BaseModel):
    request_ids: list[str]
    title: Optional[str] = None
    created_by: Optional[str] = None
    summary: Optional[str] = None


@router.post("")
def create_release(body: CreateReleaseBody):
    """Create a release from a set of staged requests and launch a prod
    promotion job run per request. Each member's release_status flips to
    'releasing'; deploy_prod sets it to 'released' on success."""
    all_by_id = {r["request_id"]: r for r in event_store.list_requests(limit=200)}
    valid = [i for i in body.request_ids if i in all_by_id]
    if not valid:
        raise HTTPException(status_code=400, detail="no valid requests to release")

    cards = [_request_card(all_by_id[i]) for i in valid]
    summary = body.summary or _ai_summary(cards)
    release_id = "rel-" + uuid.uuid4().hex[:8]
    n = event_store.list_releases(limit=500)
    title = body.title or f"Release {len(n) + 1}"

    event_store.create_release(release_id, title, summary,
                               body.created_by or "platform-team", valid)

    launches = []
    for rid in valid:
        event_store.update_request(rid, release_status="releasing", release_id=release_id)
        try:
            launch = runner.launch("promote", rid)
            launches.append({"request_id": rid, "launch": launch})
        except Exception as e:  # noqa: BLE001
            event_store.update_request(rid, release_status="staged")
            launches.append({"request_id": rid, "error": str(e)[:200]})

    return {"release_id": release_id, "title": title, "summary": summary,
            "request_count": len(valid), "launches": launches}


@router.get("/{release_id}")
def get_one(release_id: str):
    rel = event_store.fetch_release(release_id)
    if not rel:
        raise HTTPException(status_code=404, detail="release not found")
    all_by_id = {r["request_id"]: _request_card(r)
                 for r in event_store.list_requests(limit=200)}
    members = _members_for(rel, all_by_id)
    return {
        "release_id": rel.get("release_id"),
        "title": rel.get("title"),
        "summary": rel.get("summary"),
        "created_by": rel.get("created_by"),
        "created_at": str(rel.get("created_at")),
        "status": _derive_status(members, rel.get("status")),
        "members": members,
    }
