"""Agent guidelines (SOPs / specialist knowledge) — view + edit endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from agent import guidelines as g

router = APIRouter()


class UpdateBody(BaseModel):
    prompt: str
    updated_by: Optional[str] = "vaishnavi.mohan@databricks.com"
    max_iterations: Optional[int] = None
    loop_action: Optional[str] = None  # "proceed" | "requeue"


@router.get("")
def list_guidelines():
    items = g.list_all()
    # Confluence integration is disabled for the PoC — never surface linked
    # state, so every agent shows as inline-editable (Delta-backed).
    if not g.confluence_enabled():
        for it in items:
            it["confluence_page_id"] = None
            it["confluence_url"] = None
    return {"guidelines": items, "confluence_enabled": g.confluence_enabled()}


@router.get("/{agent_name}")
def get_guideline(agent_name: str):
    item = g.fetch_one(agent_name)
    if not item:
        raise HTTPException(status_code=404, detail=f"unknown agent: {agent_name}")
    return item


@router.post("/{agent_name}")
def update_guideline(agent_name: str, body: UpdateBody):
    try:
        return g.update(agent_name, body.prompt, body.updated_by or "unknown",
                        max_iterations=body.max_iterations,
                        loop_action=body.loop_action)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/{agent_name}/reset")
def reset_guideline(agent_name: str, updated_by: str = "unknown"):
    try:
        return g.reset(agent_name, updated_by)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# =====================================================================
# Confluence-backed guidelines
# =====================================================================

class ConfluenceLinkBody(BaseModel):
    page: str    # Confluence page URL or bare page id
    updated_by: Optional[str] = "vaishnavi.mohan@databricks.com"


@router.get("/confluence/status")
def confluence_status(probe: bool = False):
    """Is the Confluence integration configured in this environment?

    `configured` only means credentials are present. With `probe=1` we also make
    one real API call, because "configured" and "usable" are different things:
    Databricks App egress IPs are not on the Atlassian IP allowlist for this
    site, so a perfectly valid token still 403s from inside the App (the same
    token works from a laptop on the VPN). Without the probe the UI would claim
    the integration is on and then fail on first use.
    """
    import os

    from tools import confluence_client
    out = {"configured": confluence_client.configured(),
           "base_url": os.environ.get("CONFLUENCE_BASE_URL", "")}
    if probe and out["configured"]:
        try:
            # Cheapest authenticated call the client has: whoami. It exercises
            # exactly the auth + egress path a page fetch would.
            confluence_client.whoami()
            out["reachable"] = True
        except Exception as e:  # noqa: BLE001 — the reason is the payload here
            msg = str(e)
            out["reachable"] = False
            out["reason"] = msg[:300]
            out["ip_allowlisted"] = "IP allowlist" not in msg
    return out


@router.post("/{agent_name}/confluence")
def link_confluence(agent_name: str, body: ConfluenceLinkBody):
    """Link this agent's guidelines to a Confluence page. The page content
    becomes the live prompt (fetched + cached at LLM-call time)."""
    from tools.confluence_client import ConfluenceError
    try:
        return g.set_confluence_link(agent_name, body.page,
                                     body.updated_by or "unknown")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ConfluenceError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.delete("/{agent_name}/confluence")
def unlink_confluence(agent_name: str, updated_by: str = "unknown"):
    try:
        return g.remove_confluence_link(agent_name, updated_by)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{agent_name}/confluence/preview")
def confluence_preview(agent_name: str, refresh: bool = False):
    """Fetch the linked page for display in the Agent Config UI.
    `refresh=true` busts the cache (the 'I just edited the page' path)."""
    link = g.get_confluence_link(agent_name)
    if not link:
        raise HTTPException(status_code=404, detail="no Confluence page linked")
    from tools import confluence_client
    try:
        page = confluence_client.get_page_cached(link["page_id"],
                                                 force_refresh=refresh)
    except confluence_client.ConfluenceError as e:
        raise HTTPException(status_code=502, detail=str(e))
    return page
