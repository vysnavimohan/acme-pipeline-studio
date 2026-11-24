"""Source data browser + the persisted profiling step.

GET  /api/sources           — the source catalog (persisted; profiling step)
POST /api/sources/refresh   — re-profile the source systems (button in UI)

The catalog is the authoritative table list shown in the data browser AND
injected into the intake agent's prompt (so it can't hallucinate names).
Adding a new source table → click Refresh → it appears everywhere.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter

from common import source_catalog

LOG = logging.getLogger("ops.sources")

router = APIRouter()


@router.get("")
def list_sources(refresh: bool = False):
    return source_catalog.get(force=refresh)


@router.post("/refresh")
def refresh_sources():
    data = source_catalog.refresh()
    n = sum(len(s.get("tables", [])) for s in data["systems"])
    return {"ok": True, "table_count": n, "systems": data["systems"],
            "profiled_at": data.get("profiled_at")}
