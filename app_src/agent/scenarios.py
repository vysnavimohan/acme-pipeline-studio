"""Example asks for the New Request page, and the home page's Lakebase probe.

The New Request page starts empty. The example chips only TYPE an ask into the box
(they never pre-fill a spec or an intent). Each example is an ordinary ask that the
grill has to work through; none of them is a template.

`lakebase_available()` stays: the app's home page uses it to say whether the
BILLING/NETWORK Lakebase sources are live.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

# Example asks offered as chips. Clicking one types the text into the box; the analyst
# still sends it and still answers the grill.
EXAMPLE_ASKS: list[dict[str, str]] = [
    {"key": "margin_cash", "label": "Margin and cash (agent + dashboard)",
     "ask": ("A Genie Agent the finance team can ask where we're leaking margin and what's happening "
             "with cash, plus a dashboard to track both every week.")},
    {"key": "discount_branch", "label": "Discounting by branch (dashboard)",
     "ask": "A dashboard for category managers showing discounting by branch."},
    {"key": "dso_segment", "label": "DSO by segment (Genie Agent)",
     "ask": "A Genie Agent for credit control about DSO by segment."},
    {"key": "delivery_times", "label": "Delivery times (dashboard)",
     "ask": "Dashboard of delivery times for the warehouse team."},
]


@lru_cache(maxsize=1)
def lakebase_available() -> bool:
    """True when the BILLING/NETWORK Lakebase instances are configured.

    Cheap + non-throwing: we check that a source is *configured* (a local
    DATABASE_URL override, an external Postgres, or the Lakebase instances
    existing in the workspace) and do NOT open a connection. Cached, since the
    answer can't change within a process. Set OPS_LAKEBASE_ENABLED=0 to force it
    off, or 1 to force it on."""
    flag = os.environ.get("OPS_LAKEBASE_ENABLED", "").strip()
    if flag == "0":
        return False
    if flag == "1":
        return True
    try:
        from common import sources
        if any(os.environ.get(f"DATABASE_URL_{s.upper()}") for s in sources.SYSTEMS):
            return True
        if sources.external_pg_configured():
            return True
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient()
        db_api = getattr(client, "database", None) or getattr(client, "postgres", None)
        if db_api is None:
            return False
        for s in sources.SYSTEMS:
            db_api.get_database_instance(name=sources.lakebase_instance(s))
        return True
    except Exception:  # noqa: BLE001 - never break the form over a probe
        return False


def list_scenarios() -> list[dict[str, Any]]:
    """The example asks offered as chips on /request/new. They type into the box; they
    do not pre-fill anything."""
    return [dict(e) for e in EXAMPLE_ASKS]
