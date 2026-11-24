"""Pre-built example requests for the "reset = seeded demo state" flow.

When a demoer resets the app, the tracker should immediately show
already-deployed example runs so the audience can browse a completed flow
even when there isn't time to run a live one.

Every fixture is a SNAPSHOT of a REAL end-to-end run (full event log, real
pipeline / job / PR ids), captured into src/agent/fixtures/*.json. So every
button on the tracker's asset row opens a live workspace object, and the
seeded demo reflects the exact current architecture (consolidated Lakeflow
Connect ingestion + multi-file SDP). To refresh a fixture, re-run the request
and re-snapshot (see the README / memory for the snapshot one-liner).
"""

from __future__ import annotations

import json
import pathlib
from typing import Any


def _load_snapshot_fixtures() -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Load every real-run snapshot in src/agent/fixtures/, ordered by
    filename (so the demo ordering is stable/curated by file name)."""
    out: list[tuple[dict[str, Any], list[dict[str, Any]]]] = []
    fdir = pathlib.Path(__file__).parent / "fixtures"
    if fdir.is_dir():
        for f in sorted(fdir.glob("*.json")):
            if f.name == "releases.json":
                continue  # release records, not request snapshots
            try:
                snap = json.loads(f.read_text())
                out.append((snap["request"], snap["events"]))
            except Exception:
                pass
    return out


def get_seed_fixtures() -> list[tuple[dict[str, Any], list[dict[str, Any]]]]:
    """Return (request, events) pairs to seed after a reset — the real-run
    snapshots in src/agent/fixtures/."""
    return _load_snapshot_fixtures()


def get_seed_releases() -> list[dict[str, Any]]:
    """Return pre-built release records to seed after a reset, captured from a
    real promotion run into src/agent/fixtures/releases.json (a JSON list).
    Empty if the file is absent."""
    f = pathlib.Path(__file__).parent / "fixtures" / "releases.json"
    if f.is_file():
        try:
            data = json.loads(f.read_text())
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []
