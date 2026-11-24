"""Agentic Auto Loader config suggestion for a landing-zone dataset.

For a first-time ("new") dataset, the analyst clicks **Suggest config** and an
agent proposes the Auto Loader setup. The deterministic `file_profiler` is the
FLOOR — it reliably detects the delimiter, header and column positions (which a
positional multi-char split depends on). An LLM pass then REFINES the
human-facing parts (column names → snake_case, Silver type hints, JSON flags)
and writes a short rationale + caveats. If the LLM is unavailable or returns a
mismatching column count, we fall back to the deterministic config — so the
suggestion is always safe to deploy.

Invoked on-click only (never on page load) to keep token spend controlled.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from agent import llm

_SAMPLE_ROWS = 5
_SAMPLE_VAL = 120


def config_from_profile(profile: dict, dataset: str,
                        landing_glob: Optional[str] = None) -> dict:
    """Deterministic FileSource-shaped config from a file_profiler profile.
    This is the safe floor (correct delimiter / column count / order)."""
    cols = [
        {"name": c["name"], "type": c.get("inferred_type", "string"),
         "is_json": bool(c.get("is_json", False)), "source_name": c["name"]}
        for c in profile.get("columns", [])
    ]
    return {
        "dataset": dataset,
        "landing_path": landing_glob or profile.get("ingest_glob", ""),
        "file_format": profile.get("reader_format", "text"),
        "delimiter": profile.get("delimiter", ""),
        "has_header": bool(profile.get("has_header", True)),
        "columns": cols,
    }


def _loads_lenient(text: str) -> Optional[dict]:
    """Parse JSON that may be wrapped in ```json fences or surrounding prose."""
    if not text:
        return None
    t = text.strip()
    m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", t, re.DOTALL)
    if m:
        t = m.group(1)
    else:
        i, j = t.find("{"), t.rfind("}")
        if i != -1 and j != -1 and j > i:
            t = t[i:j + 1]
    try:
        return json.loads(t)
    except Exception:
        return None


def _refine_with_llm(profile: dict, det: dict) -> tuple[Optional[list[dict]], str]:
    """Ask the LLM to refine column names/types + write a rationale. Returns
    (refined_columns | None, rationale). Best-effort: any failure → (None, "")."""
    from langchain_core.messages import HumanMessage, SystemMessage

    det_cols = det.get("columns", [])
    sample_rows = [[(str(v)[:_SAMPLE_VAL]) for v in r]
                   for r in (profile.get("sample_rows") or [])[:_SAMPLE_ROWS]]
    col_brief = [{"i": i, "name": c["name"], "type": c["type"], "is_json": c["is_json"],
                  "sample": (profile.get("columns") or [{}] * (i + 1))[i].get("sample", "")[:_SAMPLE_VAL]
                  if i < len(profile.get("columns") or []) else ""}
                 for i, c in enumerate(det_cols)]

    system = (
        "You are a senior Databricks data engineer onboarding a landing-zone file "
        "to Auto Loader (cloudFiles) under the Lakeflow Framework. The delimiter, "
        "header detection and the NUMBER and ORDER of columns are already fixed by a "
        "deterministic profiler and MUST NOT change (Bronze splits positionally). "
        "Your job is ONLY to (a) propose clean snake_case column names where the "
        "detected names are weak/missing, (b) confirm a Silver type hint per column "
        "(string|long|double|timestamp|boolean|date; keep JSON and dirty values as "
        "string), (c) flag JSON columns, and (d) write a short rationale + caveats. "
        "Return STRICT JSON only: {\"columns\":[{\"name\":..,\"type\":..,\"is_json\":..}],"
        "\"file_format\":\"text|csv\",\"rationale\":\"..\",\"caveats\":[\"..\"]}. "
        "The columns array MUST have exactly the same length and order as the input."
    )
    user = json.dumps({
        "dataset": det.get("dataset"),
        "file_format": det.get("file_format"),
        "delimiter": det.get("delimiter"),
        "has_header": det.get("has_header"),
        "detected_columns": col_brief,
        "sample_rows": sample_rows,
        "profiler_notes": profile.get("notes", []),
    }, indent=2)

    try:
        resp = llm.get_llm().invoke([SystemMessage(content=system), HumanMessage(content=user)])
        data = _loads_lenient(resp.content) or {}
    except Exception:
        return None, ""

    rationale = (data.get("rationale") or "").strip()
    caveats = data.get("caveats") or []
    if caveats:
        rationale = (rationale + "  Caveats: " + "; ".join(str(c) for c in caveats)).strip()

    refined = data.get("columns")
    if not isinstance(refined, list) or len(refined) != len(det_cols):
        # Column count drifted — unsafe to use the LLM columns (positional split).
        return None, rationale

    out = []
    for det_c, llm_c in zip(det_cols, refined):
        # Preserve the original header token so the header-line drop still
        # matches even when the LLM renames the column.
        source_name = det_c.get("source_name") or det_c["name"]
        if not isinstance(llm_c, dict):
            out.append(det_c)
            continue
        name = str(llm_c.get("name") or det_c["name"]).strip() or det_c["name"]
        typ = str(llm_c.get("type") or det_c["type"]).strip().lower() or det_c["type"]
        out.append({"name": name, "type": typ,
                    "is_json": bool(llm_c.get("is_json", det_c["is_json"])),
                    "source_name": source_name})
    return out, rationale


def suggest_config(profile: dict, dataset: str, *,
                   landing_glob: Optional[str] = None, use_llm: bool = True) -> dict:
    """Suggest an Auto Loader config for a dataset given its profiler output.
    Returns {config, rationale, profile, deterministic, error?}."""
    if profile.get("error"):
        return {"error": profile["error"], "config": None, "profile": profile}

    det = config_from_profile(profile, dataset, landing_glob)
    rationale = ""
    config = dict(det)
    if use_llm:
        refined_cols, rationale = _refine_with_llm(profile, det)
        if refined_cols:
            config = {**det, "columns": refined_cols}
    if not rationale:
        kind = profile.get("delimiter_kind", "none")
        rationale = (f"Detected a {kind} delimiter {det['delimiter']!r} with "
                     f"{len(det['columns'])} columns"
                     f"{' and a header row' if det['has_header'] else ''}; "
                     f"reading as cloudFiles.format={det['file_format']}.")
    return {"config": config, "rationale": rationale, "profile": profile,
            "deterministic": det}
