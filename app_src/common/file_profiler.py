"""Profile a delimited text file so the agent can ingest it with Auto Loader.

Landing-zone vendor extracts (e.g. an ERP extract) are
custom-delimited text/DAT files — a sample vendor `*.dat` file uses the
multi-character delimiter ``@$!~``, has a header row, embeds JSON in some
columns, and carries dirty values (e.g. ``2026-99-99`` dates). Spark's CSV
reader only accepts a SINGLE-character ``sep``, so multi-char delimiters must
be read as ``text`` (one line per row) and split with ``selectExp``.

This module is pure (operates on text) so it is unit-testable, plus a thin
helper to read the head of a file that has landed in a Unity Catalog Volume.
The output feeds both the Landing Zone onboarding UI and the agent's Auto
Loader bronze dataflowspec generation.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional

# Candidate delimiters, longest/most-specific first so a multi-char delimiter
# wins over an incidental single char it happens to contain.
_MULTI_CHAR_CANDIDATES = ["@$!~", "~@~", "|~|", "@@", "|~", "~|", "::", ""]
_SINGLE_CHAR_CANDIDATES = ["", "\t", "|", "¦", ";", "~", ",", ":"]

_MAX_SAMPLE_LINES = 50
_MAX_VALUE_SAMPLE = 200  # chars kept per sampled value for the UI


def _split_lines(text: str) -> list[str]:
    return [ln for ln in text.replace("\r\n", "\n").replace("\r", "\n").split("\n") if ln.strip()]


def _score_delimiter(lines: list[str], delim: str) -> tuple[float, int]:
    """Return (consistency, modal_column_count) for splitting `lines` on `delim`.
    Consistency = fraction of lines whose column count equals the modal count."""
    counts: dict[int, int] = {}
    for ln in lines:
        n = len(ln.split(delim))
        counts[n] = counts.get(n, 0) + 1
    # Ignore the degenerate "1 column" outcome (delimiter not present)
    real = {n: c for n, c in counts.items() if n > 1}
    if not real:
        return (0.0, 1)
    modal_count = max(real, key=lambda n: real[n])
    consistency = real[modal_count] / len(lines)
    return (consistency, modal_count)


def detect_delimiter(lines: list[str]) -> tuple[Optional[str], str]:
    """Best delimiter for the sample lines. Returns (delimiter, kind) where
    kind is 'multi-char' | 'single-char' | 'none'. Multi-char candidates are
    preferred on ties because Spark CSV can't parse them (drives reader choice)."""
    best: tuple[float, int, int, Optional[str], str] = (0.0, 1, 0, None, "none")
    # (consistency, modal_count, delim_len, delim, kind)
    for kind, candidates in (("multi-char", _MULTI_CHAR_CANDIDATES),
                             ("single-char", _SINGLE_CHAR_CANDIDATES)):
        for d in candidates:
            cons, cols = _score_delimiter(lines, d)
            if cols <= 1 or cons < 0.6:
                continue
            cand = (cons, cols, len(d), d, kind)
            # prefer: higher consistency, then more columns, then longer delim
            if (cand[0], cand[1], cand[2]) > (best[0], best[1], best[2]):
                best = cand
    return (best[3], best[4])


_IDENT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_ \-./]*$")
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+\.\d+$")
# A real ISO-ish date/timestamp; rejects dirty values like 2026-99-99.
_TS_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])"
                    r"([ T]([01]\d|2[0-3]):[0-5]\d(:[0-5]\d)?)?$")


def _looks_like_header(first_cols: list[str], rest_rows: list[list[str]]) -> bool:
    """Header heuristic: first row cells are identifier-like, and at least one
    column's data rows look different in type from the header cell."""
    if not first_cols:
        return False
    ident_like = sum(1 for c in first_cols if _IDENT_RE.match(c.strip())) / len(first_cols)
    if ident_like < 0.7:
        return False
    # If any data row has a numeric cell where the header is a word, that's a header.
    for row in rest_rows[:10]:
        for h, v in zip(first_cols, row):
            if _INT_RE.match(v.strip()) and not _INT_RE.match(h.strip()):
                return True
    # Otherwise accept when header cells are non-numeric words
    return ident_like >= 0.9


def _infer_type(values: list[str]) -> str:
    """Suggested Spark type from sampled values (used for Silver hints; Bronze
    stays string for a faithful 1:1 landing)."""
    vals = [v.strip() for v in values if v is not None and v.strip() != ""]
    if not vals:
        return "string"
    def all_match(rx) -> bool:
        return all(rx.match(v) for v in vals)
    if all(v[:1] in ("{", "[") for v in vals):
        return "string"   # JSON kept as string
    if all_match(_INT_RE):
        return "long"
    if all_match(_FLOAT_RE):
        return "double"
    if all_match(_TS_RE):
        return "timestamp"
    return "string"


def _is_json(values: list[str]) -> bool:
    seen = 0
    for v in values:
        s = (v or "").strip()
        if not s:
            continue
        if s[:1] not in ("{", "["):
            return False
        try:
            json.loads(s)
        except Exception:
            return False
        seen += 1
    return seen > 0


def profile_text(sample_text: str, filename: Optional[str] = None) -> dict[str, Any]:
    """Profile delimited text. Returns delimiter, header, columns (name +
    inferred type + json flag + sample), reader format, and notes."""
    lines = _split_lines(sample_text)[:_MAX_SAMPLE_LINES]
    notes: list[str] = []
    if not lines:
        return {"filename": filename, "error": "empty file / no readable lines"}

    delim, kind = detect_delimiter(lines)
    if not delim:
        # Single-column file (no delimiter found) — treat whole line as one col.
        delim, kind = ("", "none")
        notes.append("No delimiter detected — treating each line as a single value.")

    rows = [ln.split(delim) if delim else [ln] for ln in lines]
    n_cols = max((len(r) for r in rows), default=1)
    # Pad short rows so column sampling is safe
    rows = [r + [""] * (n_cols - len(r)) for r in rows]

    header = _looks_like_header(rows[0], rows[1:]) if len(rows) > 1 else False
    if header:
        names = [c.strip() or f"col_{i}" for i, c in enumerate(rows[0])]
        data_rows = rows[1:]
    else:
        names = [f"col_{i}" for i in range(n_cols)]
        data_rows = rows

    columns = []
    for i, name in enumerate(names):
        col_vals = [r[i] for r in data_rows if i < len(r)]
        columns.append({
            "name": name,
            "inferred_type": _infer_type(col_vals),
            "is_json": _is_json(col_vals),
            "sample": (col_vals[0][:_MAX_VALUE_SAMPLE] if col_vals else ""),
        })

    reader_format = "csv" if kind == "single-char" else "text"
    if kind == "multi-char":
        notes.append(f"Multi-character delimiter {delim!r} — Spark CSV can't parse it; "
                     "Bronze reads cloudFiles.format=text and splits with selectExp.")
    if any(c["is_json"] for c in columns):
        notes.append("Embedded JSON detected — kept as STRING in Bronze (parse downstream in Silver).")

    return {
        "filename": filename,
        "delimiter": delim,
        "delimiter_kind": kind,
        "has_header": header,
        "n_columns": len(names),
        "reader_format": reader_format,
        "columns": columns,
        "sample_rows": [r for r in data_rows[:5]],
        "n_sample_lines": len(lines),
        "notes": notes,
    }


# --- Volume / workspace file reading --------------------------------------

def read_volume_head(path: str, max_bytes: int = 256 * 1024) -> str:
    """Read up to `max_bytes` from the head of a UC Volume file (``/Volumes/...``)
    via the Databricks Files API. Returns decoded text (utf-8, errors replaced)."""
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    resp = w.files.download(path)
    stream = resp.contents
    data = stream.read(max_bytes) if hasattr(stream, "read") else bytes(stream)[:max_bytes]
    if isinstance(data, str):
        data = data.encode("utf-8", "replace")
    return data.decode("utf-8", "replace")


def profile_volume_file(path: str, max_bytes: int = 256 * 1024) -> dict[str, Any]:
    """Read the head of a Volume file and profile it."""
    import os
    text = read_volume_head(path, max_bytes=max_bytes)
    return profile_text(text, filename=os.path.basename(path))
