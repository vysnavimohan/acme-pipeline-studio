#!/usr/bin/env python3
"""Critic for a Your company intake spec. Applies the hard ambiguity rules in SKILL.md section 3.

Usage:  python3 validate_spec.py spec.json
Output: SPEC_OK, or up to 3 numbered questions (highest priority first).
Exit:   0 = SPEC_OK, 1 = questions raised, 2 = bad input.
Priority: 1 = blocks the build, 2 = changes results, 3 = tidy-up.
"""
import json
import re
import sys

SNAKE = re.compile(r"^[a-z][a-z0-9_]*$")
ROLLUP_MODES = {"exclude", "keep", "separate"}
METRIC_WORDS = re.compile(r"\b(rate|ratio|percent|percentage|bps|margin|kpi|return|yield|growth|share|dso|dio)\b|%", re.I)


def _dict(value):
    return value if isinstance(value, dict) else {}


def _text(spec):
    src = _dict(spec.get("source"))
    rules = _dict(spec.get("transform")).get("business_rules") or []
    return {
        "intent": str(spec.get("intent") or "").lower(),
        "tables": " ".join(src.get("tables") or []).lower(),
        "rules": " ".join(rules).lower(),
    }


def hard_rules(spec):
    out = []
    intent = str(spec.get("intent") or "").strip()
    if len(intent.split()) < 12:
        out.append((2, "Restate the intent in at least 12 words: which margin, working-capital or stock decision does it support, and for whom?"))

    src = _dict(spec.get("source"))
    tables = src.get("tables") or []
    tgt = _dict(spec.get("target"))
    table = str(tgt.get("table") or "")
    if not (tgt.get("catalog") and tgt.get("schema") and table) or not SNAKE.match(table):
        out.append((1, "Name the silver target: catalog (vmohan_demo_catalog, vmohan_demo_catalog or vmohan_demo_catalog), schema, and a snake_case table name."))

    join = _dict(spec.get("transform")).get("join_with")
    if len(tables) > 1 and not join and not src.get("primary_key"):
        out.append((1, "Several source tables and no join key: which column joins them, and is it unique on each side?"))
    if join and (join.get("rollup_rows") or "") not in ROLLUP_MODES:
        out.append((1, f"The join on {join.get('on')} can pick up branch or category subtotal rows. "
                       "Exclude, keep, or report them separately?"))

    dq = _dict(spec.get("dq"))
    if not dq.get("not_null") and not dq.get("unique"):
        out.append((2, "No data-quality rules. Which keys must not be null, and which key must be unique?"))

    for gold in spec.get("gold") or []:
        if not _dict(gold).get("grain"):
            out.append((2, f"Gold fact '{_dict(gold).get('name', '?')}' has no grain: what is one row?"))

    if METRIC_WORDS.search(intent) and not spec.get("metric_view"):
        out.append((3, "This is a reusable metric. Add a governed metric view in sem (name, measures, dimensions), or decline it with a reason?"))

    if "personal" in (src.get("data_flags") or []) and not _dict(spec.get("governance")).get("sensitive_columns"):
        out.append((2, "The source has customer contact data. Which columns are personal, and is the treatment mask, hash or drop?"))

    if str(src.get("late_arriving", "unknown")).lower() == "unknown":
        out.append((3, "Can ERP lines arrive late or be restated after month close? Give the window, or say none."))
    return out


def _credit_notes_decided(spec):
    t = _text(spec)
    touches = any(w in t["tables"] or w in t["intent"] for w in ("sales", "invoice", "ar_", "credit", "order"))
    decided = any(w in t["rules"] for w in ("credit", "negative", "return"))
    if touches and not decided:
        return [(2, "Credit notes and negative lines: net them against the original invoice, report them separately, or exclude them? Record the rule.")]
    return []


def _rebate_basis(spec):
    t = _text(spec)
    if "rebate" in t["intent"] + " " + t["tables"] and not ("accru" in t["rules"] or "paid" in t["rules"]):
        return [(2, "Rebates: is the metric on accrued (P&L view) or paid (cash view)? Name the basis in the business rules.")]
    return []


# Org-specific rules. Each takes the spec dict and returns a list of (priority, question).
CUSTOM_RULES = [_credit_notes_decided, _rebate_basis]


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    try:
        with open(sys.argv[1], encoding="utf-8") as fh:
            spec = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"cannot read spec: {exc}")
        sys.exit(2)

    found = hard_rules(spec)
    for rule in CUSTOM_RULES:
        found += rule(spec)
    if not found:
        print("SPEC_OK")
        sys.exit(0)
    found.sort(key=lambda pq: pq[0])
    for number, (_, question) in enumerate(found[:3], 1):
        print(f"{number}. {question}")
    sys.exit(1)


if __name__ == "__main__":
    main()
