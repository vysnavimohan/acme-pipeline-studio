"""Grill test suite: drives /api/intake through 5 asks with the live LLM and checks the behaviour.

The driver is adaptive: after the opening ask it answers whatever the grill asked next (by next_step:
purpose / outputs / audience / questions / measures / cadence), then waits for the reuse job to finish.

Usage:
  .venv/bin/python ops/grill_tests.py http://127.0.0.1:8770 [case ...]
  SWEEP_TOKEN=... .venv/bin/python ops/grill_tests.py https://<app-url> [case ...]
Writes a JSON transcript per case to /tmp/grill_tests/<case>.json and prints PASS/FAIL per check.
Only drives the grill + the reuse step (no submit, no build).
"""
import json
import os
import re
import sys
import time
import urllib.request

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8770"
TOKEN = os.environ.get("SWEEP_TOKEN", "")
OUT = "/tmp/grill_tests"
os.makedirs(OUT, exist_ok=True)

CASES = {
    "vague": {"ask": "Create a new AI/BI dashboard", "only_first": True},
    "flagship": {
        "ask": "I want a Genie Agent the finance team can ask where we're leaking margin and what's happening "
               "with cash, and a dashboard so we can track it every week.",
        "answers": {
            "purpose": "To see each week where gross margin is leaking against plan and what is happening with cash.",
            "outputs": "Both: the Genie Agent for ad-hoc questions and a dashboard for the weekly meeting.",
            "audience": "FP&A to explain the gap to plan, category managers for pricing and freight, credit control for overdue debt.",
            "questions": "Those questions are right.",
            "measures": "Use those measures and definitions.",
            "cadence": "Weekly, Monday 07:00 Sydney.",
        },
        "expect_outputs": {"dashboard", "genie_agent"},
    },
    "discount_dashboard": {
        "ask": "A dashboard for category managers showing discounting by branch.",
        "answers": {
            "purpose": "To see which branches give away the most discount so we can coach them.",
            "outputs": "Just a dashboard, no Genie Agent.",
            "audience": "Category managers decide which branches need pricing coaching.",
            "questions": "Yes, those questions.",
            "measures": "Yes, use those measures.",
            "cadence": "Weekly is fine.",
        },
        "expect_outputs": {"dashboard"},
    },
    "dso_agent": {
        "ask": "A Genie Agent for credit control about DSO by customer segment.",
        "answers": {
            "purpose": "So credit control can see which customer segments pay slowest and chase them first.",
            "outputs": "Only the Genie Agent, no dashboard.",
            "audience": "Credit control decide which segments to chase first.",
            "questions": "Those questions are fine.",
            "measures": "Yes, those measures.",
            "cadence": "Ad hoc, daily freshness is fine.",
        },
        "expect_outputs": {"genie_agent"},
    },
    "delivery_raw": {
        "ask": "A dashboard of delivery times for the warehouse team.",
        "answers": {
            "purpose": "To see which carriers deliver late so warehouse managers can escalate them.",
            "outputs": "Just a dashboard.",
            "audience": "Warehouse managers decide which carriers to escalate.",
            "questions": "On-time delivery % and average days from order to delivery, by carrier and branch.",
            "measures": "On-time delivery % and average delivery days.",
            "cadence": "Weekly.",
        },
        "expect_outputs": {"dashboard"},
        "expect_raw": True,
    },
    "competitor_no_data": {
        "ask": "A dashboard comparing our prices with competitor prices for the category managers.",
        "answers": {
            "purpose": "To see where competitors undercut us so category managers can respond.",
            "outputs": "Just a dashboard.",
            "audience": "Category managers decide where to match competitor prices.",
            "questions": "Our price vs competitor price by product and branch.",
            "measures": "Price gap to competitor %.",
            "cadence": "Weekly.",
        },
        "expect_no_data": True,
    },
}


def _hdr():
    return {"Content-Type": "application/json", **({"Authorization": f"Bearer {TOKEN}"} if TOKEN else {})}


def post(path, body):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), method="POST", headers=_hdr())
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.loads(r.read().decode(), strict=False)


def get(path):
    req = urllib.request.Request(BASE + path, headers=_hdr())
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode(), strict=False)


def run_case(name, case):
    log, sid, msg = [], None, case["ask"]
    last = {}
    for _ in range(10):
        body = {"user_message": msg, **({"session_id": sid} if sid else {})}
        t0 = time.time()
        last = post("/api/intake/chat", body)
        sid = last.get("session_id")
        step = last.get("next_step") or ""
        log.append({"user": msg, "assistant": last.get("assistant_message"), "next_step": step,
                    "stage": last.get("stage"), "secs": round(time.time() - t0, 1)})
        if case.get("only_first") or step == "consume" or (last.get("stage") or "") not in ("", "grill"):
            break
        msg = case["answers"].get(step) or "Yes, that's right."
    journey = {}
    for _ in range(40):                        # wait for the reuse job (planner) to finish
        journey = get(f"/api/intake/journey/{sid}")
        jobs = (journey.get("journey") or {}).get("jobs") or {}
        if not any((j or {}).get("status") == "running" for j in jobs.values()):
            break
        time.sleep(5)
    json.dump({"log": log, "last": last, "journey": journey}, open(f"{OUT}/{name}.json", "w"), indent=2, default=str)
    return log, last, journey


def checks(name, case, log, last, journey):
    j = journey.get("journey") or {}
    snap = journey.get("spec_snapshot") or last.get("spec_snapshot") or {}
    need = j.get("need") or (snap.get("data_product") or {}).get("need") or {}
    reuse = j.get("reuse") or {}
    first = (log[0]["assistant"] or "").lower()
    out = []
    if name == "vague":
        out.append(("asks about purpose", bool(re.search(r"what is (the|it|this)[^?]*for|what .*(need to know|decide)", first))))
        out.append(("no margin/cash assumed", not re.search(r"margin|leak|cash|dso", first)))
        out.append(("intent still empty", not (snap.get("intent") or "").strip()))
        return out
    out.append(("grill reached the reuse check", bool(reuse.get("items")) or reuse.get("buildable") is not None))
    if "expect_outputs" in case:
        outs = set(need.get("outputs") or [])
        out.append((f"outputs captured = {sorted(case['expect_outputs'])}", outs == case["expect_outputs"]))
        out.append(("reuse is buildable", reuse.get("buildable") is True))
        out.append(("coverage % computed", isinstance(reuse.get("coverage_pct"), (int, float))))
        out.append(("intent not the margin story", name == "flagship"
                    or "leaking margin" not in (snap.get("intent") or "").lower()))
    if case.get("expect_raw"):
        items = reuse.get("items") or []
        out.append(("RAW item found (raw feed, pipeline needed)", any(x.get("decision") == "RAW" for x in items)))
    if case.get("expect_no_data"):
        items = reuse.get("items") or []
        out.append(("NO_DATA reported", any(x.get("decision") == "NO_DATA" for x in items) or bool(reuse.get("no_data"))))
        out.append(("not buildable", reuse.get("buildable") is False))
        said = " ".join((t["assistant"] or "") for t in log).lower()
        out.append(("never steers to margin", "margin" not in said))
    return out


def main():
    total = failed = 0
    only = sys.argv[2:] or list(CASES)
    for name in only:
        case = CASES[name]
        try:
            log, last, journey = run_case(name, case)
            res = checks(name, case, log, last, journey)
        except Exception as e:  # noqa: BLE001
            res, log, journey = [(f"ran without error ({e})", False)], [], {}
        reuse = (journey.get("journey") or {}).get("reuse") or {}
        print(f"\n== {name}  ({len(log)} turns, {sum(t['secs'] for t in log):.0f}s)  "
              f"coverage={reuse.get('coverage_pct')} buildable={reuse.get('buildable')}")
        for t in log[:2]:
            print("   user:", t["user"][:90])
            print("   agent:", (t["assistant"] or "").replace("\n", " ")[:240])
        for item in (reuse.get("items") or [])[:6]:
            print(f"     · {item.get('decision'):8s} {str(item.get('need'))[:60]}  → {item.get('asset') or item.get('missing') or ''}")
        for label, ok in res:
            total += 1
            failed += 0 if ok else 1
            print(f"   {'PASS' if ok else 'FAIL'}  {label}")
    print(f"\n{total - failed}/{total} checks passed. Transcripts in {OUT}/")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
