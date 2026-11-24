"""Synthetic **Tech-Spike** landing feeds — the four-feed medallion demo.

The headline scenario ("all four feeds → Bronze + Silver each → Gold facts that
join across them") is the one that exercises everything the accelerator can do:
multi-dataset ingestion, per-feed JSON flattening, and Gold that joins three
Silvers. It could never run on a fresh deployment, because
`scenarios._campaign_full_medallion_scenario()` returns None unless ALL FOUR
datasets are onboarded and the deploy only ever seeded one. So the chip simply
did not render, and the scenario had never been driven end to end.

This module makes the four feeds part of the deployment. They are SYNTHETIC and
generated here in code — no customer data is committed or shipped — but they
reproduce the real feed shapes faithfully, because the shape is the thing being
demonstrated:

  * the ``@$!~`` multi-character delimiter (Auto Loader cannot split on it
    natively, which is why the framework reads `text` and splits with a regex)
  * a header row that must be dropped by predicate, not by reader option
  * embedded JSON columns per feed, which Silver flattens into typed columns
  * deliberately dirty ``2026-99-99`` timestamps that must stay STRING in Bronze

The single most important property is the **shared MSISDN cohort**: all four
feeds draw from one pool of subscribers and one set of campaigns, so the Gold
fact that joins comm-history to incoming-SMS to reward-history on `msisdn`
actually produces rows. Independently random feeds would join to nothing and the
demo's punchline — a real cross-feed join — would silently be zero rows.

Deterministic (a seeded LCG, no `random`/`datetime.now`) so every deployment gets
byte-identical files and row counts are reproducible in a demo.
"""

from __future__ import annotations

import json
from typing import Any

DELIM = "@$!~"

# --- the shared cohort ------------------------------------------------------
# Small enough to seed fast, large enough that Gold aggregations look real.
COHORT_SIZE = 220        # distinct MSISDNs seen across ALL four feeds
CAMPAIGN_COUNT = 12      # distinct campaigns (drives dim_campaign's row count)

_MESSAGES = [
    "Thanks for recharging. Double data has been added to your account and will "
    "expire in 28 days. Check your balance in the app.",
    "Your Prepaid service will be cancelled tomorrow. Recharge to keep your number.",
    "Would you like to donate some of your data to people who need it?",
    "DemoTel: your recharge was successful. Enjoy your plan.",
]
_TEMPLATES = ["PRE-Renew25-March", "PRE-Bonus50-April", "PRE-Winback-May",
              "PRE-Roaming-UDD-June"]
_END_POINTS = ["SEND_SMS", "DATA_BALANCE_CHECK", "RECHARGE_REDEMPTION", "SEND_EMAIL"]
_STEP_TYPES = ["WAIT", "ENTRY", "EXIT", "DECISION"]
_DETECTORS = ["OCP588_DYD_DONATE_ED", "OCP_665_WELCOME_28PLAN_DAY25_ED",
              "OCP_701_RENEWAL_ED", "OCP_412_WINBACK_ED"]


def _rng(seed: int):
    """Deterministic LCG — same files on every deployment, no RNG dependency.

    Takes the HIGH bits, not the low ones. A power-of-two-modulus LCG has
    notoriously poor low-order bits, and `state % n` uses exactly those: seeded
    naively, 1,200 draws over a 220-element cohort reached only 109 of them,
    which would look like a data-quality problem in the demo and would thin the
    cross-feed join for no reason.
    """
    state = {"x": (seed or 1) & 0x7FFFFFFF}

    def nxt(n: int) -> int:
        state["x"] = (1103515245 * state["x"] + 12345) & 0x7FFFFFFF
        return ((state["x"] >> 12) * max(n, 1)) >> 19
    return nxt


def cohort(size: int = COHORT_SIZE) -> list[str]:
    """The shared MSISDN pool. Every feed samples from THIS list, which is what
    makes the cross-feed Gold join produce rows."""
    nxt = _rng(20260401)
    return [f"04{12000000 + nxt(87999999):08d}"[:10] for _ in range(size)]


def campaigns(count: int = CAMPAIGN_COUNT) -> list[str]:
    nxt = _rng(77001)
    return [f"UC{nxt(999999):06d}A{nxt(999999):06d}_WF{nxt(999999):06d}"
            for _ in range(count)]


def _ts(nxt, day: int) -> str:
    return f"2026-04-{day:02d} {nxt(24):02d}:{nxt(60):02d}:{nxt(60):02d}"


# =====================================================================
# Feed 1 — resp_fd_cust_cam_comm_hist (27 cols, JSON in MESSAGE + FULFIL_JSON)
# =====================================================================

COMM_HIST_HEADER = [
    "MSISDN", "CAM_ID", "STEP_NUMBER", "COMM_ID", "COMM_TYPE", "MESSAGE",
    "PARTREFNUMBER", "DATA_ENCODING", "MESSAGE_ENCODING", "MESSAGE_STATUS",
    "SEQUENCE_NUMBER", "MESSAGE_ID", "GENERATED_TIME", "SENT_TIME",
    "DELIVERED_TIME", "RETRY_ON_DEL_FAIL", "RETRY_ATTEMPT_NUMBER",
    "MAX_NUMBER_OF_RETRY", "RETRY_AFTER_TIME", "TRANSACTION_ID", "LANG_CODE",
    "EVENT_GENERATED", "SERVICE_OP", "PRIORITY", "FULFIL_JSON", "MESSAGE_TYPE",
    "ACTION_STATE",
]


def comm_hist_rows(rows: int, msisdns: list[str], cams: list[str]) -> list[list[str]]:
    nxt = _rng(510001)
    out = []
    for i in range(rows):
        m = msisdns[nxt(len(msisdns))]
        comm_type = "sendEmail" if nxt(4) == 0 else "sendSMS"
        msg = json.dumps([{"message": _MESSAGES[nxt(len(_MESSAGES))]}])
        fulfil = json.dumps({"END_POINT": "SEND_SMS", "messageType": "Information Message",
                             "contactId": m, "priority": str(nxt(3)),
                             "TEMPLATE_NAME": _TEMPLATES[nxt(len(_TEMPLATES))],
                             "group": "SEND_SMS"})
        sent = _ts(nxt, 1 + nxt(28))
        out.append([
            m, cams[nxt(len(cams))], f"T{nxt(9)}",
            f"COMM_WFSTEP{nxt(999999)}_LM{nxt(999999)}", comm_type, msg,
            "0", "0", "0", "1" if nxt(10) else "0", "100000000", "",
            # Deliberately dirty: must stay STRING through Bronze.
            "2026-99-99 00:00:52" if nxt(6) == 0 else sent,
            sent, sent if nxt(3) else "", "0", "0", "0", sent,
            f"{nxt(9999999):07d}-{nxt(9999):04d}-{i:05d}", "1", "0", "0", "25",
            fulfil, "Information Message", "0",
        ])
    return out


# =====================================================================
# Feed 2 — resp_fd_cust_cam_reward_hist (17 cols, JSON in REWARD_PARAMS)
# =====================================================================

REWARD_HIST_HEADER = [
    "EVENT_GENERATED", "MSISDN", "CAM_ID", "STEP_NUMBER", "REWARD_ID",
    "REWARD_SENT_TIME", "REWARD_STATUS", "RESPONSE_TIME", "END_POINT",
    "TRANSACTION_ID", "REWARD_VALIDITY", "FUNCTION", "REWARD_PARAMS",
    "RECHARGE_AMOUNT", "REF_TRANSACTION_ID", "ACTION_STATE", "RESPONSE_DETAIL",
]


def reward_hist_rows(rows: int, msisdns: list[str], cams: list[str]) -> list[list[str]]:
    nxt = _rng(620002)
    out = []
    for i in range(rows):
        m = msisdns[nxt(len(msisdns))]
        ep = _END_POINTS[nxt(len(_END_POINTS))]
        ts = _ts(nxt, 1 + nxt(28))
        params = json.dumps({"MRTM_STEP_RESPONSE": "", "END_POINT": ep,
                             "EVENTDETECTORNAME": _DETECTORS[nxt(len(_DETECTORS))],
                             "name": "DEMO_CHECK", "MRTM_VERSION": "2.0.26",
                             "priority": str(nxt(3)), "group": ep})
        out.append([
            str(nxt(9)), m, cams[nxt(len(cams))], f"T{nxt(9)}",
            f"REWARD_LM{nxt(999999)}{nxt(9999)}", ts, "0" if nxt(8) else "1", "",
            ep, f"{nxt(9999999):07d}-{nxt(9999):04d}-{i:05d}", ts, "apiCall",
            params, "", str(9990000000 + nxt(9999999)), "0", "PPIN-OK",
        ])
    return out


# =====================================================================
# Feed 3 — resp_fd_fact_hist (20 cols, JSON in INITIAL_JSON)
# =====================================================================

FACT_HIST_HEADER = [
    "MSISDN", "CAM_ID", "STEP_NUMBER", "STEP_TYPE", "STEP_ID", "CAM_ENTRY_DATE",
    "STEP_ENTRY_DATE", "MOVE_ON_EVENT", "TARGETFLAG", "TRANSACTION_ID",
    "INITIAL_JSON", "STEP_EXIT_DATE", "TREATMENT_ID", "EVENTDETECTORNAME",
    "SEGMENTNAME", "OFFERNAME", "OFFERCATEGORY", "OFFERSUBCATEGORY",
    "STATUS_INDICATOR", "ACTION_STATE",
]


def fact_hist_rows(rows: int, msisdns: list[str], cams: list[str]) -> list[list[str]]:
    nxt = _rng(730003)
    out = []
    for i in range(rows):
        m = msisdns[nxt(len(msisdns))]
        cam = cams[nxt(len(cams))]
        step_type = _STEP_TYPES[nxt(len(_STEP_TYPES))]
        entry = _ts(nxt, 1 + nxt(28))
        detector = _DETECTORS[nxt(len(_DETECTORS))]
        initial = json.dumps({"SELECTION_METHOD": "HIGHEST_PRIORITY",
                              "STEP_ID": f"TE{nxt(9)}", "MSISDN": m,
                              "EVENTDETECTORNAME": detector,
                              "STEP_TYPE": step_type, "FIRST_STEP": "",
                              "VISITED_CAM_IDS": [cam]})
        out.append([
            m, cam, f"T{nxt(9)}", step_type,
            f"{step_type}_WF{nxt(999999)}{nxt(9999)}", entry, entry,
            f"ERED:{detector}:RS_CHECK_{cam}_AS_CAM_ID:T{nxt(9)}",
            "1" if nxt(3) else "0",
            f"{nxt(9999999):07d}-{nxt(9999):04d}-{i:05d}", initial, entry,
            f"TREAT{nxt(9999):04d}", detector,
            ["PREPAID_BASE", "HIGH_VALUE", "WINBACK"][nxt(3)],
            ["BONUS_DATA", "DOUBLE_DATA", "RENEWAL_OFFER"][nxt(3)],
            "PREPAID", ["DATA", "VOICE"][nxt(2)], "1" if nxt(5) else "0", "0",
        ])
    return out


# =====================================================================
# Feed 4 — resp_fd_incoming_sms (17 cols, JSON in INCOMING_JSON)
# =====================================================================

INCOMING_SMS_HEADER = [
    "TRANSACTION_ID", "MSISDN", "SOURCE_NAME", "TRIGGER_NAME", "EVENT_TIME",
    "RULESET_ID", "RULESET_SUCCESS", "CURRENT_STEP", "NEXT_STEP", "STATUS",
    "COMMENTS", "ECMP_TRANSACTION_ID", "INCOMING_JSON", "SEQUENCE_NUMBER",
    "MESSAGE_ID", "CAM_ID", "USER_MESSAGE_REFERENCE",
]


def incoming_sms_rows(rows: int, msisdns: list[str], cams: list[str]) -> list[list[str]]:
    nxt = _rng(840004)
    out = []
    for i in range(rows):
        m = msisdns[nxt(len(msisdns))]
        status = ["open", "delivered", "failed", "clicked"][nxt(4)]
        tmpl = _TEMPLATES[nxt(len(_TEMPLATES))]
        incoming = json.dumps({"STATUS": status, "MSISDN": m, "TEMPLATE_NAME": tmpl,
                               "RESULT_CODE": "0", "SOURCE_NAME": "COMM",
                               "TRIGGER_NAME": "COMM_DELIVERY_LISTENER",
                               "CONTACT_ID_TYPE": "sms", "CONTACT_ID": m,
                               "TEMPLATE_ID": str(3000 + nxt(999))})
        out.append([
            f"{nxt(9999999):07d}-{nxt(9999):04d}-{i:05d}", m, "COMM",
            "COMM_DELIVERY_LISTENER", _ts(nxt, 1 + nxt(28)), "", "-999", "", "",
            status, "", "", incoming, "", "", cams[nxt(len(cams))], "",
        ])
    return out


# =====================================================================
# Dataset definitions — the seeder + the scenarios both read THESE
# =====================================================================

def _cols(header: list[str], json_cols: set[str]) -> list[dict[str, Any]]:
    """Auto Loader column config for a feed.

    `source_name` keeps the ORIGINAL header token: the agent renames columns to
    snake_case, but Bronze drops the header row by matching the FILE's text, so
    the original casing has to survive the rename (this was a real bug once).
    Everything is STRING in Bronze — including the dirty timestamps, which is the
    point of a faithful 1:1 landing.
    """
    return [{"name": h.lower(), "type": "string", "is_json": h in json_cols,
             "source_name": h} for h in header]


SPIKE_FEEDS: list[dict[str, Any]] = [
    {
        "dataset": "resp_fd_cust_cam_comm_hist",
        "filename": "resp_fd_cust_cam_comm_hist_20260401_001.dat",
        "silver_table": "comm_hist_clean",
        "rows": 1200,
        "header": COMM_HIST_HEADER,
        "json_columns": {"MESSAGE", "FULFIL_JSON"},
        "builder": comm_hist_rows,
        "description": ("Campaign communication history — one row per comm sent "
                        "(SMS/email), with the message + fulfilment JSON."),
    },
    {
        "dataset": "resp_fd_cust_cam_reward_hist",
        "filename": "resp_fd_cust_cam_reward_hist_20260401_001.dat",
        "silver_table": "reward_hist_clean",
        "rows": 700,
        "header": REWARD_HIST_HEADER,
        "json_columns": {"REWARD_PARAMS"},
        "builder": reward_hist_rows,
        "description": "Reward history — rewards issued per customer and campaign.",
    },
    {
        "dataset": "resp_fd_fact_hist",
        "filename": "resp_fd_fact_hist_20260401_001.dat",
        "silver_table": "fact_hist_clean",
        "rows": 900,
        "header": FACT_HIST_HEADER,
        "json_columns": {"INITIAL_JSON"},
        "builder": fact_hist_rows,
        "description": ("Campaign step/journey history — the customer's path "
                        "through a campaign, with the initial-state JSON."),
    },
    {
        "dataset": "resp_fd_incoming_sms",
        "filename": "resp_fd_incoming_sms_20260401_001.dat",
        "silver_table": "incoming_sms_clean",
        "rows": 1000,
        "header": INCOMING_SMS_HEADER,
        "json_columns": {"INCOMING_JSON"},
        "builder": incoming_sms_rows,
        "description": ("Inbound SMS / delivery-status events — the customer's "
                        "response side of a campaign."),
    },
]

SPIKE_DATASETS = tuple(f["dataset"] for f in SPIKE_FEEDS)


def feed_by_dataset(dataset: str) -> dict[str, Any] | None:
    return next((f for f in SPIKE_FEEDS if f["dataset"] == dataset), None)


def file_bytes_for(dataset: str, *, rows: int | None = None) -> bytes:
    """The landing file for one feed: header row + delimited data rows."""
    feed = feed_by_dataset(dataset)
    if not feed:
        raise KeyError(dataset)
    msisdns, cams = cohort(), campaigns()
    data = feed["builder"](rows or feed["rows"], msisdns, cams)
    lines = [DELIM.join(feed["header"])]
    lines += [DELIM.join(r) for r in data]
    return ("\n".join(lines) + "\n").encode("utf-8")


def autoloader_config(dataset: str, landing_glob: str) -> dict[str, Any]:
    """FileSource-shaped config, matching what `autoloader_advisor` would
    produce — so the seeder can onboard these deterministically instead of
    relying on the profiler guessing a 4-character delimiter."""
    feed = feed_by_dataset(dataset)
    if not feed:
        raise KeyError(dataset)
    return {
        "dataset": dataset,
        "landing_path": landing_glob,
        "file_format": "text",       # multi-char delimiter → read as text + split
        "delimiter": DELIM,
        "has_header": True,
        "columns": _cols(feed["header"], feed["json_columns"]),
    }
