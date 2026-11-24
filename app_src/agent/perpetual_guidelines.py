"""Acme Corp finance guideline overrides — the domain layer.

Pipeline Studio ships GENERIC Databricks guidelines. This module seeds the
Acme Corp finance OVERRIDES (Margin & Cash) so the agents build in-domain:
gross margin and leakage drivers (discounts, rebates, freight), DSO and overdue
AR, inventory ageing, USD amounts, and the governance rules for customer data
on shared dashboards.

The module and function names are kept for import compatibility with
`agent/event_store.py`; the content is Acme Corp, not any other domain.

Seeded via `guidelines.update(...)`, the SAME path the in-app Agent config editor
uses, so the overrides show as editable and can be reset to the generic default.
Idempotent and best-effort: never blocks a deploy.

This is the "governance of the AGENTS" layer, complementing the Unity AI Gateway
"governance of the MODEL CALLS" layer: the gateway governs what model the agents
may call; these guidelines govern what the agents are allowed to produce.
"""

from __future__ import annotations

from agent import prompts as default_prompts

# Appended to the build prompts (additive, so upstream prompt improvements still flow).
_ACME_CONTEXT = """

# Acme Corp — finance domain overrides (Margin & Cash)
- **Scope:** Acme Corp's ERP (sales invoices, AR, inventory, freight, supplier rebates,
  FP&A plan). Amounts are USD and carry the `_usd` suffix.
- **Vocabulary:** net revenue, COGS, gross margin (GM%), pre-rebate margin, leakage
  drivers (discount, rebate, freight), plan GM vs actual (bps), DSO, overdue AR (>30 days),
  aged inventory (>180 days).
- **Reuse first:** the governed metric views `mv_revenue_margin`, `mv_working_capital` and
  `mv_inventory_ageing` define the numbers. Build on them; do not re-derive GM, DSO or
  ageing from raw tables in a new definition.
- **Governance:** customer names and contact details are personal or confidential data.
  Classify them `confidential` in column metadata, and never put them on a shared dashboard
  or in a shared Genie Agent answer unless the requester states a documented need.
- **Grain:** keep `branch_id` and `category` in the grain so branch-level leakage stays visible.
"""

_ACME_APP_CONTEXT = """

# Acme Corp — app overrides (Margin & Cash)
- **Headline KPIs:** GM% vs plan (percent, with bps delta), leakage by driver (money),
  DSO (days), overdue AR over 30 days (money), inventory over 180 days (money).
- **Charts:** GM% trend vs plan (line); leakage by driver and category (bar); DSO trend (line);
  overdue AR by bucket (bar); top branches by GM gap (table).
- **Scope:** one company view with optional branch and category filters. Do not show
  individual customer names on a shared page.
- Money for balances and amounts, percent for rates, days for DSO, int for counts.
"""

_ACME_TEST_CONTEXT = """

# Acme Corp — test additions (finance)
- **Margin identity:** gross margin = net revenue - COGS, within a small tolerance.
- **Discount ceiling:** `discount_pct` above the approved ceiling is flagged for review, not dropped.
- **Paid means closed:** an invoice with a paid date must not carry an open balance.
- **Plan coverage:** every month and category in actuals exists in `fpa_plan` (or is flagged).
"""

_ACME_REVIEW_CONTEXT = """

# Acme Corp — review focus (finance)
When writing "Worth a closer look", check finance specifics first:
- **Reuse:** does the change re-derive GM, DSO or ageing instead of using the metric view?
- **Definitions:** is leakage defined as pre-rebate margin minus GM, and is the same definition
  used by the Genie Agent and the dashboard?
- **Customer data:** are customer names and contacts classified `confidential` and kept off
  shared outputs?
- **Units:** are USD amounts, percentages and day counts labelled with the right unit?
"""

_ACME_DEPLOY_CONTEXT = """

# Acme Corp — deploy SOP additions (finance)
- Before the prod build, confirm the prod metric views and dashboard use the same definitions as dev.
- Compare each month's GM and open AR totals between dev and prod; a whole-company total can
  hide a single branch dropping to zero, so also check per-branch counts.
- Treat customer-level tables as `confidential` in prod grants, same as dev.
"""

# agent_name -> the full override prompt (default + Acme Corp context).
_OVERRIDES = {
    "spec_critic": default_prompts.SPEC_CRITIC + _ACME_CONTEXT,
    "source_profiler": default_prompts.SOURCE_PROFILER + _ACME_CONTEXT,
    "pipeline_generator": default_prompts.PIPELINE_GENERATOR + _ACME_CONTEXT,
    "standards_validator": default_prompts.STANDARDS_VALIDATOR + _ACME_CONTEXT,
    "test_generator": default_prompts.TEST_GENERATOR + _ACME_TEST_CONTEXT,
    "pr_reviewer": default_prompts.PR_REVIEWER + _ACME_REVIEW_CONTEXT,
    "deploy_agent": default_prompts.DEPLOY_AGENT + _ACME_DEPLOY_CONTEXT,
    "app_generator": default_prompts.APP_GENERATOR + _ACME_APP_CONTEXT,
    "app_standards_validator": default_prompts.APP_STANDARDS_VALIDATOR + _ACME_APP_CONTEXT,
}


def seed_perpetual_guidelines(updated_by: str = "acme-seed") -> dict:
    """Install the Acme Corp finance overrides. Idempotent (update() replaces by agent_name).
    Best-effort: returns a per-agent status map; never raises."""
    from agent import guidelines
    out: dict[str, str] = {}
    for agent_name, prompt in _OVERRIDES.items():
        try:
            guidelines.update(agent_name, prompt, updated_by=updated_by)
            out[agent_name] = "seeded"
        except Exception as e:  # noqa: BLE001
            out[agent_name] = f"skipped: {str(e)[:80]}"
    return out
