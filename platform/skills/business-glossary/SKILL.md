---
name: business-glossary
description: >
  Define and maintain Acme Corp business terms: gross margin, price realisation, DSO, collections risk, inventory
  ageing, and which measure is the number. Use when the user asks what a term means, which figure is authoritative, to add a term, or to
  resolve two definitions that disagree (for example "margin" in sales vs finance). Not for measure SQL (use metric-view-author).
metadata:
  version: "1.0.0"
  layer: domain
  domain: warehousing
  type: context
  scope: workspace
  owner: "Finance Data Owner"
---

# Business glossary: Acme Corp standard

One term, one definition, one owner, one source of truth. Genie Agents, dashboards and the Monday Margin
Pulse use these names exactly, so a number means the same thing wherever it appears.

## Rules
1. **Term names match measure names** in `vmohan_demo_catalog.cfo_workbench.sem_` (same spelling and case).
2. **Every term names a source of truth:** a metric view measure, never a raw table.
3. **"Margin" alone is not allowed** in outputs. Say "Gross Margin Pct" (actual), "Plan Gross Margin Pct", or "Gross Margin vs Plan Bps".
4. **"Revenue" alone is not allowed.** Use "Net Revenue" (after discounts, ACTUAL lines, AUD).
5. **DSO** is always "DSO Days", computed in the working-capital view; the target (42 days) is set by the CFO office and shown beside the actual, never inside it.
6. **Owner sign-off** on any change to a definition; versions are kept in this file.

## Core terms
| Term | Definition (short) | Source of truth | Owner | Do not use for |
|---|---|---|---|---|
| Net Revenue | Sales after discounts, ACTUAL lines, AUD | `mv_revenue_margin` · `Net Revenue` | Finance Systems Lead | Gross sales, invoiced amount |
| Gross Margin Pct | Gross Margin / Net Revenue | `mv_revenue_margin` · `Gross Margin Pct` | Finance Systems Lead | Contribution margin |
| Gross Margin vs Plan Bps | Actual GM% minus plan GM%, in bps; negative is below plan | `mv_revenue_margin` · `Gross Margin vs Plan Bps` | CFO Office Analytics Lead | Price variance |
| Discount Leakage | Discount value on ACTUAL lines (leakage by category and account) | `mv_revenue_margin` · `Discount Leakage` | Pricing Lead | Rebates (see rebate accrual) |
| DSO Days | Open AR over credit sales, in days (91-day basis, illustrative) | `mv_working_capital` · `DSO Days` | Credit Control Lead | Days to pay |
| Collections Risk Exposure | Open AR on invoices flagged HIGH by the collections-risk model | `mv_working_capital` · `Collections Risk Exposure` | Credit Control Lead | Bad debt provision |
| Inventory Over 180 Days | Inventory value with age over 180 days | `mv_inventory_ageing` · `Inventory Over 180 Days` | Supply Chain Lead | Obsolescence provision |
| Forecast Quarter-End | Model forecast at the total-company row for the quarter-end period | `ml.predictions_revenue_forecast` (entity Total Company) | CFO Office Analytics Lead | Budget |

## Steps
1. Search this file and the metric view dictionary before adding a term.
2. Write the row: term, definition, unit and sign, source of truth, owner, do-not-use list.
3. Confirm the measure exists with the owner; then publish as table comments and to the agent instructions.

## Output contract
```
Term: <name>   Status: certified | draft   Version: n (date)
Definition: <one sentence, unit, sign>   Source: <view.measure>   Owner: <role>
Do not use for: ...
```

## References
- `../metric-view-author/SKILL.md`: the measures named here.
- `../genie-agent-author/SKILL.md`: the agent instructions that use these terms.
