---
name: monday-margin-pulse
description: >
  Produce the Office of the CFO Monday Margin Pulse for the Acme Corp executive team and board: one page, fixed sections,
  governed numbers only, with the revenue forecast and collections risk. Use when the user asks to prepare, draft or format the
  Monday Margin Pulse, the weekly margin brief, the board briefing, or "the standard brief for this week". Not for ad-hoc analysis (use the Genie Agents) or regulatory extracts.
metadata:
  version: "1.0.0"
  layer: use-case
  domain: reporting
  type: output-format
  scope: workspace
  owner: "CFO Office Analytics Lead"
---

# Monday Margin Pulse: Acme Corp standard

A standing weekly brief the CFO issues every Monday morning (and at month-end close). The format is fixed so
the executive team and board see the same document each week. This skill fills the format from governed
numbers; it never adds a metric.

## Governed sources
| Source | Used for |
|---|---|
| `vmohan_demo_catalog.cfo_workbench.mv_revenue_margin` | Net Revenue, Gross Margin Pct, Gross Margin vs Plan Bps, Discount Leakage |
| `vmohan_demo_catalog.cfo_workbench.mv_working_capital` | DSO Days, Collections Risk Exposure |
| `vmohan_demo_catalog.cfo_workbench.mv_inventory_ageing` | Inventory Over 180 Days |
| `vmohan_demo_catalog.cfo_workbench.ml_predictions_revenue_forecast` | quarter-end forecast and band (standard predictions contract) |
| `vmohan_demo_catalog.cfo_workbench.ml_predictions_collections_risk` and `vmohan_demo_catalog.cfo_workbench.ml_risk_drivers` | collections watch item and its top drivers |

## Rules
1. **Never fabricate numbers.** Each figure comes from a gather query. An empty result becomes "no data for this period" in its section.
2. **Fixed format.** Use `assets/brief_template.md`. Do not add, drop or reorder sections.
3. **CFO voice.** Sections 1 and 7 are first person and directive ("I expect…", "the team will…").
4. **Healthy framing.** ✅ for in-control measures; ⚠️ only for genuine watch items, each with a number and an owner. Fasteners margin and collections risk are the two watch items in this week's standard.
5. **One page.** Tight bullets, no SQL, no table dumps.
6. **From "CFO, Office of the CFO" to "Executive Leadership Team" (weekly) or "Board" (month-end).** Role titles only.
7. **Use the governed measure names** exactly; do not re-derive margin or DSO from raw amounts.
8. **Label the data:** period, data-as-of time, and "synthetic data" in any demo copy.

## Gather (latest closed week and month unless the user names a period)
```sql
-- (a) Net revenue, margin and plan variance by week, last 2 weeks
SELECT `Order Week`, MEASURE(`Net Revenue`) AS net_revenue, MEASURE(`Gross Margin Pct`) AS gm_pct,
       MEASURE(`Gross Margin vs Plan Bps`) AS gm_vs_plan_bps
FROM vmohan_demo_catalog.cfo_workbench.mv_revenue_margin WHERE `Order Week` >= date_sub(current_date(), 14) GROUP BY ALL ORDER BY ALL;

-- (b) Fasteners and top-20 account leakage, latest week
SELECT `Product Category`, `Customer Name`, MEASURE(`Discount Leakage`) AS discount_leakage,
       MEASURE(`Gross Margin vs Plan Bps`) AS gm_vs_plan_bps
FROM vmohan_demo_catalog.cfo_workbench.mv_revenue_margin WHERE `Order Week` = DATE_TRUNC('WEEK', date_sub(current_date(), 7))
GROUP BY ALL ORDER BY discount_leakage DESC LIMIT 20;

-- (c) Working capital: DSO and collections risk, latest month
SELECT MEASURE(`DSO Days`) AS dso_days, MEASURE(`Collections Risk Exposure`) AS collections_risk
FROM vmohan_demo_catalog.cfo_workbench.mv_working_capital WHERE `Invoice Month` = DATE_TRUNC('MONTH', current_date()) GROUP BY ALL;

-- (d) Inventory ageing
SELECT MEASURE(`Inventory Over 180 Days`) AS inv_over_180 FROM vmohan_demo_catalog.cfo_workbench.mv_inventory_ageing GROUP BY ALL;

-- (e) Quarter-end revenue forecast and band (total-company row)
SELECT period, forecast, lower, upper FROM vmohan_demo_catalog.cfo_workbench.ml_predictions_revenue_forecast
WHERE entity = 'Total Company' AND period = LAST_DAY(ADD_MONTHS(DATE_TRUNC('QUARTER', current_date()), 2));

-- (f) Top collections-risk drivers (for the watch item)
SELECT * FROM vmohan_demo_catalog.cfo_workbench.ml_risk_drivers ORDER BY 1 LIMIT 3;
```
Run the gather queries on the serverless SQL warehouse. If a view or table is missing, the section says so.

## Output contract
The brief in the fixed format of `assets/brief_template.md`, ending with: "Source: governed views as of <timestamp>."

## References
- `assets/brief_template.md`: the verbatim format, filled with the synthetic contract values.
- `../metric-view-author/SKILL.md`: the measures used above.
- `../business-glossary/SKILL.md`: term names.
