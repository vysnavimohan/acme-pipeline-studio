---
name: metric-view-author
description: >
  Author a Unity Catalog metric view (YAML, version 1.1) for Acme Corp's governed measures, add dimensions and a KPI
  dictionary entry, then certify it. Use when the user asks for a KPI, a metric view, a MEASURE() query, a vmohan_demo_catalog.cfo_workbench.mv_* view, or a measure
  definition for dashboards, Genie or Power BI. Not for physical tables (use dimensional-modelling) or BI-tool models (use powerbi-handoff).
metadata:
  version: "1.0.0"
  layer: domain
  domain: warehousing
  type: workflow
  scope: workspace
  owner: "Finance Systems Lead"
---

# Metric view author: Acme Corp standard

The one place a Acme Corp business measure is defined. Power BI, the AI/BI dashboards and the two Genie
Agents query `vmohan_demo_catalog.cfo_workbench.sem_` with `MEASURE()`; nobody re-derives margin or DSO in a report.

## Rules
1. **Views in `vmohan_demo_catalog.cfo_workbench.sem_`:** `mv_revenue_margin` (sales lines), `mv_working_capital` (AR invoices), `mv_inventory_ageing` (inventory snapshots). One fact source each.
2. **Version 1.1 YAML** in `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML AS $$ ... $$`. Serverless SQL warehouse; Runtime 17.2+ for `comment`.
3. **Plan and actuals share one fact table** via `scenario` ('ACTUAL' / 'PLAN'). Every plan measure filters `scenario = 'PLAN'` explicitly; never compare plan and actual in one unfiltered sum.
4. **Atomic first, composed second.** Margin %, bps vs plan and DSO are composed with `MEASURE()`.
5. **Never put a measure in WHERE or GROUP BY.** Use HAVING or an outer query.
6. **Every measure has a comment** with the unit (AUD) and sign. Negative bps means below plan.
7. **Query with `MEASURE()` and `GROUP BY ALL`.** No `SELECT *` against a metric view.
8. **Certify in Catalog Explorer** after the first clean run; uncertified views are not used by the pulse brief.
9. **Deploy as code** via a bundle-managed SQL job from the repo; no manual edits in `vmohan_demo_catalog`.

## Steps
1. Confirm the question and the fact table with dimensional-modelling.
2. Start from `assets/metric_view_template.yaml`; write the other two views in the same shape.
3. Create in `vmohan_demo_catalog`; run one `MEASURE()` query per measure; reconcile Net Revenue to `SUM(net_amount)` on the source.
4. Promote through the bundle job; certify; add each measure to the KPI dictionary and the glossary.

## Output contract
```
View: vmohan_demo_catalog.cfo_workbench.mv_<domain>   Source: <fact>   Grain: <grain>
Measures: n (atomic n, composed n)   Plan filter: explicit on all plan measures
Reconciliation: Net Revenue matches source (yes | diff)
Certified: yes (date, role) | pending
```

## References
- `assets/metric_view_template.yaml`: the margin view (Net Revenue, Gross Margin Pct, Gross Margin vs Plan Bps, Discount Leakage).
- `../business-glossary/SKILL.md`: the term names these measures use.

## Company-specific notes
- Margin view measure names are used by the pulse brief and the Margin & Pricing agent. Renaming a measure is a breaking change; add the new name and retire the old one after the dependents move.
- Working-capital view: dimensions `Invoice Month`, `Branch`, `Customer Name`; measures `Open AR`, `Credit Sales`, `DSO Days` (credit sales over 91 days, illustrative; Finance confirms), `Collections Risk Exposure` (open AR where `risk_flag = 'HIGH'`, the flag written from the collections-risk predictions).
- Inventory view: `Inventory Value`, `Inventory Over 180 Days`, `Inventory Over 180 Days Share`.
