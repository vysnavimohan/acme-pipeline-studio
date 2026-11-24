---
name: reuse-first
description: >
  Check what already exists before building a new Genie space, Power BI dataset, dashboard, metric view,
  KPI or table at Acme Corp. Searches the sem metric views, gold tables, Genie spaces and Power BI
  datasets, scores how much of the request is covered, and recommends reuse above the threshold. Use when
  asked to build a dashboard, margin or working-capital report, Genie space or new table, or when
  intake-grill reaches the consume step. Not for authoring the asset itself.
metadata:
  version: "1.0.0"
  layer: common
  domain: common
  type: critic
  scope: workspace
  owner: Analytics Lead
---

# Reuse first: Acme Corp standard

Most new requests are partly built already. Find what exists, score the coverage, and recommend reuse
before anything new is created. Duplicate margin definitions in a Power BI model and a Genie
instruction are the cost this prevents.

## Rules
1. Search before building. Order: `sem` metric views, gold tables, Genie spaces and Power BI datasets over gold, then pipelines and jobs.
2. Split the request into atomic asks: each measure (for example gross margin %), dimension (branch, category, customer), grain, filter and date column. Coverage = matched atomic asks / total atomic asks.
3. If coverage is above 70% (illustrative; the Analytics Lead sets it), recommend reuse and stop. Offer net-new only for the listed gaps.
4. A metric that exists in `sem` is reused. Never redefine gross margin, DSO or inventory ageing in a Power BI measure, a Genie instruction or a dashboard.
5. A matching name is not a match. Compare grain, formula, filter and date column. Mark near-matches as partial.
6. Record every asset checked, including the misses. An empty search is a finding, not a skip.
7. Name assets by title or full name only (for example `vmohan_demo_catalog.cfo_workbench.mv_revenue_margin`). No links, IDs or hostnames in the output.

## Steps
1. Read the atomic asks from the spec: `gold`, `metric_view.measures`, `metric_view.dimensions`, source tables.
2. Search each layer. For `sem` and gold, query `information_schema` by schema and comment text. For Genie spaces and Power BI datasets, use workspace search and the owner list.
3. Mark each ask matched, partial or gap, with its covering asset.
4. Compute coverage and fill the reuse card. Apply rule 3.
5. Return the card to intake-grill, which sets `consume.decision` and `consume.reuse_coverage_pct`.

## Output contract
```
Coverage: <n>% (<matched>/<total> atomic asks)   Decision: REUSE | BUILD NET-NEW
Matched:  <asset> (<type>): <asks it covers>
Partial:  <asset> differs on <grain | formula | filter | date column>
Gap:      <ask>: nothing found
Next:     <open these> | <build only the gap list>
```

## References
None. The search order is in Rules and Steps.

## Company-specific notes
- Check first: `vmohan_demo_catalog.cfo_workbench.mv_revenue_margin` (revenue, gross margin %, price realisation), `vmohan_demo_catalog.cfo_workbench.mv_working_capital` (DSO, DPO, DIO), `vmohan_demo_catalog.cfo_workbench.mv_inventory_ageing`.
- Genie spaces: "Margin & Pricing" and "Working Capital". Power BI datasets over gold: check the owner list before any new report.
- A Power BI measure that re-computes margin from gold is a partial match at best; point it at the `sem` definition.
