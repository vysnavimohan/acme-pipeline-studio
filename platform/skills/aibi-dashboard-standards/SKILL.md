---
name: aibi-dashboard-standards
description: >
  Apply the Acme Corp standard to AI/BI (Lakeview) dashboards: an executive page, margin, working capital, inventory and forecast
  pages, KPI tiles, filters and a data-as-of tile. Use when the user asks to build, review or restyle a Acme Corp dashboard, add a KPI tile, or set filters.
  Not for dashboard JSON mechanics (use databricks-aibi-dashboards) or for Power BI (use powerbi-handoff).
metadata:
  version: "1.0.0"
  layer: domain
  domain: reporting
  type: standards
  scope: workspace
  owner: "BI Lead"
---

# AI/BI dashboard standards: Acme Corp

Dashboards inside Databricks cover the operational detail; Power BI remains the primary reporting surface.
Every tile reads a metric view, and every page says when it was refreshed.

## Rules
1. **Datasets:** `vmohan_demo_catalog.cfo_workbench.sem_` metric views with `MEASURE()`, or certified `vmohan_demo_catalog.cfo_workbench.gold_` tables. Test each dataset SQL on the serverless SQL warehouse before deploy.
2. **Pages (five at most):** 1 Executive (Net Revenue, Gross Margin Pct, DSO Days, Inventory Over 180 Days, Forecast Quarter-End); 2 Margin & Pricing (by category, branch, customer; Discount Leakage); 3 Working Capital (DSO Days vs 42-day target, Collections Risk Exposure); 4 Inventory ageing; 5 Forecast (quarter-end band $351–361M, point $356M, synthetic).
3. **KPI tile:** value, comparison (prior period or plan), unit. Status colour only for the comparison, using the agreed status palette.
4. **Filters:** Order Week or Month (default: latest closed), Branch, Product Category. Defaults are the latest closed period.
5. **Data-as-of tile** on every page from the source table's latest load time.
6. **Business labels only:** names from the glossary, no table or column names on the page.
7. **Customer PII** (contacts) never appears on a shared page; use Customer Name at account level only.

## Steps
1. Confirm the audience (Executive Leadership Team; Pricing; Credit Control; Supply Chain) and the three questions each page answers.
2. Map each tile to a measure; list the datasets.
3. Build the JSON with the built-in dashboard skill; test every dataset SQL first.
4. Review against rules 2–7; record reviewer role and date in the dashboard description.

## Output contract
```
Dashboard: <name>   Pages: 5   Audience: <role>   Refresh: <cadence>
Datasets: n (all from sem or certified gold: yes | no)   Tests: n pass
Data-as-of tile: yes   Review: <role>, <date>
```

## References
- `../metric-view-author/SKILL.md`: the measures.
- `../powerbi-handoff/SKILL.md`: the same measures in Power BI.

## Company-specific notes
- The forecast page reads the revenue forecast predictions table (standard contract); the band is shown as a range, never as a single point without the range.
- Synthetic values appear only in the show-and-tell mock, labelled as synthetic data.
