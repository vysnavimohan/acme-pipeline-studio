---
name: medallion-sdp
description: >
  Write Your company bronze, silver and gold layers as serverless Spark Declarative Pipelines
  (pyspark.pipelines): naming, expectations, materialized views and the shared-ingest pattern. Use
  when the user asks to build, review or refactor a bronze, silver or gold pipeline, add a
  Dynamics or legacy-warehouse table, add expectations, or place a new entity into an existing
  ingest pipeline. Not for SCD history (scd2-auto-cdc), source intake (dynamics-erp-ingest) or failed
  runs (pipeline-triage).
metadata:
  version: "1.0.0"
  layer: domain
  domain: data-engineering
  type: standards
  scope: workspace
  owner: Data Engineering Lead
---

# Medallion on SDP: Your company standard

All Your company pipelines use the same layers, names and patterns so a reviewer can read any pipeline
without a walkthrough. Worked code is in references/patterns.md. The workspace is serverless-only.

## Rules
1. **API.** `from pyspark import pipelines as dp`. Use `@dp.table`, `@dp.materialized_view`,
   `@dp.temporary_view`, `@dp.expect_or_drop` / `@dp.expect` / `@dp.expect_or_fail`,
   `dp.create_streaming_table` and `dp.create_auto_cdc_flow`. Never `import dlt` or `apply_changes`.
2. **Serverless pipelines only.** Never set cluster configuration. Catalog `vmohan_demo_catalog`, `vmohan_demo_catalog` and
   `vmohan_demo_catalog`, promoted through the bundle.
3. **Schemas:** `bronze_dynamics`, `bronze_legacy_dw` (migration extracts), `silver`, `gold`, and the
   semantic schema `sem` for metric views (`vmohan_demo_catalog.cfo_workbench.mv_revenue_margin`, `vmohan_demo_catalog.cfo_workbench.mv_working_capital`,
   `vmohan_demo_catalog.cfo_workbench.mv_inventory_ageing`).
4. **Bronze** is 1:1 with the landed data plus `_ingested_at` and `_source_file`.
5. **Silver** is typed, trimmed and keyed. Every key column has `expect_or_drop` on not-null. Value
   rules start as `expect` (warn) and move to enforcement after the owner reviews failure counts.
6. **Gold** is batch. Dimensions and aggregates are materialized views. Fact names: `fct_sales_line`,
   `fct_ar_invoice`, `fct_inventory_snapshot`, `fct_rebate_accrual`, `fct_freight_cost`. Dimension names:
   `dim_branch`, `dim_product` (with `category`), `dim_customer`, `dim_date`.
7. **Changing rows** from the ERP go through AUTO CDC (scd2-auto-cdc), never a hand-written merge.
8. **Shared ingest.** One ingest pipeline for Dynamics per environment. A new entity is one entry in the
   entity list, in one PR. Concurrent requests for the same source merge into the same change.
9. **Surrogate keys** are SHA-256 hashes of business keys, stable across reruns.
10. **Customer data:** contact PII is tagged and masked before gold; no names of people in object names.

## Steps
1. Read the source card (dynamics-erp-ingest) or the existing pipeline; identify the layer of each change.
2. Add bronze entities through the shared ingest list.
3. Write silver: types, trims, date parsing, key and mandatory-field expectations.
4. Write gold: one materialized view per output table, one named flow per table, grain in the comment.
5. Register COMMENTs and tags in the same change.
6. Validate in dev: expectation failure counts, row counts per layer, and a rerun with no duplicates.
7. Open the PR with the expectation summary and the grain of each gold table.

## Output contract
```
pipeline | new or changed datasets | layer | grain | keys | expectations (drop / warn / fail)
dev run: rows per layer | expectation failures | PR link
```

## References
- references/patterns.md: bronze, silver, gold, naming and the shared-ingest code.
- ../scd2-auto-cdc/SKILL.md: history and deletes.
- ../dynamics-erp-ingest/SKILL.md: the ERP feed.
- ../pipeline-triage/SKILL.md: failed or slow runs.
