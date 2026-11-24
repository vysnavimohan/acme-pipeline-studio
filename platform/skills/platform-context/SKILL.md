---
name: platform-context
description: >
  Acme Corp platform facts: the vmohan_demo_catalog.cfo_workbench location, schema and naming rules, serverless-only
  compute, the ERP and legacy-warehouse sources, Power BI and Lakebase placement, and owners. Use when
  creating or moving any Databricks object for Acme Corp, when asked where something should go, which
  catalog, schema or compute to use, or who owns a dataset. Not for business definitions (use
  business-glossary) or governance rules (use governance-by-construction).
metadata:
  version: "1.0.0"
  layer: common
  domain: common
  type: context
  scope: workspace
  owner: Platform Lead
---

# Acme Corp platform context

Facts the workspace cannot tell you. Read the relevant section before creating, moving or naming any
object, so the build lands in the right place the first time. Names here are synthetic (show-and-tell).

## Rules
1. Use only the catalog and schema in section 1. Never create a catalog; ask the Platform Lead.
2. Demo build: one catalog and one schema for every environment. Dev, test and prod point at the same location, so promotion means changing the CATALOG and SCHEMA config values and re-running bootstrap, not copying objects.
3. Serverless only: jobs, pipelines and SQL warehouses. Never create a classic cluster. A build that needs one stops and asks.
4. Use only the approved Azure services in section 4. A new service needs the Platform Lead first.
5. Every new dataset has a named owner (section 5). No owner, no build.
6. Store only facts the workspace cannot show. Do not copy table schemas or row counts into this file.

## 1. Catalog and schema
| Item | Value |
|---|---|
| Catalog | `vmohan_demo_catalog` (read from the config, never hard-coded) |
| Schema | `cfo_workbench` |
| Environments | dev, test and prod all point here in the demo |

## 2. Table layers (one schema, layer-prefixed names)
| Prefix | Holds | Rule |
|---|---|---|
| `raw_erp` (volume) | landing files per source | no business logic |
| `bronze_<source>` | raw landing per source, 1:1 with the file or feed | no business logic |
| `silver_<entity>` | cleansed, typed, keyed tables | DQ expectations required |
| `gold_<fct or dim>` | facts and dimensions at a stated grain | grain in the table comment |
| `mv_<domain>` | governed metric views | one definition per metric |
| `feat_<entity>_<grain>` | point-in-time feature tables | owner tag and comments |
| `ml_<output>`, `eval_<evidence>`, `brief_<output>` | model outputs; evaluation and governance evidence; published briefs | written by ML and brief jobs only |
| `ops_<log>` | run logs, DQ results, build records | append only |

Gold domains: sales and margin (`gold_fct_sales_line`, `gold_dim_product` with `category`, `gold_dim_branch`, `gold_dim_customer`); working capital (`gold_fct_ar_invoice`); inventory (`gold_fct_inventory_snapshot`); rebates (`gold_fct_supplier_rebate`); freight (`gold_fct_freight`); plan (`gold_plan_gross_margin`); conformed `gold_dim_date` (week start on the sales line).

## 3. Naming
- snake_case for catalogs, schemas, tables and columns. No spaces. No customer or person names in any object name.
- Prefixes: `fct_` facts, `dim_` dimensions, `mv_` metric views, `ml_` predictions. The layer prefix (`silver_`, `gold_`, `ml_`, `eval_`) is added to the object name because all layers share one schema.
- Source names come from section 6.

## 4. Compute, sources and BI
- Compute: serverless jobs, pipelines and SQL warehouses. No classic clusters.
- Ingestion: ERP feed lands via ADF, into ADLS, read by Auto Loader. The feed runs about every 5-10 minutes; the ERP is moving off Synapse Spark.
- Legacy: an on-premises SQL Server data warehouse is being decommissioned into Databricks. Parity checks run before any cut-over.
- BI: Power BI stays the primary BI surface, on Databricks SQL over the metric views (`mv_*`). Confirm metric-view support in the Power BI version in use before relying on it.
- Lakeflow Connect is a later phase; do not design for it now.
- .NET applications: Lakebase, one Lakebase database for the application estate, one schema per SQL Server database, read replicas for read scaling. Cross-database joins belong in Databricks SQL, not in Lakebase.
- Approved Azure services: the set listed in the Azure subscription standard held by the Platform Lead.

## 5. Owners (role titles)
| Area | Owner |
|---|---|
| Platform, compute and promotion | Platform Lead |
| Margin and pricing data | Finance Data Owner |
| Working capital and collections | Credit and Collections Data Owner |
| Sales and customer master | Sales Operations Data Owner |
| Customer contact data (PII) | Customer Data Steward |

## 6. Source systems
| Logical name | Pattern | Landing |
|---|---|---|
| `dynamics_erp` | ADF, ADLS, Auto Loader, micro-batch | `bronze_dynamics_erp` |
| `legacy_sql_dw` | migration to Databricks; reconciled by parity checks | `bronze_legacy_sql_dw` |
| `supplier_rebates` | scheduled file load | `bronze_supplier_rebates` |
| `freight` | scheduled file load | `bronze_freight` |

## Steps
1. Read the sections that match the request (target, compute, owner, source).
2. If the target is not listed, stop and ask. Do not invent a name.
3. Write the placement into the intake spec (`target`, `governance.owner`).

## Output contract
```
Placement: <catalog>.cfo_workbench.<layer_prefixed_table> | compute: serverless | owner: <role>
```

## References
None. The facts are in the sections above.
