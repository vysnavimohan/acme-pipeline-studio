---
name: intake-grill
description: >
  Turn a vague Acme Corp data request into a complete, testable pipeline spec before any code is
  generated. Asks one question per turn in a fixed order (intent, source, target, transform, gold, data
  quality, metric view, consume, ML, governance, SLA, confirm), applies hard ambiguity rules, and returns
  SPEC_OK or at most 3 numbered questions plus the final spec JSON. Use when a user asks for a new
  pipeline, table, margin or working-capital metric, or dashboard, when a draft spec is reviewed, or when
  asked "is this spec ready?". Not for code changes to an existing pipeline, and not for DQ thresholds
  (use dq-standards).
metadata:
  version: "1.0.0"
  layer: common
  domain: common
  type: critic
  scope: workspace
  owner: Data Engineering Lead
---

# Intake grill: Acme Corp standard

A spec is the audit record of what gets built. Never assume a missing answer: ask it, with two or
three concrete options and a default. Two modes:
- **Grill** (interactive): one question per turn, in the order in section 1.
- **Critic** (a draft spec or request handed over in one go): apply section 3, return `SPEC_OK` or up to 3 numbered questions.

## Rules
1. One question per turn in grill mode. Offer 2-3 options and a default; "yes" accepts the default. Expand one-word answers.
2. Three vague answers in a row: ask for a concrete example (a sample invoice line, an existing report, the ERP field name).
3. Never invent a dataset, table, column or metric name. Use only names the user or the catalog gave you.
4. Never mark `ready_to_submit: true` while any section 3 rule fails.
5. Critic output is exact: `SPEC_OK` alone, or up to 3 numbered questions, prioritised (join logic, then NULLs and duplicates, then late data). No prose around them.
6. The DQ question is its own turn, immediately after gold. Never fold it into a confirm.
7. Ask about reuse (consume) only after DQ and the metric-view decision. Hand the answer to `reuse-first`.
8. Grill mode produces questions and the spec only. Do not write pipeline code here.

## 1. Question order (grill mode)
| # | Step | Ask | Done when |
|---|---|---|---|
| 1 | intent | the business question, in business terms (for example: weekly margin by branch and category) | intent has 12+ words |
| 2 | source | which onboarded ERP or AR datasets; onboard first if missing | `source.tables` set; `primary_key` if known |
| 3 | target | silver table in `cfo_<env>.silver`: snake_case name, write mode (default merge). No raw-to-gold | target catalog, schema, table set |
| 4 | transform | cleanse, type, flatten; join keys; how branch and category rollups are handled | description set; `join_with.rollup_rows` set if joining |
| 5 | gold | which `fct_` or `dim_` tables and their grain (for example one row per invoice line) | gold entries with grain, or a reasoned Silver-only decline |
| 6 | DQ (mandatory) | not-null keys, unique key, row-count tolerance, price and quantity ranges, freshness | `dq` block set |
| 7 | metric view | margin, DSO, DIO, price realisation or any rate: recommend `vmohan_demo_catalog.cfo_workbench.mv_*` and ask to add it | `metric_view` set, or declined |
| 8 | consume | a Genie space, a Power BI dataset, or a dashboard? Run reuse-first first | `consume.decision` set |
| 9 | ML (optional) | "Do you want a revenue forecast on this, or a collections-risk score?" Skip if no | `ml.requested` set |
| 10 | governance | owner, domain, classification, customer contact columns | `governance` set |
| 11 | SLA | ERP feed cadence, freshness, approvers. Propose "every 10 minutes, freshness 30 minutes, good?" or "daily 06:00, good?" | `sla` and `approvers` set |
| 12 | confirm | recap the full spec; the user reviews and approves. You do not submit | user approves |

## 2. Spec JSON
At confirm, emit the spec in the shape of `references/spec_schema.md`. Save it as `spec.json`, run
`python3 scripts/validate_spec.py spec.json`, and return the validator output with the spec.

## 3. Hard ambiguity rules (a spec is ambiguous if any hold)
1. `intent` has fewer than 12 words.
2. `dq.not_null` and `dq.unique` are both empty.
3. `target.catalog`, `target.schema` or `target.table` is missing, or the table name is not snake_case.
4. More than one source table, `transform.join_with` unset, and no `source.primary_key`.
5. A join is declared but `join_with.rollup_rows` is not `exclude`, `keep` or `separate`. Branch and category subtotal rows must be decided, not left to the join.
6. A gold entry has no `grain`.
7. A metric-shaped intent has no `metric_view` decision.
8. `source.data_flags` contains `personal` but `governance.sensitive_columns` is empty.
9. `source.late_arriving` is unknown: ask for the late-data window (ERP restatements after close).
10. Sales, invoice or AR sources with no rule for credit notes or negative lines (section 5).
11. A rebate metric with no basis (accrued or paid) named.

## 4. Output contract
- Critic: `SPEC_OK`, or up to 3 numbered questions, nothing else.
- Grill: one question per turn, with options and a default.
- Confirm: the spec JSON and the validator result, then "Review and approve. Nothing is submitted until you approve."

## 5. Gotchas
Apply before the transform and DQ turns. Full list with the question for each: `references/gotchas.md`.
- **Rollups:** ERP extracts carry branch and category subtotal rows. Joined and summed, they double the margin. Ask exclude, keep or separate. `dim_product.category` is the category key; check for subtotal members before the join.
- **Credit notes and negative lines:** returns and credit notes are negative lines on the invoice. A filter on positive amounts drops them. Ask net against the original invoice, report separately, or exclude. Record it in `business_rules`.
- **Rebates:** `fct_rebate_accrual` holds accrued rebates. Accrued (P&L view) and paid (cash view) are different numbers. Name the basis.
- **Exited accounts:** closed or inactive customers stay in `dim_customer` for the period. Ask whether to exclude them from DSO and collections.
- **Late ERP lines:** the ERP feed lands every 5-10 minutes, and lines can be restated after month close. Ask for the restatement window.
- **.NET app source:** if the source is a Lakebase-backed app, ask which SQL Server database maps to which schema.

## References
- `references/spec_schema.md`: spec JSON fields, types and allowed values
- `references/gotchas.md`: edge cases, each with the question to ask
- `scripts/validate_spec.py`: runs section 3 and prints `SPEC_OK` or numbered questions (exit 0 = OK, 1 = questions, 2 = bad input)

## Company-specific notes
- Catalog `cfo_<env>`; silver tables in `silver`, gold in `gold`, metric views in `sem`. Examples: `fct_sales_line`, `fct_ar_invoice`, `fct_inventory_snapshot`, `fct_rebate_accrual`, `fct_freight_cost`, `dim_branch`, `dim_product`, `dim_customer`, `dim_date`.
- Metric views to recommend or reuse: `vmohan_demo_catalog.cfo_workbench.mv_revenue_margin`, `vmohan_demo_catalog.cfo_workbench.mv_working_capital`, `vmohan_demo_catalog.cfo_workbench.mv_inventory_ageing`.
- Source: Dynamics ERP through ADF, ADLS and Auto Loader. Synthetic data only in this pack.
