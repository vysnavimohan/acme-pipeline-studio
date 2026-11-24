---
name: dq-standards
description: >
  Data-quality standards for Acme Corp silver and gold tables: which checks use fail, drop or warn; how
  not-null and uniqueness are enforced; row-count tolerance, price and quantity ranges, and freshness
  against the ERP feed; and how expectations are generated from the spec. Use when a spec has a DQ block,
  when writing SDP expectations, or when asked what DQ rule to use. Not for masking (use
  governance-by-construction) or for pipeline failure diagnosis (use pipeline-triage).
metadata:
  version: "1.0.0"
  layer: common
  domain: common
  type: standards
  scope: workspace
  owner: Data Engineering Lead
---

# DQ standards: Acme Corp

Every silver table is gated by data-quality rules that come from the approved spec. Pick the action by
what the failure means. A check that is not in the spec does not exist.

## Rules
1. Every silver primary-key column has a not-null expectation (`fail`) and a uniqueness test (dataset check, section 2).
2. Every silver table has a row-count tolerance against the last successful run. Default 5% (illustrative; the Data Engineering Lead sets it).
3. Generate expectations from the spec's `dq` block with `scripts/expectations_from_spec.py`. Do not hand-write them.
4. Choose the action by meaning: `fail` for broken keys or structure; `drop` for a bad row that is quarantined; `warn` for a soft signal that is still valid data.
5. Every value range cites its source (a business rule or a documented limit).
6. A drop is never silent: drop counts are visible in the pipeline event log, and a drop rate above 1% (illustrative) raises an alert.
7. No `display()` or `.show()` in pipeline code. They break automated runs.
8. Freshness is checked against the feed cadence in the spec. A stale ERP feed is a DQ failure, not a warning.

## 1. Action taxonomy
| Action | SDP form | Use for | Example |
|---|---|---|---|
| fail | `@dp.expect_all_or_fail({...})` | keys and structure | `invoice_line_id IS NOT NULL` |
| drop | `@dp.expect_all_or_drop({...})` | a bad row that should not reach silver | `unit_price > 0` |
| warn | `@dp.expect_all({...})` | a valid but unusual value to watch | `discount_pct BETWEEN 0 AND 0.6` |

## 2. Dataset-level checks (not row expectations)
| Check | How | Action on breach |
|---|---|---|
| Uniqueness on the key | `GROUP BY <keys> HAVING COUNT(*) > 1` returns zero rows | fail the run |
| Row-count tolerance | compare with the last successful run in `ops` | fail if outside tolerance |
| Freshness | `max(<load timestamp>)` against the feed cadence | fail if older than the SLA |
| Control totals | silver net sales reconciles to the ERP control extract | fail if they differ |

## Steps
1. Read the `dq` block from the spec.
2. Run `python3 scripts/expectations_from_spec.py spec.json` and paste the Python output into the silver definition.
3. Add the section 2 checks to the job as tasks after the load.
4. Run the pipeline in `vmohan_demo_catalog`. Confirm drop counts, and that the uniqueness test returns zero rows.
5. Record the DQ table (Output contract) in the build record.

## Output contract
| Check | Kind | Columns or rule | Action | Threshold | Source of threshold |
|---|---|---|---|---|---|
| name | expectation / dataset | columns or expression | fail / drop / warn | value | spec field or rule |

## References
None. The generator is `scripts/expectations_from_spec.py`.

## Company-specific notes
- Unit prices and standard costs are positive. Credit lines carry negative extended amounts and quantities by design, so do not apply a positive-amount rule to them.
- On-hand quantity can be negative in the ERP. Ask before enforcing a floor of zero.
- Freshness for the ERP feed: 30 minutes against a 5-10 minute cadence (illustrative; the Data Engineering Lead sets it).
- Customer master: one row per account; duplicates may be merged accounts, so ask before dropping.
