# Intake spec schema (Your company)

The spec is one JSON object. Every key is present; use `null`, `[]` or `false` when unknown, and
let the critic ask about it. Save as `spec.json`.

```json
{
  "intent": "string, 12+ words, business terms",
  "request_id": "string, opaque request reference (used for cost tags; no names)",
  "source": {
    "system": "dynamics_erp | legacy_sql_dw | supplier_rebates | freight",
    "tables": ["onboarded dataset name"],
    "primary_key": ["column"],
    "data_flags": ["personal", "rollup_rows_possible", "credit_notes_possible"],
    "late_arriving": "true | false | unknown"
  },
  "target": {
    "catalog": "vmohan_demo_catalog | vmohan_demo_catalog | vmohan_demo_catalog",
    "schema": "silver",
    "table": "snake_case_name",
    "write_mode": "merge | append | overwrite"
  },
  "transform": {
    "description": "plain-English cleanse, typing and flattening",
    "join_with": {"on": ["column"], "how": "left | inner", "rollup_rows": "exclude | keep | separate"},
    "business_rules": ["one rule per item, including credit-note treatment and rebate basis"]
  },
  "gold": [
    {"name": "snake_case", "intent": "string", "grain": "column, column, period"}
  ],
  "dq": {
    "not_null": ["column"],
    "unique": ["column"],
    "row_count_tolerance_pct": 5.0,
    "ranges": [{"column": "string", "min": "number or null", "max": "number or null", "action": "fail | drop | warn"}],
    "freshness_minutes": 30
  },
  "metric_view": {
    "name": "cfo_<env>.vmohan_demo_catalog.cfo_workbench.mv_name",
    "measures": ["string"],
    "dimensions": ["string"]
  },
  "consume": {
    "genie": false,
    "dashboard": false,
    "reuse_coverage_pct": null,
    "decision": "reuse | build | n/a"
  },
  "ml": {
    "requested": false,
    "type": "forecast | risk | anomaly | null",
    "grain": "branch, category, week | account, week",
    "horizon": "13 weeks | n/a"
  },
  "governance": {
    "owner": "Finance Data Owner | role",
    "domain": "sales-and-margin | working-capital | inventory | procurement",
    "classification": "public | internal | confidential | restricted",
    "sensitive_columns": [{"column": "string", "class": "contact-pii | financial | none", "treatment": "mask | hash | drop | none"}]
  },
  "sla": {
    "mode": "batch | streaming",
    "schedule_cron": "*/10 * * * * (ERP feed) | 0 6 * * * (daily)",
    "freshness_minutes": 30
  },
  "approvers": ["team or role"],
  "ready_to_submit": false
}
```

## Field notes
- `request_id`: the finops tag value. Standing feeds use a standing id (for example `STANDING-ERP-01`).
- `join_with.rollup_rows`: required whenever a join is declared (critic rule 5).
- `metric_view` is `null` for non-metric intents; for margin, DSO, DIO or price realisation it must be present or declined with a reason.
- `ml.grain` and `ml.horizon` apply to the revenue forecast (branch x category x week, 13-week horizon) and the collections-risk score (account x week).
