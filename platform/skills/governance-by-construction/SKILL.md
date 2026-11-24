---
name: governance-by-construction
description: >
  Make every new Your company table compliant when it is created: business comments on tables and columns,
  classification tags, tag-driven masks for customer contact data, and no sensitive values in object,
  job or log names. Use when building or changing a silver or gold table, when a spec declares personal
  columns, or when asked about comments, tags, masking, classification or access. Not for DQ rules (use
  dq-standards) or the promotion path (use dab-promotion).
metadata:
  version: "1.0.0"
  layer: common
  domain: common
  type: standards
  scope: workspace
  owner: Data Governance Lead
---

# Governance by construction: Your company standard

Governance is set in the same change that creates the table, not reviewed afterwards. A table is not
done until its comments, owner, classification and masks are in place and verified.

## Rules
1. Every table has a `COMMENT` that says what one row is, the owner, the source and the refresh cadence.
2. Every silver and gold column has a `COMMENT` with its business meaning. Derived columns state the formula (for example gross margin % = (net sales - standard cost) / net sales).
3. Customer contact columns (name, email, phone, address, contact person) are tagged `contact-pii` in the same change that creates them.
4. Contact PII is masked for every role outside Finance, Credit and Collections and Customer Service. Tag the data, not the grant.
5. No customer contact values in object names, tags, job or pipeline names, or ADF and pipeline logs.
6. Set `classification` from the spec. Customer contact data is `confidential`; margin and pricing are `confidential`; none is `public`.
7. Verify masking as a restricted principal. An admin query proves nothing.

## Steps
1. Read `governance` and `sensitive_columns` from the spec. Confirm each class and treatment.
2. Add table and column comments in the table definition (skeleton in section 3).
3. Set the `contact-pii` tag on the contact columns (section 4).
4. Create or update the mask policy for the tag value.
5. Verify: read the tags from `system.information_schema`, and run the masked query as a restricted principal.
6. Write the checklist (Output contract) into the build record.

## 3. Comment skeleton (SDP table)
```python
from pyspark import pipelines as dp

@dp.table(
    name="silver_customer",
    comment="One row per customer account. Owner: Sales Operations Data Owner. Source: dynamics_erp via ADF. Refresh: 10-minute micro-batch.",
    table_properties={"quality": "silver", "owner": "Sales Operations Data Owner"},
)
def silver_customer():
    ...  # column comments go in the schema DDL: "customer_id BIGINT COMMENT 'Customer account key'"
```

## 4. Tags and masks
- Tag a column: `ALTER TABLE cfo_<env>.silver.<table> ALTER COLUMN <col> SET TAGS ('contact-pii' = 'true');`
- The governed tag policy for the key must exist before a policy can match it. The Data Governance Lead registers it.
- Schema-level mask (covers matching columns in every table in the schema, including new ones):
```sql
CREATE POLICY mask_contact_pii ON SCHEMA vmohan_demo_catalog.cfo_workbench.silver_
  COLUMN MASK vmohan_demo_catalog.cfo_workbench.mask_contact
  TO `account users`
  FOR TABLES MATCH COLUMNS hasTagValue('contact-pii', 'true') AS c
  ON COLUMN c;
```
- `vmohan_demo_catalog.cfo_workbench.mask_contact` returns the value only for group members, for example `RETURN CASE WHEN is_account_group_member('finance-credit') THEN v ELSE NULL END`.
- Confirm the policy grammar against the current release before use.

## Output contract
| Object | Table comment | Column comments | Owner | Classification | Mask policy | Verified as restricted principal |
|---|---|---|---|---|---|---|
| cfo_<env>.<schema>.<table> | yes/no | n of n | role | class | policy name | yes/no |

## References
None. The comment skeleton and policy grammar are in sections 3 and 4.

## Company-specific notes
- Pulse dashboards show customer names for collections work. Restrict the names to the credit team; show account numbers elsewhere.
- ADF pipeline logs print row samples by default. Turn sampling off for any feed that carries contact columns.
- Lakebase copies of contact data carry the same tags; the read replicas inherit the restriction.
