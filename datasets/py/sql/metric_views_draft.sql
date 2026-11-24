-- @@STATEMENT
-- Allocation rule (approved by the user): each supplier's monthly rebate accrual (gold_fct_supplier_rebate, grain
-- category x month) is allocated to that category's sales lines in the same invoice month, pro-rata by line COGS.
-- Reported cogs_usd is net of accrued rebates (platform definition), so pre-rebate COGS = cogs_usd + allocated accrual.
-- Rebate data covers 2026-08 to 2026-11 only; lines outside those months get 0 allocation.
CREATE OR REPLACE VIEW {{CATALOG}}.cfo_workbench.gold_sales_line_rebate_alloc
COMMENT 'Sales lines with the monthly supplier rebate accrual allocated pro-rata by line COGS within category and invoice month. SYNTHETIC data.'
AS
WITH l AS (
  SELECT s.*, CAST(DATE_TRUNC('MONTH', s.invoice_date) AS DATE) AS period_month
  FROM {{CATALOG}}.cfo_workbench.gold_fct_sales_line s
),
cm AS (
  SELECT category, period_month, SUM(cogs_usd) AS cogs_month FROM l GROUP BY category, period_month
),
rb AS (
  SELECT category, CAST(period_month AS DATE) AS period_month, SUM(rebate_accrued_usd) AS accrued_month
  FROM {{CATALOG}}.cfo_workbench.gold_fct_supplier_rebate GROUP BY category, period_month
)
SELECT l.*,
       COALESCE(rb.accrued_month * l.cogs_usd / NULLIF(cm.cogs_month, 0), 0) AS rebate_alloc_usd
FROM l
LEFT JOIN cm ON l.category = cm.category AND l.period_month = cm.period_month
LEFT JOIN rb ON l.category = rb.category AND l.period_month = rb.period_month;
-- @@STATEMENT
CREATE OR REPLACE VIEW {{CATALOG}}.cfo_workbench.mv_revenue_margin_draft
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "DRAFT: revenue and gross margin BEFORE supplier rebate accrual (pro-rata allocation by line COGS, approved rule). Extends mv_revenue_margin; not certified. SYNTHETIC data."
source: {{CATALOG}}.cfo_workbench.gold_sales_line_rebate_alloc
joins:
  - name: product
    source: {{CATALOG}}.cfo_workbench.gold_dim_product
    "on": source.product_id = product.product_id
  - name: dim_branch
    source: {{CATALOG}}.cfo_workbench.gold_dim_branch
    "on": source.branch_id = dim_branch.branch_id
dimensions:
  - name: Week Start
    expr: week_start
  - name: Invoice Date
    expr: invoice_date
  - name: Category
    expr: category
  - name: Product
    expr: product.product_name
  - name: Branch
    expr: dim_branch.branch_name
  - name: Region
    expr: dim_branch.region
measures:
  - name: Net Revenue
    expr: SUM(net_revenue_usd)
    comment: "Invoiced revenue after discounts, credit notes netted at source (USD)."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: COGS Reported
    expr: SUM(cogs_usd)
    comment: "COGS as reported, net of accrued supplier rebates (USD)."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Rebate Accrual Allocated
    expr: SUM(rebate_alloc_usd)
    comment: "Monthly supplier rebate accrual per category, allocated to lines pro-rata by line COGS in the same invoice month (approved rule)."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: COGS Pre Rebate
    expr: MEASURE(`COGS Reported`) + MEASURE(`Rebate Accrual Allocated`)
    comment: "COGS before supplier rebate accrual = reported COGS + allocated accrual (USD)."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Gross Margin Pre Rebate USD
    expr: MEASURE(`Net Revenue`) - MEASURE(`COGS Pre Rebate`)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Gross Margin Pre Rebate Pct
    expr: MEASURE(`Gross Margin Pre Rebate USD`) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: "Gross margin before supplier rebate accrual, as a share of net revenue (draft definition)."
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: Gross Margin Reported Pct
    expr: (MEASURE(`Net Revenue`) - MEASURE(`COGS Reported`)) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: "Gross margin as reported (after rebate accrual); same definition as mv_revenue_margin."
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
$$
