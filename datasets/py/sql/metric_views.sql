-- @@STATEMENT
CREATE OR REPLACE VIEW {{CATALOG}}.cfo_workbench.mv_revenue_margin
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Revenue, gross margin and price realisation by week, category, branch. SYNTHETIC data."
source: {{CATALOG}}.cfo_workbench.gold_fct_sales_line
joins:
  - name: product
    source: {{CATALOG}}.cfo_workbench.gold_dim_product
    "on": source.product_id = product.product_id
  - name: dim_branch
    source: {{CATALOG}}.cfo_workbench.gold_dim_branch
    "on": source.branch_id = dim_branch.branch_id
  - name: plan
    source: {{CATALOG}}.cfo_workbench.gold_plan_gross_margin
    "on": source.category = plan.category
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
  - name: COGS
    expr: SUM(cogs_usd)
    comment: "Cost of goods sold, net of accrued supplier rebates (USD)."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Gross Margin USD
    expr: MEASURE(`Net Revenue`) - MEASURE(`COGS`)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Gross Margin Pct
    expr: MEASURE(`Gross Margin USD`) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: "Gross margin as a share of net revenue."
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: List Revenue
    expr: SUM(quantity * list_unit_price_usd)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Price Realisation Pct
    expr: MEASURE(`Net Revenue`) / NULLIF(MEASURE(`List Revenue`), 0)
    comment: "Net revenue as a share of list-price revenue."
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: Invoice Lines
    expr: COUNT(1)
  - name: Plan Gross Margin USD
    expr: SUM(net_revenue_usd * plan.plan_gross_margin_pct / 100)
    comment: "Plan gross margin in USD: net revenue x plan GM % for the category (scenario PLAN)."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Plan Gross Margin Pct
    expr: MEASURE(`Plan Gross Margin USD`) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: "Plan gross margin as a share of net revenue (scenario PLAN)."
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
  - name: GM vs Plan Bps
    expr: (MEASURE(`Gross Margin Pct`) - MEASURE(`Plan Gross Margin Pct`)) * 10000
    comment: "Gross margin minus plan gross margin, in basis points. Negative = below plan."
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
$$
-- @@STATEMENT
CREATE OR REPLACE VIEW {{CATALOG}}.cfo_workbench.mv_working_capital
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Accounts receivable, DSO and collections exposure as of the extract cut-off. SYNTHETIC data."
source: {{CATALOG}}.cfo_workbench.gold_fct_ar_invoice
joins:
  - name: dim_customer
    source: {{CATALOG}}.cfo_workbench.gold_dim_customer
    "on": source.customer_id = dim_customer.customer_id
  - name: cust_rank
    source: {{CATALOG}}.cfo_workbench.gold_customer_rank
    "on": source.customer_id = cust_rank.customer_id
dimensions:
  - name: As Of Date
    expr: as_of_date
  - name: Customer
    expr: dim_customer.customer_name
  - name: Segment
    expr: dim_customer.segment
  - name: Top 200 Account
    expr: cust_rank.is_top_200
measures:
  - name: Open AR
    expr: SUM(open_balance_usd)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Billed Last 90 Days
    expr: SUM(CASE WHEN invoice_date BETWEEN DATE_SUB(as_of_date, 89) AND as_of_date THEN invoice_amount_usd END)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: DSO Days
    expr: MEASURE(`Open AR`) / NULLIF(MEASURE(`Billed Last 90 Days`) / 90, 0)
    comment: "Days sales outstanding: open AR / average daily billing over the last 90 days."
    format:
      type: number
      decimal_places:
        type: exact
        places: 1
  - name: Past Due Over 30 Days
    expr: SUM(CASE WHEN DATEDIFF(as_of_date, due_date) > 30 THEN open_balance_usd END)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Collections Exposure Top 200
    expr: SUM(CASE WHEN DATEDIFF(as_of_date, due_date) > 30 AND cust_rank.is_top_200 THEN open_balance_usd END)
    comment: "Open balance more than 30 days past due for the top-200 accounts ranked by trailing 90-day revenue."
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
$$
-- @@STATEMENT
CREATE OR REPLACE VIEW {{CATALOG}}.cfo_workbench.mv_inventory_ageing
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Inventory value on hand and ageing by branch and category. SYNTHETIC data."
source: {{CATALOG}}.cfo_workbench.gold_fct_inventory_snapshot
joins:
  - name: product
    source: {{CATALOG}}.cfo_workbench.gold_dim_product
    "on": source.product_id = product.product_id
  - name: dim_branch
    source: {{CATALOG}}.cfo_workbench.gold_dim_branch
    "on": source.branch_id = dim_branch.branch_id
dimensions:
  - name: Snapshot Date
    expr: snapshot_date
  - name: Category
    expr: product.category
  - name: Branch
    expr: dim_branch.branch_name
  - name: Age Band
    expr: CASE WHEN age_days <= 90 THEN '0-90 days' WHEN age_days <= 180 THEN '91-180 days' ELSE 'Over 180 days' END
measures:
  - name: Inventory Value
    expr: SUM(inventory_value_usd)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Inventory Over 180 Days
    expr: SUM(CASE WHEN age_days > 180 THEN inventory_value_usd END)
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 0
  - name: Aged Share Pct
    expr: MEASURE(`Inventory Over 180 Days`) / NULLIF(MEASURE(`Inventory Value`), 0)
    format:
      type: percentage
      decimal_places:
        type: exact
        places: 1
$$
