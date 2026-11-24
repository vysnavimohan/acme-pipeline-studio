CREATE OR REPLACE VIEW vmohan_demo_catalog.{schema}.mv_margin_leakage
WITH METRICS
LANGUAGE YAML
COMMENT 'Acme Corp margin leakage: discounting, freight and rebate dependency vs plan. Built for REQ-0142 (Margin & Cash). SYNTHETIC data.'
AS $$
version: 1.1

source: |
  SELECT s.*, p.plan_gross_margin_pct, b.branch_name, b.region, c.segment AS customer_segment,
         COALESCE(f.freight_cost_usd * s.net_revenue_usd
                  / NULLIF(SUM(s.net_revenue_usd) OVER (PARTITION BY s.week_start, s.branch_id), 0), 0) AS freight_alloc_usd
  FROM vmohan_demo_catalog.cfo_workbench.gold_sales_line_rebate_alloc s
  LEFT JOIN vmohan_demo_catalog.cfo_workbench.gold_fct_freight f
    ON s.week_start = f.week_start AND s.branch_id = f.branch_id
  LEFT JOIN vmohan_demo_catalog.cfo_workbench.gold_plan_gross_margin p
    ON s.category = p.category
  LEFT JOIN vmohan_demo_catalog.cfo_workbench.gold_dim_branch b
    ON s.branch_id = b.branch_id
  LEFT JOIN vmohan_demo_catalog.cfo_workbench.gold_dim_customer c
    ON s.customer_id = c.customer_id

dimensions:
  - name: Week Start
    expr: week_start
    comment: Monday of the invoice week
  - name: Category
    expr: category
    comment: Product category
  - name: Branch
    expr: branch_name
    comment: Selling branch
  - name: Region
    expr: region
    comment: Branch region
  - name: Customer Segment
    expr: customer_segment
    comment: Customer segment (Trade, Retail, Industrial, Government)

measures:
  - name: Net Revenue
    expr: SUM(net_revenue_usd)
    comment: Invoiced revenue after discounts, USD
  - name: List Revenue
    expr: SUM(quantity * list_unit_price_usd)
    comment: Revenue at list price, USD
  - name: Discount Leakage USD
    expr: MEASURE(`List Revenue`) - MEASURE(`Net Revenue`)
    comment: Revenue given away through discounting (list minus net), USD
  - name: COGS
    expr: SUM(cogs_usd)
    comment: Reported cost of goods sold (net of supplier rebates), USD
  - name: Gross Margin USD
    expr: MEASURE(`Net Revenue`) - MEASURE(`COGS`)
    comment: Net revenue minus reported COGS, USD
  - name: Gross Margin Pct
    expr: MEASURE(`Gross Margin USD`) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: Gross margin as a share of net revenue
  - name: Plan Gross Margin Pct
    expr: SUM(net_revenue_usd * plan_gross_margin_pct / 100) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: FP&A plan gross margin, revenue-weighted
  - name: GM vs Plan Bps
    expr: (MEASURE(`Gross Margin Pct`) - MEASURE(`Plan Gross Margin Pct`)) * 10000
    comment: Gross margin gap to plan in basis points (negative = leaking)
  - name: Rebate Accrual USD
    expr: SUM(rebate_alloc_usd)
    comment: Supplier rebate accrual allocated to the line, USD
  - name: Gross Margin Pre Rebate Pct
    expr: (MEASURE(`Gross Margin USD`) - MEASURE(`Rebate Accrual USD`)) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: Gross margin before supplier rebates (how much margin depends on rebates)
  - name: Freight Cost USD
    expr: SUM(freight_alloc_usd)
    comment: Outbound freight allocated pro-rata to revenue within week and branch, USD
  - name: Contribution Margin USD
    expr: MEASURE(`Gross Margin USD`) - MEASURE(`Freight Cost USD`)
    comment: Gross margin after freight, USD
  - name: Contribution Margin Pct
    expr: MEASURE(`Contribution Margin USD`) / NULLIF(MEASURE(`Net Revenue`), 0)
    comment: Contribution margin as a share of net revenue
$$
