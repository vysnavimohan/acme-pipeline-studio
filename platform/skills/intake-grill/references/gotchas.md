# Intake gotchas: Your company

Each entry: the trap, why it changes the build, and the question to ask before the spec is accepted.

## Branch and category rollups (ERP extracts)
- **Trap:** the ERP sales and inventory extracts carry branch and category subtotal rows alongside detail lines. Summed after a join, margin and revenue double-count.
- **Ask:** "Subtotal rows on `<branch | category>`: exclude, keep, or report separately?" Record it as `join_with.rollup_rows`.
- **Check:** `dim_product.category` is the category key. Confirm the subtotal member values with the ERP owner before the join.

## Credit notes and negative lines
- **Trap:** returns and credit notes arrive as negative lines against the original invoice. A `WHERE amount > 0` filter, or a DQ rule that requires positive amounts, drops them silently.
- **Ask:** "Credit notes: net against the original invoice, report as separate lines, or exclude from margin?" Put the answer in `business_rules`.
- **DQ note:** unit prices stay positive. Extended amounts and quantities on credit lines are negative by design.

## Rebates: accrued or paid
- **Trap:** `fct_rebate_accrual` holds accrued rebates. Accrued (P&L) and paid (cash) differ by timing, and supplier rebates may be settled later.
- **Ask:** "Is the rebate metric on accrued (P&L) or paid (cash)? Both?" Name the basis in the metric view description.

## Freight cost allocation
- **Trap:** freight is recorded per shipment, but margin is per invoice line. Allocating it one way or another changes branch margins.
- **Ask:** "Allocate freight per invoice line by value, by weight, or report it above the line?"

## Exited or inactive customers
- **Trap:** closed accounts stay in `dim_customer`. DSO and collections-risk lists that include them overstate the book.
- **Ask:** "Exclude inactive customers from DSO and collections, from the month they closed, or keep them?"

## Late and restated ERP lines
- **Trap:** the ERP feed lands every 5-10 minutes. Lines can be restated after month close, after a figure was reported.
- **Ask:** "How late can lines arrive, and should a closed month be reprocessed when a line is restated?"

## Negative on-hand inventory
- **Trap:** on-hand quantity can go negative in the ERP when stock is issued before receipt is posted.
- **Ask:** "Enforce on-hand >= 0 (fail), warn, or allow negatives and report them?"

## .NET application sources (Lakebase)
- **Trap:** a Lakebase-backed application schema is per SQL Server database. Two databases with the same table names collide without a schema mapping.
- **Ask:** "Which SQL Server database maps to which schema, and is this a read-only copy?"
