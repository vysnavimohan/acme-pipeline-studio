# SYNTHETIC ERP feed -> medallion (Lakeflow Spark Declarative Pipelines, serverless).
# Catalog/schema come from pipeline settings; landing root from configuration key `landing_root`.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_root")


def _landed(entity):
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{LANDING}/{entity}")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path")))


# ---------- bronze: Auto Loader from the landing volume ----------
@dp.table(name="bronze_sales_invoice_lines", comment="Raw sales invoice lines as landed (weekly extracts).")
def bronze_sales_invoice_lines():
    return _landed("sales_invoice_lines")


@dp.table(name="bronze_ar_transactions", comment="AR invoice snapshot as landed at the extract cut-off.")
def bronze_ar_transactions():
    return _landed("ar_transactions")


@dp.table(name="bronze_inventory_on_hand", comment="Inventory on hand snapshot as landed.")
def bronze_inventory_on_hand():
    return _landed("inventory_on_hand")


@dp.table(name="bronze_customers", comment="Customer master as landed (feeds SCD2 history).")
def bronze_customers():
    return _landed("dim_customer")


@dp.table(name="bronze_products", comment="Product master as landed.")
def bronze_products():
    return _landed("dim_product")


@dp.table(name="bronze_branches", comment="Branch master as landed.")
def bronze_branches():
    return _landed("dim_branch")


@dp.table(name="bronze_freight_charges", comment="Weekly freight charges by branch as landed.")
def bronze_freight_charges():
    return _landed("freight_charges")


@dp.table(name="bronze_fpa_plan", comment="FP&A plan gross margin % by category and month.")
def bronze_fpa_plan():
    return _landed("fpa_plan")


@dp.table(name="bronze_supplier_rebates", comment="Supplier rebate accrual and receipts, monthly.")
def bronze_supplier_rebates():
    return _landed("supplier_rebates")


# ---------- silver: typed, validated, PII-tagged ----------
@dp.table(
    name="silver_sales_invoice_line",
    comment="Validated sales invoice lines. Credit notes netted at source. Contact data not in this table.",
    cluster_by=["invoice_date", "branch_id"],
)
@dp.expect_all_or_drop({
    "invoice_id_not_null": "invoice_id IS NOT NULL",
    "line_no_not_null": "line_no IS NOT NULL",
    "branch_id_not_null": "branch_id IS NOT NULL",
    "product_id_not_null": "product_id IS NOT NULL",
    "quantity_positive": "quantity > 0",
    "list_price_non_negative": "list_unit_price_usd >= 0",
})
@dp.expect("gm_pct_in_range", "net_revenue_usd = 0 OR (1 - cogs_usd / net_revenue_usd) BETWEEN -0.5 AND 0.9")
def silver_sales_invoice_line():
    return (spark.readStream.table("bronze_sales_invoice_lines")
            .withColumn("invoice_date", F.to_date("invoice_date"))
            .withColumn("week_start", F.date_sub(F.col("invoice_date"), F.pmod(F.dayofweek("invoice_date") + 5, F.lit(7))))
            .dropDuplicates(["invoice_id", "line_no"]))


dp.create_streaming_table(
    name="silver_ar_transaction",
    comment="AR invoice balances at the cut-off. Open balance = amount not yet paid.",
    cluster_by=["customer_id"],
)
dp.create_auto_cdc_flow(
    target="silver_ar_transaction",
    source="bronze_ar_transactions",
    keys=["invoice_id"],
    sequence_by=F.col("_ingested_at"),
    stored_as_scd_type=1,
)


@dp.table(name="silver_inventory_on_hand", comment="Inventory on hand by branch and SKU at the snapshot date.")
@dp.expect_all_or_drop({"qty_non_negative": "quantity_on_hand >= 0", "cost_non_negative": "unit_cost_usd >= 0"})
def silver_inventory_on_hand():
    return (spark.read.table("bronze_inventory_on_hand")
            .withColumn("last_receipt_date", F.to_date("last_receipt_date"))
            .withColumn("snapshot_date", F.to_date("snapshot_date")))


# SCD Type 2 customer history from the Dataverse-style change feed. Order is the ERP change order
# (versionnumber, never ingest time). Rows with is_deleted = true close the customer's history.
dp.create_streaming_table(
    name="silver_customer_hist",
    comment="Customer dimension with full SCD Type 2 history (__START_AT / __END_AT), ordered by versionnumber.",
)
dp.create_auto_cdc_flow(
    target="silver_customer_hist",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=F.col("versionnumber"),
    apply_as_deletes=F.expr("is_deleted = true"),
    stored_as_scd_type=2,
    track_history_column_list=["segment", "industry_segment", "payment_terms_days", "credit_limit_usd", "account_manager"],
)


# ---------- gold: facts and conformed dimensions ----------
@dp.materialized_view(name="gold_fct_sales_line", comment="Fact: one row per sales invoice line (line x day).", cluster_by=["invoice_date"])
def gold_fct_sales_line():
    return spark.read.table("silver_sales_invoice_line").select(
        "invoice_id", "line_no", "invoice_date", "week_start", "customer_id", "branch_id", "product_id",
        "category", "quantity", "list_unit_price_usd", "discount_pct", "net_revenue_usd", "cogs_usd", "currency")


@dp.materialized_view(name="gold_fct_ar_invoice", comment="Fact: one row per AR invoice with open balance at the cut-off.")
def gold_fct_ar_invoice():
    return spark.read.table("silver_ar_transaction").select(
        "invoice_id", "customer_id", "invoice_date", "due_date", "invoice_amount_usd", "paid_amount_usd",
        "paid_date", "open_balance_usd", "as_of_date", "dispute_date")


@dp.materialized_view(name="gold_fct_inventory_snapshot", comment="Fact: inventory on hand value by branch and SKU, with age.")
def gold_fct_inventory_snapshot():
    return (spark.read.table("silver_inventory_on_hand")
            .withColumn("inventory_value_usd", F.round(F.col("quantity_on_hand") * F.col("unit_cost_usd"), 2))
            .withColumn("age_days", F.datediff("snapshot_date", "last_receipt_date")))


@dp.materialized_view(name="gold_dim_product", comment="Conformed product dimension.")
def gold_dim_product():
    return spark.read.table("bronze_products").select("product_id", "sku", "product_name", "category", "unit_cost_usd", "list_price_usd")


@dp.materialized_view(name="gold_dim_branch", comment="Conformed branch dimension.")
def gold_dim_branch():
    return spark.read.table("bronze_branches").select("branch_id", "branch_name", "region")


@dp.materialized_view(name="gold_fct_freight", comment="Fact: weekly freight charges by branch.")
def gold_fct_freight():
    return spark.read.table("bronze_freight_charges").select("week_start", "branch_id", "freight_cost_usd")


@dp.materialized_view(name="gold_fct_supplier_rebate", comment="Fact: supplier rebate accrued and received by month.")
def gold_fct_supplier_rebate():
    return spark.read.table("bronze_supplier_rebates").select("supplier_id", "category", "period_month", "rebate_accrued_usd", "rebate_received_usd")


@dp.materialized_view(name="gold_plan_gross_margin", comment="FP&A plan gross margin % by category and month.")
def gold_plan_gross_margin():
    return spark.read.table("bronze_fpa_plan").select("period_month", "category", "plan_gross_margin_pct")


@dp.materialized_view(name="gold_dim_customer", comment="Conformed customer dimension: current SCD2 row per customer.")
def gold_dim_customer():
    return (spark.read.table("silver_customer_hist")
            .where(F.col("__END_AT").isNull())
            .select("customer_id", "customer_name", "segment", "industry_segment", "account_manager",
                    "payment_terms_days", "credit_limit_usd"))


@dp.materialized_view(name="gold_customer_rank", comment="Account ranking by trailing 90-day invoiced revenue; top_200 flag used for collections exposure.")
def gold_customer_rank():
    rev = (spark.read.table("gold_fct_ar_invoice")
           .where(F.col("invoice_date").between(F.date_sub(F.lit("2026-11-15").cast("date"), 89), F.lit("2026-11-15").cast("date")))
           .groupBy("customer_id").agg(F.sum("invoice_amount_usd").alias("revenue_90d_usd")))
    from pyspark.sql.window import Window
    w = Window.orderBy(F.desc("revenue_90d_usd"))
    return rev.withColumn("account_rank", F.row_number().over(w)).withColumn("is_top_200", F.col("account_rank") <= 200)
