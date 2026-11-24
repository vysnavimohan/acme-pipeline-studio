# Medallion SDP patterns (Python, `pyspark.pipelines`)

Current API only: `from pyspark import pipelines as dp`. Table names below are unqualified; the
pipeline's default catalog and schema (pipeline settings) decide where they land. Landing paths use the `vmohan_demo_catalog` catalog; the pipeline settings set the same catalog.

## 1. Bronze: 1:1 with the landed files, plus audit columns

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = "/Volumes/vmohan_demo_catalog/landing/orders"

@dp.table(
    name="bronze_orders",
    comment="Raw order lines as landed. No business logic. One row per landed record.",
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(LANDING)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

- Keep the `_rescued_data` column (Auto Loader adds it for JSON, CSV and XML). Count its non-null
  rows in a monitoring query; never filter it out in bronze.
- `addNewColumns` stops the stream once when a new field appears, then restarts. Expect one
  failed-then-recovered update per new field; alert on repeated failures only.

## 2. Silver: typed, cleansed, with expectations

```python
@dp.table(name="silver_orders", comment="Typed order lines. Keys validated, dates parsed.")
@dp.expect_or_drop("order_line_key_not_null", "order_id IS NOT NULL AND line_no IS NOT NULL")
@dp.expect_or_drop("order_date_present", "order_date IS NOT NULL")
@dp.expect("amount_non_negative", "net_amount >= 0")
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")
        .select(
            F.col("order_id").cast("string"),
            F.col("line_no").cast("int"),
            F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
            F.col("net_amount").cast("decimal(18,2)"),
            F.upper(F.trim("currency")).alias("currency"),
            F.col("_ingested_at"),
        )
    )
```

- `expect_or_drop` for keys and mandatory dates, `expect` (warn) for value rules you want to
  watch before enforcing, `expect_or_fail` only for rules whose breach makes publishing wrong.
- Do not de-duplicate with `dropDuplicates` on a stream. Changing rows go through AUTO CDC
  (see the scd2-auto-cdc skill).

## 3. Gold: materialized views (batch)

```python
@dp.materialized_view(name="fct_sales_line", comment="Sales fact. Grain = one order line.")
def fct_sales_line():
    return (
        spark.read.table("silver_orders")
        .withColumn("line_key", F.sha2(F.concat_ws("|", "order_id", "line_no"), 256))
        .select("line_key", "order_id", "line_no", "order_date", "net_amount", "currency")
    )
```

- Surrogate keys are a hash of the business key, so they are stable across reruns and rebuilds.
- Gold reads silver with `spark.read.table` (batch). Use a materialized view for aggregates and
  conformed dimensions; use a streaming table only where rows must be appended as they arrive.

## 4. Naming

| Layer | Schema | Table pattern | Example |
|---|---|---|---|
| Bronze | `bronze_<source>` | `<entity>` as landed (snake_case) | `bronze_core.orders` |
| Silver | `silver` | `<entity>` plural, business name | `silver.orders` |
| Gold facts | `gold` | `fct_<process>_<grain>` | `gold.fct_sales_line` |
| Gold dims | `gold` | `dim_<entity>` | `gold.dim_customer` |
| Metric views | `sem` | `mv_<domain>` | `vmohan_demo_catalog.cfo_workbench.mv_revenue_margin` |

Column rules: snake_case; audit columns start with `_`; keys end in `_id` or `_key`; dates end in
`_date`; timestamps end in `_at` or `_ts`.

## 5. Shared-ingest pattern: one pipeline per source, requests merge into it

```python
# One file per source per environment. A new entity is a new dict, not a new pipeline.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_ROOT = "/Volumes/vmohan_demo_catalog/landing/"
ENTITIES = [
    {"name": "orders", "format": "json"},
    {"name": "customers", "format": "json"},
]

def make_bronze(entity):
    @dp.table(name="bronze_" + entity["name"], comment="Raw landed " + entity["name"] + ".")
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", entity["format"])
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(LANDING_ROOT + entity["name"])
            .withColumn("_ingested_at", F.current_timestamp())
        )
    return _bronze

for _entity in ENTITIES:
    make_bronze(_entity)
```

Rules for the pattern:
1. A new request for an existing source adds a row to `ENTITIES` through one PR. It does not create
   a pipeline.
2. Two requests for the same source that arrive together merge into the same open change. Do not
   create parallel pipelines for each requester.
3. Split a source into a second pipeline only for a different cadence or a different failure
   domain, and say why in the PR.
4. The factory function (`make_bronze`) binds each entity at definition time. Do not define
   the decorated functions inside a plain `for` loop without it; late binding loads the last entity
   into every table.

## 6. Pipeline settings checklist (DAB resource)
- Serverless compute on. Catalog and schema set. Tags for cost (`request_id`, `owner`, `cost_centre`).
- Failure notifications to the owner group. Development mode in dev only.
- Configuration keys for paths, not hard-coded per environment.
