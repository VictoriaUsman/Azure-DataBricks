# Databricks notebook source
# MAGIC %md
# MAGIC # Warehouse Layer — Star Schema
# MAGIC
# MAGIC **Purpose:** Build a classic star schema on top of Gold/Silver tables, ready for
# MAGIC Power BI or any BI tool. This layer also doubles as the Azure Synapse / Azure SQL
# MAGIC equivalent in a full cloud deployment.
# MAGIC
# MAGIC **Schema:**
# MAGIC ```
# MAGIC                    ┌───────────────┐
# MAGIC                    │  dim_date     │
# MAGIC                    └───────┬───────┘
# MAGIC                            │
# MAGIC ┌──────────┐    ┌──────────┴──────────┐    ┌────────────┐
# MAGIC │ dim_store│────│   fact_sales        │────│ dim_product│
# MAGIC └──────────┘    └──────────┬──────────┘    └────────────┘
# MAGIC                            │
# MAGIC                    ┌───────┴───────┐
# MAGIC                    │ dim_customer  │
# MAGIC                    └───────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

DATABASE  = "retail_platform"
GOLD_PATH = "dbfs:/retail_platform/gold"
DW_PATH   = "dbfs:/retail_platform/gold/warehouse"

spark.sql(f"USE {DATABASE}")
logger = PipelineLogger("warehouse_star_schema")

# COMMAND ----------

def save_dim(df, name: str) -> None:
    path = f"{DW_PATH}/{name}"
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema","true").save(path))
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{name} USING DELTA LOCATION '{path}'")
    n = df.count()
    logger.info(f"Saved {name}", rows_written=n)
    print(f"  {name}: {n:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Date

# COMMAND ----------

def build_dim_date():
    """
    Generate a full date dimension for 2023-2024.
    In production this would be a permanent table rebuilt annually.
    """
    t0 = time.time()

    dates = spark.sql("""
        SELECT
            CAST(date_format(d, 'yyyyMMdd') AS INT)  AS date_key,
            d                                         AS full_date,
            YEAR(d)                                   AS year,
            QUARTER(d)                                AS quarter,
            MONTH(d)                                  AS month,
            date_format(d, 'MMMM')                   AS month_name,
            date_format(d, 'MMM')                    AS month_abbr,
            WEEKOFYEAR(d)                             AS week_of_year,
            DAYOFMONTH(d)                             AS day_of_month,
            DAYOFWEEK(d)                              AS day_of_week,
            date_format(d, 'EEEE')                   AS day_name,
            CASE WHEN DAYOFWEEK(d) IN (1,7) THEN 1 ELSE 0 END AS is_weekend,
            date_format(d, 'yyyy-MM')                AS year_month,
            CONCAT('Q', QUARTER(d), '-', YEAR(d))   AS quarter_label
        FROM (
            SELECT explode(
                sequence(DATE '2023-01-01', DATE '2024-12-31', INTERVAL 1 DAY)
            ) AS d
        )
    """)

    save_dim(dates, "dim_date")
    logger.info("dim_date complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Customer

# COMMAND ----------

def build_dim_customer():
    t0   = time.time()
    base = spark.table("silver_customers")
    rfm  = spark.table("gold_customer_summary").select(
        "customer_id", "customer_tier", "lifetime_value", "purchase_frequency", "recency_days"
    )

    df = (
        base
        .join(rfm, "customer_id", "left")
        .select(
            F.monotonically_increasing_id().alias("customer_key"),  # surrogate key
            "customer_id", "customer_name", "email",
            "city", "segment",
            F.coalesce("customer_tier", F.lit("Unknown")).alias("customer_tier"),
            F.coalesce("lifetime_value", F.lit(0.0)).alias("lifetime_value"),
            F.coalesce("purchase_frequency", F.lit(0)).alias("purchase_frequency"),
        )
    )

    save_dim(df, "dim_customer")
    logger.info("dim_customer complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Product

# COMMAND ----------

def build_dim_product():
    t0   = time.time()
    perf = spark.table("gold_product_performance").select(
        "product_id", "return_rate", "unique_buyers", "category_rank"
    )

    df = (
        spark.table("silver_products")
        .join(perf, "product_id", "left")
        .select(
            F.monotonically_increasing_id().alias("product_key"),
            "product_id", "product_name", "category", "subcategory", "unit_price",
            F.coalesce("return_rate",   F.lit(0.0)).alias("return_rate"),
            F.coalesce("category_rank", F.lit(999)).alias("category_rank"),
        )
    )

    save_dim(df, "dim_product")
    logger.info("dim_product complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension: Store

# COMMAND ----------

def build_dim_store():
    t0 = time.time()

    df = (
        spark.table("silver_stores")
        .select(
            F.monotonically_increasing_id().alias("store_key"),
            "store_id", "store_name", "city", "region", "country",
        )
    )

    save_dim(df, "dim_store")
    logger.info("dim_store complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact: Sales

# COMMAND ----------

def build_fact_sales():
    """
    Grain: one row per completed/pending transaction.
    Returns are stored with negative amounts so aggregates net correctly.
    All foreign keys use the natural business keys (not surrogates) for
    simplicity on Community Edition — in production join to dim tables for
    surrogate keys.
    """
    t0 = time.time()

    # Bring in dim keys
    dim_customer = spark.table("dim_customer").select("customer_key", "customer_id")
    dim_product  = spark.table("dim_product") .select("product_key",  "product_id")
    dim_store    = spark.table("dim_store")   .select("store_key",    "store_id")

    fact = (
        spark.table("silver_transactions")
        .join(dim_customer, "customer_id", "left")
        .join(dim_product,  "product_id",  "left")
        .join(dim_store,    "store_id",    "left")
        .withColumn(
            "date_key",
            F.col("transaction_date").cast("string").regexp_replace("-","").cast(IntegerType())
        )
        # Sign revenue negatively for returns
        .withColumn("net_revenue",
            F.when(F.col("status") == "returned", -F.col("revenue_net"))
             .otherwise(F.col("revenue_net"))
        )
        .select(
            "transaction_id",
            "date_key", "customer_key", "product_key", "store_key",
            "transaction_date",
            "quantity", "unit_price", "discount",
            "total_amount", "net_revenue",
            "status", "is_returned",
            "year", "month", "year_month",
        )
    )

    path = f"{DW_PATH}/fact_sales"
    (
        fact.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .option("overwriteSchema", "true")
        .option("delta.enableChangeDataFeed", "true")
        .save(path)
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.fact_sales USING DELTA LOCATION '{path}'")

    n = fact.count()
    logger.info("fact_sales complete", rows_written=n, duration_ms=int((time.time()-t0)*1000))
    print(f"  fact_sales: {n:,} rows")

# COMMAND ----------

print("=" * 60)
print("STAR SCHEMA BUILD — START")
print("=" * 60)

build_dim_date()
build_dim_customer()
build_dim_product()
build_dim_store()
build_fact_sales()

print("\nSTAR SCHEMA BUILD — COMPLETE")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize fact table — biggest performance win
# MAGIC OPTIMIZE retail_platform.fact_sales ZORDER BY (customer_key, product_key, date_key);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify foreign key integrity (should return 0 orphan rows)
# MAGIC SELECT 'orphan_customers' AS check_name,
# MAGIC        COUNT(*) AS orphan_count
# MAGIC FROM   retail_platform.fact_sales f
# MAGIC LEFT JOIN retail_platform.dim_customer d ON f.customer_key = d.customer_key
# MAGIC WHERE  d.customer_key IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'orphan_products', COUNT(*)
# MAGIC FROM   retail_platform.fact_sales f
# MAGIC LEFT JOIN retail_platform.dim_product d ON f.product_key = d.product_key
# MAGIC WHERE  d.product_key IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'orphan_stores', COUNT(*)
# MAGIC FROM   retail_platform.fact_sales f
# MAGIC LEFT JOIN retail_platform.dim_store d ON f.store_key = d.store_key
# MAGIC WHERE  d.store_key IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total revenue by year (quick sanity check)
# MAGIC SELECT   year,
# MAGIC          FORMAT_NUMBER(SUM(net_revenue), 2) AS total_revenue,
# MAGIC          COUNT(*)                           AS transactions
# MAGIC FROM     retail_platform.fact_sales
# MAGIC GROUP BY year
# MAGIC ORDER BY year
