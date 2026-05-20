# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Business Aggregates
# MAGIC
# MAGIC **Purpose:** Build pre-aggregated, business-ready tables from Silver.
# MAGIC Gold tables are optimized for BI queries — small, fast, named after business concepts.
# MAGIC
# MAGIC **Tables created:**
# MAGIC | Table                          | Grain                   | Use case              |
# MAGIC |--------------------------------|-------------------------|-----------------------|
# MAGIC | `gold_sales_daily`             | store × product × day   | Daily P&L, trending   |
# MAGIC | `gold_sales_monthly`           | category × month        | Exec dashboard        |
# MAGIC | `gold_customer_summary`        | customer (all-time)     | Segmentation, RFM     |
# MAGIC | `gold_product_performance`     | product (all-time)      | Assortment analysis   |
# MAGIC | `gold_store_performance`       | store × month           | Regional ops          |

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

import re as _re
import time
from pyspark.sql import functions as F, Window

DATABASE  = "retail_platform"
GOLD_PATH = "dbfs:/retail_platform/gold"
BATCH_ID  = dbutils.widgets.get("batch_id") if "batch_id" in [w.name for w in dbutils.widgets.getAll()] else "manual_run"

if not _re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', BATCH_ID):
    raise ValueError(f"Invalid batch_id: '{BATCH_ID}'. Must match [a-zA-Z0-9_-]{{1,64}}")

spark.sql(f"USE {DATABASE}")

logger = PipelineLogger("gold_aggregation", run_id=BATCH_ID)

# COMMAND ----------

# ── Helper ────────────────────────────────────────────────────────────────────

def save_gold(df, table_name: str, partition_col: str | None = None) -> int:
    path         = f"{GOLD_PATH}/{table_name.replace('gold_', '')}"
    full_name    = f"{DATABASE}.{table_name}"

    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(path)

    spark.sql(f"CREATE TABLE IF NOT EXISTS {full_name} USING DELTA LOCATION '{path}'")
    count = df.count()
    logger.info(f"Saved {full_name}", rows_written=count)
    print(f"  {full_name}: {count:,} rows")
    return count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Daily Sales Summary

# COMMAND ----------

def build_gold_sales_daily():
    t0 = time.time()

    txn   = spark.table("silver_transactions")
    prod  = spark.table("silver_products").select("product_id", "category", "subcategory")
    store = spark.table("silver_stores").select("store_id", "region")

    df = (
        txn
        .join(prod,  "product_id", "left")
        .join(store, "store_id",   "left")
        .filter(F.col("status") != "returned")    # exclude returns from revenue
        .groupBy(
            "transaction_date", "store_id", "region",
            "product_id", "category", "subcategory"
        )
        .agg(
            F.count("transaction_id")         .alias("transaction_count"),
            F.sum("quantity")                 .alias("units_sold"),
            F.sum("revenue_net")              .alias("revenue"),
            F.avg("discount")                 .alias("avg_discount_rate"),
            F.sum("is_returned")              .alias("return_count"),
            F.countDistinct("customer_id")    .alias("unique_customers"),
        )
        .withColumn("revenue",           F.round("revenue", 2))
        .withColumn("avg_discount_rate", F.round("avg_discount_rate", 4))
    )

    save_gold(df, "gold_sales_daily", partition_col="transaction_date")
    logger.info("gold_sales_daily complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Monthly Sales by Category

# COMMAND ----------

def build_gold_sales_monthly():
    t0 = time.time()

    df = (
        spark.table("silver_transactions")
        .join(spark.table("silver_products").select("product_id","category"), "product_id", "left")
        .filter(F.col("status") != "returned")
        .groupBy("year_month", "year", "month", "category")
        .agg(
            F.sum("revenue_net")           .alias("revenue"),
            F.sum("quantity")              .alias("units_sold"),
            F.count("transaction_id")      .alias("transaction_count"),
            F.countDistinct("customer_id") .alias("unique_customers"),
        )
        .withColumn("revenue", F.round("revenue", 2))
        # Month-over-month revenue change
        .withColumn(
            "revenue_prev_month",
            F.lag("revenue").over(
                Window.partitionBy("category").orderBy("year_month")
            )
        )
        .withColumn(
            "revenue_mom_pct",
            F.round(
                (F.col("revenue") - F.col("revenue_prev_month"))
                / F.col("revenue_prev_month") * 100,
                2
            )
        )
    )

    save_gold(df, "gold_sales_monthly", partition_col="year")
    logger.info("gold_sales_monthly complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Customer Lifetime Summary (RFM base)

# COMMAND ----------

def build_gold_customer_summary():
    t0 = time.time()

    txn  = spark.table("silver_transactions").filter(F.col("status") == "completed")
    cust = spark.table("silver_customers")

    # RFM: Recency / Frequency / Monetary
    latest_date = txn.agg(F.max("transaction_date")).collect()[0][0]

    rfm = (
        txn.groupBy("customer_id")
        .agg(
            F.max("transaction_date")          .alias("last_purchase_date"),
            F.count("transaction_id")          .alias("purchase_frequency"),
            F.sum("revenue_net")               .alias("lifetime_value"),
            F.avg("revenue_net")               .alias("avg_order_value"),
            F.countDistinct("product_id")      .alias("distinct_products"),
            F.countDistinct("store_id")        .alias("distinct_stores"),
            F.sum("is_returned")               .alias("total_returns"),
        )
        .withColumn("recency_days",
            F.datediff(F.lit(latest_date), F.col("last_purchase_date")))
        .withColumn("lifetime_value",  F.round("lifetime_value",  2))
        .withColumn("avg_order_value", F.round("avg_order_value", 2))
        # Quintile-based RFM scores (1=best, 5=worst for R; 5=best for F,M)
        .withColumn("r_score", F.ntile(5).over(Window.orderBy("recency_days")))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy("purchase_frequency")))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy("lifetime_value")))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
        .withColumn("customer_tier",
            F.when(F.col("rfm_score") >= 13, "Champions")
            .when(F.col("rfm_score") >= 10,  "Loyal")
            .when(F.col("rfm_score") >= 7,   "Potential")
            .otherwise("At Risk")
        )
    )

    df = rfm.join(
        cust.select("customer_id", "segment", "city"),
        "customer_id", "left"
    )

    save_gold(df, "gold_customer_summary")
    logger.info("gold_customer_summary complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Product Performance

# COMMAND ----------

def build_gold_product_performance():
    t0 = time.time()

    df = (
        spark.table("silver_transactions")
        .join(spark.table("silver_products"), "product_id", "left")
        .groupBy("product_id", "product_name", "category", "subcategory", "unit_price")
        .agg(
            F.sum(F.when(F.col("status")=="completed", F.col("revenue_net")).otherwise(0))
                                                    .alias("revenue_completed"),
            F.sum(F.when(F.col("status")=="returned", F.col("revenue_net")).otherwise(0))
                                                    .alias("revenue_returned"),
            F.sum("quantity")                       .alias("units_sold"),
            F.count("transaction_id")               .alias("transaction_count"),
            F.avg("discount")                       .alias("avg_discount"),
            F.countDistinct("customer_id")          .alias("unique_buyers"),
        )
        .withColumn("return_rate",
            F.round(F.col("revenue_returned") / (F.col("revenue_completed") + 0.01) * 100, 2))
        .withColumn("revenue_completed", F.round("revenue_completed", 2))
        # Rank within category by revenue
        .withColumn("category_rank",
            F.rank().over(
                Window.partitionBy("category").orderBy(F.col("revenue_completed").desc())
            ))
    )

    save_gold(df, "gold_product_performance")
    logger.info("gold_product_performance complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Store Performance

# COMMAND ----------

def build_gold_store_performance():
    t0 = time.time()

    df = (
        spark.table("silver_transactions")
        .join(spark.table("silver_stores"), "store_id", "left")
        .filter(F.col("status") != "returned")
        .groupBy("store_id", "store_name", "city", "region", "year_month", "year", "month")
        .agg(
            F.sum("revenue_net")           .alias("revenue"),
            F.sum("quantity")              .alias("units_sold"),
            F.count("transaction_id")      .alias("transaction_count"),
            F.countDistinct("customer_id") .alias("unique_customers"),
            F.avg("revenue_net")           .alias("avg_transaction_value"),
        )
        .withColumn("revenue",               F.round("revenue", 2))
        .withColumn("avg_transaction_value", F.round("avg_transaction_value", 2))
        # Regional rank by revenue per month
        .withColumn("region_rank",
            F.rank().over(
                Window.partitionBy("region", "year_month")
                      .orderBy(F.col("revenue").desc())
            ))
    )

    save_gold(df, "gold_store_performance", partition_col="year")
    logger.info("gold_store_performance complete", duration_ms=int((time.time()-t0)*1000))

# COMMAND ----------

# ── Execute ───────────────────────────────────────────────────────────────────
print("=" * 60)
print("GOLD AGGREGATION — START")
print("=" * 60)

build_gold_sales_daily()
build_gold_sales_monthly()
build_gold_customer_summary()
build_gold_product_performance()
build_gold_store_performance()

print("\nGOLD AGGREGATION — COMPLETE")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta OPTIMIZE + ZORDER on the largest Gold table
# MAGIC OPTIMIZE retail_platform.gold_sales_daily
# MAGIC ZORDER BY (store_id, category, transaction_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spot check: Top 5 categories by 2024 revenue
# MAGIC SELECT   category,
# MAGIC          ROUND(SUM(revenue), 2) AS total_revenue,
# MAGIC          SUM(units_sold)        AS units
# MAGIC FROM     retail_platform.gold_sales_monthly
# MAGIC WHERE    year = 2024
# MAGIC GROUP BY category
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 5
