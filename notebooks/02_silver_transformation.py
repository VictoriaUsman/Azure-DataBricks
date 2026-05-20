# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning, Validation & Enrichment
# MAGIC
# MAGIC **Purpose:** Promote Bronze data to clean, typed, validated Silver tables.
# MAGIC No aggregation happens here — Silver is the "single version of truth" for
# MAGIC analysts who need row-level data.
# MAGIC
# MAGIC **What this notebook demonstrates:**
# MAGIC - Type casting and date parsing
# MAGIC - Null handling and deduplication
# MAGIC - Business rule enforcement (reject bad rows, quarantine them)
# MAGIC - Derived columns (revenue_net, year_month)
# MAGIC - Incremental merge using Delta Lake MERGE INTO (upsert pattern)
# MAGIC - Data quality validation with pass/fail scorecard

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/validation

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType

DATABASE    = "retail_platform"
BRONZE_PATH = "dbfs:/retail_platform/bronze"
SILVER_PATH = "dbfs:/retail_platform/silver"
BATCH_ID    = dbutils.widgets.get("batch_id") if "batch_id" in [w.name for w in dbutils.widgets.getAll()] else "manual_run"

spark.sql(f"USE {DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Silver Transactions

# COMMAND ----------

def build_silver_transactions() -> None:
    t0     = time.time()
    logger = PipelineLogger("silver_transactions")

    logger.info("Reading bronze_sales_transactions")

    bronze = spark.table("retail_platform.bronze_sales_transactions")

    # ── Type casting ──────────────────────────────────────────────────────────
    df = (
        bronze
        .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
        .withColumn("quantity",         F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",       F.col("unit_price").cast(DoubleType()))
        .withColumn("discount",         F.col("discount").cast(DoubleType()))
        .withColumn("total_amount",     F.col("total_amount").cast(DoubleType()))
    )

    # ── Deduplication — keep latest ingestion of each transaction_id ──────────
    window = "transaction_id"
    df = (
        df.withColumn(
            "row_num",
            F.row_number().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.partitionBy(window).orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    rows_raw = df.count()

    # ── Quarantine: reject obvious bad rows but don't fail the pipeline ───────
    good = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("customer_id").isNotNull()    &
        F.col("product_id").isNotNull()     &
        F.col("store_id").isNotNull()       &
        F.col("transaction_date").isNotNull() &
        (F.col("quantity") > 0)             &
        (F.col("total_amount") >= 0)        &
        F.col("status").isin("completed", "returned", "pending")
    )

    bad = df.subtract(good)
    rows_rejected = bad.count()

    # Save quarantined rows for investigation
    if rows_rejected > 0:
        bad.write.format("delta").mode("append").save(
            f"{SILVER_PATH}/_quarantine/transactions"
        )
        logger.warn(f"Quarantined {rows_rejected} bad rows")

    # ── Derived columns ───────────────────────────────────────────────────────
    silver = (
        good
        .withColumn("revenue_net",   F.round(F.col("total_amount"), 2))
        .withColumn("year_month",    F.date_format("transaction_date", "yyyy-MM"))
        .withColumn("year",          F.year("transaction_date"))
        .withColumn("month",         F.month("transaction_date"))
        .withColumn("day_of_week",   F.dayofweek("transaction_date"))
        .withColumn("is_returned",   (F.col("status") == "returned").cast("int"))
        # Keep Bronze audit columns
        .select(
            "transaction_id", "customer_id", "product_id", "store_id",
            "transaction_date", "year_month", "year", "month", "day_of_week",
            "quantity", "unit_price", "discount", "total_amount", "revenue_net",
            "is_returned", "status",
            "_ingested_at", "_source_file", "_batch_id",
        )
    )

    # ── Validation scorecard ──────────────────────────────────────────────────
    validator = DataValidator(silver, "silver_transactions")
    result = validator.run_all(
        not_null_cols  = ["transaction_id", "customer_id", "product_id", "transaction_date"],
        key_cols       = ["transaction_id"],
        numeric_ranges = {"total_amount": (0, 1_000_000), "quantity": (1, 10_000)},
        date_cols      = [],    # already cast — NULLs handled in quarantine
    )
    print(f"  Validation: {result.passed_rows:,} passed / {result.failed_rows:,} failed "
          f"({result.failure_pct:.1f}%) — {'PASS' if result.passed else 'FAIL'}")

    if not result.passed:
        logger.error("Validation threshold exceeded", rows_rejected=result.failed_rows)
        raise ValueError(f"Silver validation failed: {result.failure_pct:.1f}% bad rows")

    # ── Incremental MERGE (upsert) ────────────────────────────────────────────
    # This is the production-grade pattern: re-running is idempotent.
    target_path  = f"{SILVER_PATH}/transactions"
    target_table = "retail_platform.silver_transactions"

    # First run: create the table
    if not _table_exists(target_table):
        (
            silver.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("year", "month")
            .option("delta.enableChangeDataFeed", "true")
            .save(target_path)
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA LOCATION '{target_path}'")
    else:
        # Subsequent runs: merge to avoid duplicates
        silver.createOrReplaceTempView("silver_transactions_incoming")
        spark.sql(f"""
            MERGE INTO {target_table} AS target
            USING silver_transactions_incoming AS source
            ON target.transaction_id = source.transaction_id
            WHEN MATCHED AND source._ingested_at > target._ingested_at THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """)

    rows_written = spark.table(target_table).count()
    duration_ms  = int((time.time() - t0) * 1000)

    logger.info(
        "silver_transactions complete",
        rows_read    = rows_raw,
        rows_written = rows_written,
        rows_rejected= rows_rejected,
        duration_ms  = duration_ms,
    )
    print(f"  Written {rows_written:,} rows in {duration_ms} ms")


def _table_exists(full_name: str) -> bool:
    db, tbl = full_name.split(".")
    return tbl in [t.name for t in spark.catalog.listTables(db)]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Silver Dimension Tables

# COMMAND ----------

def build_silver_dim(
    bronze_table: str,
    silver_table: str,
    pk_col:       str,
    rename_map:   dict | None = None,
    fill_map:     dict | None = None,
) -> None:
    """Generic cleaner for small dimension tables (customers, products, stores)."""
    t0     = time.time()
    logger = PipelineLogger(f"silver_{silver_table.split('_', 1)[1]}")

    df = spark.table(f"retail_platform.{bronze_table}")

    # Strip whitespace from all string columns
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    # Apply renames
    if rename_map:
        for old, new in rename_map.items():
            df = df.withColumnRenamed(old, new)

    # Fill known defaults
    if fill_map:
        for col_name, default in fill_map.items():
            df = df.fillna({col_name: default})

    # Drop rows missing primary key
    before = df.count()
    df = df.filter(F.col(pk_col).isNotNull())
    dropped = before - df.count()
    if dropped:
        logger.warn(f"Dropped {dropped} rows with null PK ({pk_col})")

    # Dedup on PK
    df = df.dropDuplicates([pk_col])

    target_path  = f"{SILVER_PATH}/{silver_table.replace('silver_', '')}"
    target_table = f"retail_platform.{silver_table}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} USING DELTA LOCATION '{target_path}'")

    duration_ms = int((time.time() - t0) * 1000)
    count = df.count()
    logger.info(f"{silver_table} complete", rows_written=count, duration_ms=duration_ms)
    print(f"  {silver_table}: {count:,} rows in {duration_ms} ms")

# COMMAND ----------

# ── Execute all Silver transforms ─────────────────────────────────────────────
print("=" * 60)
print("SILVER TRANSFORMATION — START")
print("=" * 60)

build_silver_transactions()

build_silver_dim(
    bronze_table = "bronze_customers",
    silver_table = "silver_customers",
    pk_col       = "customer_id",
    fill_map     = {"segment": "Unknown", "city": "Unknown"},
)

build_silver_dim(
    bronze_table = "bronze_products",
    silver_table = "silver_products",
    pk_col       = "product_id",
    fill_map     = {"category": "Uncategorized"},
)

build_silver_dim(
    bronze_table = "bronze_stores",
    silver_table = "silver_stores",
    pk_col       = "store_id",
    fill_map     = {"region": "Unknown", "country": "US"},
)

print("\nSILVER TRANSFORMATION — COMPLETE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Optimizations — run after initial load
# MAGIC
# MAGIC These commands are key interview talking points:
# MAGIC - **OPTIMIZE**: compacts small files into larger ones (reduces read latency)
# MAGIC - **ZORDER BY**: co-locates related data on disk so filters on those columns skip more files

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE retail_platform.silver_transactions
# MAGIC ZORDER BY (customer_id, product_id, transaction_date);
# MAGIC
# MAGIC -- Confirm: check file count before/after in DESCRIBE DETAIL
# MAGIC DESCRIBE DETAIL retail_platform.silver_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts across all Silver tables
# MAGIC SELECT 'silver_transactions' AS tbl, COUNT(*) AS rows FROM retail_platform.silver_transactions
# MAGIC UNION ALL SELECT 'silver_customers', COUNT(*) FROM retail_platform.silver_customers
# MAGIC UNION ALL SELECT 'silver_products',  COUNT(*) FROM retail_platform.silver_products
# MAGIC UNION ALL SELECT 'silver_stores',    COUNT(*) FROM retail_platform.silver_stores
# MAGIC ORDER BY tbl
