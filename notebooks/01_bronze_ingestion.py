# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Purpose:** Ingest raw CSV files from the landing zone into Delta tables with zero
# MAGIC business transformation. This layer is the single source of truth for raw data.
# MAGIC
# MAGIC **Azure equivalent:** In a real Azure deployment, Azure Data Factory triggers this
# MAGIC step, copying files from blob storage into ADLS Gen2 and then running this notebook
# MAGIC via an ADF Databricks Notebook activity.
# MAGIC
# MAGIC | Layer  | Path                              | Format |
# MAGIC |--------|-----------------------------------|--------|
# MAGIC | Landing| `dbfs:/retail_platform/landing/`  | CSV    |
# MAGIC | Bronze | `dbfs:/retail_platform/bronze/`   | Delta  |
# MAGIC
# MAGIC **What we add at Bronze (metadata only):**
# MAGIC - `_ingested_at` — UTC timestamp of this run
# MAGIC - `_source_file` — originating filename
# MAGIC - `_batch_id`    — run identifier for lineage

# COMMAND ----------

# MAGIC %run ../utils/data_generator   <- run this first time to create landing CSVs

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/retry

# COMMAND ----------

# MAGIC %run ../utils/notifier

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# COMMAND ----------

# ── Config ────────────────────────────────────────────────────────────────────
LANDING_PATH = "dbfs:/FileStore/retail_platform/landing"
BRONZE_PATH  = "dbfs:/retail_platform/bronze"
DATABASE     = "retail_platform"
BATCH_ID     = dbutils.widgets.get("batch_id") if "batch_id" in [w.name for w in dbutils.widgets.getAll()] else "manual_run"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE} LOCATION 'dbfs:/retail_platform/database'")

logger   = PipelineLogger(stage="bronze_ingestion", run_id=BATCH_ID)
notifier = Notifier.from_secrets()   # reads Slack webhook + SMTP from Databricks Secrets

# COMMAND ----------

# ── Schema definitions ────────────────────────────────────────────────────────
# Explicit schemas avoid schema inference on every run (faster + safer).

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

SCHEMAS = {
    "sales_transactions": StructType([
        StructField("transaction_id",   StringType(),  False),
        StructField("customer_id",      StringType(),  False),
        StructField("product_id",       StringType(),  False),
        StructField("store_id",         StringType(),  False),
        StructField("transaction_date", StringType(),  False),  # kept as string in Bronze
        StructField("quantity",         IntegerType(), True),
        StructField("unit_price",       DoubleType(),  True),
        StructField("discount",         DoubleType(),  True),
        StructField("total_amount",     DoubleType(),  True),
        StructField("status",           StringType(),  True),
    ]),
    "customers": StructType([
        StructField("customer_id",   StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email",         StringType(), True),
        StructField("city",          StringType(), True),
        StructField("segment",       StringType(), True),
    ]),
    "products": StructType([
        StructField("product_id",    StringType(), False),
        StructField("product_name",  StringType(), True),
        StructField("category",      StringType(), True),
        StructField("subcategory",   StringType(), True),
        StructField("unit_price",    DoubleType(), True),
    ]),
    "stores": StructType([
        StructField("store_id",   StringType(), False),
        StructField("store_name", StringType(), True),
        StructField("city",       StringType(), True),
        StructField("region",     StringType(), True),
        StructField("country",    StringType(), True),
    ]),
}

# COMMAND ----------

# ── Ingestion function ────────────────────────────────────────────────────────

def ingest_to_bronze(table_name: str, schema: StructType) -> None:
    """
    Read one CSV from the landing zone and write/overwrite the Bronze Delta table.
    Uses OVERWRITE for dimension tables (full reload) and APPEND for transactions
    — in production this would use Autoloader for incremental ingestion.

    Delta writes are wrapped with @retry (3 attempts, exponential backoff) to
    handle transient cluster preemptions or ADLS throttling.
    """
    t0          = time.time()
    source_file = f"{LANDING_PATH}/{table_name}.csv"

    logger.info(f"Starting ingestion: {table_name}", rows_read=0)

    # Read raw CSV — badRecordsPath captures unparseable rows without failing the job
    bad_records_path = f"{BRONZE_PATH}/_bad_records/{table_name}"
    df = (
        spark.read
        .option("header", "true")
        .option("badRecordsPath", bad_records_path)
        .schema(schema)
        .csv(source_file)
    )

    rows_read = df.count()

    # Add Bronze metadata columns
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_batch_id",    F.lit(BATCH_ID))
    )

    target_path  = f"{BRONZE_PATH}/{table_name}"
    target_table = f"{DATABASE}.bronze_{table_name}"

    # ── Retry-wrapped Delta write ─────────────────────────────────────────────
    # Retries on IOError / RuntimeError (transient DBFS / Delta log conflicts).
    # base_delay=5s with 2x backoff: waits 5s, 10s before final failure.
    @retry(
        max_attempts   = 3,
        base_delay     = 5.0,
        backoff_factor = 2.0,
        exceptions     = (IOError, RuntimeError, Exception),
        on_retry       = lambda attempt, exc: logger.warn(
            f"Delta write retry {attempt} for {table_name}: {exc}"
        ),
    )
    def _write_delta():
        if table_name == "sales_transactions":
            (
                df.write
                .format("delta")
                .mode("append")
                .partitionBy("transaction_date")
                .option("mergeSchema", "true")
                .option("delta.enableChangeDataFeed", "true")
                .save(target_path)
            )
        else:
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(target_path)
            )

    _write_delta()

    # Register in metastore so SQL queries work
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table}
        USING DELTA LOCATION '{target_path}'
    """)

    duration_ms = int((time.time() - t0) * 1000)
    logger.info(
        f"Ingestion complete: {target_table}",
        rows_read=rows_read,
        rows_written=rows_read,
        rows_rejected=0,
        duration_ms=duration_ms,
    )
    print(f"  {table_name}: {rows_read:,} rows in {duration_ms} ms")

# COMMAND ----------

# ── Run ingestion for all tables ──────────────────────────────────────────────
print("=" * 60)
print("BRONZE INGESTION — START")
print("=" * 60)

pipeline_start = time.time()
failed_tables  = []

for tbl, schema in SCHEMAS.items():
    try:
        ingest_to_bronze(tbl, schema)
    except Exception as e:
        logger.error(f"Ingestion FAILED for {tbl}: {e}")
        notifier.send_pipeline_alert(
            stage   = "bronze_ingestion",
            level   = "ERROR",
            message = f"Table '{tbl}' failed after all retries: {e}",
            run_id  = BATCH_ID,
        )
        failed_tables.append(tbl)

total_ms = int((time.time() - pipeline_start) * 1000)

if failed_tables:
    raise RuntimeError(f"Bronze ingestion failed for: {failed_tables}")

print("\nBRONZE INGESTION — COMPLETE")
notifier.send_pipeline_alert(
    stage        = "bronze_ingestion",
    level        = "INFO",
    message      = f"All {len(SCHEMAS)} tables ingested successfully.",
    run_id       = BATCH_ID,
    duration_ms  = total_ms,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'bronze_sales_transactions' AS tbl, COUNT(*) AS row_count FROM retail_platform.bronze_sales_transactions
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_customers',   COUNT(*) FROM retail_platform.bronze_customers
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_products',    COUNT(*) FROM retail_platform.bronze_products
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_stores',      COUNT(*) FROM retail_platform.bronze_stores
# MAGIC ORDER BY tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm Delta history is being tracked (enables time-travel)
# MAGIC DESCRIBE HISTORY retail_platform.bronze_sales_transactions LIMIT 5
