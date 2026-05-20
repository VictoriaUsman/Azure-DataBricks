# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 2 — dim_customer
# MAGIC
# MAGIC **Purpose:** Maintain full change history for `dim_customer`. When a customer's
# MAGIC `segment`, `city`, or `customer_tier` changes, the old row is closed and a new
# MAGIC active row is inserted — preserving the attribute values that were true at the
# MAGIC time of every historical transaction.
# MAGIC
# MAGIC **Schema additions over a Type 1 dimension:**
# MAGIC | Column                | Type    | Meaning                                  |
# MAGIC |-----------------------|---------|------------------------------------------|
# MAGIC | `effective_start_date`| DATE    | First day this row version was valid     |
# MAGIC | `effective_end_date`  | DATE    | Last day valid (NULL = currently active) |
# MAGIC | `is_current`          | BOOLEAN | True for the one active version per customer |
# MAGIC
# MAGIC **SCD2-tracked attributes** (a change in any of these creates a new row):
# MAGIC - `segment`       — Consumer / Corporate / Home Office
# MAGIC - `city`          — customer's home city
# MAGIC - `customer_tier` — Champions / Loyal / Potential / At Risk (from RFM)
# MAGIC
# MAGIC **Type 1 attributes** (updated in-place, no history created):
# MAGIC - `customer_name`, `email`, `lifetime_value`, `purchase_frequency`
# MAGIC   — corrections and metric updates that do not need a history trail

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType
from datetime import date
import time

DATABASE   = "retail_platform"
DW_PATH    = "dbfs:/retail_platform/gold/warehouse"
BATCH_ID   = dbutils.widgets.get("batch_id") if "batch_id" in [w.name for w in dbutils.widgets.getAll()] else "manual_run"
TODAY      = str(date.today())
FAR_FUTURE = "9999-12-31"

spark.sql(f"USE {DATABASE}")
logger = PipelineLogger("scd2_dim_customer", run_id=BATCH_ID)

# Columns whose change triggers a new SCD2 row
SCD2_COLS = ["segment", "city", "customer_tier"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Build incoming snapshot

# COMMAND ----------

def build_incoming() -> "DataFrame":
    """
    Join silver_customers with gold RFM to produce the full set of
    current attribute values for every customer.
    This is what dim_customer *should* look like today.
    """
    return (
        spark.table("silver_customers")
        .join(
            spark.table("gold_customer_summary").select(
                "customer_id", "customer_tier", "lifetime_value", "purchase_frequency"
            ),
            "customer_id", "left"
        )
        .select(
            "customer_id", "customer_name", "email", "city", "segment",
            F.coalesce(F.col("customer_tier"),      F.lit("Unknown")).alias("customer_tier"),
            F.coalesce(F.col("lifetime_value"),     F.lit(0.0))      .alias("lifetime_value"),
            F.coalesce(F.col("purchase_frequency"), F.lit(0))        .alias("purchase_frequency"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — First run: create dim_customer with SCD2 columns

# COMMAND ----------

def _table_exists(name: str) -> bool:
    db, tbl = name.split(".")
    return tbl in [t.name for t in spark.catalog.listTables(db)]


def initial_load(incoming):
    """First-run only: write every customer as currently active."""
    t0 = time.time()
    df = (
        incoming
        .withColumn("customer_key",         F.monotonically_increasing_id())
        .withColumn("effective_start_date", F.lit(TODAY).cast(DateType()))
        .withColumn("effective_end_date",   F.lit(None) .cast(DateType()))
        .withColumn("is_current",           F.lit(True))
    )

    path = f"{DW_PATH}/dim_customer"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("is_current")        # fast filter for active-only queries
        .save(path)
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.dim_customer
        USING DELTA LOCATION '{path}'
    """)
    n = df.count()
    logger.info("dim_customer initial load complete",
                rows_written=n, duration_ms=int((time.time()-t0)*1000))
    print(f"  Initial load: {n:,} customers written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Incremental run: detect changes and apply SCD2

# COMMAND ----------

def _scd2_change_condition(incoming_alias: str, current_alias: str) -> "Column":
    """Returns a Column that is True when any SCD2-tracked attribute differs."""
    cond = None
    for col in SCD2_COLS:
        diff = F.col(f"{incoming_alias}.{col}") != F.col(f"{current_alias}.{col}")
        cond = diff if cond is None else (cond | diff)
    return cond


def incremental_scd2(incoming):
    t0 = time.time()

    current_active = spark.table(f"{DATABASE}.dim_customer").filter(F.col("is_current"))

    # ── Identify changed customers ────────────────────────────────────────────
    changed = (
        incoming.alias("inc")
        .join(current_active.alias("cur"), "customer_id")
        .filter(_scd2_change_condition("inc", "cur"))
        .select("inc.*")
    )
    n_changed = changed.count()

    # ── Identify brand-new customers ──────────────────────────────────────────
    all_known = spark.table(f"{DATABASE}.dim_customer").select("customer_id").distinct()
    new_customers = incoming.join(all_known, "customer_id", "left_anti")
    n_new = new_customers.count()

    print(f"  Changed customers : {n_changed:,}")
    print(f"  New customers     : {n_new:,}")

    # ── Step A: Close old rows for changed customers ──────────────────────────
    # Set effective_end_date = yesterday, is_current = False.
    # We use TODAY - 1 day so the date ranges are contiguous:
    #   old row: start → TODAY-1
    #   new row: TODAY → NULL
    if n_changed > 0:
        changed_ids = [r.customer_id for r in changed.select("customer_id").collect()]

        DeltaTable.forName(spark, f"{DATABASE}.dim_customer").update(
            condition = (
                F.col("customer_id").isin(changed_ids) &
                F.col("is_current")
            ),
            set = {
                "effective_end_date": F.date_sub(F.lit(TODAY).cast(DateType()), 1),
                "is_current":         F.lit(False),
            }
        )
        logger.info(f"Closed {n_changed} changed customer rows")

    # ── Step B: Insert new active rows (changed + brand-new) ─────────────────
    rows_to_insert = changed.union(new_customers)

    if n_changed + n_new > 0:
        new_rows = (
            rows_to_insert
            .withColumn("customer_key",         F.monotonically_increasing_id())
            .withColumn("effective_start_date", F.lit(TODAY).cast(DateType()))
            .withColumn("effective_end_date",   F.lit(None).cast(DateType()))
            .withColumn("is_current",           F.lit(True))
        )
        new_rows.write.format("delta").mode("append").saveAsTable(f"{DATABASE}.dim_customer")
        logger.info(f"Inserted {n_changed + n_new} new/updated customer rows")

    # ── Step C: Type-1 update for non-SCD2 attributes ────────────────────────
    # customer_name, email, lifetime_value, purchase_frequency update in-place
    # on the current row — no history needed for these.
    type1_updates = (
        incoming.alias("inc")
        .join(current_active.alias("cur"), "customer_id")
    )
    if type1_updates.count() > 0:
        DeltaTable.forName(spark, f"{DATABASE}.dim_customer").alias("dim").merge(
            source    = type1_updates.select("inc.*").alias("src"),
            condition = "dim.customer_id = src.customer_id AND dim.is_current = true"
        ).whenMatchedUpdate(set={
            "customer_name":      "src.customer_name",
            "email":              "src.email",
            "lifetime_value":     "src.lifetime_value",
            "purchase_frequency": "src.purchase_frequency",
        }).execute()

    duration_ms = int((time.time() - t0) * 1000)
    total = spark.table(f"{DATABASE}.dim_customer").count()
    active = spark.table(f"{DATABASE}.dim_customer").filter(F.col("is_current")).count()

    logger.info(
        "scd2_dim_customer complete",
        rows_written = n_changed + n_new,
        duration_ms  = duration_ms,
    )
    print(f"  Total rows (all versions) : {total:,}")
    print(f"  Active rows (is_current)  : {active:,}")
    print(f"  History rows              : {total - active:,}")

# COMMAND ----------

# ── Execute ───────────────────────────────────────────────────────────────────
print("=" * 60)
print("SCD TYPE 2 — dim_customer — START")
print("=" * 60)

incoming = build_incoming()

if not _table_exists(f"{DATABASE}.dim_customer"):
    print("  First run detected — performing initial load")
    initial_load(incoming)
else:
    print("  Incremental run — applying SCD2 logic")
    incremental_scd2(incoming)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimise after load

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE retail_platform.dim_customer ZORDER BY (customer_id, is_current);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — current vs history split

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     is_current,
# MAGIC     COUNT(*)                          AS row_count,
# MAGIC     COUNT(DISTINCT customer_id)       AS unique_customers,
# MAGIC     MIN(effective_start_date)         AS earliest_version,
# MAGIC     MAX(effective_start_date)         AS latest_version
# MAGIC FROM retail_platform.dim_customer
# MAGIC GROUP BY is_current

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample: customers with more than one version (proof SCD2 is working)
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     segment,
# MAGIC     city,
# MAGIC     customer_tier,
# MAGIC     effective_start_date,
# MAGIC     effective_end_date,
# MAGIC     is_current
# MAGIC FROM retail_platform.dim_customer
# MAGIC WHERE customer_id IN (
# MAGIC     SELECT customer_id
# MAGIC     FROM   retail_platform.dim_customer
# MAGIC     GROUP BY customer_id
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC ORDER BY customer_id, effective_start_date
