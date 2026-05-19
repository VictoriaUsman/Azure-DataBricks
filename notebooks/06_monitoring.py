# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Monitoring & Health Dashboard
# MAGIC
# MAGIC **Purpose:** Query the `pipeline_logs` Delta table written by every notebook
# MAGIC to give a unified view of pipeline health, data quality trends, and failure history.
# MAGIC
# MAGIC This is the notebook you'd pin as a Databricks dashboard or schedule to alert on failure.

# COMMAND ----------

spark.sql("USE retail_platform")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Latest Run Status per Stage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_stage,
# MAGIC     run_id,
# MAGIC     log_level,
# MAGIC     message,
# MAGIC     rows_read,
# MAGIC     rows_written,
# MAGIC     rows_rejected,
# MAGIC     ROUND(rows_rejected / NULLIF(rows_written, 0) * 100, 2) AS rejection_pct,
# MAGIC     duration_ms,
# MAGIC     logged_at
# MAGIC FROM (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY pipeline_stage ORDER BY logged_at DESC) AS rn
# MAGIC     FROM   retail_platform.pipeline_logs
# MAGIC     WHERE  log_level IN ('INFO','ERROR')
# MAGIC ) ranked
# MAGIC WHERE rn = 1
# MAGIC ORDER BY pipeline_stage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 — Error History (last 7 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     logged_at,
# MAGIC     pipeline_stage,
# MAGIC     run_id,
# MAGIC     message
# MAGIC FROM  retail_platform.pipeline_logs
# MAGIC WHERE log_level = 'ERROR'
# MAGIC   AND logged_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC ORDER BY logged_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 — Throughput Trend (rows written per stage per day)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE(logged_at)  AS run_date,
# MAGIC     pipeline_stage,
# MAGIC     SUM(rows_written) AS total_rows_written,
# MAGIC     AVG(duration_ms)  AS avg_duration_ms,
# MAGIC     COUNT(*)          AS log_events
# MAGIC FROM  retail_platform.pipeline_logs
# MAGIC WHERE log_level = 'INFO'
# MAGIC   AND rows_written IS NOT NULL
# MAGIC GROUP BY DATE(logged_at), pipeline_stage
# MAGIC ORDER BY run_date DESC, pipeline_stage

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 — Delta Table Health

# COMMAND ----------

# MAGIC %sql
# MAGIC -- File counts and sizes across all tables (smaller = better optimized)
# MAGIC SELECT 'fact_sales'           AS table_name, * FROM (DESCRIBE DETAIL retail_platform.fact_sales)
# MAGIC UNION ALL
# MAGIC SELECT 'silver_transactions', * FROM (DESCRIBE DETAIL retail_platform.silver_transactions)
# MAGIC UNION ALL
# MAGIC SELECT 'gold_sales_daily',    * FROM (DESCRIBE DETAIL retail_platform.gold_sales_daily)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 — Data Freshness Check

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     'silver_transactions'    AS table_name,
# MAGIC     MAX(transaction_date)    AS latest_date,
# MAGIC     DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days_since_refresh
# MAGIC FROM retail_platform.silver_transactions
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC     'gold_sales_daily',
# MAGIC     MAX(transaction_date),
# MAGIC     DATEDIFF(CURRENT_DATE(), MAX(transaction_date))
# MAGIC FROM retail_platform.gold_sales_daily
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 — Automated Alerts
# MAGIC
# MAGIC Reads `pipeline_logs` and dispatches real Slack + email notifications via
# MAGIC `utils/notifier.py`. Credentials live in a Databricks Secret Scope — nothing
# MAGIC is hardcoded. This cell is designed to run as the final step of a Databricks
# MAGIC Job so failures trigger the Job's built-in retry/notification logic too.

# COMMAND ----------

# MAGIC %run ../utils/notifier

# COMMAND ----------

from pyspark.sql import functions as F

notifier = Notifier.from_secrets()
logs     = spark.table("retail_platform.pipeline_logs")

# ── Check 1: errors in the last hour ─────────────────────────────────────────
error_rows = (
    logs
    .filter(F.col("log_level") == "ERROR")
    .filter(F.col("logged_at") >= F.current_timestamp() - F.expr("INTERVAL 1 HOUR"))
    .orderBy(F.col("logged_at").desc())
)
recent_errors = error_rows.count()

# ── Check 2: data quality — rejection rate ────────────────────────────────────
rejection_stats = (
    logs
    .filter(F.col("rows_written").isNotNull() & (F.col("rows_written") > 0))
    .withColumn("rejection_pct", F.col("rows_rejected") / F.col("rows_written") * 100)
    .agg(F.max("rejection_pct").alias("max_rejection_pct"))
    .collect()[0]["max_rejection_pct"]
) or 0.0

# ── Check 3: data freshness ───────────────────────────────────────────────────
latest_date_row = spark.sql("""
    SELECT MAX(transaction_date) AS latest_date
    FROM   retail_platform.silver_transactions
""").collect()[0]
days_stale = spark.sql("""
    SELECT DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days
    FROM   retail_platform.silver_transactions
""").collect()[0]["days"] or 0

# ── Print summary ─────────────────────────────────────────────────────────────
print("=" * 55)
print("PIPELINE HEALTH SUMMARY")
print("=" * 55)
print(f"  Recent errors (last 1h)  : {recent_errors}")
print(f"  Max rejection rate       : {rejection_stats:.2f}%")
print(f"  Data freshness (days old): {days_stale}")

# ── Dispatch notifications ────────────────────────────────────────────────────
if recent_errors > 0:
    # Summarise error messages from the log
    error_summary = "\n".join(
        f"  [{r.pipeline_stage}] {r.message}"
        for r in error_rows.limit(5).collect()
    )
    notifier.send_pipeline_alert(
        stage   = "pipeline_monitoring",
        level   = "ERROR",
        message = f"{recent_errors} error(s) in the last hour:\n{error_summary}",
    )
    print(f"\n  ALERT sent — {recent_errors} error(s) detected.")

elif rejection_stats > 5.0:
    notifier.send_pipeline_alert(
        stage   = "pipeline_monitoring",
        level   = "WARN",
        message = (f"High data rejection rate: {rejection_stats:.1f}% of rows rejected. "
                   f"Check dbfs:/retail_platform/silver/_quarantine/"),
        rows_rejected = int(rejection_stats),
    )
    print(f"\n  WARN sent — rejection rate {rejection_stats:.1f}% exceeds 5% threshold.")

elif days_stale > 1:
    notifier.send_pipeline_alert(
        stage   = "pipeline_monitoring",
        level   = "WARN",
        message = (f"Data is {days_stale} day(s) stale. "
                   f"Latest transaction date: {latest_date_row.latest_date}. "
                   f"Check if the ingestion job ran."),
    )
    print(f"\n  WARN sent — data is {days_stale} day(s) stale.")

else:
    notifier.send_pipeline_alert(
        stage   = "pipeline_monitoring",
        level   = "INFO",
        message = "All pipeline checks passed. No errors, rejection rate normal, data fresh.",
    )
    print("\n  Pipeline status: HEALTHY — INFO notification sent.")
