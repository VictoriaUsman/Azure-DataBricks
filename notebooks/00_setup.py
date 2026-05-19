# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Setup
# MAGIC Run this once per cluster to initialize the database, DBFS paths, and confirm
# MAGIC Delta Lake is available. All other notebooks depend on this having been run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 — Cluster requirements
# MAGIC - Databricks Runtime: **13.3 LTS** or higher (includes Delta Lake 2.4+)
# MAGIC - Node type: Single-node is fine for Community Edition
# MAGIC - Libraries: none — all dependencies are built into the runtime

# COMMAND ----------

# Verify Delta Lake version
import delta
print(f"Delta Lake version : {delta.__version__}")
print(f"Spark version      : {spark.version}")

# COMMAND ----------

# ── Create DBFS directory structure ──────────────────────────────────────────
# Mirrors what ADLS Gen2 container layout would look like in production.
#
#   retail_platform/
#   ├── landing/          <- raw CSV files (ADF would drop files here)
#   ├── bronze/           <- raw Delta tables
#   ├── silver/           <- cleaned Delta tables
#   ├── gold/             <- aggregated Delta tables (star schema)
#   ├── database/         <- metastore location
#   └── pipeline_logs/    <- structured run logs

PATHS = [
    "dbfs:/FileStore/retail_platform/landing",
    "dbfs:/retail_platform/bronze",
    "dbfs:/retail_platform/silver",
    "dbfs:/retail_platform/gold",
    "dbfs:/retail_platform/database",
    "dbfs:/retail_platform/pipeline_logs",
]

for p in PATHS:
    dbutils.fs.mkdirs(p)
    print(f"Created: {p}")

# COMMAND ----------

# ── Create database ───────────────────────────────────────────────────────────
spark.sql("""
    CREATE DATABASE IF NOT EXISTS retail_platform
    LOCATION 'dbfs:/retail_platform/database'
    COMMENT 'Retail Sales Analytics Platform — all layers'
""")

spark.sql("USE retail_platform")
print("Database 'retail_platform' ready.")

# COMMAND ----------

# ── Spark configuration ───────────────────────────────────────────────────────
# These settings are important for performance — worth mentioning in interviews.

spark.conf.set("spark.sql.shuffle.partitions",     "8")   # reduce for small datasets
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled",     "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled",       "true")
spark.conf.set("spark.sql.adaptive.enabled",                        "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",     "true")

print("Spark config applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup complete — run notebooks in this order:
# MAGIC
# MAGIC | # | Notebook                    | Layer  | Description                         |
# MAGIC |---|-----------------------------|--------|-------------------------------------|
# MAGIC | 0 | `00_setup`                  | —      | This notebook                       |
# MAGIC | 1 | `utils/data_generator`      | —      | Generate synthetic CSV files        |
# MAGIC | 2 | `01_bronze_ingestion`       | Bronze | Ingest raw CSVs → Delta             |
# MAGIC | 3 | `02_silver_transformation`  | Silver | Clean, validate, enrich             |
# MAGIC | 4 | `03_gold_aggregation`       | Gold   | Business-ready aggregates           |
# MAGIC | 5 | `04_warehouse_star_schema`  | Gold   | Star schema for BI consumption      |
# MAGIC | 6 | `05_analytics_queries`      | —      | KPI queries (Power BI data source)  |
# MAGIC | 7 | `06_monitoring`             | —      | Pipeline health dashboard           |
