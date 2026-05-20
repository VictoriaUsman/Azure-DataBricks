# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Logger Utility
# MAGIC Structured logging for all pipeline stages. Writes to a Delta `pipeline_logs` table
# MAGIC so monitoring queries (notebook 06) can query run history without scraping stdout.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime, timezone
import uuid

LOGS_TABLE = "retail_platform.pipeline_logs"

LOG_SCHEMA = StructType([
    StructField("log_id",         StringType(),    False),
    StructField("run_id",         StringType(),    False),
    StructField("pipeline_stage", StringType(),    False),
    StructField("log_level",      StringType(),    False),   # INFO | WARN | ERROR
    StructField("message",        StringType(),    False),
    StructField("rows_read",      LongType(),      True),
    StructField("rows_written",   LongType(),      True),
    StructField("rows_rejected",  LongType(),      True),
    StructField("duration_ms",    LongType(),      True),
    StructField("logged_at",      TimestampType(), False),
])

# COMMAND ----------

class PipelineLogger:
    """Thin wrapper that writes one row per log event to the Delta logs table."""

    def __init__(self, stage: str, run_id: str | None = None):
        self.stage  = stage
        self.run_id = run_id or str(uuid.uuid4())
        self._spark = SparkSession.getActiveSession()
        self._ensure_table()

    # ── public API ────────────────────────────────────────────────────────────

    def info(self, message: str, **metrics):
        self._write("INFO", message, **metrics)

    def warn(self, message: str, **metrics):
        self._write("WARN", message, **metrics)

    def error(self, message: str, **metrics):
        self._write("ERROR", message, **metrics)

    # ── internals ─────────────────────────────────────────────────────────────

    def _write(self, level: str, message: str, **metrics):
        print(f"[{level}] [{self.stage}] {message}")
        row = [(
            str(uuid.uuid4()),
            self.run_id,
            self.stage,
            level,
            message,
            metrics.get("rows_read"),
            metrics.get("rows_written"),
            metrics.get("rows_rejected"),
            metrics.get("duration_ms"),
            datetime.now(timezone.utc),
        )]
        df = self._spark.createDataFrame(row, LOG_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(LOGS_TABLE)

    def _ensure_table(self):
        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {LOGS_TABLE}
            USING DELTA
            LOCATION 'dbfs:/retail_platform/pipeline_logs'
            COMMENT 'Structured pipeline execution logs'
        """)
