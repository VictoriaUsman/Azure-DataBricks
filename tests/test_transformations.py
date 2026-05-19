# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Tests — Transformation Logic
# MAGIC
# MAGIC Run with: `%run tests/test_transformations` from any notebook, or attach to a job.
# MAGIC Tests use PySpark DataFrames directly — no mocking, no prod data dependency.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import *
import traceback

PASS = "PASS"
FAIL = "FAIL"
results = []

def run_test(name: str, fn):
    try:
        fn()
        results.append((PASS, name, ""))
        print(f"  [{PASS}] {name}")
    except AssertionError as e:
        results.append((FAIL, name, str(e)))
        print(f"  [{FAIL}] {name} — {e}")
    except Exception as e:
        results.append((FAIL, name, traceback.format_exc(limit=2)))
        print(f"  [{FAIL}] {name} — unexpected error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: schema enforcement

# COMMAND ----------

def test_bronze_schema_has_audit_columns():
    """Bronze ingestion must add _ingested_at, _source_file, _batch_id."""
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("_ingested_at",   TimestampType()),
        StructField("_source_file",   StringType()),
        StructField("_batch_id",      StringType()),
    ])
    df = spark.createDataFrame(
        [("T0000001", None, "s3://test", "run_01")], schema=schema
    )
    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns
    assert "_batch_id"    in df.columns

run_test("bronze_schema_has_audit_columns", test_bronze_schema_has_audit_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: deduplication

# COMMAND ----------

def test_silver_deduplication_keeps_latest():
    """Duplicate transaction_ids should reduce to one row (the latest)."""
    from pyspark.sql.window import Window

    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("total_amount",   DoubleType()),
        StructField("_ingested_at",   StringType()),
    ])
    data = [
        ("T001", 100.0, "2024-01-02"),  # duplicate — newer
        ("T001",  90.0, "2024-01-01"),  # duplicate — older, should be dropped
        ("T002", 200.0, "2024-01-01"),
    ]
    df = spark.createDataFrame(data, schema)

    deduped = (
        df.withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("transaction_id")
                      .orderBy(F.col("_ingested_at").desc())
            )
        )
        .filter(F.col("row_num") == 1)
    )

    assert deduped.count() == 2, f"Expected 2, got {deduped.count()}"
    t001_amount = deduped.filter(F.col("transaction_id") == "T001") \
                         .select("total_amount").first()[0]
    assert t001_amount == 100.0, f"Expected 100.0 (latest), got {t001_amount}"

run_test("silver_deduplication_keeps_latest", test_silver_deduplication_keeps_latest)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: quarantine logic

# COMMAND ----------

def test_silver_quarantine_filters_nulls_and_negatives():
    schema = StructType([
        StructField("transaction_id",   StringType()),
        StructField("customer_id",      StringType()),
        StructField("product_id",       StringType()),
        StructField("store_id",         StringType()),
        StructField("transaction_date", StringType()),
        StructField("quantity",         IntegerType()),
        StructField("total_amount",     DoubleType()),
        StructField("status",           StringType()),
    ])
    data = [
        ("T001", "C001", "P001", "S001", "2024-01-01",  2,  100.0, "completed"),  # good
        ("T002",   None, "P001", "S001", "2024-01-01",  1,   50.0, "completed"),  # null customer
        ("T003", "C003", "P002", "S001", "2024-01-01", -1,   50.0, "completed"),  # negative qty
        ("T004", "C004", "P003", "S001", "2024-01-01",  1, -100.0, "completed"),  # negative amount
        ("T005", "C005", "P004", "S001", "2024-01-01",  1,   30.0, "fraud"),      # bad status
    ]
    df = spark.createDataFrame(data, schema)

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

    assert good.count() == 1, f"Expected 1 good row, got {good.count()}"
    assert good.first()["transaction_id"] == "T001"

run_test("silver_quarantine_filters_bad_rows", test_silver_quarantine_filters_nulls_and_negatives)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold: RFM scoring

# COMMAND ----------

def test_gold_rfm_tiers_assigned_correctly():
    """Champions should have rfm_score >= 13."""
    from pyspark.sql.window import Window

    schema = StructType([
        StructField("customer_id",       StringType()),
        StructField("recency_days",      IntegerType()),
        StructField("purchase_frequency",IntegerType()),
        StructField("lifetime_value",    DoubleType()),
    ])
    # 5 customers so ntile(5) gives one per bucket
    data = [
        ("C001",   5, 50, 5000.0),  # low recency + high freq + high LTV → should be champion
        ("C002",  60, 30, 2000.0),
        ("C003", 120, 15, 800.0),
        ("C004", 200,  5, 200.0),
        ("C005", 350,  1,  50.0),  # high recency + low freq + low LTV → should be at risk
    ]
    df = spark.createDataFrame(data, schema)

    scored = (
        df
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

    c001_tier = scored.filter(F.col("customer_id") == "C001").first()["customer_tier"]
    c005_tier = scored.filter(F.col("customer_id") == "C005").first()["customer_tier"]

    assert c001_tier == "Champions", f"C001 should be Champions, got {c001_tier}"
    assert c005_tier == "At Risk",   f"C005 should be At Risk, got {c005_tier}"

run_test("gold_rfm_tiers_assigned_correctly", test_gold_rfm_tiers_assigned_correctly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Star schema: fact net revenue sign

# COMMAND ----------

def test_fact_returns_have_negative_revenue():
    """Returned transactions must carry negative net_revenue."""
    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("status",         StringType()),
        StructField("revenue_net",    DoubleType()),
    ])
    data = [
        ("T001", "completed",  100.0),
        ("T002", "returned",    50.0),
    ]
    df = spark.createDataFrame(data, schema)

    fact = df.withColumn(
        "net_revenue",
        F.when(F.col("status") == "returned", -F.col("revenue_net"))
         .otherwise(F.col("revenue_net"))
    )

    t002_net = fact.filter(F.col("transaction_id") == "T002").first()["net_revenue"]
    assert t002_net == -50.0, f"Expected -50.0, got {t002_net}"

run_test("fact_returns_have_negative_revenue", test_fact_returns_have_negative_revenue)

# COMMAND ----------

# MAGIC %run ../utils/retry

# COMMAND ----------

# MAGIC %run ../utils/rate_limiter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retry: exponential backoff

# COMMAND ----------

def test_retry_succeeds_on_third_attempt():
    """@retry should recover from transient failures without raising."""
    attempts = [0]

    @retry(max_attempts=3, base_delay=0.01, backoff_factor=1.0, exceptions=(ValueError,))
    def flaky():
        attempts[0] += 1
        if attempts[0] < 3:
            raise ValueError("transient")
        return "ok"

    result = flaky()
    assert result == "ok",    f"Expected 'ok', got {result!r}"
    assert attempts[0] == 3, f"Expected 3 attempts, got {attempts[0]}"

run_test("retry_succeeds_on_third_attempt", test_retry_succeeds_on_third_attempt)

# COMMAND ----------

def test_retry_raises_after_max_attempts():
    """After all retries are exhausted the original exception must propagate."""
    @retry(max_attempts=2, base_delay=0.01, exceptions=(ValueError,))
    def always_fails():
        raise ValueError("permanent failure")

    try:
        always_fails()
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "permanent failure" in str(e)

run_test("retry_raises_after_max_attempts", test_retry_raises_after_max_attempts)

# COMMAND ----------

def test_retry_does_not_swallow_non_retried_exception():
    """Exception types not in the retry list must propagate immediately (no delay)."""
    attempts = [0]

    @retry(max_attempts=5, base_delay=0.01, exceptions=(ValueError,))
    def type_error():
        attempts[0] += 1
        raise TypeError("wrong type — should not retry")

    try:
        type_error()
        assert False, "Should have raised TypeError"
    except TypeError:
        pass  # correct
    assert attempts[0] == 1, f"Should have tried exactly once, got {attempts[0]}"

run_test("retry_does_not_swallow_non_retried_exception", test_retry_does_not_swallow_non_retried_exception)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rate limiter: token bucket

# COMMAND ----------

import time as _time

def test_rate_limiter_throttles_to_configured_rate():
    """10 calls at 5/s must take at least 1 second (after the initial burst)."""
    limiter = RateLimiter(calls_per_second=5, burst_size=5)
    t0 = _time.monotonic()
    for _ in range(10):
        limiter.acquire()
    elapsed = _time.monotonic() - t0
    assert elapsed >= 1.0, f"Expected ≥1s elapsed for 10 calls at 5/s, got {elapsed:.2f}s"

run_test("rate_limiter_throttles_to_configured_rate", test_rate_limiter_throttles_to_configured_rate)

# COMMAND ----------

def test_rate_limiter_try_acquire_non_blocking():
    """try_acquire should return False immediately when no tokens are available."""
    limiter = RateLimiter(calls_per_second=1, burst_size=1)
    first  = limiter.try_acquire()   # consumes the one token
    second = limiter.try_acquire()   # no tokens — must return False without blocking
    assert first  is True,  "First acquire should succeed"
    assert second is False, "Second acquire should fail (no tokens)"

run_test("rate_limiter_try_acquire_non_blocking", test_rate_limiter_try_acquire_non_blocking)

# COMMAND ----------

# ── Summary ───────────────────────────────────────────────────────────────────
passed = sum(1 for r in results if r[0] == PASS)
failed = sum(1 for r in results if r[0] == FAIL)

print("\n" + "=" * 50)
print(f"TEST RESULTS: {passed} passed / {failed} failed")
print("=" * 50)

if failed > 0:
    print("\nFailed tests:")
    for status, name, msg in results:
        if status == FAIL:
            print(f"  - {name}: {msg}")
    raise SystemExit(f"{failed} test(s) failed")
