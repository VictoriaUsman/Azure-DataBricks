# Databricks notebook source
# MAGIC %md
# MAGIC # Data Validation Utility
# MAGIC Reusable checks applied at the Silver layer before data is promoted.
# MAGIC Returns a summary dict consumed by the logger so every run has a quality scorecard.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ValidationResult:
    table:          str
    total_rows:     int
    passed_rows:    int
    failed_rows:    int
    failure_pct:    float
    checks:         dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.failure_pct < 5.0  # tolerate up to 5 % bad rows


# COMMAND ----------

class DataValidator:
    """
    Schema-agnostic validators for the Silver transformation layer.

    Usage
    -----
    validator = DataValidator(df, "silver_transactions")
    result    = validator.run_all()
    if not result.passed:
        raise ValueError(f"Validation failed: {result.failure_pct:.1f}% bad rows")
    """

    def __init__(self, df: DataFrame, table_name: str):
        self.df    = df
        self.name  = table_name
        self._bad: Optional[DataFrame] = None

    # ── individual checks ─────────────────────────────────────────────────────

    def check_nulls(self, columns: list[str]) -> dict[str, int]:
        """Return null-count per column."""
        return {
            col: self.df.filter(F.col(col).isNull()).count()
            for col in columns
        }

    def check_duplicates(self, key_columns: list[str]) -> int:
        """Return number of duplicate rows on the given key."""
        total  = self.df.count()
        unique = self.df.select(key_columns).distinct().count()
        return total - unique

    def check_value_range(self, column: str, min_val, max_val) -> int:
        """Return rows outside [min_val, max_val]."""
        return self.df.filter(
            (F.col(column) < min_val) | (F.col(column) > max_val)
        ).count()

    def check_referential_integrity(
        self, column: str, valid_values_df: DataFrame, ref_column: str
    ) -> int:
        """Return rows whose column value has no match in valid_values_df."""
        valid = valid_values_df.select(ref_column).distinct()
        orphans = self.df.join(
            valid, self.df[column] == valid[ref_column], how="left_anti"
        )
        return orphans.count()

    def check_date_format(self, column: str, date_format: str = "yyyy-MM-dd") -> int:
        """Return rows where the date column cannot be parsed."""
        return self.df.filter(
            F.to_date(F.col(column), date_format).isNull()
        ).count()

    # ── composite runner ──────────────────────────────────────────────────────

    def run_all(
        self,
        not_null_cols:   list[str] | None = None,
        key_cols:        list[str] | None = None,
        numeric_ranges:  dict             | None = None,   # {"col": (min, max)}
        date_cols:       list[str] | None = None,
    ) -> ValidationResult:

        total  = self.df.count()
        checks = {}
        total_failures = 0

        if not_null_cols:
            null_counts = self.check_nulls(not_null_cols)
            checks["null_counts"] = null_counts
            total_failures += sum(null_counts.values())

        if key_cols:
            dupe_count = self.check_duplicates(key_cols)
            checks["duplicate_key_rows"] = dupe_count
            total_failures += dupe_count

        if numeric_ranges:
            range_failures = {}
            for col, (lo, hi) in numeric_ranges.items():
                cnt = self.check_value_range(col, lo, hi)
                range_failures[col] = cnt
                total_failures += cnt
            checks["out_of_range"] = range_failures

        if date_cols:
            date_failures = {}
            for col in date_cols:
                cnt = self.check_date_format(col)
                date_failures[col] = cnt
                total_failures += cnt
            checks["invalid_dates"] = date_failures

        # cap at total so pct never exceeds 100
        failed = min(total_failures, total)
        passed = total - failed

        return ValidationResult(
            table       = self.name,
            total_rows  = total,
            passed_rows = passed,
            failed_rows = failed,
            failure_pct = (failed / total * 100) if total else 0.0,
            checks      = checks,
        )
