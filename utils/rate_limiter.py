# Databricks notebook source
# MAGIC %md
# MAGIC # Rate Limiter Utility
# MAGIC
# MAGIC Token-bucket rate limiter for external API calls made during ingestion.
# MAGIC Used when the Bronze layer ingests from REST APIs (CRM, inventory, payment
# MAGIC systems) rather than plain CSV files.
# MAGIC
# MAGIC Usage:
# MAGIC ```python
# MAGIC limiter = RateLimiter(calls_per_second=5)
# MAGIC
# MAGIC for page in range(total_pages):
# MAGIC     with limiter:                          # blocks until a token is available
# MAGIC         resp = requests.get(api_url, params={"page": page})
# MAGIC ```

# COMMAND ----------

import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass


# COMMAND ----------


class RateLimiter:
    """
    Thread-safe token-bucket rate limiter.

    Allows bursts up to `burst_size` calls, then throttles to
    `calls_per_second` thereafter. Use as a context manager or call
    `.acquire()` directly.

    Parameters
    ----------
    calls_per_second : sustained throughput (tokens refilled per second)
    burst_size       : max tokens that can accumulate (default = calls_per_second)
    """

    def __init__(self, calls_per_second: float, burst_size: int | None = None):
        if calls_per_second <= 0:
            raise ValueError("calls_per_second must be positive")

        self._rate       = calls_per_second
        self._capacity   = float(burst_size or calls_per_second)
        self._tokens     = self._capacity
        self._last_refill= time.monotonic()
        self._lock       = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def acquire(self, tokens: float = 1.0) -> float:
        """
        Block until `tokens` are available. Returns the wait time in seconds.
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return 0.0
                # Calculate how long until enough tokens are available
                deficit      = tokens - self._tokens
                wait_secs    = deficit / self._rate

            time.sleep(wait_secs)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Non-blocking acquire. Returns True if tokens were available."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
        return False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_):
        pass

    # ── Internals ─────────────────────────────────────────────────────────────

    def _refill(self) -> None:
        now     = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens      = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now


# COMMAND ----------


@dataclass
class ApiCallStats:
    """Tracks API call metrics for logging."""
    total_calls:    int   = 0
    throttled_calls:int   = 0
    total_wait_ms:  float = 0.0

    def record(self, wait_secs: float) -> None:
        self.total_calls += 1
        if wait_secs > 0.001:
            self.throttled_calls += 1
            self.total_wait_ms   += wait_secs * 1000

    def summary(self) -> str:
        pct = (self.throttled_calls / self.total_calls * 100) if self.total_calls else 0
        return (f"API calls: {self.total_calls} total, "
                f"{self.throttled_calls} throttled ({pct:.1f}%), "
                f"{self.total_wait_ms:.0f} ms total wait")


# COMMAND ----------


def paginated_api_fetch(
    fetch_fn,            # callable(page: int) -> list[dict]
    total_pages:  int,
    calls_per_sec:float = 5.0,
    burst_size:   int   = 10,
    start_page:   int   = 1,
) -> list[dict]:
    """
    Fetch all pages from a paginated REST API with rate limiting and stats tracking.

    Example
    -------
    rows = paginated_api_fetch(
        fetch_fn      = lambda page: requests.get(url, params={"page": page}).json(),
        total_pages   = 50,
        calls_per_sec = 5,
    )
    """
    limiter = RateLimiter(calls_per_second=calls_per_sec, burst_size=burst_size)
    stats   = ApiCallStats()
    results = []

    for page in range(start_page, total_pages + 1):
        wait = limiter.acquire()
        stats.record(wait)

        page_data = fetch_fn(page)
        results.extend(page_data if isinstance(page_data, list) else [page_data])

        if page % 10 == 0:
            print(f"  Fetched page {page}/{total_pages} — {stats.summary()}")

    print(f"\n  Done. {stats.summary()}")
    return results


# COMMAND ----------

# ── Self-test ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 10 calls at 5/s should take ~2 seconds (accounting for burst)
    limiter = RateLimiter(calls_per_second=5, burst_size=5)
    stats   = ApiCallStats()

    t0 = time.monotonic()
    for _ in range(10):
        wait = limiter.acquire()
        stats.record(wait)
    elapsed = time.monotonic() - t0

    assert elapsed >= 1.0, f"Should have throttled (elapsed={elapsed:.2f}s)"
    assert stats.throttled_calls > 0, "Expected some throttled calls"
    print(f"[rate_limiter] self-test passed in {elapsed:.2f}s — {stats.summary()}")
