# Databricks notebook source
# MAGIC %md
# MAGIC # Retry Utility — Exponential Backoff
# MAGIC
# MAGIC Provides a `@retry` decorator and a `retry_call()` function for wrapping
# MAGIC any pipeline operation that can transiently fail (file reads, API calls,
# MAGIC Delta writes on busy clusters).
# MAGIC
# MAGIC Usage:
# MAGIC ```python
# MAGIC @retry(max_attempts=3, base_delay=2.0, exceptions=(IOError, RuntimeError))
# MAGIC def write_delta(df, path):
# MAGIC     df.write.format("delta").save(path)
# MAGIC ```

# COMMAND ----------

import time
import random
import functools
import logging
from typing import Callable, Tuple, Type

_log = logging.getLogger(__name__)

# COMMAND ----------


def retry(
    max_attempts:   int                        = 3,
    base_delay:     float                      = 1.0,    # seconds
    max_delay:      float                      = 60.0,
    backoff_factor: float                      = 2.0,
    jitter:         bool                       = True,   # avoids thundering-herd
    exceptions:     Tuple[Type[Exception], ...] = (Exception,),
    on_retry:       Callable | None            = None,   # optional callback(attempt, exc)
):
    """
    Decorator that retries the wrapped function up to `max_attempts` times.

    Delay schedule: base_delay * backoff_factor^(attempt-1), capped at max_delay.
    With jitter: delay *= uniform(0.75, 1.25) to spread retries across workers.

    Parameters
    ----------
    max_attempts    : total tries (1 = no retry)
    base_delay      : initial wait in seconds
    max_delay       : ceiling on wait time
    backoff_factor  : multiplier applied each attempt
    jitter          : add ±25% random spread to delay
    exceptions      : tuple of exception types to retry on; others propagate immediately
    on_retry        : optional callable(attempt: int, exc: Exception) called before each retry
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        _log.error(
                            f"[retry] {fn.__name__} failed after {max_attempts} attempts: {exc}"
                        )
                        raise

                    delay = min(base_delay * (backoff_factor ** (attempt - 1)), max_delay)
                    if jitter:
                        delay *= random.uniform(0.75, 1.25)

                    _log.warning(
                        f"[retry] {fn.__name__} attempt {attempt}/{max_attempts} failed "
                        f"({type(exc).__name__}: {exc}). Retrying in {delay:.1f}s..."
                    )

                    if on_retry:
                        on_retry(attempt, exc)

                    time.sleep(delay)
            raise last_exc  # unreachable but keeps type checkers happy

        wrapper._max_attempts = max_attempts  # expose for introspection / tests
        return wrapper
    return decorator


# COMMAND ----------


def retry_call(
    fn:             Callable,
    args:           tuple         = (),
    kwargs:         dict          = None,
    max_attempts:   int           = 3,
    base_delay:     float         = 1.0,
    max_delay:      float         = 60.0,
    backoff_factor: float         = 2.0,
    jitter:         bool          = True,
    exceptions:     tuple         = (Exception,),
    on_retry:       Callable | None = None,
):
    """
    Functional alternative to the decorator — useful when you can't modify the
    function definition (e.g., calling a third-party library).

    Example
    -------
    result = retry_call(
        requests.get, args=("https://api.example.com/data",),
        kwargs={"timeout": 10},
        max_attempts=4,
        exceptions=(requests.exceptions.RequestException,),
    )
    """
    decorated = retry(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        backoff_factor=backoff_factor,
        jitter=jitter,
        exceptions=exceptions,
        on_retry=on_retry,
    )(fn)
    return decorated(*(args or ()), **(kwargs or {}))


# COMMAND ----------

# ── Quick self-test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    call_count = 0

    @retry(max_attempts=3, base_delay=0.1, exceptions=(ValueError,))
    def flaky_fn():
        global call_count
        call_count += 1
        if call_count < 3:
            raise ValueError(f"transient error #{call_count}")
        return "success"

    result = flaky_fn()
    assert result == "success",    f"Expected 'success', got {result!r}"
    assert call_count == 3,        f"Expected 3 attempts, got {call_count}"

    # Non-retried exception should propagate immediately
    @retry(max_attempts=5, base_delay=0.01, exceptions=(ValueError,))
    def wrong_exc():
        raise TypeError("should not retry")

    try:
        wrong_exc()
        assert False, "Should have raised"
    except TypeError:
        pass  # correct

    print("[retry] self-test passed")
