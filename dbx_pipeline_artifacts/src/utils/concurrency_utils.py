# Databricks notebook source
import time
import random


_CONCURRENT_RETRY_MARKERS = (
    "CONCURRENT_APPEND",           # DELTA_CONCURRENT_APPEND (MERGE / INSERT)
    "CONCURRENT_DELETE_DELETE",    # DELTA_CONCURRENT_DELETE_DELETE (OPTIMIZE / two DELETEs)
    "CONCURRENT_DELETE_READ",      # DELTA_CONCURRENT_DELETE_READ (READ snapshot invalidated)
)

_RETRY_DELAYS = (5, 10, 20, 30, 45, 60)  # seconds between attempts, after the first


def _retry_on_concurrent_append(operation_fn, description="MERGE",
                                max_retries=1 + len(_RETRY_DELAYS)):
    """
    Retry a Delta MERGE / OPTIMIZE / DELETE on transient concurrency conflicts
    with a long backoff schedule tuned for high-concurrency stewardship runs
    (10+ near-simultaneous operations on the same table). Default schedule:
    5, 10, 20, 30, 45, 60 seconds between attempts (7 attempts total, ~170s
    worst-case before propagating the error).

    Spark DataFrames are lazy — re-invoking the operation naturally re-reads
    underlying tables at the latest committed version on each retry.

    Retries on: CONCURRENT_APPEND (WHOLE_TABLE_READ, ROW_LEVEL_CHANGES),
    CONCURRENT_DELETE_DELETE (two OPTIMIZEs / two DELETEs), and
    CONCURRENT_DELETE_READ (snapshot invalidated by concurrent delete).
    Schema / permission / other errors propagate immediately.
    """
    for attempt in range(max_retries):
        try:
            return operation_fn()
        except Exception as e:
            err_str = str(e)
            is_retryable = any(marker in err_str for marker in _CONCURRENT_RETRY_MARKERS)
            if is_retryable and attempt < max_retries - 1:
                base = _RETRY_DELAYS[attempt]
                wait = base + random.uniform(0, base * 0.2)  # 20% jitter
                print(
                    f"[retry] {description}: Delta concurrency conflict on "
                    f"attempt {attempt + 1}/{max_retries}; sleeping {wait:.1f}s"
                )
                time.sleep(wait)
            else:
                raise
