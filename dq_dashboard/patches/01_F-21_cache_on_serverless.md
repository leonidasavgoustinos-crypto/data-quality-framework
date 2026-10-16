# 01 · F-21: `cache()` aborts every run on serverless (Blocker)

**Root cause**: `framework/dq_framework.py:83` calls `df_filtered.cache()`. On serverless compute this raises
`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute`. The exception reaches the run-level
`except`, so every table fails before any rule runs (pilot 1b: 6/6 calls `code_error`, 0 rules evaluated).

**Fix** (`utils.py` +10, `dq_framework.py` 1 line): new `cache_if_supported(df)`. It calls `df.cache()`; **only** when the error
message contains `NOT_SUPPORTED_WITH_SERVERLESS` does it return the DataFrame uncached. Any other error is re-raised unchanged.
`dq_framework.py:83` becomes `df_filtered = cache_if_supported(df_filtered)` (`cache()` returns the same DataFrame, so the
assignment changes nothing on classic clusters).

**Behaviour change**: none on classic/all-purpose clusters. On serverless, runs now work (uncached: each rule re-reads the
filtered partition, so it is slower but gives the same results).

**Risk**: low. The match is on the error *class name* in the message; if Databricks renames it, the old behaviour (abort) returns.

**Tests**
- Local: `tests/test_framework_patches.py`, checks "F-21 cache unsupported on serverless -> uncached" (fails on base, passes from 01)
  and "F-21 other cache errors still raise" (passes on all stages).
- Databricks: rerun pilot 1b with `cache_mode=off` (driver shim disabled). Expected: rules evaluated, no `NOT_SUPPORTED_WITH_SERVERLESS` in run_errors.
