# 07 · F-22: `run_dq_and_insert` inserts even when error-level rules fail (High) · status: applied on the feature branch (`09a098b`)

**Confirmed by code trace** (repo at 9c671ac):
1. `framework/combined_dq_and_df_load.py:69-76` calls `run_data_quality(...)` and stores the return value as `job_id`.
2. A rule that finds bad rows does **not** raise. `close_result_rule` sets status = policy category (`framework/utils.py:233`) and
   `close_result` sets the run to `error`/`warning`. `run_data_quality` returns `params.result.result_id`
   (`framework/dq_framework.py:47`), a uuid generated at `framework/utils.py:133`.
3. The audit log is never written: `open_audit_log` / `close_audit_log` are commented out (`framework/dq_framework.py:21`, `:41`).
   Even if they were enabled, their key is a *different* uuid, `params.meta.job_id` (`framework/utils.py:72`).
4. `combined_dq_and_df_load.py:77-82` looks up `analytics_audit.<env>.processing_job_status WHERE job_id = '<result_id>'`, which
   finds nothing. So `dq_status` is empty, the condition at `:84` is false, and the insert at `:89-93` runs.
5. Consequence A: error-level DQ failures never block the insert (reproduced locally: status `error` → `inserted=True`).
   Consequence B: in a workspace without the audit table (e.g. Free Edition here), `spark.table` at `:78` raises, so **every**
   insert is blocked, even when DQ passed.

**Fix** (+8/−4, `combined_dq_and_df_load.py` only): read the run's own row `analytics_dq_<env>.results.result WHERE result_id = '<id>'`
(the same `analytics_dq_{env}` naming the module already uses at :17/:35). Insert only if the status is `pass`. `warning` /
`error` / `code_error` / missing row → `dbutils.notebook.exit(... status ...)`. This matches the original intent:
`close_audit_log` marks the job `completed` only when the result is `pass` (`utils.py:115`).
Exceptions from `run_data_quality` (code errors, default `raise`) still propagate.

**Behaviour change**: yes, the gate now actually works. Note `run_mode="error"` is the default here, so warning rules don't run
unless a caller passes `run_mode="all"`. In that case a `warning` blocks too. **Colleague to confirm** whether warnings should block.

**Tests** (local, `tests/test_other_patches.py`): pass → insert; warning / error / code_error / missing row → no insert
(fail on the original); `run_data_quality` raises → no insert (all stages).
In the sandbox build: yes (`run_dq_and_insert` isn't called by the dashboard driver, but the wheel matches the end state).
