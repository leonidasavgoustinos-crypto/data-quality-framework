# 02 · F-20: failure path overwrites already-closed rows (High)

**Root cause**: `params.result.result_table_id` / `result_rule_id` keep the ids of the **previous** table/rule after they are closed.
When a later table crashes before `open_result_table` (e.g. a missing source in `filter_by_filter_field`), or before its first
`open_result_rule` (e.g. in `drop_quarantine`), the run-level `except` (`dq_framework.py:49-56` → `utils.py:42-68`) updates those
stale ids. The closed result of table N-1 and its last rule become `code_error`.
Reproduced on Databricks (pilot 1b re-run, day 10-03): table 15's `warning` row and its PK rule row (1 real duplicate) were
overwritten as `code_error`, so a real finding was hidden. The crashing table has no row at all.

**Fix** (`dq_framework.py` +6, `utils.py` +12)
- Reset `result_table_id` and `result_rule_id` to `None` at the start of `_run_dq_for_single_table`, and `result_rule_id` at the
  start of `_run_single_rule`.
- In the three `update_*_execution_failure` functions: return when the id is `None`, and add `status = 'running'` to the
  update condition, so a closed row can never be overwritten.

**Behaviour change**: only in the failure path. Closed rows keep their real status; `running` rows are still marked `code_error`.

**Risk**: low. Edge case: if `close_result()` itself fails after updating the status, the execution row keeps that status instead
of `code_error` (previously it was overwritten). The exception is still raised.

**Tests**
- Local: "F-20 table crash keeps previous table/rule rows" (fails on base and 01, passes from 02); "rule crash marks only that
  rule/table/run" (all stages).
- Databricks: pilot rerun with the same fault injection (day 10-03, 2nd table). Expected: table 15's rows keep their status.
  Query: `status='code_error' AND rows_evaluated IS NOT NULL` → 0 rows.
