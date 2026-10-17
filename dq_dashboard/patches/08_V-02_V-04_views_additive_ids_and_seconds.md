# 08 · V-02 / V-04: reporting views, additive ids + numeric durations (Med) · status: applied on the feature branch (`a526ee6`)

**Root cause**: `v_dq_results` exposes no ids, so you can't tell the latest run, a rule assignment's history, or whether a row
is a code_error. The performance views give durations only as `mm:ss` strings, which can't be charted or averaged.

**Fix** (`setup/03_reporting_views.py`, +11 / 3 lines get a trailing comma; **nothing removed or renamed**)
- `v_dq_results`: new columns at the end: `result_id`, `result_table_id`, `result_rule_id`, `rule_assignment_id`, `error_message`.
  No join or filter changes, so the row count and existing column values are identical.
- `v_dq_table_execution_performance`: `avg_seconds_per_rule` (numeric, `try_divide` → NULL instead of an error when 0 rules).
- `v_dq_execution_performance`: `duration_seconds_for_execution`, `avg_seconds_per_table`.
- New columns are computed only from existing GROUP BY columns, so grouping (and therefore rows) is unchanged. Ids were
  deliberately **not** added to the GROUP BY views, because that would change their row counts.

**Risk**: low. Consumers using `SELECT *` get extra columns at the end.

**Tests**
- Local: every original line is still present, in order (only a trailing comma appended on 3 lines); new columns present.
- Databricks (to run when allowed, read-only on the real views): create the patched views in the sandbox schema and compare:
  `SELECT <original columns> FROM sandbox.v_dq_results EXCEPT ALL SELECT * FROM analytics_dq_dev.reporting.v_dq_results` → 0 rows,
  in both directions, for each of the 5 views.
In the sandbox build: yes (the patched view text is in the build copy; it isn't deployed over the real `reporting` views).
