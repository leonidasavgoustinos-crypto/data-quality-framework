# 03 · F-01 / F-19 / F-02: ids written at open time, error_message stored (High) · status: applied on the feature branch (`38b3051`)

**Root cause**: `open_result_table` / `open_result_rule` (`utils.py:180-188`, `:219-227`) insert only the row id, status and
start time. All foreign keys are written in `close_*`. A rule/table that fails therefore keeps `result_id`, `result_table_id`,
`table_id`, `rule_assignment_id` … = NULL. Such rows are invisible in `v_dq_results`, which joins on `result_id`, and cannot be
attributed. The exception text is captured (`dq_framework.py:51`) but never written (`result_rule.error_message` is always NULL).
`close_result_rule` lists `rule_type_id` twice and never writes `rule_dimension_id` (F-15/F-02).
`result.project_id` stays NULL on failed runs (F-03).
Pilot evidence: the `PK_violation_custom_error` rule row had no result_id and no rule_assignment_id; only the external trace could attribute it.

**Fix** (`utils.py` +21/−6)
- `open_result`: also insert `project_id` (known by then).
- `open_result_table`: also insert `result_id`, `table_id`, `project_id`.
- `open_result_rule`: also insert `result_table_id`, `result_id`, `project_id`, `rule_assignment_id`, `policy_id`,
  `rule_type_id`, `rule_dimension_id` (all known by then).
- `update_result_rule_execution_failure`: also set `error_message`.
- `close_result_rule`: replace the duplicate `rule_type_id` key with `rule_dimension_id`.
- Small helper `_sql_value()` renders `None` as `NULL` and numbers unquoted. Strings go through the existing `sql_literal()`.

**Behaviour change**: the values written are the same ones `close_*` already writes, just earlier. Failed rows now carry ids
and the error message. `rule_dimension_id` is now populated, where it was always NULL.

**Risk**: low. The INSERTs have more columns, all of which already exist in `setup/01_ddl.py`. `error_message` keeps the
existing `''` escaping from `dq_framework.py:51` (doubled quotes in the stored text; see the Low list in README).

**Tests**
- Local: "F-01/F-19 failed rule row has ids + error_message" and "clean run: rule_dimension_id written" (fail on base–02, pass from 03).
- Databricks: pilot rerun. Expected: the `PK_violation_custom_error` row has result_id, result_table_id, rule_assignment_id=71 and
  `error_message` LIKE '%PARSE_SYNTAX_ERROR%'. `SELECT count(*) FROM result_rule WHERE result_id IS NULL` = 0.
