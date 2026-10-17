# 04 · F-03: one failing rule aborts the whole run → opt-in `on_rule_error` (High) · status: applied on the feature branch (`c221144`)

**Root cause / intent**: `run_data_quality` re-raises on the first failure (`dq_framework.py:49-56`). Remaining rules and tables
are skipped and `close_result` never runs. **This abort is relied upon**: `combined_dq_and_df_load.run_dq_and_insert`
(README "Embedded in a pipeline") lets the exception stop the notebook before `insertInto`. Changing the default to
"continue" would let pipelines insert data after a broken rule. So the default stays as it is.

**Fix** (`dq_framework.py` +33, `utils.py` +10, `run_time_params.py` +2): new keyword `on_rule_error="raise"`.
- `"raise"` (default): exactly the current behaviour (with patches 02/03 the bookkeeping is now correct).
- `"continue"`:
  - A failing rule is marked `code_error` with its `error_message` and the next rule runs.
  - A failing table gets a `code_error` result_table row (opened if the crash came before `open_result_table`) and the next table runs.
  - The table status is `code_error` if any of its rules failed. The execution status is `code_error` if any table is
    `code_error` (precedence code_error > error > warning > pass).
  - `run_data_quality` returns the result_id normally, so callers must check `results.result.status`.
- Invalid values raise `ValueError` before anything is written.

**Behaviour change**: none for existing callers. Only callers that pass `on_rule_error="continue"` see the new behaviour.
The `close_result` / `close_result_table` code_error branches cannot trigger in `"raise"` mode.

**Risk**: low–medium, new code path.
- In continue mode a table-level failure has nowhere to store its message (`result_table` has no `error_message` column; see
  the Med list in README).
- If a rule fails *after* it was closed (e.g. in `prepare_quarantine_rows`), the rule row keeps its status but the table counts
  a code_error.

**Tests**
- Local: "default on error: raise and stop (unchanged)" (all stages); "on_rule_error=continue evaluates the rest", "continue:
  crashed table gets a code_error row", "invalid on_rule_error rejected" (04 only).
- Databricks: pilot rerun with the driver passing `on_rule_error="continue"`. Expected on day 10-03: table 4 gets a
  `code_error` row with the fault message, and table 3 is still evaluated.
