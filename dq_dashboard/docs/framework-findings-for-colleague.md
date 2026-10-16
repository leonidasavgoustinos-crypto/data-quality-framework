# Framework findings: status for review

Repo `data-quality-framework` @ `9c671ac` (branch feature/anr). Details, file:line and evidence: `framework_findings.md`.
Patches: `patches/` (one `.diff` + `.md` each, local tests in `patches/tests/`). Patches 05/06 are already committed on feature/anr (`72e3a0c`, not merged);
the rest is applied together at the end (`patches/APPLY_AT_END.md`).

Status: 🟡 patch written, waiting for your review/OK · 🔴 open, your decision (no patch yet) · ⚪ informational

## Patched (🟡)
| ID | Sev | Finding (one line) | Patch | Status |
|---|---|---|---|---|
| F-21 | Blocker | `df.cache()` raises on serverless, so no rule runs at all | 01 | 🟡 |
| F-20 | High | crash before a table/rule opens overwrites the *previous* closed rows as `code_error` (reproduced on Databricks) | 02 | 🟡 |
| F-01 | High | failed rows have no `result_id`/`table_id`/`rule_assignment_id` and are invisible in `v_dq_results` | 03 | 🟡 |
| F-19 | High | ids are written only on close; should be written on open | 03 | 🟡 |
| F-02 | High | `error_message`, `rule_dimension_id` never written (`alert`/`alert_id` still not, see 🔴) | 03 | 🟡 |
| F-15 | Low | duplicate `rule_type_id` key in `close_result_rule` (should be `rule_dimension_id`) | 03 | 🟡 |
| F-03 | High | first failing rule aborts the whole run. **Default unchanged** (`raise`; `run_dq_and_insert` relies on it); new opt-in `on_rule_error="continue"` | 04 | 🟡 |
| F-22 | High | `run_dq_and_insert` inserts even when error rules fail (gate reads an audit table that is never written, by the wrong id). Question: should `warning` block too? | 07 | 🟡 |
| V-02 | Med | `v_dq_results` has no id columns → added (additive only) | 08 | 🟡 |
| V-04 | Med | perf views have `mm:ss` strings only → numeric seconds added next to them (the GROUP BY collapse remains, see 🔴) | 08 | 🟡 |
| F-13 | Med | setup `CREATE OR REPLACE` wipes results/config on rerun → `IF NOT EXISTS` + explicit `reset_tables=true` | 05 | 🟡 committed in `72e3a0c` on feature/anr, waiting for your OK before merge |
| C-01 | High | demo notebook TRUNCATEs config tables without warning → confirmation guard | 06 | 🟡 committed in `72e3a0c` on feature/anr, waiting for your OK before merge |

## Open: your decision (🔴, no patch)
| ID | Sev | Finding | Suggested fix (one line) |
|---|---|---|---|
| F-04 | High | `PK_violation_custom_error` has invalid SQL (`from_customer_error`). With the default `raise` it **aborts every gold/project_2 run**. We keep it ACTIVE in the sandbox (it feeds the code_error widgets); no config change from us | Is it a deliberate negative test? If yes, move it to a dedicated test project (or `apply_at='test'`); if not, fix `from_customer_error` → `from` |
| F-05 | Med | `non_reusable_statement` hard-codes `analytics_fs_dev.risk.*` and fixed codes | use `${table}`/`${i:…}` placeholders or derive the catalog from the environment |
| F-06 | Med | `sql_literal(False)` renders `'False'` (string vs boolean) in `check_thresholds` | render bool/int/None literals by type (`TRUE`/`FALSE`, numbers, `NULL`) |
| F-07 | Med | no quarantine when **all** rows fail (`utils.py:469`) | drop the `rows_evaluated != rows_failed` condition (or document the intent) |
| F-08 | Med | quarantine tables dropped/overwritten every run; no timestamps | append with `quarantined_at`, `released_at`, `release_reason`; never drop |
| F-09 | Med | any failed row = rule fails; no tolerance | optional `threshold_pct` on assignment or policy |
| F-10 | Med | evaluated partition/filter value not stored | add `filter_value` to `result_table`/`result_rule` |
| F-11 | Med | unknown `engine_type` → `UnboundLocalError` | raise a clear `ValueError` |
| F-12 | Med | `run_dqx` reads `params_dict["column"]` before checking it exists | use `.get`, remove unused `col_list` |
| F-14 | Med | `run_policy` has no schedule/expected frequency (missed runs undetectable) | add `expected_cron` / `max_staleness` |
| F-13b | Low | `setup/01_ddl.py:9` hard-codes `environment = 'dev'`, overriding the widget | delete the line |
| F-16 | Low | `running` rows stay forever if the driver dies | on open, mark stale `running` rows `abandoned` |
| F-17 | Low | `rows_*` are `INT` (overflow > 2.1 B rows) | `BIGINT` |
| F-18 | Low | SQL built with f-strings; audit log commented out | DataFrame API / parameter markers; decide on the audit log (see F-22) |
| F-02b | Med | `alert` / `alert_id` columns never written | write them when alerting exists, or drop the columns |
| F-23 | Low | `error_message` is stored with doubled quotes (escaping meant for SQL, `dq_framework.py:51`) | don't escape when writing via `F.lit` |
| F-24 | Med | `result_table` has no `error_message` → table-level failures (e.g. missing source) lose their text | add `error_message STRING` to `result_table` |
| V-01 | Med | `is_active` in both views is `apply_at.is_active` (misleading) | alias it `apply_at_is_active`; add assignment/policy `is_active` |
| V-03 | Low | `project` comes from the run (`t.project_id`), not the assignment | expose both |
| V-04b | Low | perf views GROUP BY all columns (collapse identical rows) and exclude `code_error` | select per id without GROUP BY; keep code_error with a flag |
| V-05 | Low | `ORDER BY` inside views | remove |
| R-01 | Med | reconciliation: `if r["result_old"]` turns a count of 0 into NULL | `if r["result_old"] is not None` |
| R-02 | Med | reconciliation: only pass/fail, no run id, driver-local timestamp | add `run_id`, optional tolerance → `partial` |
| R-03 | Med | reconciliation: hash-based compare, no per-column diff/keys; `_missing_in_*` overwritten | join on a business key; per-column diff counts |
| R-04 | Low | reconciliation: no `CREATE SCHEMA`; hard-coded catalog/workspace paths | parameterise; create the schema |
| C-02 | Med | registered tables point to non-existent catalogs; no validation at registration | check `tableExists` in `07_table.py` |
| C-03 | Low | 3 active templates unassigned; only 4 dimensions configured | review |
| C-04 | Med | table 3 uses `crsid` as `filter_field`, so only ~1 customer is checked per run. We leave the config as is | use `date` as `filter_field` |
