-- teardown.sql: TEMPLATE. Never run it by hand; use scripts/teardown.py, which renders the placeholders
-- from the bundle target (env_config), checks the workspace host, and enforces per-step target rules:
--
--   python3 -I scripts/teardown.py --target free --step 1              # preview only (default)
--   python3 -I scripts/teardown.py --target free --step 1 --execute    # delete by recorded ids, then re-preview
--   ... --step 1 --execute --reset-capture --call-ids a,b,c            # sandbox only: also clear capture rows + trace files of those calls
--   python3 -I scripts/teardown.py --target free --step 2 --execute    # sandbox only
--
-- Step | Allowed on        | What
--   1  | free, company     | DELETE rows in <dq_catalog>.results.* by ids recorded in the dashboard schema
--      | free (sandbox)    |   + optional --reset-capture --call-ids ... (explicit list)
--   2  | free (sandbox)    | DROP quarantine tables created by backfill runs (guarded)
--   3  | free (sandbox)    | DROP the sandbox schemas (dashboard schema, generated source schemas) CASCADE
--   4  | free (sandbox)    | DROP the generated source catalogs (never <dq_catalog>, never in company)
--   5  | free (sandbox)    | bundle destroy (job + dashboard draft) + delete the legacy code folder
-- Pre-existing objects (config tables, results tables, reporting views) are never dropped.

-- @step 1
-- @targets: free, company
-- Rows written by driver runs into the pre-existing results tables. Deleted ONLY by recorded ids, never by
-- timestamp: run_calendar.result_id + rule_trace.result_id/result_table_id/result_rule_id (code_error rows
-- carry no result_id, F-01). Make sure all trace files in the trace volume are loaded (the driver does this
-- at start-up and after every call). Unmatched rows are REPORTED, never deleted.
-- @preview
SELECT 'result' AS tbl,
       count_if((result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)     OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL))) AS matched_to_delete,
       count_if(NOT (result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)     OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL))) AS unmatched_keep_and_report
  FROM {{dq_catalog}}.results.result
UNION ALL
SELECT 'result_table',
       count_if((result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)     OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL)     OR result_table_id IN (SELECT result_table_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_table_id IS NOT NULL))),
       count_if(NOT (result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)     OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL)     OR result_table_id IN (SELECT result_table_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_table_id IS NOT NULL)))
  FROM {{dq_catalog}}.results.result_table
UNION ALL
SELECT 'result_rule',
       count_if((result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)     OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL)     OR result_rule_id IN (SELECT result_rule_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_rule_id IS NOT NULL))),
       count_if(NOT (result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)     OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL)     OR result_rule_id IN (SELECT result_rule_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_rule_id IS NOT NULL)))
  FROM {{dq_catalog}}.results.result_rule
UNION ALL
SELECT 'run_calendar_calls_without_result_id', count(*), NULL
  FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NULL
UNION ALL
SELECT 'result_rule_all_ids_null', NULL, count(*)
  FROM {{dq_catalog}}.results.result_rule WHERE result_id IS NULL AND result_rule_id IS NULL;
-- `x IN (subquery)` is NULL when x is NULL; the last line catches rows that would hide from both columns.
-- @execute
DELETE FROM {{dq_catalog}}.results.result_rule
 WHERE (result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)
    OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL)
    OR result_rule_id IN (SELECT result_rule_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_rule_id IS NOT NULL));
DELETE FROM {{dq_catalog}}.results.result_table
 WHERE (result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)
    OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL)
    OR result_table_id IN (SELECT result_table_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_table_id IS NOT NULL));
DELETE FROM {{dq_catalog}}.results.result
 WHERE (result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE result_id IS NOT NULL)
    OR result_id IN (SELECT result_id FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE result_id IS NOT NULL));
-- @capture_reset
-- Optional, SANDBOX ONLY (sandbox_enabled=true): `--reset-capture --call-ids <id,id,...>` after the deletes
-- above. Clears the capture rows of exactly those driver calls (explicit list, never "everything") so the same
-- (day, apply_at, project) can be re-run. Their trace folders are removed from the trace volume by teardown.py.
-- {{reset_call_ids}} / {{reset_result_ids}} are filled by teardown.py from the verified list.
DELETE FROM {{dq_catalog}}.{{dashboard_schema}}.quarantine_snapshot WHERE result_id IN ({{reset_result_ids}});
DELETE FROM {{dq_catalog}}.{{dashboard_schema}}.run_errors WHERE call_id IN ({{reset_call_ids}});
DELETE FROM {{dq_catalog}}.{{dashboard_schema}}.rule_trace WHERE call_id IN ({{reset_call_ids}});
DELETE FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar WHERE call_id IN ({{reset_call_ids}});

-- @step 2
-- @targets: free
-- @sandbox_only
-- GUARD: the quarantine schema contained 0 tables at Phase 0 (2026-10-15, 00_gap_analysis.md). teardown.py
-- aborts if this returns any row, i.e. a quarantine table this work did not create.
-- @guard
SELECT table_name AS unexpected_quarantine_table
  FROM {{dq_catalog}}.information_schema.tables
 WHERE table_schema = 'quarantine' AND lower(table_name) NOT IN ({{quarantine_table_list}});
-- @execute
{{quarantine_drops}}

-- @step 3
-- @targets: free
-- @sandbox_only
-- Each schema is dropped only if its COMMENT is 'dq_dashboard sandbox - generated' (checked by teardown.py).
-- @execute
{{schema_drops}}

-- @step 4
-- @targets: free
-- @sandbox_only
-- Each catalog is dropped only if it is NOT <dq_catalog> and its COMMENT is 'dq_dashboard sandbox - generated'.
-- @execute
{{catalog_drops}}

-- @step 5
-- @targets: free
-- @sandbox_only
-- Non-SQL objects (run by teardown.py; never on company: no bundle destroy there).
-- @shell
databricks bundle destroy -t {{target}} --auto-approve
{{legacy_code_root_delete}}
