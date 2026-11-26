-- Smoke tests: every object the dashboard reads must exist and be queryable.
-- Works on an empty install (no data needed). A failing statement fails the task.
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_status;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_dimension;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_results;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_executions;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_rule_daily;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_table_daily;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_quality_score;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_coverage;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_table_daily_ext;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_rule_daily_ext;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_coverage_ext;
SELECT COUNT(*) FROM ${catalog}.${schema}.v_simple_active_dimension_ext;

-- The status lookup must contain exactly the four framework statuses.
SELECT CASE WHEN COUNT(*) = 4 THEN 'ok'
            ELSE raise_error('v_simple_status must have 4 rows, has ' || COUNT(*)) END
FROM ${catalog}.${schema}.v_simple_status;
