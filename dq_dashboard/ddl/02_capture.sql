-- @targets: free, company
-- Capture tables + trace volume in the dashboard schema. Allowed on every target: they hold only
-- OBSERVED data (or stay empty). In company they act as empty stubs so the Phase 2 views can LEFT JOIN
-- them. Never put generated/fake rows here. Rendered by scripts/run_ddl.py (env_config placeholders).
-- On free these already exist (history/02_sandbox_applied_free_2026-10-15.sql + history/03_pilot_applied_*);
-- IF NOT EXISTS makes this a no-op there.
CREATE SCHEMA IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}} COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.sandbox_log (
  backfill_started_at TIMESTAMP, phase STRING, note STRING
) COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.run_calendar (
  result_id STRING, simulated_date DATE, partition_value STRING,
  apply_at STRING, project STRING, call_status STRING,
  real_start TIMESTAMP, real_end TIMESTAMP, call_id STRING
) COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.run_errors (
  result_id STRING, simulated_date DATE, apply_at STRING, project STRING, error_message STRING, call_id STRING
) COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.rule_trace (
  result_id STRING, result_table_id STRING, result_rule_id STRING,
  table_id BIGINT, rule_assignment_id BIGINT, traced_at TIMESTAMP,
  call_id STRING, seq INT, func_name STRING
) COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.quarantine_snapshot (
  result_id STRING, simulated_date DATE, quarantine_table STRING, result_rule_id STRING,
  category STRING, rows BIGINT
) COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.columns_snapshot (
  simulated_date DATE, table_catalog STRING, table_schema STRING, table_name STRING,
  column_name STRING, data_type STRING, ordinal_position INT
) COMMENT 'dq_dashboard sandbox - generated';

CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.config_history_snapshot (
  config_table STRING, version BIGINT, `timestamp` TIMESTAMP, user_name STRING,
  operation STRING, operation_parameters STRING, captured_at TIMESTAMP
) COMMENT 'dq_dashboard sandbox - generated';

CREATE VOLUME IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.trace_vol
  COMMENT 'dq_dashboard sandbox - generated';
