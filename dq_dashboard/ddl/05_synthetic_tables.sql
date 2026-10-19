-- @targets: free, company
-- Synthetic tables in the dashboard schema. Each one has the columns of the real view it complements, plus
-- is_synthetic (always TRUE). The dashboard views combine real and synthetic rows behind one switch,
-- {{use_synthetic}} (bundle variable synthetic_enabled). In company these tables are created EMPTY, so the views
-- compile and return real rows only. Rows are written only by scripts/gen_synthetic.py (sandbox only).
-- Rendered by scripts/run_ddl.py (env_config placeholders).

-- results_synthetic: history before the real runs, same grain and columns as v_results_ext
-- (without the derived is_latest_for_day, which the combined view computes).
CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.results_synthetic (
  row_level STRING,
  run_date DATE,
  run_ts TIMESTAMP,
  is_simulated_date BOOLEAN,
  partition_value STRING,
  result_id STRING,
  result_table_id STRING,
  result_rule_id STRING,
  table_id BIGINT,
  project_id BIGINT,
  rule_assignment_id BIGINT,
  policy_id BIGINT,
  rule_template_id BIGINT,
  rule_type_id BIGINT,
  rule_dimension_id BIGINT,
  project STRING,
  data_layer STRING,
  catalog STRING,
  criticality STRING,
  rule_dimension STRING,
  apply_at STRING,
  `schema` STRING,
  table_name STRING,
  table_nk STRING,
  quarantine STRING,
  filter_field STRING,
  filter_field_type STRING,
  project_description STRING,
  rule_name STRING,
  rule_description STRING,
  rule_type STRING,
  rule_scope STRING,
  is_reusable BOOLEAN,
  engine_type STRING,
  statement STRING,
  parameters_identifiers STRING,
  parameters_values STRING,
  policy_category STRING,
  quarantine_status BOOLEAN,
  is_project_specific BOOLEAN,
  status STRING,
  start_timestamp TIMESTAMP,
  end_timestamp TIMESTAMP,
  duration_s DOUBLE,
  rows_evaluated INT,
  rows_pass INT,
  rows_failed INT,
  rows_quarantined BIGINT,
  quarantine_table STRING,
  error_message STRING,
  error_summary STRING,
  table_status STRING,
  rules_evaluated INT,
  rules_pass INT,
  rules_warning INT,
  rules_error INT,
  start_timestamp_table TIMESTAMP,
  end_timestamp_table TIMESTAMP,
  table_duration_s DOUBLE,
  execution_status STRING,
  tables_evaluated INT,
  tables_pass INT,
  tables_warning INT,
  tables_error INT,
  start_timestamp_execution TIMESTAMP,
  end_timestamp_execution TIMESTAMP,
  execution_duration_s DOUBLE,
  crit_weight INT,
  is_pass BOOLEAN,
  is_scored BOOLEAN,
  is_synthetic BOOLEAN
) COMMENT 'dq_dashboard sandbox - generated';

-- rule_assignment_synthetic: demo rule assignments for the dimensions without real rules (completeness,
-- consistency, timeliness), same columns as v_assignment_active. Their results are in results_synthetic (synthetic
-- history and the real run days). The real configuration tables are never changed.
CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.rule_assignment_synthetic (
  rule_assignment_id BIGINT,
  table_id BIGINT,
  project_id BIGINT,
  rule_template_id BIGINT,
  policy_id BIGINT,
  apply_at_id BIGINT,
  project STRING,
  data_layer STRING,
  catalog STRING,
  criticality STRING,
  rule_dimension STRING,
  `schema` STRING,
  table_name STRING,
  table_nk STRING,
  apply_at STRING,
  rule_name STRING,
  rule_type STRING,
  engine_type STRING,
  rule_scope STRING,
  policy_category STRING,
  quarantine_status BOOLEAN,
  parameters_identifiers STRING,
  parameters_values STRING,
  assignment_is_active BOOLEAN,
  template_is_active BOOLEAN,
  policy_is_active BOOLEAN,
  table_is_active BOOLEAN,
  apply_at_is_active BOOLEAN,
  is_effective_active BOOLEAN,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  added_by STRING,
  is_synthetic BOOLEAN
) COMMENT 'dq_dashboard sandbox - generated';

-- quarantine_lifecycle_synthetic: one row per batch of quarantined rows (one rule result), with the time the rows
-- entered quarantine, the time they left it and how (released, reprocessed, discarded); NULL = still in quarantine.
-- The framework stores neither time (it replaces a quarantine table on every run). Recorded (unshifted) times.
CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.quarantine_lifecycle_synthetic (
  batch_id STRING,
  result_rule_id STRING,
  rule_assignment_id BIGINT,
  table_id BIGINT,
  quarantine_table STRING,
  rows_quarantined BIGINT,
  entered_ts TIMESTAMP,
  left_ts TIMESTAMP,
  outcome STRING,
  is_synthetic BOOLEAN
) COMMENT 'dq_dashboard sandbox - generated';

-- incident_triage_synthetic: triage of the derived incidents (see v_incidents): acknowledgement and owning team.
-- incident_key = rule_assignment_id || ':' || recorded first failing day. Not stored by the framework.
CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.incident_triage_synthetic (
  incident_key STRING,
  owner STRING,
  acknowledged_ts TIMESTAMP,
  is_synthetic BOOLEAN
) COMMENT 'dq_dashboard sandbox - generated';

-- alert_log_synthetic: alerts sent for the derived incidents and their outcome (fixed, accepted risk, noise;
-- NULL = not assessed yet). Not stored by the framework (result_rule.alert is never written). Recorded times.
CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.alert_log_synthetic (
  alert_id STRING,
  incident_key STRING,
  rule_assignment_id BIGINT,
  sent_ts TIMESTAMP,
  channel STRING,
  alert_kind STRING,
  outcome STRING,
  is_synthetic BOOLEAN
) COMMENT 'dq_dashboard sandbox - generated';
