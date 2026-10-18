-- @targets: free, company
-- Dashboard views: the only sources of the dashboard datasets. Each one combines the real data with its
-- *_synthetic complement behind one switch, {{use_synthetic}} (bundle variable synthetic_enabled: true in free,
-- false in company). With false the synthetic tables are never read and every view returns real rows only.
-- Requires 04_v_results_ext.sql and 05_synthetic_tables.sql. Rendered by scripts/run_ddl.py.

-- v_results_all: v_results_ext + results_synthetic (same columns), is_synthetic flag, and is_latest_for_day
-- recomputed over the union (one TRUE per run_date, table_id, apply_at, project, rule assignment).
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_results_all
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH unioned AS (
  SELECT
    row_level, run_date, run_ts, is_simulated_date, partition_value, result_id, result_table_id,
    result_rule_id, table_id, project_id, rule_assignment_id, policy_id, rule_template_id, rule_type_id,
    rule_dimension_id, project, data_layer, catalog, criticality, rule_dimension, apply_at, `schema`,
    table_name, table_nk, quarantine, filter_field, filter_field_type, project_description, rule_name,
    rule_description, rule_type, rule_scope, is_reusable, engine_type, statement, parameters_identifiers,
    parameters_values, policy_category, quarantine_status, is_project_specific, status, start_timestamp,
    end_timestamp, duration_s, rows_evaluated, rows_pass, rows_failed, rows_quarantined, quarantine_table,
    error_message, error_summary, table_status, rules_evaluated, rules_pass, rules_warning, rules_error,
    start_timestamp_table, end_timestamp_table, table_duration_s, execution_status, tables_evaluated,
    tables_pass, tables_warning, tables_error, start_timestamp_execution, end_timestamp_execution,
    execution_duration_s, crit_weight, is_pass, is_scored,
    FALSE AS is_synthetic
  FROM {{dq_catalog}}.{{dashboard_schema}}.v_results_ext
  UNION ALL
  SELECT
    row_level, run_date, run_ts, is_simulated_date, partition_value, result_id, result_table_id,
    result_rule_id, table_id, project_id, rule_assignment_id, policy_id, rule_template_id, rule_type_id,
    rule_dimension_id, project, data_layer, catalog, criticality, rule_dimension, apply_at, `schema`,
    table_name, table_nk, quarantine, filter_field, filter_field_type, project_description, rule_name,
    rule_description, rule_type, rule_scope, is_reusable, engine_type, statement, parameters_identifiers,
    parameters_values, policy_category, quarantine_status, is_project_specific, status, start_timestamp,
    end_timestamp, duration_s, rows_evaluated, rows_pass, rows_failed, rows_quarantined, quarantine_table,
    error_message, error_summary, table_status, rules_evaluated, rules_pass, rules_warning, rules_error,
    start_timestamp_table, end_timestamp_table, table_duration_s, execution_status, tables_evaluated,
    tables_pass, tables_warning, tables_error, start_timestamp_execution, end_timestamp_execution,
    execution_duration_s, crit_weight, is_pass, is_scored,
    TRUE AS is_synthetic
  FROM {{dq_catalog}}.{{dashboard_schema}}.results_synthetic
  WHERE {{use_synthetic}}
)
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY run_date, table_id, apply_at, project, COALESCE(rule_assignment_id, -1)
    ORDER BY start_timestamp DESC, result_id DESC
  ) = 1 AS is_latest_for_day
FROM unioned;

-- v_runs: one row per call (execution). Real calls come from results.result, so calls without any table result
-- are included; scope (apply_at, project) and the simulated date come from run_calendar when present, else from
-- the call's own rows. run_ts/run_end_ts are calendar-aware (simulated date + real time of day).
-- is_latest_for_day: the newest call per (run_date, apply_at, project).
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_runs
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH calendar AS (
  SELECT result_id, MAX(simulated_date) AS simulated_date, MAX(apply_at) AS apply_at, MAX(project) AS project
  FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar
  WHERE result_id IS NOT NULL AND {{simulated_dates_enabled}}
  GROUP BY result_id
),
detail AS (
  SELECT result_id, is_synthetic,
         MAX(run_date) AS run_date, MIN(run_ts) AS run_ts, MAX(apply_at) AS apply_at, MAX(project) AS project,
         MAX(execution_status) AS execution_status,
         MIN(start_timestamp_execution) AS start_timestamp_execution, MAX(end_timestamp_execution) AS end_timestamp_execution,
         MAX(tables_evaluated) AS tables_evaluated, MAX(tables_pass) AS tables_pass, MAX(tables_warning) AS tables_warning,
         MAX(tables_error) AS tables_error,
         COUNT(DISTINCT IF(table_status = 'code_error', result_table_id, NULL)) AS tables_code_error,
         COUNT(result_rule_id) AS rules_run,
         COUNT_IF(row_level = 'rule' AND status = 'pass') AS rules_pass,
         COUNT_IF(row_level = 'rule' AND status = 'warning') AS rules_warning,
         COUNT_IF(row_level = 'rule' AND status = 'error') AS rules_error,
         COUNT_IF(row_level = 'rule' AND status = 'code_error') AS rules_code_error,
         SUM(rows_evaluated) AS rows_evaluated, SUM(rows_failed) AS rows_failed, SUM(rows_quarantined) AS rows_quarantined
  FROM {{dq_catalog}}.{{dashboard_schema}}.v_results_all
  GROUP BY result_id, is_synthetic
),
real_runs AS (
  SELECT
    r.result_id,
    COALESCE(c.simulated_date, d.run_date, CAST(r.start_timestamp AS DATE))                     AS run_date,
    COALESCE(d.run_ts, r.start_timestamp)                                                        AS run_ts,
    COALESCE(c.apply_at, d.apply_at)                                                             AS apply_at,
    COALESCE(c.project, d.project, p.project)                                                    AS project,
    r.status AS execution_status, r.start_timestamp AS start_timestamp_execution, r.end_timestamp AS end_timestamp_execution,
    r.tables_evaluated, r.tables_pass, r.tables_warning, r.tables_error,
    COALESCE(d.tables_code_error, 0) AS tables_code_error, COALESCE(d.rules_run, 0) AS rules_run,
    COALESCE(d.rules_pass, 0) AS rules_pass, COALESCE(d.rules_warning, 0) AS rules_warning,
    COALESCE(d.rules_error, 0) AS rules_error, COALESCE(d.rules_code_error, 0) AS rules_code_error,
    d.rows_evaluated, d.rows_failed, d.rows_quarantined,
    c.simulated_date IS NOT NULL AS is_simulated_date,
    FALSE AS is_synthetic
  FROM {{dq_catalog}}.results.result r
  LEFT JOIN calendar c ON c.result_id = r.result_id
  LEFT JOIN detail d ON d.result_id = r.result_id AND NOT d.is_synthetic
  LEFT JOIN {{dq_catalog}}.metadata.project p ON p.project_id = r.project_id
),
synthetic_runs AS (
  SELECT result_id, run_date, run_ts, apply_at, project, execution_status, start_timestamp_execution,
         end_timestamp_execution, tables_evaluated, tables_pass, tables_warning, tables_error, tables_code_error,
         rules_run, rules_pass, rules_warning, rules_error, rules_code_error, rows_evaluated, rows_failed,
         rows_quarantined, TRUE AS is_simulated_date, TRUE AS is_synthetic
  FROM detail
  WHERE is_synthetic
),
runs AS (
  SELECT * FROM real_runs
  UNION ALL
  SELECT * FROM synthetic_runs
)
SELECT
  *,
  apply_at || CASE WHEN project IS NULL THEN '' ELSE ' / ' || project END                       AS scope,
  (unix_micros(end_timestamp_execution) - unix_micros(start_timestamp_execution)) / 1e6         AS duration_s,
  timestampadd(MICROSECOND, unix_micros(end_timestamp_execution) - unix_micros(start_timestamp_execution), run_ts)
                                                                                                AS run_end_ts,
  execution_status NOT IN ('code_error', 'running')                                             AS is_successful,
  ROW_NUMBER() OVER (PARTITION BY run_date, apply_at, project ORDER BY run_ts DESC, result_id DESC) = 1
                                                                                                AS is_latest_for_day
FROM runs;

-- v_assignment_active: one row per rule assignment (real configuration only) with the five is_active flags.
-- is_effective_active = assignment, template, policy, table and apply_at are all active.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active
COMMENT 'dq_dashboard sandbox - generated'
AS
SELECT
  ra.rule_assignment_id, ra.table_id, ra.project_id, ra.rule_template_id, ra.policy_id, ra.apply_at_id,
  p.project, m.layer AS data_layer, m.catalog, rp.criticality, rd.rule_dimension,
  m.`schema`, m.table AS table_name, m.table_nk, ap.apply_at, rt.name AS rule_name, ty.rule_type, rt.engine_type,
  rt.scope AS rule_scope, rp.category AS policy_category, rp.quarantine AS quarantine_status,
  ra.parameters_identifiers, ra.parameters_values,
  ra.is_active AS assignment_is_active, rt.is_active AS template_is_active, rp.is_active AS policy_is_active,
  m.is_active AS table_is_active, ap.is_active AS apply_at_is_active,
  COALESCE(ra.is_active AND rt.is_active AND rp.is_active AND m.is_active AND ap.is_active, FALSE) AS is_effective_active,
  ra.valid_from, ra.valid_to, ra.added_by
FROM {{dq_catalog}}.configuration.rule_assignment ra
LEFT JOIN {{dq_catalog}}.metadata.`table` m             ON m.table_id = ra.table_id
LEFT JOIN {{dq_catalog}}.metadata.project p             ON p.project_id = ra.project_id
LEFT JOIN {{dq_catalog}}.configuration.run_policy rp    ON rp.policy_id = ra.policy_id
LEFT JOIN {{dq_catalog}}.configuration.rule_template rt ON rt.rule_template_id = ra.rule_template_id
LEFT JOIN {{dq_catalog}}.metadata.rule_dimension rd     ON rd.rule_dimension_id = rt.rule_dimension_id
LEFT JOIN {{dq_catalog}}.metadata.rule_type ty          ON ty.rule_type_id = rt.rule_type_id
LEFT JOIN {{dq_catalog}}.configuration.apply_at ap      ON ap.apply_at_id = ra.apply_at_id;

-- v_data_mode: one row describing what the dashboard shows. data_mode = 'Demo data' while use_synthetic is true.
-- as_of_ts is the reference for "time since" widgets: the end of the newest run when dates are simulated
-- (simulated_dates_enabled), otherwise the current time.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_data_mode
COMMENT 'dq_dashboard sandbox - generated'
AS
SELECT
  {{use_synthetic}} AS use_synthetic,
  {{simulated_dates_enabled}} AS simulated_dates,
  CASE WHEN {{use_synthetic}} THEN 'Demo data' ELSE 'Real data' END AS data_mode,
  CASE WHEN {{use_synthetic}}
       THEN concat('Demo data: synthetic history ', s.first_day, ' to ', s.last_day, ', real runs ', r.first_day,
                   ' to ', r.last_day, '. Switch off with synthetic_enabled=false.')
       ELSE concat('Real data: runs ', COALESCE(CAST(r.first_day AS STRING), 'none'), ' to ',
                   COALESCE(CAST(r.last_day AS STRING), 'none'), '.') END AS data_mode_label,
  s.first_day AS synthetic_first_day, s.last_day AS synthetic_last_day,
  r.first_day AS real_first_day, r.last_day AS real_last_day,
  CASE WHEN {{simulated_dates_enabled}} THEN x.max_end ELSE current_timestamp() END AS as_of_ts
FROM (SELECT MIN(run_date) AS first_day, MAX(run_date) AS last_day FROM {{dq_catalog}}.{{dashboard_schema}}.v_runs WHERE is_synthetic) s
CROSS JOIN (SELECT MIN(run_date) AS first_day, MAX(run_date) AS last_day FROM {{dq_catalog}}.{{dashboard_schema}}.v_runs WHERE NOT is_synthetic) r
CROSS JOIN (SELECT MAX(run_end_ts) AS max_end FROM {{dq_catalog}}.{{dashboard_schema}}.v_runs) x;
