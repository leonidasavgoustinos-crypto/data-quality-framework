-- @targets: free, company
-- v_results_ext: the base view for the dashboard datasets. Rendered by scripts/run_ddl.py (env_config placeholders).
--
-- Grain: one row per rule result (results.result_rule), plus one row per table result that has no rule rows
-- (row_level = 'table', e.g. a table that failed before any rule ran). Executions without any table result are not
-- included; read results.result for those.
--
-- Reads the base results.* tables, not reporting.v_dq_results, so it does not depend on the reporting views of the
-- target (ids, error_message and numeric durations come straight from the base tables). Config is joined the same
-- way v_dq_results joins it (by id, no validity filter).
--
-- run_date / run_ts: with simulated_dates_enabled the run is mapped to dashboard_schema.run_calendar.simulated_date
-- (run_ts keeps the real time of day); otherwise, or without a calendar row, the real start of the execution.
-- is_latest_for_day: TRUE for the newest row per (run_date, table_id, apply_at, project, rule assignment). Daily
-- widgets filter on it so that reruns of a day are not double-counted; run-history widgets keep every row.
-- A table-level row (table fault) is not latest when a later call of the same (run_date, table_id, apply_at,
-- project) exists, e.g. a rerun after the fault.
-- rows_quarantined: rows written to quarantine by that rule result, from dashboard_schema.quarantine_snapshot
-- (the quarantine tables only hold the latest run). NULL = no snapshot row for that rule result.
-- crit_weight: high = 3, medium = 2, low = 1 (weighted DQ score). is_scored: pass, warning or error;
-- code_error and running are not scored. DQ score = sum(is_pass) / sum(is_scored).
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_results_ext
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH base AS (
  SELECT
    CASE WHEN u.result_rule_id IS NULL THEN 'table' ELSE 'rule' END             AS row_level,
    r.result_id, t.result_table_id, u.result_rule_id,
    t.table_id, t.project_id, u.rule_assignment_id, u.policy_id, u.rule_type_id, u.rule_dimension_id,
    COALESCE(u.status, t.status)                                                AS status,
    COALESCE(u.start_timestamp, t.start_timestamp)                              AS start_timestamp,
    COALESCE(u.end_timestamp, t.end_timestamp)                                  AS end_timestamp,
    u.rows_evaluated, u.rows_pass, u.rows_failed, u.statement, u.error_message,
    t.status AS table_status, t.rules_evaluated, t.rules_pass, t.rules_warning, t.rules_error,
    t.start_timestamp AS start_timestamp_table, t.end_timestamp AS end_timestamp_table,
    r.status AS execution_status, r.tables_evaluated, r.tables_pass, r.tables_warning, r.tables_error,
    r.start_timestamp AS start_timestamp_execution, r.end_timestamp AS end_timestamp_execution
  FROM {{dq_catalog}}.results.result_table t
  LEFT JOIN {{dq_catalog}}.results.result_rule u ON u.result_table_id = t.result_table_id
  LEFT JOIN {{dq_catalog}}.results.result r      ON r.result_id = t.result_id
),
quarantined AS (
  SELECT result_rule_id, SUM(`rows`) AS rows_quarantined, MAX(quarantine_table) AS quarantine_table
  FROM {{dq_catalog}}.{{dashboard_schema}}.quarantine_snapshot
  WHERE result_rule_id IS NOT NULL
  GROUP BY result_rule_id
),
calendar AS (
  SELECT result_id, MAX(simulated_date) AS simulated_date, MAX(partition_value) AS partition_value,
         MAX(apply_at) AS apply_at
  FROM {{dq_catalog}}.{{dashboard_schema}}.run_calendar
  WHERE result_id IS NOT NULL AND {{simulated_dates_enabled}}
  GROUP BY result_id
),
ext AS (
  SELECT
    b.row_level,
    COALESCE(c.simulated_date, CAST(COALESCE(b.start_timestamp_execution, b.start_timestamp_table) AS DATE)) AS run_date,
    CASE WHEN c.simulated_date IS NULL THEN COALESCE(b.start_timestamp_execution, b.start_timestamp_table)
         ELSE CAST(c.simulated_date AS TIMESTAMP)
              + (b.start_timestamp_execution - date_trunc('DAY', b.start_timestamp_execution)) END   AS run_ts,
    c.simulated_date IS NOT NULL                                                AS is_simulated_date,
    c.partition_value,
    b.result_id, b.result_table_id, b.result_rule_id, b.table_id, b.project_id, b.rule_assignment_id,
    b.policy_id, ra.rule_template_id, b.rule_type_id, b.rule_dimension_id,
    -- filter columns (same names in every dashboard view)
    p.project, m.layer AS data_layer, m.catalog, rp.criticality, rd.rule_dimension,
    COALESCE(c.apply_at, ap.apply_at)                                           AS apply_at,
    -- table
    m.schema, m.table AS table_name, m.table_nk, m.quarantine, m.filter_field, m.filter_field_type,
    p.project_description,
    -- rule
    rt.name AS rule_name, rt.description AS rule_description, ty.rule_type, rt.scope AS rule_scope,
    rt.is_reusable, rt.engine_type, b.statement, ra.parameters_identifiers, ra.parameters_values,
    -- policy
    rp.category AS policy_category, rp.quarantine AS quarantine_status, ap.is_project_specific,
    -- rule result
    b.status, b.start_timestamp, b.end_timestamp,
    (unix_micros(b.end_timestamp) - unix_micros(b.start_timestamp)) / 1e6       AS duration_s,
    b.rows_evaluated, b.rows_pass, b.rows_failed,
    q.rows_quarantined, q.quarantine_table,
    b.error_message,
    NULLIF(trim(substring_index(b.error_message, 'JVM stacktrace:', 1)), '')    AS error_summary,
    -- table and execution results
    b.table_status, b.rules_evaluated, b.rules_pass, b.rules_warning, b.rules_error,
    b.start_timestamp_table, b.end_timestamp_table,
    (unix_micros(b.end_timestamp_table) - unix_micros(b.start_timestamp_table)) / 1e6 AS table_duration_s,
    b.execution_status, b.tables_evaluated, b.tables_pass, b.tables_warning, b.tables_error,
    b.start_timestamp_execution, b.end_timestamp_execution,
    (unix_micros(b.end_timestamp_execution) - unix_micros(b.start_timestamp_execution)) / 1e6 AS execution_duration_s,
    -- scoring
    CASE rp.criticality WHEN 'high' THEN 3 WHEN 'medium' THEN 2 WHEN 'low' THEN 1 END AS crit_weight,
    b.status = 'pass'                                                           AS is_pass,
    b.status IN ('pass', 'warning', 'error')                                    AS is_scored
  FROM base b
  LEFT JOIN calendar c                                   ON c.result_id = b.result_id
  LEFT JOIN quarantined q                                ON q.result_rule_id = b.result_rule_id
  LEFT JOIN {{dq_catalog}}.metadata.`table` m            ON m.table_id = b.table_id
  LEFT JOIN {{dq_catalog}}.metadata.project p            ON p.project_id = b.project_id
  LEFT JOIN {{dq_catalog}}.metadata.rule_type ty         ON ty.rule_type_id = b.rule_type_id
  LEFT JOIN {{dq_catalog}}.configuration.rule_assignment ra ON ra.rule_assignment_id = b.rule_assignment_id
  LEFT JOIN {{dq_catalog}}.configuration.run_policy rp   ON rp.policy_id = ra.policy_id
  LEFT JOIN {{dq_catalog}}.configuration.rule_template rt ON rt.rule_template_id = ra.rule_template_id
  LEFT JOIN {{dq_catalog}}.metadata.rule_dimension rd    ON rd.rule_dimension_id = rt.rule_dimension_id
  LEFT JOIN {{dq_catalog}}.configuration.apply_at ap     ON ap.apply_at_id = ra.apply_at_id
)
SELECT
  *,
  -- latest_flag:begin (same expression in 06_dashboard_views.sql v_results_all; tested by tests/test_latest_flag.py)
  ROW_NUMBER() OVER (
    PARTITION BY run_date, table_id, apply_at, project, COALESCE(rule_assignment_id, -1)
    ORDER BY start_timestamp DESC, result_id DESC
  ) = 1
  AND (row_level = 'rule'
       OR named_struct('ts', COALESCE(start_timestamp_execution, start_timestamp), 'id', result_id)
          <=> MAX(named_struct('ts', COALESCE(start_timestamp_execution, start_timestamp), 'id', result_id))
              OVER (PARTITION BY run_date, table_id, apply_at, project)) AS is_latest_for_day
  -- latest_flag:end
FROM ext;
