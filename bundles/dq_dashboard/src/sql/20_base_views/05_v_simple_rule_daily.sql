-- Owner: eva.routsi@ms.d-one.ai. Copied from the live view analytics_dq_dev.reporting.v_simple_rule_daily on 2026-11-26 (source of truth).
-- Grain: one row per rule_key (rule assignment, or result_rule_id when there is no assignment) and run day.
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_simple_rule_daily AS
WITH daily AS (
  SELECT
    COALESCE(CAST(r.rule_assignment_id AS STRING), r.result_rule_id) AS rule_key,
    r.run_date,
    r.rule_assignment_id,
    r.result_rule_id,
    r.table_id,
    r.table_label,
    r.project_label,
    r.rule_name,
    r.rule_description,
    r.dimension_label,
    r.criticality_label,
    r.quarantine,
    r.checked_columns,
    r.status,
    r.status_label,
    r.status_symbol,
    r.status_order,
    r.rows_evaluated,
    r.rows_pass,
    r.rows_failed,
    CASE
      WHEN r.status IN ('warning', 'error') THEN 'failing'
      WHEN r.status = 'pass'                THEN 'pass'
      ELSE 'code_error'
    END                                                              AS state
  FROM ${catalog}.${schema}.v_simple_results r
  WHERE r.is_latest_for_day
    AND r.result_rule_id IS NOT NULL
    AND r.status_label IS NOT NULL
),
marked AS (
  SELECT
    d.*,
    CASE
      WHEN LAG(d.state) OVER (PARTITION BY d.rule_key ORDER BY d.run_date) <=> d.state THEN 0
      ELSE 1
    END AS is_streak_start
  FROM daily d
),
grouped AS (
  SELECT
    m.*,
    SUM(m.is_streak_start) OVER (
      PARTITION BY m.rule_key ORDER BY m.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS streak_id
  FROM marked m
)
SELECT
  rule_key,
  run_date,
  rule_assignment_id,
  result_rule_id,
  table_id,
  table_label,
  project_label,
  rule_name,
  rule_description,
  dimension_label,
  criticality_label,
  quarantine,
  checked_columns,
  status,
  status_label,
  status_symbol,
  status_order,
  rows_evaluated,
  rows_pass,
  rows_failed,
  state,
  MIN(run_date) OVER (PARTITION BY rule_key, streak_id)                     AS streak_start,
  ROW_NUMBER() OVER (PARTITION BY rule_key, streak_id ORDER BY run_date)    AS streak_pos
FROM grouped;
