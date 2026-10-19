-- @targets: free, company
-- v_rule_daily: one row per rule assignment and run day (the latest rule result of the day, from v_results_all),
-- with the status history logic used by the Tables, Rules and Incidents pages. Requires 06_dashboard_views.sql.
-- Rendered by scripts/run_ddl.py.
--
-- state:        'failing' (warning or error), 'code_error' (not evaluated) or 'pass'.
-- run_seq:      1, 2, ... per assignment in run-day order (missed days do not break a series).
-- is_flip:      the scored result (pass vs warning/error) differs from the previous scored day of the assignment;
--               code_error days are skipped. Flapping = flips counted over a window (e.g. >= 5 in 14 days).
-- streak_*:     consecutive run days with the same state: first day of the streak and position of this day inside
--               it (1 = first day). "Failing since" = streak_start of a 'failing' day.
-- rule_label:   rule name with its column(s); assignment_label adds table and apply_at.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_rule_daily
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH d AS (
  SELECT
    run_date, recorded_run_date, run_ts, is_synthetic, result_id, result_rule_id, partition_value,
    rule_assignment_id, table_id, project, data_layer, catalog, criticality, rule_dimension, apply_at,
    `schema`, table_name, concat(`schema`, '.', table_name) AS table_label,
    rule_name, rule_template_id, rule_type, rule_scope, policy_category, quarantine_status,
    parameters_identifiers, parameters_values,
    concat(rule_name, COALESCE(concat('(', COALESCE(get_json_object(parameters_identifiers, '$.column'),
                                                     array_join(from_json(get_json_object(parameters_identifiers, '$.columns'),
                                                                          'ARRAY<STRING>'), ', ')), ')'), ''))
                                                                          AS rule_label,
    status, is_pass, is_scored, crit_weight, rows_evaluated, rows_failed, rows_quarantined, error_summary,
    duration_s,
    CASE WHEN status IN ('warning', 'error') THEN 'failing' WHEN status = 'pass' THEN 'pass' ELSE 'code_error' END AS state
  FROM {{dq_catalog}}.{{dashboard_schema}}.v_results_all
  WHERE row_level = 'rule' AND is_latest_for_day
),
seq AS (
  SELECT
    *,
    concat(rule_label, ' · ', table_name, ' · ', apply_at)                     AS assignment_label,
    ROW_NUMBER() OVER w                                                         AS run_seq,
    LAG(status) OVER w                                                          AS prev_status,
    LAG(state) OVER w                                                           AS prev_state,
    LAST_VALUE(IF(is_scored, is_pass, NULL)) IGNORE NULLS OVER (
      PARTITION BY rule_assignment_id ORDER BY run_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )                                                                           AS prev_scored_pass
  FROM d
  WINDOW w AS (PARTITION BY rule_assignment_id ORDER BY run_date)
),
chg AS (
  SELECT
    *,
    is_scored AND prev_scored_pass IS NOT NULL AND is_pass <> prev_scored_pass  AS is_flip,
    prev_state IS NULL OR state <> prev_state                                   AS is_streak_start
  FROM seq
)
-- every window uses the same partitioning and order (one shuffle): the current streak starts at the newest day
-- up to this one where the state changed
SELECT
  * EXCEPT (prev_scored_pass, prev_state, is_streak_start),
  MAX(IF(is_streak_start, run_date, NULL)) OVER r                               AS streak_start,
  run_seq - MAX(IF(is_streak_start, run_seq, NULL)) OVER r + 1                  AS streak_pos
FROM chg
WINDOW r AS (PARTITION BY rule_assignment_id ORDER BY run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
