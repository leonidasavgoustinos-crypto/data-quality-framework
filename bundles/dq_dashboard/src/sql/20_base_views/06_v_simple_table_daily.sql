-- Base view definition, agreed on 2026-11-26. Every deploy builds its own copy in its reporting schema.
-- Grain: one row per table_label, project_label and run day; worst_status = worst rule status of the day.
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_table_daily AS
WITH daily AS (
  SELECT
    CASE WHEN COUNT(DISTINCT r.table_id) = 1 AND COUNT(r.table_id) = COUNT(*)
         THEN MAX(r.table_id) END AS table_id,
    r.table_label,
    r.project_label,
    r.run_date,
    MIN(r.status_order)   AS status_order,
    SUM(r.rows_failed)    AS rows_failed,
    MAX(r.rows_evaluated) AS rows_evaluated
  FROM `${catalog}`.`${schema}`.v_simple_results r
  WHERE r.is_latest_for_day
    AND r.status_label IS NOT NULL
  GROUP BY r.table_label, r.project_label, r.run_date
),
marked AS (
  SELECT
    d.*,
    s.status            AS worst_status,
    s.status_label,
    s.status_symbol,
    s.status = 'pass'   AS is_pass
  FROM daily d
  JOIN `${catalog}`.`${schema}`.v_simple_status s
    ON s.status_order = d.status_order
),
streaks AS (
  SELECT
    m.*,
    CASE
      WHEN LAG(m.is_pass) OVER (PARTITION BY m.table_label, m.project_label ORDER BY m.run_date) <=> m.is_pass THEN 0
      ELSE 1
    END AS is_streak_start
  FROM marked m
),
grouped AS (
  SELECT
    s.*,
    SUM(s.is_streak_start) OVER (
      PARTITION BY s.table_label, s.project_label ORDER BY s.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS streak_id
  FROM streaks s
)
SELECT
  table_id,
  table_label,
  project_label,
  run_date,
  worst_status,
  status_label,
  status_symbol,
  status_order,
  rows_failed,
  rows_evaluated,
  CASE WHEN is_pass THEN NULL
       ELSE MIN(run_date) OVER (PARTITION BY table_label, project_label, streak_id)
  END                                                                                          AS streak_start,
  CASE WHEN is_pass THEN 0
       ELSE ROW_NUMBER() OVER (PARTITION BY table_label, project_label, streak_id ORDER BY run_date)
  END                                                                                          AS streak_runs
FROM grouped;
