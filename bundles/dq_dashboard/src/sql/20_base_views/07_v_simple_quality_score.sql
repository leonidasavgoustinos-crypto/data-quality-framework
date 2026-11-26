-- Owner: eva.routsi@ms.d-one.ai. Copied from the live view analytics_dq_dev.reporting.v_simple_quality_score on 2026-11-26 (source of truth).
-- Grain: one row per as_of in {E (k = 1), P (k = 2)}: quality score.
-- Score parameters come from bundle variables score_* (defaults = live values).
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_simple_quality_score AS
WITH params AS (
  SELECT
    ${score_k_shrink}   AS k_shrink,
    ${score_stale_factor}   AS stale_factor,
    ${score_interval_window_days}  AS interval_window_days,
    ${score_min_days_for_interval}   AS min_days_for_interval,
    ${score_warning_penalty} AS warning_penalty
),
known_tables AS (
  SELECT table_id
  FROM ${source_catalog}.${metadata_schema}.`table`
),
run_days AS (
  SELECT DISTINCT run_date
  FROM ${catalog}.${schema}.v_simple_results
  WHERE result_rule_id IS NOT NULL
),
as_of_days AS (
  SELECT 1 AS k, MAX(run_date) AS as_of
  FROM run_days
  UNION ALL
  SELECT 2 AS k, MAX(run_date) AS as_of
  FROM run_days
  WHERE run_date < (SELECT MAX(run_date) FROM run_days)
),
scored AS (
  SELECT
    r.table_id,
    r.run_date,
    r.status,
    r.crit_weight AS w,
    r.rows_evaluated,
    r.rows_pass,
    CASE
      WHEN r.status = 'pass'                               THEN 1.0
      WHEN r.status = 'error'   AND r.rows_evaluated > 0   THEN r.rows_pass / r.rows_evaluated
      WHEN r.status = 'warning' AND r.rows_evaluated > 0   THEN 1 - (1 - r.rows_pass / r.rows_evaluated) * p.warning_penalty
      WHEN r.status = 'error'                              THEN 0.0
      WHEN r.status = 'warning'                            THEN 1 - p.warning_penalty
    END           AS credit
  FROM ${catalog}.${schema}.v_simple_results r
  CROSS JOIN params p
  WHERE r.is_latest_for_day
    AND r.is_scored
    AND r.table_id IN (SELECT table_id FROM known_tables)
),
table_days AS (
  SELECT DISTINCT
    r.table_id,
    r.run_date
  FROM ${catalog}.${schema}.v_simple_results r
  WHERE r.result_rule_id IS NOT NULL
    AND r.table_id IN (SELECT table_id FROM known_tables)
),
table_last AS (
  SELECT
    a.k,
    a.as_of,
    d.table_id,
    MAX(d.run_date) AS last_day
  FROM as_of_days a
  JOIN table_days d
    ON d.run_date <= a.as_of
  GROUP BY a.k, a.as_of, d.table_id
),
window_days AS (
  SELECT
    a.k,
    d.table_id,
    d.run_date,
    DATEDIFF(d.run_date, LAG(d.run_date) OVER (PARTITION BY a.k, d.table_id ORDER BY d.run_date)) AS gap_days
  FROM as_of_days a
  CROSS JOIN params p
  JOIN table_days d
    ON d.run_date <= a.as_of
   AND d.run_date >  DATE_SUB(a.as_of, p.interval_window_days)
),
intervals AS (
  SELECT
    k,
    table_id,
    COUNT(*)         AS n_days,
    MEDIAN(gap_days) AS usual_interval
  FROM window_days
  GROUP BY k, table_id
),
table_state AS (
  SELECT
    l.k,
    l.table_id,
    l.last_day,
    COALESCE(i.n_days, 0) >= p.min_days_for_interval
      AND DATEDIFF(l.as_of, l.last_day) > p.stale_factor * GREATEST(COALESCE(i.usual_interval, 1), 1) AS is_stale
  FROM table_last l
  CROSS JOIN params p
  LEFT JOIN intervals i
    ON i.k = l.k
   AND i.table_id = l.table_id
),
table_raw AS (
  SELECT
    s.k,
    s.table_id,
    SUM(c.w * c.credit)                               AS sum_wc,
    SUM(c.w)                                          AS sum_w,
    COUNT(*)                                          AS n_scored,
    SUM(CASE WHEN c.status = 'pass' THEN 1 ELSE 0 END) AS n_pass,
    SUM(c.rows_pass)                                  AS rows_pass,
    SUM(c.rows_evaluated)                             AS rows_evaluated
  FROM table_state s
  JOIN scored c
    ON c.table_id = s.table_id
   AND c.run_date = s.last_day
  WHERE NOT s.is_stale
  GROUP BY s.k, s.table_id
),
pooled AS (
  SELECT
    k,
    SUM(sum_wc) / NULLIF(SUM(sum_w), 0) AS m
  FROM table_raw
  GROUP BY k
),
table_score AS (
  SELECT
    t.*,
    t.sum_wc / NULLIF(t.sum_w, 0)                                     AS raw_score,
    (t.sum_wc + p.k_shrink * m.m) / NULLIF(t.sum_w + p.k_shrink, 0)   AS table_score
  FROM table_raw t
  JOIN pooled m
    ON m.k = t.k
  CROSS JOIN params p
),
stale AS (
  SELECT
    k,
    SUM(CASE WHEN is_stale THEN 1 ELSE 0 END) AS stale_tables
  FROM table_state
  GROUP BY k
)
SELECT
  a.k,
  a.as_of,
  AVG(ts.table_score)                                    AS score,
  COUNT(ts.table_id)                                     AS tables_scored,
  COALESCE(MAX(st.stale_tables), 0)                      AS stale_tables,
  SUM(ts.n_pass) / NULLIF(SUM(ts.n_scored), 0)           AS rules_score,
  SUM(ts.rows_pass) / NULLIF(SUM(ts.rows_evaluated), 0)  AS row_pass_rate
FROM as_of_days a
LEFT JOIN table_score ts
  ON ts.k = a.k
LEFT JOIN stale st
  ON st.k = a.k
WHERE a.as_of IS NOT NULL
GROUP BY a.k, a.as_of;
