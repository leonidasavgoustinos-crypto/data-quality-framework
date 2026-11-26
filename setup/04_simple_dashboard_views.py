# Databricks notebook source
# DBTITLE 1,v_simple_status (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per framework status (four statuses); status_order 1 = worst; status_bg_color = light cell background.
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_status AS
# MAGIC SELECT
# MAGIC   status,
# MAGIC   status_label,
# MAGIC   status_symbol,
# MAGIC   status_order,
# MAGIC   status_color,
# MAGIC   status_bg_color
# MAGIC FROM VALUES
# MAGIC   ('error',      'Αποτυχία',      '✕', 1, '#D64545', '#FBE3E1'),
# MAGIC   ('warning',    'Προειδοποίηση', '!', 2, '#E3A033', '#FDF0D6'),
# MAGIC   ('code_error', 'Δεν έτρεξε',    '?', 3, '#8A8F98', '#ECEEF1'),
# MAGIC   ('pass',       'Επιτυχία',      '✓', 4, '#2E8B57', '#E2F3E8')
# MAGIC   AS t(status, status_label, status_symbol, status_order, status_color, status_bg_color)

# COMMAND ----------

# DBTITLE 1,v_simple_dimension (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per known rule_dimension name (section 4.3).
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_dimension AS
# MAGIC SELECT
# MAGIC   rule_dimension,
# MAGIC   dimension_label
# MAGIC FROM VALUES
# MAGIC   ('completeness', 'Πληρότητα'),
# MAGIC   ('validity',     'Εγκυρότητα'),
# MAGIC   ('uniqueness',   'Μοναδικότητα'),
# MAGIC   ('consistency',  'Συνέπεια'),
# MAGIC   ('timeliness',   'Επικαιρότητα'),
# MAGIC   ('accuracy',     'Ακρίβεια'),
# MAGIC   ('test_ui',      'Δοκιμή (test_ui)')
# MAGIC   AS t(rule_dimension, dimension_label)

# COMMAND ----------

# DBTITLE 1,v_simple_results (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per rule result (results.result_rule) plus one row per table result without rule rows. Base of every dataset.
# MAGIC -- Notes: table_label is «Άγνωστος πίνακας» for every unknown table (merged, decision C1 2026-11-02);
# MAGIC --        dimension_label is NULL when unknown (datasets show «Άγνωστο είδος», decision C2).
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_results AS
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     3 AS w_high,
# MAGIC     2 AS w_medium,
# MAGIC     1 AS w_low,
# MAGIC     2 AS w_default
# MAGIC ),
# MAGIC result_rows AS (
# MAGIC   SELECT
# MAGIC     rr.result_rule_id,
# MAGIC     rr.result_table_id,
# MAGIC     rr.result_id,
# MAGIC     rr.project_id,
# MAGIC     rr.rule_assignment_id,
# MAGIC     rr.policy_id,
# MAGIC     rr.rule_dimension_id,
# MAGIC     rr.status,
# MAGIC     rr.rows_evaluated,
# MAGIC     rr.rows_pass,
# MAGIC     rr.rows_failed,
# MAGIC     rr.start_timestamp,
# MAGIC     rr.end_timestamp
# MAGIC   FROM analytics_dq_dev.results.result_rule rr
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     CAST(NULL AS STRING) AS result_rule_id,
# MAGIC     t.result_table_id,
# MAGIC     t.result_id,
# MAGIC     t.project_id,
# MAGIC     CAST(NULL AS BIGINT) AS rule_assignment_id,
# MAGIC     CAST(NULL AS BIGINT) AS policy_id,
# MAGIC     CAST(NULL AS BIGINT) AS rule_dimension_id,
# MAGIC     t.status,
# MAGIC     CAST(NULL AS INT)    AS rows_evaluated,
# MAGIC     CAST(NULL AS INT)    AS rows_pass,
# MAGIC     CAST(NULL AS INT)    AS rows_failed,
# MAGIC     t.start_timestamp,
# MAGIC     t.end_timestamp
# MAGIC   FROM analytics_dq_dev.results.result_table t
# MAGIC   WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM analytics_dq_dev.results.result_rule rr
# MAGIC     WHERE rr.result_table_id = t.result_table_id
# MAGIC   )
# MAGIC ),
# MAGIC linked AS (
# MAGIC   SELECT
# MAGIC     x.*,
# MAGIC     COALESCE(x.result_rule_id, x.result_table_id) AS row_key,
# MAGIC     t.table_id                                    AS rt_table_id,
# MAGIC     t.project_id                                  AS rt_project_id,
# MAGIC     t.start_timestamp                             AS table_start_timestamp,
# MAGIC     COALESCE(x.result_id, t.result_id)            AS direct_result_id
# MAGIC   FROM result_rows x
# MAGIC   LEFT JOIN analytics_dq_dev.results.result_table t
# MAGIC     ON t.result_table_id = x.result_table_id
# MAGIC ),
# MAGIC window_execution AS (
# MAGIC   SELECT
# MAGIC     l.row_key,
# MAGIC     r.result_id,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY l.row_key ORDER BY r.start_timestamp DESC) AS rn
# MAGIC   FROM linked l
# MAGIC   JOIN analytics_dq_dev.results.result r
# MAGIC     ON l.direct_result_id IS NULL
# MAGIC    AND l.start_timestamp BETWEEN r.start_timestamp AND r.end_timestamp
# MAGIC ),
# MAGIC enriched AS (
# MAGIC   SELECT
# MAGIC     CAST(COALESCE(e.start_timestamp, l.table_start_timestamp, l.start_timestamp) AS DATE) AS run_date,
# MAGIC     l.start_timestamp,
# MAGIC     l.end_timestamp,
# MAGIC     e.result_id,
# MAGIC     l.result_table_id,
# MAGIC     l.result_rule_id,
# MAGIC     l.rule_assignment_id,
# MAGIC     COALESCE(l.rt_table_id, ra.table_id)                                   AS table_id,
# MAGIC     COALESCE(p.project, 'Χωρίς τομέα')                                     AS project_label,
# MAGIC     COALESCE(concat(m.schema, '.', m.`table`), 'Άγνωστος πίνακας')        AS table_label,
# MAGIC     l.status,
# MAGIC     s.status_label,
# MAGIC     s.status_symbol,
# MAGIC     s.status_order,
# MAGIC     tpl.name                                                               AS rule_name,
# MAGIC     COALESCE(NULLIF(TRIM(tpl.description), ''), tpl.name)                  AS rule_description,
# MAGIC     CASE
# MAGIC       WHEN rd.rule_dimension IS NULL THEN NULL
# MAGIC       ELSE COALESCE(d.dimension_label,
# MAGIC                     concat(upper(substr(rd.rule_dimension, 1, 1)), substr(rd.rule_dimension, 2)))
# MAGIC     END                                                                    AS dimension_label,
# MAGIC     CASE lower(rp.criticality)
# MAGIC       WHEN 'high'   THEN 'Υψηλή'
# MAGIC       WHEN 'medium' THEN 'Μεσαία'
# MAGIC       WHEN 'low'    THEN 'Χαμηλή'
# MAGIC     END                                                                    AS criticality_label,
# MAGIC     CASE lower(rp.criticality)
# MAGIC       WHEN 'high'   THEN prm.w_high
# MAGIC       WHEN 'medium' THEN prm.w_medium
# MAGIC       WHEN 'low'    THEN prm.w_low
# MAGIC       ELSE prm.w_default
# MAGIC     END                                                                    AS crit_weight,
# MAGIC     rp.quarantine                                                          AS quarantine,
# MAGIC     COALESCE(
# MAGIC       get_json_object(ra.parameters_identifiers, '$.column'),
# MAGIC       array_join(from_json(get_json_object(ra.parameters_identifiers, '$.columns'), 'array<string>'), ', ')
# MAGIC     )                                                                      AS checked_columns,
# MAGIC     l.rows_evaluated,
# MAGIC     l.rows_pass,
# MAGIC     l.rows_failed,
# MAGIC     COALESCE(l.status IN ('pass', 'warning', 'error'), FALSE)              AS is_scored
# MAGIC   FROM linked l
# MAGIC   CROSS JOIN params prm
# MAGIC   LEFT JOIN window_execution w
# MAGIC     ON w.row_key = l.row_key
# MAGIC    AND w.rn = 1
# MAGIC   LEFT JOIN analytics_dq_dev.results.result e
# MAGIC     ON e.result_id = COALESCE(l.direct_result_id, w.result_id)
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.rule_assignment ra
# MAGIC     ON ra.rule_assignment_id = l.rule_assignment_id
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.rule_template tpl
# MAGIC     ON tpl.rule_template_id = ra.rule_template_id
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.run_policy rp
# MAGIC     ON rp.policy_id = COALESCE(l.policy_id, ra.policy_id)
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.rule_dimension rd
# MAGIC     ON rd.rule_dimension_id = COALESCE(l.rule_dimension_id, tpl.rule_dimension_id)
# MAGIC   LEFT JOIN analytics_dq_dev.reporting.v_simple_dimension d
# MAGIC     ON d.rule_dimension = rd.rule_dimension
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.`table` m
# MAGIC     ON m.table_id = COALESCE(l.rt_table_id, ra.table_id)
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.project p
# MAGIC     ON p.project_id = COALESCE(l.project_id, l.rt_project_id, e.project_id, ra.project_id)
# MAGIC   LEFT JOIN analytics_dq_dev.reporting.v_simple_status s
# MAGIC     ON s.status = l.status
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY run_date, COALESCE(CAST(rule_assignment_id AS STRING), result_rule_id, result_table_id)
# MAGIC     ORDER BY start_timestamp DESC, result_rule_id DESC
# MAGIC   ) = 1 AS is_latest_for_day
# MAGIC FROM enriched

# COMMAND ----------

# DBTITLE 1,v_simple_executions (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per execution (results.result); has_rule_results = the execution wrote at least one rule result.
# MAGIC -- Used by the dashboard for «ολοκληρώθηκε» (1.1) and «εκτελέσεις του ελέγχου» (2.1).
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_executions AS
# MAGIC SELECT
# MAGIC   CAST(r.start_timestamp AS DATE)            AS run_date,
# MAGIC   r.result_id,
# MAGIC   r.start_timestamp,
# MAGIC   r.end_timestamp,
# MAGIC   r.status,
# MAGIC   COALESCE(p.project, 'Χωρίς τομέα')         AS project_label,
# MAGIC   EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM analytics_dq_dev.reporting.v_simple_results v
# MAGIC     WHERE v.result_id = r.result_id
# MAGIC       AND v.result_rule_id IS NOT NULL
# MAGIC   )                                          AS has_rule_results
# MAGIC FROM analytics_dq_dev.results.result r
# MAGIC LEFT JOIN analytics_dq_dev.metadata.project p
# MAGIC   ON p.project_id = r.project_id

# COMMAND ----------

# DBTITLE 1,v_simple_runs (unused)
# MAGIC %sql
# MAGIC -- UNUSED since 2026-11-02: the dashboard uses Eva's v_simple_executions instead. Kept until the owner decides to drop it.
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_runs AS
# MAGIC -- Grain: one row per execution (call) of the framework (results.result), with or without rule results.
# MAGIC -- Used for «ολοκληρώθηκε» (end time of the latest execution) and «εκτελέσεις του ελέγχου» (all executions ever).
# MAGIC -- run_date      : CAST(start_timestamp AS DATE), same definition as v_simple_results.
# MAGIC -- project_label : metadata.project.project, «Χωρίς τομέα» when missing.
# MAGIC SELECT
# MAGIC   r.result_id,
# MAGIC   CAST(r.start_timestamp AS DATE)                                  AS run_date,
# MAGIC   r.start_timestamp,
# MAGIC   r.end_timestamp,
# MAGIC   r.status,
# MAGIC   COALESCE(p.project, 'Χωρίς τομέα')                               AS project_label,
# MAGIC   r.tables_evaluated
# MAGIC FROM analytics_dq_dev.results.result r
# MAGIC LEFT JOIN analytics_dq_dev.metadata.project p
# MAGIC   ON r.project_id = p.project_id;

# COMMAND ----------

# DBTITLE 1,v_simple_rule_daily (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per rule_key (rule assignment, or result_rule_id when there is no assignment) and run day.
# MAGIC -- Note: rows without rule_assignment_id are kept here; datasets of page 4 filter them out (decision C3).
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_rule_daily AS
# MAGIC WITH daily AS (
# MAGIC   SELECT
# MAGIC     COALESCE(CAST(r.rule_assignment_id AS STRING), r.result_rule_id) AS rule_key,
# MAGIC     r.run_date,
# MAGIC     r.rule_assignment_id,
# MAGIC     r.result_rule_id,
# MAGIC     r.table_id,
# MAGIC     r.table_label,
# MAGIC     r.project_label,
# MAGIC     r.rule_name,
# MAGIC     r.rule_description,
# MAGIC     r.dimension_label,
# MAGIC     r.criticality_label,
# MAGIC     r.quarantine,
# MAGIC     r.checked_columns,
# MAGIC     r.status,
# MAGIC     r.status_label,
# MAGIC     r.status_symbol,
# MAGIC     r.status_order,
# MAGIC     r.rows_evaluated,
# MAGIC     r.rows_pass,
# MAGIC     r.rows_failed,
# MAGIC     CASE
# MAGIC       WHEN r.status IN ('warning', 'error') THEN 'failing'
# MAGIC       WHEN r.status = 'pass'                THEN 'pass'
# MAGIC       ELSE 'code_error'
# MAGIC     END                                                              AS state
# MAGIC   FROM analytics_dq_dev.reporting.v_simple_results r
# MAGIC   WHERE r.is_latest_for_day
# MAGIC     AND r.result_rule_id IS NOT NULL
# MAGIC     AND r.status_label IS NOT NULL
# MAGIC ),
# MAGIC marked AS (
# MAGIC   SELECT
# MAGIC     d.*,
# MAGIC     CASE
# MAGIC       WHEN LAG(d.state) OVER (PARTITION BY d.rule_key ORDER BY d.run_date) <=> d.state THEN 0
# MAGIC       ELSE 1
# MAGIC     END AS is_streak_start
# MAGIC   FROM daily d
# MAGIC ),
# MAGIC grouped AS (
# MAGIC   SELECT
# MAGIC     m.*,
# MAGIC     SUM(m.is_streak_start) OVER (
# MAGIC       PARTITION BY m.rule_key ORDER BY m.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS streak_id
# MAGIC   FROM marked m
# MAGIC )
# MAGIC SELECT
# MAGIC   rule_key,
# MAGIC   run_date,
# MAGIC   rule_assignment_id,
# MAGIC   result_rule_id,
# MAGIC   table_id,
# MAGIC   table_label,
# MAGIC   project_label,
# MAGIC   rule_name,
# MAGIC   rule_description,
# MAGIC   dimension_label,
# MAGIC   criticality_label,
# MAGIC   quarantine,
# MAGIC   checked_columns,
# MAGIC   status,
# MAGIC   status_label,
# MAGIC   status_symbol,
# MAGIC   status_order,
# MAGIC   rows_evaluated,
# MAGIC   rows_pass,
# MAGIC   rows_failed,
# MAGIC   state,
# MAGIC   MIN(run_date) OVER (PARTITION BY rule_key, streak_id)                     AS streak_start,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY rule_key, streak_id ORDER BY run_date)    AS streak_pos
# MAGIC FROM grouped

# COMMAND ----------

# DBTITLE 1,v_simple_table_daily (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per table_label, project_label and run day; worst_status = worst rule status of the day.
# MAGIC -- Note: all unknown tables share the label «Άγνωστος πίνακας» and merge into one row per day (decision C1).
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_table_daily AS
# MAGIC WITH daily AS (
# MAGIC   SELECT
# MAGIC     CASE WHEN COUNT(DISTINCT r.table_id) = 1 AND COUNT(r.table_id) = COUNT(*)
# MAGIC          THEN MAX(r.table_id) END AS table_id,
# MAGIC     r.table_label,
# MAGIC     r.project_label,
# MAGIC     r.run_date,
# MAGIC     MIN(r.status_order)   AS status_order,
# MAGIC     SUM(r.rows_failed)    AS rows_failed,
# MAGIC     MAX(r.rows_evaluated) AS rows_evaluated
# MAGIC   FROM analytics_dq_dev.reporting.v_simple_results r
# MAGIC   WHERE r.is_latest_for_day
# MAGIC     AND r.status_label IS NOT NULL
# MAGIC   GROUP BY r.table_label, r.project_label, r.run_date
# MAGIC ),
# MAGIC marked AS (
# MAGIC   SELECT
# MAGIC     d.*,
# MAGIC     s.status            AS worst_status,
# MAGIC     s.status_label,
# MAGIC     s.status_symbol,
# MAGIC     s.status = 'pass'   AS is_pass
# MAGIC   FROM daily d
# MAGIC   JOIN analytics_dq_dev.reporting.v_simple_status s
# MAGIC     ON s.status_order = d.status_order
# MAGIC ),
# MAGIC streaks AS (
# MAGIC   SELECT
# MAGIC     m.*,
# MAGIC     CASE
# MAGIC       WHEN LAG(m.is_pass) OVER (PARTITION BY m.table_label, m.project_label ORDER BY m.run_date) <=> m.is_pass THEN 0
# MAGIC       ELSE 1
# MAGIC     END AS is_streak_start
# MAGIC   FROM marked m
# MAGIC ),
# MAGIC grouped AS (
# MAGIC   SELECT
# MAGIC     s.*,
# MAGIC     SUM(s.is_streak_start) OVER (
# MAGIC       PARTITION BY s.table_label, s.project_label ORDER BY s.run_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS streak_id
# MAGIC   FROM streaks s
# MAGIC )
# MAGIC SELECT
# MAGIC   table_id,
# MAGIC   table_label,
# MAGIC   project_label,
# MAGIC   run_date,
# MAGIC   worst_status,
# MAGIC   status_label,
# MAGIC   status_symbol,
# MAGIC   status_order,
# MAGIC   rows_failed,
# MAGIC   rows_evaluated,
# MAGIC   CASE WHEN is_pass THEN NULL
# MAGIC        ELSE MIN(run_date) OVER (PARTITION BY table_label, project_label, streak_id)
# MAGIC   END                                                                                          AS streak_start,
# MAGIC   CASE WHEN is_pass THEN 0
# MAGIC        ELSE ROW_NUMBER() OVER (PARTITION BY table_label, project_label, streak_id ORDER BY run_date)
# MAGIC   END                                                                                          AS streak_runs
# MAGIC FROM grouped

# COMMAND ----------

# DBTITLE 1,v_simple_quality_score (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per as_of in {E (k = 1), P (k = 2)}: «Βαθμός ποιότητας», section 5 of the handoff.
# MAGIC -- The comparison (score at E - score at P) * 100 is computed in dataset ds_s1_kpis.
# MAGIC -- Known limits: a row that fails two rules is counted twice; the weights and k_shrink are conventions, not facts.
# MAGIC -- OPEN POINT (C4, 2026-11-02): known_tables has no is_active filter. The owner's rule is "score only tables that exist
# MAGIC -- and are active in metadata.table". Asked Eva to add it; identical numbers today (no inactive tables). SQL left as live.
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_quality_score AS
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     3   AS k_shrink,
# MAGIC     2   AS stale_factor,
# MAGIC     60  AS interval_window_days,
# MAGIC     3   AS min_days_for_interval,
# MAGIC     0.5 AS warning_penalty
# MAGIC ),
# MAGIC known_tables AS (
# MAGIC   SELECT table_id
# MAGIC   FROM analytics_dq_dev.metadata.`table`
# MAGIC ),
# MAGIC run_days AS (
# MAGIC   SELECT DISTINCT run_date
# MAGIC   FROM analytics_dq_dev.reporting.v_simple_results
# MAGIC   WHERE result_rule_id IS NOT NULL
# MAGIC ),
# MAGIC as_of_days AS (
# MAGIC   SELECT 1 AS k, MAX(run_date) AS as_of
# MAGIC   FROM run_days
# MAGIC   UNION ALL
# MAGIC   SELECT 2 AS k, MAX(run_date) AS as_of
# MAGIC   FROM run_days
# MAGIC   WHERE run_date < (SELECT MAX(run_date) FROM run_days)
# MAGIC ),
# MAGIC scored AS (
# MAGIC   SELECT
# MAGIC     r.table_id,
# MAGIC     r.run_date,
# MAGIC     r.status,
# MAGIC     r.crit_weight AS w,
# MAGIC     r.rows_evaluated,
# MAGIC     r.rows_pass,
# MAGIC     CASE
# MAGIC       WHEN r.status = 'pass'                               THEN 1.0
# MAGIC       WHEN r.status = 'error'   AND r.rows_evaluated > 0   THEN r.rows_pass / r.rows_evaluated
# MAGIC       WHEN r.status = 'warning' AND r.rows_evaluated > 0   THEN 1 - (1 - r.rows_pass / r.rows_evaluated) * p.warning_penalty
# MAGIC       WHEN r.status = 'error'                              THEN 0.0
# MAGIC       WHEN r.status = 'warning'                            THEN 1 - p.warning_penalty
# MAGIC     END           AS credit
# MAGIC   FROM analytics_dq_dev.reporting.v_simple_results r
# MAGIC   CROSS JOIN params p
# MAGIC   WHERE r.is_latest_for_day
# MAGIC     AND r.is_scored
# MAGIC     AND r.table_id IN (SELECT table_id FROM known_tables)
# MAGIC ),
# MAGIC table_days AS (
# MAGIC   SELECT DISTINCT
# MAGIC     r.table_id,
# MAGIC     r.run_date
# MAGIC   FROM analytics_dq_dev.reporting.v_simple_results r
# MAGIC   WHERE r.result_rule_id IS NOT NULL
# MAGIC     AND r.table_id IN (SELECT table_id FROM known_tables)
# MAGIC ),
# MAGIC table_last AS (
# MAGIC   SELECT
# MAGIC     a.k,
# MAGIC     a.as_of,
# MAGIC     d.table_id,
# MAGIC     MAX(d.run_date) AS last_day
# MAGIC   FROM as_of_days a
# MAGIC   JOIN table_days d
# MAGIC     ON d.run_date <= a.as_of
# MAGIC   GROUP BY a.k, a.as_of, d.table_id
# MAGIC ),
# MAGIC window_days AS (
# MAGIC   SELECT
# MAGIC     a.k,
# MAGIC     d.table_id,
# MAGIC     d.run_date,
# MAGIC     DATEDIFF(d.run_date, LAG(d.run_date) OVER (PARTITION BY a.k, d.table_id ORDER BY d.run_date)) AS gap_days
# MAGIC   FROM as_of_days a
# MAGIC   CROSS JOIN params p
# MAGIC   JOIN table_days d
# MAGIC     ON d.run_date <= a.as_of
# MAGIC    AND d.run_date >  DATE_SUB(a.as_of, p.interval_window_days)
# MAGIC ),
# MAGIC intervals AS (
# MAGIC   SELECT
# MAGIC     k,
# MAGIC     table_id,
# MAGIC     COUNT(*)         AS n_days,
# MAGIC     MEDIAN(gap_days) AS usual_interval
# MAGIC   FROM window_days
# MAGIC   GROUP BY k, table_id
# MAGIC ),
# MAGIC table_state AS (
# MAGIC   SELECT
# MAGIC     l.k,
# MAGIC     l.table_id,
# MAGIC     l.last_day,
# MAGIC     COALESCE(i.n_days, 0) >= p.min_days_for_interval
# MAGIC       AND DATEDIFF(l.as_of, l.last_day) > p.stale_factor * GREATEST(COALESCE(i.usual_interval, 1), 1) AS is_stale
# MAGIC   FROM table_last l
# MAGIC   CROSS JOIN params p
# MAGIC   LEFT JOIN intervals i
# MAGIC     ON i.k = l.k
# MAGIC    AND i.table_id = l.table_id
# MAGIC ),
# MAGIC table_raw AS (
# MAGIC   SELECT
# MAGIC     s.k,
# MAGIC     s.table_id,
# MAGIC     SUM(c.w * c.credit)                               AS sum_wc,
# MAGIC     SUM(c.w)                                          AS sum_w,
# MAGIC     COUNT(*)                                          AS n_scored,
# MAGIC     SUM(CASE WHEN c.status = 'pass' THEN 1 ELSE 0 END) AS n_pass,
# MAGIC     SUM(c.rows_pass)                                  AS rows_pass,
# MAGIC     SUM(c.rows_evaluated)                             AS rows_evaluated
# MAGIC   FROM table_state s
# MAGIC   JOIN scored c
# MAGIC     ON c.table_id = s.table_id
# MAGIC    AND c.run_date = s.last_day
# MAGIC   WHERE NOT s.is_stale
# MAGIC   GROUP BY s.k, s.table_id
# MAGIC ),
# MAGIC pooled AS (
# MAGIC   SELECT
# MAGIC     k,
# MAGIC     SUM(sum_wc) / NULLIF(SUM(sum_w), 0) AS m
# MAGIC   FROM table_raw
# MAGIC   GROUP BY k
# MAGIC ),
# MAGIC table_score AS (
# MAGIC   SELECT
# MAGIC     t.*,
# MAGIC     t.sum_wc / NULLIF(t.sum_w, 0)                                     AS raw_score,
# MAGIC     (t.sum_wc + p.k_shrink * m.m) / NULLIF(t.sum_w + p.k_shrink, 0)   AS table_score
# MAGIC   FROM table_raw t
# MAGIC   JOIN pooled m
# MAGIC     ON m.k = t.k
# MAGIC   CROSS JOIN params p
# MAGIC ),
# MAGIC stale AS (
# MAGIC   SELECT
# MAGIC     k,
# MAGIC     SUM(CASE WHEN is_stale THEN 1 ELSE 0 END) AS stale_tables
# MAGIC   FROM table_state
# MAGIC   GROUP BY k
# MAGIC )
# MAGIC SELECT
# MAGIC   a.k,
# MAGIC   a.as_of,
# MAGIC   AVG(ts.table_score)                                    AS score,
# MAGIC   COUNT(ts.table_id)                                     AS tables_scored,
# MAGIC   COALESCE(MAX(st.stale_tables), 0)                      AS stale_tables,
# MAGIC   SUM(ts.n_pass) / NULLIF(SUM(ts.n_scored), 0)           AS rules_score,
# MAGIC   SUM(ts.rows_pass) / NULLIF(SUM(ts.rows_evaluated), 0)  AS row_pass_rate
# MAGIC FROM as_of_days a
# MAGIC LEFT JOIN table_score ts
# MAGIC   ON ts.k = a.k
# MAGIC LEFT JOIN stale st
# MAGIC   ON st.k = a.k
# MAGIC WHERE a.as_of IS NOT NULL
# MAGIC GROUP BY a.k, a.as_of

# COMMAND ----------

# DBTITLE 1,v_simple_coverage (Eva, live)
# MAGIC %sql
# MAGIC -- Owner: eva.routsi@ms.d-one.ai. Copied verbatim from the live view definition on 2026-11-02 (source of truth).
# MAGIC -- Do not run this cell against the live view without Eva's agreement.
# MAGIC -- Grain: one row per active table and active dimension name (full grid); effective rule = assignment, template,
# MAGIC -- policy, apply_at and table all is_active; project_label from any assignment of the table.
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_coverage AS
# MAGIC WITH dims AS (
# MAGIC   SELECT DISTINCT rd.rule_dimension
# MAGIC   FROM analytics_dq_dev.metadata.rule_dimension rd
# MAGIC   WHERE rd.is_active
# MAGIC ),
# MAGIC tables AS (
# MAGIC   SELECT
# MAGIC     m.table_id,
# MAGIC     concat(m.schema, '.', m.`table`)      AS table_label,
# MAGIC     COALESCE(size(m.primary_key), 0) > 0  AS has_primary_key
# MAGIC   FROM analytics_dq_dev.metadata.`table` m
# MAGIC   WHERE m.is_active
# MAGIC ),
# MAGIC table_project AS (
# MAGIC   SELECT
# MAGIC     ra.table_id,
# MAGIC     MIN(p.project) AS project
# MAGIC   FROM analytics_dq_dev.configuration.rule_assignment ra
# MAGIC   JOIN analytics_dq_dev.metadata.project p
# MAGIC     ON p.project_id = ra.project_id
# MAGIC   GROUP BY ra.table_id
# MAGIC ),
# MAGIC effective AS (
# MAGIC   SELECT
# MAGIC     ra.rule_assignment_id,
# MAGIC     ra.table_id,
# MAGIC     rd.rule_dimension
# MAGIC   FROM analytics_dq_dev.configuration.rule_assignment ra
# MAGIC   JOIN analytics_dq_dev.configuration.rule_template tpl
# MAGIC     ON tpl.rule_template_id = ra.rule_template_id
# MAGIC    AND tpl.is_active
# MAGIC   JOIN analytics_dq_dev.configuration.run_policy rp
# MAGIC     ON rp.policy_id = ra.policy_id
# MAGIC    AND rp.is_active
# MAGIC   JOIN analytics_dq_dev.configuration.apply_at ap
# MAGIC     ON ap.apply_at_id = ra.apply_at_id
# MAGIC    AND ap.is_active
# MAGIC   JOIN analytics_dq_dev.metadata.`table` m
# MAGIC     ON m.table_id = ra.table_id
# MAGIC    AND m.is_active
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.rule_dimension rd
# MAGIC     ON rd.rule_dimension_id = tpl.rule_dimension_id
# MAGIC   WHERE ra.is_active
# MAGIC ),
# MAGIC uniqueness AS (
# MAGIC   SELECT DISTINCT table_id
# MAGIC   FROM effective
# MAGIC   WHERE rule_dimension = 'uniqueness'
# MAGIC )
# MAGIC SELECT
# MAGIC   t.table_id,
# MAGIC   COALESCE(t.table_label, 'Άγνωστος πίνακας')                        AS table_label,
# MAGIC   COALESCE(tp.project, 'Χωρίς τομέα')                                AS project_label,
# MAGIC   d.rule_dimension,
# MAGIC   COALESCE(dl.dimension_label,
# MAGIC            concat(upper(substr(d.rule_dimension, 1, 1)), substr(d.rule_dimension, 2))) AS dimension_label,
# MAGIC   COUNT(DISTINCT e.rule_assignment_id)                               AS active_rule_count,
# MAGIC   t.has_primary_key,
# MAGIC   u.table_id IS NOT NULL                                             AS has_uniqueness_rule
# MAGIC FROM tables t
# MAGIC CROSS JOIN dims d
# MAGIC LEFT JOIN effective e
# MAGIC   ON e.table_id = t.table_id
# MAGIC  AND e.rule_dimension = d.rule_dimension
# MAGIC LEFT JOIN table_project tp
# MAGIC   ON tp.table_id = t.table_id
# MAGIC LEFT JOIN uniqueness u
# MAGIC   ON u.table_id = t.table_id
# MAGIC LEFT JOIN analytics_dq_dev.reporting.v_simple_dimension dl
# MAGIC   ON dl.rule_dimension = d.rule_dimension
# MAGIC GROUP BY t.table_id, t.table_label, tp.project, d.rule_dimension, dl.dimension_label, t.has_primary_key, u.table_id

# COMMAND ----------

# DBTITLE 1,v_simple_table_daily_ext
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_table_daily_ext AS
# MAGIC -- Grain: as v_simple_table_daily (one row per table_label, project_label and run day). Read-only extension; does not change Eva's view.
# MAGIC -- day_rank : 1 = newest run day of the table_label in its area, 2 = the one before, ... (pages 3 and 4, "last 10 runs").
# MAGIC SELECT
# MAGIC   t.*,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY t.table_label, t.project_label ORDER BY t.run_date DESC) AS day_rank
# MAGIC FROM analytics_dq_dev.reporting.v_simple_table_daily t;

# COMMAND ----------

# DBTITLE 1,v_simple_rule_daily_ext
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_rule_daily_ext AS
# MAGIC -- Grain: as v_simple_rule_daily (one row per rule_key and run day). Read-only extension; does not change Eva's view.
# MAGIC -- day_rank : 1 = newest run day of the rule_key, 2 = the one before, ... (page 4).
# MAGIC SELECT
# MAGIC   d.*,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY d.rule_key ORDER BY d.run_date DESC) AS day_rank
# MAGIC FROM analytics_dq_dev.reporting.v_simple_rule_daily d;

# COMMAND ----------

# DBTITLE 1,v_simple_coverage_ext
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_simple_coverage_ext AS
# MAGIC -- Grain: as v_simple_coverage (one row per active table and active dimension name). Read-only extension.
# MAGIC -- table_active_rule_count : effective active rule assignments of the table in any dimension (counter 4.1);
# MAGIC --                           same "effective" rule as v_simple_coverage (assignment, template, policy, apply_at, table all is_active).
# MAGIC WITH effective AS (
# MAGIC   SELECT
# MAGIC     ra.table_id,
# MAGIC     COUNT(DISTINCT ra.rule_assignment_id) AS table_active_rule_count
# MAGIC   FROM analytics_dq_dev.configuration.rule_assignment ra
# MAGIC   JOIN analytics_dq_dev.configuration.rule_template tpl
# MAGIC     ON tpl.rule_template_id = ra.rule_template_id
# MAGIC    AND tpl.is_active
# MAGIC   JOIN analytics_dq_dev.configuration.run_policy rp
# MAGIC     ON rp.policy_id = ra.policy_id
# MAGIC    AND rp.is_active
# MAGIC   JOIN analytics_dq_dev.configuration.apply_at ap
# MAGIC     ON ap.apply_at_id = ra.apply_at_id
# MAGIC    AND ap.is_active
# MAGIC   JOIN analytics_dq_dev.metadata.`table` m
# MAGIC     ON m.table_id = ra.table_id
# MAGIC    AND m.is_active
# MAGIC   WHERE ra.is_active
# MAGIC   GROUP BY ra.table_id
# MAGIC )
# MAGIC SELECT
# MAGIC   c.*,
# MAGIC   COALESCE(e.table_active_rule_count, 0) AS table_active_rule_count
# MAGIC FROM analytics_dq_dev.reporting.v_simple_coverage c
# MAGIC LEFT JOIN effective e
# MAGIC   ON e.table_id = c.table_id;