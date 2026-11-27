-- Base view definition, agreed on 2026-11-26. Every deploy builds its own copy in its reporting schema.
-- Grain: one row per rule result plus one row per table result without rule rows. Base of every dataset.
-- Criticality weights come from bundle variables weight_* (defaults 3 / 2 / 1 / 2 = live values).
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_results AS
WITH params AS (
  SELECT
    ${weight_high} AS w_high,
    ${weight_medium} AS w_medium,
    ${weight_low} AS w_low,
    ${weight_default} AS w_default
),
result_rows AS (
  SELECT
    rr.result_rule_id,
    rr.result_table_id,
    rr.result_id,
    rr.project_id,
    rr.rule_assignment_id,
    rr.policy_id,
    rr.rule_dimension_id,
    rr.status,
    rr.rows_evaluated,
    rr.rows_pass,
    rr.rows_failed,
    rr.start_timestamp,
    rr.end_timestamp
  FROM `${source_catalog}`.`${results_schema}`.result_rule rr
  UNION ALL
  SELECT
    CAST(NULL AS STRING) AS result_rule_id,
    t.result_table_id,
    t.result_id,
    t.project_id,
    CAST(NULL AS BIGINT) AS rule_assignment_id,
    CAST(NULL AS BIGINT) AS policy_id,
    CAST(NULL AS BIGINT) AS rule_dimension_id,
    t.status,
    CAST(NULL AS INT)    AS rows_evaluated,
    CAST(NULL AS INT)    AS rows_pass,
    CAST(NULL AS INT)    AS rows_failed,
    t.start_timestamp,
    t.end_timestamp
  FROM `${source_catalog}`.`${results_schema}`.result_table t
  WHERE NOT EXISTS (
    SELECT 1
    FROM `${source_catalog}`.`${results_schema}`.result_rule rr
    WHERE rr.result_table_id = t.result_table_id
  )
),
linked AS (
  SELECT
    x.*,
    COALESCE(x.result_rule_id, x.result_table_id) AS row_key,
    t.table_id                                    AS rt_table_id,
    t.project_id                                  AS rt_project_id,
    t.start_timestamp                             AS table_start_timestamp,
    COALESCE(x.result_id, t.result_id)            AS direct_result_id
  FROM result_rows x
  LEFT JOIN `${source_catalog}`.`${results_schema}`.result_table t
    ON t.result_table_id = x.result_table_id
),
window_execution AS (
  SELECT
    l.row_key,
    r.result_id,
    ROW_NUMBER() OVER (PARTITION BY l.row_key ORDER BY r.start_timestamp DESC) AS rn
  FROM linked l
  JOIN `${source_catalog}`.`${results_schema}`.result r
    ON l.direct_result_id IS NULL
   AND l.start_timestamp BETWEEN r.start_timestamp AND r.end_timestamp
),
enriched AS (
  SELECT
    CAST(COALESCE(e.start_timestamp, l.table_start_timestamp, l.start_timestamp) AS DATE) AS run_date,
    l.start_timestamp,
    l.end_timestamp,
    e.result_id,
    l.result_table_id,
    l.result_rule_id,
    l.rule_assignment_id,
    COALESCE(l.rt_table_id, ra.table_id)                                   AS table_id,
    COALESCE(p.project, 'No project')                                         AS project_label,
    COALESCE(concat(m.schema, '.', m.`table`), 'Unknown table')           AS table_label,
    l.status,
    s.status_label,
    s.status_symbol,
    s.status_order,
    tpl.name                                                               AS rule_name,
    COALESCE(NULLIF(TRIM(tpl.description), ''), tpl.name)                  AS rule_description,
    CASE
      WHEN rd.rule_dimension IS NULL THEN NULL
      ELSE COALESCE(d.dimension_label,
                    concat(upper(substr(rd.rule_dimension, 1, 1)), substr(rd.rule_dimension, 2)))
    END                                                                    AS dimension_label,
    CASE lower(rp.criticality)
      WHEN 'high'   THEN 'High'
      WHEN 'medium' THEN 'Medium'
      WHEN 'low'    THEN 'Low'
    END                                                                    AS criticality_label,
    CASE lower(rp.criticality)
      WHEN 'high'   THEN prm.w_high
      WHEN 'medium' THEN prm.w_medium
      WHEN 'low'    THEN prm.w_low
      ELSE prm.w_default
    END                                                                    AS crit_weight,
    rp.quarantine                                                          AS quarantine,
    COALESCE(
      get_json_object(ra.parameters_identifiers, '$.column'),
      array_join(from_json(get_json_object(ra.parameters_identifiers, '$.columns'), 'array<string>'), ', ')
    )                                                                      AS checked_columns,
    l.rows_evaluated,
    l.rows_pass,
    l.rows_failed,
    COALESCE(l.status IN ('pass', 'warning', 'error'), FALSE)              AS is_scored
  FROM linked l
  CROSS JOIN params prm
  LEFT JOIN window_execution w
    ON w.row_key = l.row_key
   AND w.rn = 1
  LEFT JOIN `${source_catalog}`.`${results_schema}`.result e
    ON e.result_id = COALESCE(l.direct_result_id, w.result_id)
  LEFT JOIN `${source_catalog}`.`${configuration_schema}`.rule_assignment ra
    ON ra.rule_assignment_id = l.rule_assignment_id
  LEFT JOIN `${source_catalog}`.`${configuration_schema}`.rule_template tpl
    ON tpl.rule_template_id = ra.rule_template_id
  LEFT JOIN `${source_catalog}`.`${configuration_schema}`.run_policy rp
    ON rp.policy_id = COALESCE(l.policy_id, ra.policy_id)
  LEFT JOIN `${source_catalog}`.`${metadata_schema}`.rule_dimension rd
    ON rd.rule_dimension_id = COALESCE(l.rule_dimension_id, tpl.rule_dimension_id)
  LEFT JOIN `${catalog}`.`${schema}`.v_simple_dimension d
    ON d.rule_dimension = rd.rule_dimension
  LEFT JOIN `${source_catalog}`.`${metadata_schema}`.`table` m
    ON m.table_id = COALESCE(l.rt_table_id, ra.table_id)
  LEFT JOIN `${source_catalog}`.`${metadata_schema}`.project p
    ON p.project_id = COALESCE(l.project_id, l.rt_project_id, e.project_id, ra.project_id)
  LEFT JOIN `${catalog}`.`${schema}`.v_simple_status s
    ON s.status = l.status
)
SELECT
  *,
  ROW_NUMBER() OVER (
    PARTITION BY run_date, COALESCE(CAST(rule_assignment_id AS STRING), result_rule_id, result_table_id)
    ORDER BY start_timestamp DESC, result_rule_id DESC
  ) = 1 AS is_latest_for_day
FROM enriched;
