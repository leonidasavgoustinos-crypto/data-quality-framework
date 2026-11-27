-- Base view definition, agreed on 2026-11-26. Every deploy builds its own copy in its reporting schema.
-- Grain: one row per active table and active dimension name (full grid).
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_coverage AS
WITH dims AS (
  SELECT DISTINCT rd.rule_dimension
  FROM `${source_catalog}`.`${metadata_schema}`.rule_dimension rd
  WHERE rd.is_active
),
tables AS (
  SELECT
    m.table_id,
    concat(m.schema, '.', m.`table`)      AS table_label,
    COALESCE(size(m.primary_key), 0) > 0  AS has_primary_key
  FROM `${source_catalog}`.`${metadata_schema}`.`table` m
  WHERE m.is_active
),
table_project AS (
  SELECT
    ra.table_id,
    MIN(p.project) AS project
  FROM `${source_catalog}`.`${configuration_schema}`.rule_assignment ra
  JOIN `${source_catalog}`.`${metadata_schema}`.project p
    ON p.project_id = ra.project_id
  GROUP BY ra.table_id
),
effective AS (
  SELECT
    ra.rule_assignment_id,
    ra.table_id,
    rd.rule_dimension
  FROM `${source_catalog}`.`${configuration_schema}`.rule_assignment ra
  JOIN `${source_catalog}`.`${configuration_schema}`.rule_template tpl
    ON tpl.rule_template_id = ra.rule_template_id
   AND tpl.is_active
  JOIN `${source_catalog}`.`${configuration_schema}`.run_policy rp
    ON rp.policy_id = ra.policy_id
   AND rp.is_active
  JOIN `${source_catalog}`.`${configuration_schema}`.apply_at ap
    ON ap.apply_at_id = ra.apply_at_id
   AND ap.is_active
  JOIN `${source_catalog}`.`${metadata_schema}`.`table` m
    ON m.table_id = ra.table_id
   AND m.is_active
  LEFT JOIN `${source_catalog}`.`${metadata_schema}`.rule_dimension rd
    ON rd.rule_dimension_id = tpl.rule_dimension_id
  WHERE ra.is_active
),
uniqueness AS (
  SELECT DISTINCT table_id
  FROM effective
  WHERE rule_dimension = 'uniqueness'
)
SELECT
  t.table_id,
  COALESCE(t.table_label, 'Unknown table')                           AS table_label,
  COALESCE(tp.project, 'No project')                                    AS project_label,
  d.rule_dimension,
  COALESCE(dl.dimension_label,
           concat(upper(substr(d.rule_dimension, 1, 1)), substr(d.rule_dimension, 2))) AS dimension_label,
  COUNT(DISTINCT e.rule_assignment_id)                               AS active_rule_count,
  t.has_primary_key,
  u.table_id IS NOT NULL                                             AS has_uniqueness_rule
FROM tables t
CROSS JOIN dims d
LEFT JOIN effective e
  ON e.table_id = t.table_id
 AND e.rule_dimension = d.rule_dimension
LEFT JOIN table_project tp
  ON tp.table_id = t.table_id
LEFT JOIN uniqueness u
  ON u.table_id = t.table_id
LEFT JOIN `${catalog}`.`${schema}`.v_simple_dimension dl
  ON dl.rule_dimension = d.rule_dimension
GROUP BY t.table_id, t.table_label, tp.project, d.rule_dimension, dl.dimension_label, t.has_primary_key, u.table_id;
