-- Extension of v_simple_coverage (read-only; does not change the base view).
-- table_active_rule_count: effective active rule assignments of the table over all dimensions.
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_simple_coverage_ext AS
WITH effective AS (
  SELECT
    ra.table_id,
    COUNT(DISTINCT ra.rule_assignment_id) AS table_active_rule_count
  FROM ${source_catalog}.${configuration_schema}.rule_assignment ra
  JOIN ${source_catalog}.${configuration_schema}.rule_template tpl
    ON tpl.rule_template_id = ra.rule_template_id
   AND tpl.is_active
  JOIN ${source_catalog}.${configuration_schema}.run_policy rp
    ON rp.policy_id = ra.policy_id
   AND rp.is_active
  JOIN ${source_catalog}.${configuration_schema}.apply_at ap
    ON ap.apply_at_id = ra.apply_at_id
   AND ap.is_active
  JOIN ${source_catalog}.${metadata_schema}.`table` m
    ON m.table_id = ra.table_id
   AND m.is_active
  WHERE ra.is_active
  GROUP BY ra.table_id
)
SELECT
  c.*,
  COALESCE(e.table_active_rule_count, 0) AS table_active_rule_count
FROM ${catalog}.${schema}.v_simple_coverage c
LEFT JOIN effective e
  ON e.table_id = c.table_id;
