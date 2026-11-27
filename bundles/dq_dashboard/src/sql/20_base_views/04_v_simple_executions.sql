-- Base view definition, agreed on 2026-11-26. Every deploy builds its own copy in its reporting schema.
-- Grain: one row per execution (results.result); has_rule_results = the execution wrote at least one rule result.
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_executions AS
SELECT
  CAST(r.start_timestamp AS DATE)            AS run_date,
  r.result_id,
  r.start_timestamp,
  r.end_timestamp,
  r.status,
  COALESCE(p.project, 'No project')             AS project_label,
  EXISTS (
    SELECT 1
    FROM `${catalog}`.`${schema}`.v_simple_results v
    WHERE v.result_id = r.result_id
      AND v.result_rule_id IS NOT NULL
  )                                          AS has_rule_results
FROM `${source_catalog}`.`${results_schema}`.result r
LEFT JOIN `${source_catalog}`.`${metadata_schema}`.project p
  ON p.project_id = r.project_id;
