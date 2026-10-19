-- @targets: free, company
-- v_table_registry: one row per registered table (metadata.table, real configuration only), whether or not it has
-- rule assignments, with its rule coverage. Used by the coverage-gap widget (tables that lack rules in a
-- dimension; tables with a primary key but no uniqueness rule). Rendered by scripts/run_ddl.py.
--
-- active_assignments / dimensions_covered: effective active assignments of the table (same is_effective_active
-- as v_assignment_active) and the dimensions they cover. has_uniqueness_rule: at least one of them is in the
-- uniqueness dimension. missing_dimensions: dimensions (active in metadata.rule_dimension, or used by an
-- effective active assignment, e.g. a demo assignment) without any effective active assignment on the table. layer_order: gold 1, silver 2, fs 3, any other layer 4 (gold and silver first).
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_table_registry
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH cov AS (
  SELECT table_id,
         COUNT(*)                                         AS active_assignments,
         array_sort(collect_set(rule_dimension))          AS dimensions_covered,
         COUNT_IF(rule_dimension = 'uniqueness') > 0      AS has_uniqueness_rule
  FROM {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active
  WHERE is_effective_active
  GROUP BY table_id
),
dims AS (
  SELECT array_sort(collect_set(rule_dimension)) AS all_dimensions
  FROM (SELECT rule_dimension FROM {{dq_catalog}}.metadata.rule_dimension WHERE is_active
        UNION ALL
        SELECT rule_dimension FROM {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active WHERE is_effective_active)
)
SELECT
  m.table_id, m.catalog, m.`schema`, m.table AS table_name, concat(m.`schema`, '.', m.table) AS table_label,
  m.layer AS data_layer, m.table_nk, m.primary_key, COALESCE(size(m.primary_key), 0) > 0 AS has_primary_key,
  m.quarantine, m.filter_field, m.is_active AS table_is_active,
  COALESCE(c.active_assignments, 0)                       AS active_assignments,
  COALESCE(c.dimensions_covered, array())                 AS dimensions_covered,
  array_except(d.all_dimensions, COALESCE(c.dimensions_covered, array())) AS missing_dimensions,
  COALESCE(c.has_uniqueness_rule, FALSE)                  AS has_uniqueness_rule,
  CASE m.layer WHEN 'gold' THEN 1 WHEN 'silver' THEN 2 WHEN 'fs' THEN 3 ELSE 4 END AS layer_order
FROM {{dq_catalog}}.metadata.`table` m
LEFT JOIN cov c ON c.table_id = m.table_id
CROSS JOIN dims d;
