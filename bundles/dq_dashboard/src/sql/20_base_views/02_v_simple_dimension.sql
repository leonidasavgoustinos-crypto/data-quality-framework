-- Owner: eva.routsi@ms.d-one.ai. Copied from the live view analytics_dq_dev.reporting.v_simple_dimension on 2026-11-26 (source of truth).
-- Grain: one row per known rule_dimension name.
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_simple_dimension AS
SELECT
  rule_dimension,
  dimension_label
FROM VALUES
  ('completeness', 'Completeness'),
  ('validity',     'Validity'),
  ('uniqueness',   'Uniqueness'),
  ('consistency',  'Consistency'),
  ('timeliness',   'Timeliness'),
  ('accuracy',     'Accuracy'),
  ('test_ui',      'Test (test_ui)')
  AS t(rule_dimension, dimension_label);
