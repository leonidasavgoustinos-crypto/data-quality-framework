-- Base view definition, agreed on 2026-11-26. Every deploy builds its own copy in its reporting schema.
-- Grain: one row per known rule_dimension name.
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_dimension AS
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
