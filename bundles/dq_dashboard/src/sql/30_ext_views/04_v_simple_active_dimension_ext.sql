-- Active dimension names, so the dashboard (ds_s4_coverage) reads only from the reporting schema
-- and never needs the framework catalog / schema names.
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_simple_active_dimension_ext AS
SELECT DISTINCT
  rule_dimension,
  is_active
FROM ${source_catalog}.${metadata_schema}.rule_dimension
WHERE is_active;
