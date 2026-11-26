-- DQ framework configuration tables. Same columns as setup/01_ddl, but IF NOT EXISTS:
-- existing tables (and their data) are never replaced.

CREATE TABLE IF NOT EXISTS ${source_catalog}.${configuration_schema}.apply_at (
  apply_at_id         BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  apply_at            STRING,
  apply_at_nk         STRING GENERATED ALWAYS AS (CONCAT(apply_at, '.', CASE WHEN is_project_specific = TRUE THEN 'project_specific' ELSE 'project_agnostic' END)),
  is_project_specific BOOLEAN,
  is_active           BOOLEAN,
  added_by            STRING,
  valid_from          TIMESTAMP,
  valid_to            TIMESTAMP
)
USING DELTA
COMMENT 'Specifies the Medallion Architecture layer where the data processing is applied.';

CREATE TABLE IF NOT EXISTS ${source_catalog}.${configuration_schema}.rule_assignment (
  rule_assignment_id     BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  rule_template_id       BIGINT,
  template_nk            STRING,
  policy_id              BIGINT,
  project_id             BIGINT,
  project_nk             STRING,
  table_id               BIGINT,
  table_nk               STRING,
  apply_at_id            BIGINT,
  apply_at_nk            STRING,
  parameters_values      STRING,
  parameters_identifiers STRING,
  rule_assignment_nk     STRING GENERATED ALWAYS AS (
    CONCAT(
      template_nk,
      '.',
      CASE WHEN project_nk = '' OR project_nk IS NULL THEN '_NULL_' ELSE project_nk END,
      '.',
      table_nk,
      '.',
      apply_at_nk,
      '.',
      CASE
        WHEN parameters_identifiers = '' OR parameters_identifiers IS NULL THEN '_NULL_'
        ELSE COALESCE(to_json(from_json(parameters_identifiers, 'MAP<STRING, STRING>')), '_NULL_')
      END
    )
  ),
  added_by               STRING,
  is_active              BOOLEAN,
  valid_from             TIMESTAMP,
  valid_to               TIMESTAMP
)
USING DELTA
COMMENT 'Configures a rule template for a specific dataset/column, defining parameters and if needed, target dataset/column information for comparison';

CREATE TABLE IF NOT EXISTS ${source_catalog}.${configuration_schema}.run_policy (
  policy_id   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  criticality STRING,
  quarantine  BOOLEAN,
  category    STRING,
  policy_nk   STRING GENERATED ALWAYS AS (
    CONCAT(category, '.', criticality, '.', CASE WHEN quarantine THEN 'quarantine' ELSE 'no_quarantine' END)
  ),
  added_by    STRING,
  is_active   BOOLEAN,
  valid_from  TIMESTAMP,
  valid_to    TIMESTAMP
)
USING DELTA
COMMENT 'Defines execution behavior for violations per criticality level, including fail-fast vs quarantine and alert strategy';

CREATE TABLE IF NOT EXISTS ${source_catalog}.${configuration_schema}.rule_template (
  rule_template_id  BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  name              STRING,
  description       STRING,
  rule_type_id      BIGINT,
  rule_dimension_id BIGINT,
  template_nk       STRING GENERATED ALWAYS AS (name),
  is_reusable       BOOLEAN,
  scope             STRING,
  engine_type       STRING,
  statement         STRING,
  added_by          STRING,
  is_active         BOOLEAN,
  valid_from        TIMESTAMP,
  valid_to          TIMESTAMP
)
USING DELTA
COMMENT 'Reusable rule definitions with placeholders for parameters and metadata entries, and default settings that drive SQL or DQX checks.';
