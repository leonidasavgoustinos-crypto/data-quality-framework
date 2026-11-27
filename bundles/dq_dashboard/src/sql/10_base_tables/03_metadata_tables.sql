-- DQ framework metadata tables. Same columns as setup/01_ddl, but IF NOT EXISTS:
-- existing tables (and their data) are never replaced.

CREATE TABLE IF NOT EXISTS `${source_catalog}`.`${metadata_schema}`.`table` (
  table_id          BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  table_nk          STRING GENERATED ALWAYS AS (concat(`catalog`, '.', `schema`, '.', `table`)),
  quarantine        STRING GENERATED ALWAYS AS (concat('${source_catalog}', '.', '${quarantine_schema}', '.', `schema`, '_', `table`)),
  environment       STRING,
  `catalog`         STRING,
  `schema`          STRING,
  `table`           STRING,
  layer             STRING,
  primary_key       ARRAY<STRING>,
  filter_field      STRING,
  filter_field_type STRING,
  added_by          STRING,
  is_active         BOOLEAN,
  valid_from        TIMESTAMP,
  valid_to          TIMESTAMP
)
USING DELTA
COMMENT 'Platform tables with metadata information about primary keys, layer, schema, catalog etc.';

CREATE TABLE IF NOT EXISTS `${source_catalog}`.`${metadata_schema}`.rule_type (
  rule_type_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  rule_type_nk STRING GENERATED ALWAYS AS (rule_type),
  rule_type    STRING,
  description  STRING,
  added_by     STRING,
  is_active    BOOLEAN,
  valid_from   TIMESTAMP,
  valid_to     TIMESTAMP
)
USING DELTA
COMMENT 'Business or Technical rule';

CREATE TABLE IF NOT EXISTS `${source_catalog}`.`${metadata_schema}`.rule_dimension (
  rule_dimension_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  rule_dimension_nk STRING GENERATED ALWAYS AS (rule_dimension),
  rule_dimension    STRING,
  description       STRING,
  added_by          STRING,
  is_active         BOOLEAN,
  valid_from        TIMESTAMP,
  valid_to          TIMESTAMP
)
USING DELTA
COMMENT 'Data quality dimensions to be used (e.g completeness, uniqueness, validity, consistency)';

CREATE TABLE IF NOT EXISTS `${source_catalog}`.`${metadata_schema}`.`project` (
  project_id          BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  `project`           STRING,
  project_nk          STRING GENERATED ALWAYS AS (`project`),
  project_description STRING,
  is_active           BOOLEAN,
  valid_from          TIMESTAMP,
  valid_to            TIMESTAMP,
  added_by            STRING
)
USING DELTA
COMMENT 'Project Information';
