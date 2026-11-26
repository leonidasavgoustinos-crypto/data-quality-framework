-- DQ framework result tables. Same columns as setup/01_ddl, but IF NOT EXISTS:
-- existing tables (and their data) are never replaced.
-- Placeholders ${...} are filled by src/notebooks/run_sql_folder from job parameters.

CREATE TABLE IF NOT EXISTS ${source_catalog}.${results_schema}.result (
  result_id        STRING,
  project_id       BIGINT,
  status           STRING,
  tables_evaluated INT,
  tables_error     INT,
  tables_warning   INT,
  tables_pass      INT,
  start_timestamp  TIMESTAMP,
  end_timestamp    TIMESTAMP
)
USING DELTA
COMMENT 'High level result per execution of the DQ framework, to be used by the orchestrator';

CREATE TABLE IF NOT EXISTS ${source_catalog}.${results_schema}.result_rule (
  result_rule_id     STRING,
  result_table_id    STRING,
  result_id          STRING,
  project_id         BIGINT,
  rule_assignment_id BIGINT,
  policy_id          BIGINT,
  rows_evaluated     INT,
  rows_failed        INT,
  rows_pass          INT,
  error_message      STRING,
  status             STRING,
  rule_type_id       BIGINT,
  rule_dimension_id  BIGINT,
  statement          STRING,
  alert              BOOLEAN,
  alert_id           BIGINT,
  start_timestamp    TIMESTAMP,
  end_timestamp      TIMESTAMP
)
USING DELTA
COMMENT 'Data quality results on rule level';

CREATE TABLE IF NOT EXISTS ${source_catalog}.${results_schema}.result_table (
  result_table_id STRING,
  result_id       STRING,
  table_id        BIGINT,
  project_id      BIGINT,
  rules_error     INT,
  rules_warning   INT,
  rules_pass      INT,
  rules_evaluated INT,
  status          STRING,
  start_timestamp TIMESTAMP,
  end_timestamp   TIMESTAMP
)
USING DELTA
COMMENT 'Data quality results summarized on table level and dq dimensions, multiple rules are applied on 1 table';
