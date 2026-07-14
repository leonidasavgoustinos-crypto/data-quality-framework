# Databricks notebook source
dbutils.widgets.text("target_environment", "")
environment = dbutils.widgets.get("target_environment")
environment = 'dev'
name = "analytics_dq_"
catalog = f"{name}{environment}"
schemas = ["metadata", "results", "configuration", "quarantine", "reporting"]


for schema in schemas:
   spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


schema = "results"

###### result ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.result (
    result_id   STRING,
    project_id  BIGINT,
    status  STRING,
    tables_evaluated INT,
    tables_error INT,
    tables_warning INT,
    tables_pass INT,
    start_timestamp TIMESTAMP,
    end_timestamp   TIMESTAMP
)
USING DELTA
COMMENT 'High level result per execution of the DQ framework, to be used by the orchestrator'"""
 
try:   
    spark.sql(sql_statement) 
except Exception as e:
    print(e)
 
###### rule ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.result_rule (
    result_rule_id  STRING,
    result_table_id STRING,
    result_id   STRING,
    project_id  BIGINT,
    rule_assignment_id  BIGINT,
    policy_id BIGINT,
    rows_evaluated int,
    rows_failed int,
    rows_pass INT,
    error_message   STRING,
    status  STRING,
    rule_type_id bigint,
    rule_dimension_id bigint,
    statement STRING,
    alert boolean,
    alert_id bigint,
    start_timestamp TIMESTAMP,
    end_timestamp   TIMESTAMP 
)
USING DELTA
COMMENT 'Data quality results on rule level'""" 

try:  
    spark.sql(sql_statement)
except Exception as e:
    print(e)
 
###### table ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.result_table (
    result_table_id STRING,
    result_id   STRING,
    table_id    BIGINT,
    project_id  BIGINT,
    rules_error  INT,
    rules_warning INT,
    rules_pass INT,
    rules_evaluated int,
    status  STRING,
    start_timestamp TIMESTAMP,
    end_timestamp   TIMESTAMP
)
USING DELTA
COMMENT 'Data quality results summarized on table level and dq dimensions, multiple rules are applied on 1 table'"""
 
try:   
    spark.sql(sql_statement) 
except Exception as e:
    print(e)

schema = "configuration" 

###### apply_at ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.apply_at (
    apply_at_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    apply_at STRING,
    apply_at_nk STRING generated always as (CONCAT(apply_at, '.', CASE WHEN is_project_specific = TRUE then 'project_specific' else 'project_agnostic' END)),
    is_project_specific BOOLEAN,
    is_active BOOLEAN,
    added_by STRING,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
)
USING DELTA
COMMENT 'Specifies the Medallion Architecture layer where the data processing is applied.'"""
 
try:    
    spark.sql(sql_statement)
except Exception as e:
    print(e)

###### rule_assignment ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.rule_assignment (
    rule_assignment_id  BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    rule_template_id    BIGINT,
    template_nk STRING,
    policy_id   BIGINT,
    project_id bigint,
    project_nk STRING,
    table_id BIGINT,
    table_nk STRING,
    apply_at_id BIGINT,
    apply_at_nk STRING,
    parameters_values STRING,
    parameters_identifiers STRING,
    rule_assignment_nk STRING GENERATED ALWAYS AS (
        CONCAT(
            template_nk,
            '.',
            COALESCE(NULLIF(project_nk, ''), '_NULL_'),
            '.',
            table_nk,
            '.',
            apply_at_nk,
            '.',
            COALESCE(
                to_json(
                    from_json(
                        CASE 
                            WHEN parameters_identifiers = '' THEN NULL
                            ELSE parameters_identifiers
                        END,
                        'MAP<STRING, STRING>'
                    )
                ),
                '_NULL_'
            )
        )
    ),
    added_by    STRING,
    is_active   BOOLEAN,
    valid_from  TIMESTAMP,
    valid_to    TIMESTAMP 
)
USING DELTA
COMMENT 'Configures a rule template for a specific dataset/column, defining parameters and if needed, target dataset/column information for comparison'"""
 
try:    
    spark.sql(sql_statement)
except Exception as e:
    print(e)

###### run_policy ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.run_policy (
   
    policy_id   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    criticality STRING,
    quarantine BOOLEAN,
    category STRING,
    policy_nk STRING GENERATED ALWAYS AS (
        CONCAT(
            category,
            '.',
            criticality,
            '.',
            CASE WHEN quarantine THEN 'quarantine' ELSE 'no_quarantine' END
        )
    ),
    added_by STRING,
    is_active   BOOLEAN,
    valid_from  TIMESTAMP,
    valid_to    TIMESTAMP 
)
USING DELTA
COMMENT 'Defines execution behavior for violations per criticality level, including fail-fast vs quarantine and alert strategy'"""
 
try:    
    spark.sql(sql_statement)
except Exception as e:
    print(e)
 
###### rule_template ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.rule_template (
    rule_template_id    BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    name    STRING,
    description STRING,
    rule_type_id    BIGINT,
    rule_dimension_id   BIGINT,
    template_nk string generated always as (name),
    is_reusable     BOOLEAN,
    scope   STRING,
    engine_type STRING,
    statement  STRING,
    added_by STRING,
    is_active   BOOLEAN,
    valid_from  TIMESTAMP,
    valid_to    TIMESTAMP   
)
USING DELTA
COMMENT 'Reusable rule definitions with placeholders for parameters and metadata entries, and default settings that drive SQL or DQX checks.'"""
 
try:    
    spark.sql(sql_statement) 
except Exception as e:
    print(e)


schema = "metadata"

###### table ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.`table` (
    table_id    BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    table_nk STRING GENERATED ALWAYS AS (concat(`catalog`, '.', `schema`, '.', `table`)),
    quarantine    STRING GENERATED ALWAYS AS (concat('{catalog}', '.','quarantine','.', `schema`, '_', `table`)),
    environment   STRING,
    `catalog` STRING,
    `schema`  STRING,
    `table`   STRING,
    layer   STRING,
    primary_key ARRAY<STRING>,
    filter_field STRING,
    filter_field_type string,
    added_by STRING,
    is_active   BOOLEAN,
    valid_from  TIMESTAMP,
    valid_to    TIMESTAMP 
    
)
USING DELTA
COMMENT 'Platform tables with metadata information about primary keys, layer, schema, catalog etc.'"""
 
try:  
    spark.sql(sql_statement)
except Exception as e:
    print(e)

###### rule_type ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.rule_type (
    rule_type_id    BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    rule_type_nk string generated always as (rule_type),
    rule_type   STRING,
    description STRING,
    added_by STRING,
    is_active   BOOLEAN,
    valid_from  TIMESTAMP,
    valid_to    TIMESTAMP    
)
USING DELTA
COMMENT 'Business or Technical rule'"""
 
try:    
    spark.sql(sql_statement) 
except Exception as e:
    print(e)

###### rule_dimension ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.rule_dimension (
    rule_dimension_id   BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    rule_dimension_nk string generated always as (rule_dimension),
    rule_dimension  STRING,
    description STRING,
    added_by STRING,
    is_active   BOOLEAN,
    valid_from  TIMESTAMP,
    valid_to    TIMESTAMP
)
USING DELTA
COMMENT 'Data quality dimensions to be used (e.g completeness, uniqueness, validity, consistency)'"""
 
try: 
    spark.sql(sql_statement)
except Exception as e:
    print(e)

###### project ######

sql_statement = f"""CREATE OR REPLACE TABLE {catalog}.{schema}.`project` (
 project_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
 `project` STRING,
 project_nk STRING GENERATED ALWAYS AS (`project`),
 project_description STRING,
 is_active BOOLEAN,
 valid_from TIMESTAMP,
 valid_to TIMESTAMP,
 added_by STRING
)
USING DELTA
COMMENT 'Project Information'"""
 
try:  
    spark.sql(sql_statement)
except Exception as e:
    print(e)