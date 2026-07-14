# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("table", "")
dbutils.widgets.text("layer", "")
dbutils.widgets.text("primary_keys", "")
dbutils.widgets.text("filter_field", "")
dbutils.widgets.text("filter_field_type", "")
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
users_catalog = dbutils.widgets.get("catalog")
users_schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
layer = dbutils.widgets.get("layer")
primary_keys = dbutils.widgets.get("primary_keys")
primary_keys_normalized = [x.strip().replace('"', '').replace("'", "") for x in primary_keys.split(",")] if primary_keys else []
filter_field = dbutils.widgets.get("filter_field")
filter_field_type = dbutils.widgets.get("filter_field_type")
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
#################################     USER INPUTS TABLE     ##################################
##############################################################################################

user_values = [
    {
        "environment": environment,
        "catalog": users_catalog,
        "schema": users_schema,
        "table": table,
        "layer": layer,
        "primary_keys":primary_keys_normalized,
        "filter_field": filter_field,
        "filter_field_type": filter_field_type,
        "added_by": added_by
    }
]
##############################################################################################


##### table ######

df = spark.createDataFrame(user_values)
df.createOrReplaceTempView("table_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_table AS
SELECT
    CONCAT(src.catalog, '.', src.schema, '.', src.`table`) AS table_nk,
    src.environment,
    src.catalog,
    src.schema,
    src.`table`,
    src.layer,
    src.primary_keys,
    src.filter_field,
    src.filter_field_type,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.primary_keys AS string), '_NULL_'),
            coalesce(cast(src.filter_field AS string), '_NULL_'),
            coalesce(cast(src.filter_field_type AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM table_inputs src
"""

users_update = f"""
MERGE INTO {catalog}.{schema_metadata}.`table` AS t
USING prepared_table AS s
ON t.table_nk = s.table_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.primary_key AS string), '_NULL_'),
            coalesce(cast(t.filter_field AS string), '_NULL_'),
            coalesce(cast(t.filter_field_type AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.`table` (
    environment,
    catalog,
    schema,
    `table`,
    layer,
    primary_key,
    filter_field,
    filter_field_type,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.environment,
    s.catalog,
    s.schema,
    s.`table`,
    s.layer,
    s.primary_keys,
    s.filter_field,
    s.filter_field_type,
    s.added_by,
    true,
    current_timestamp()
FROM prepared_table s
LEFT ANTI JOIN {catalog}.{schema_metadata}.`table` t
    ON  t.table_nk = s.table_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.primary_key AS string), '_NULL_'),
                coalesce(cast(t.filter_field AS string), '_NULL_'),
                coalesce(cast(t.filter_field_type AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema_metadata}.`table`
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE table_nk = CONCAT('{users_catalog}', '.', '{users_schema}', '.', '{table}')
  AND is_active = TRUE
"""

rule_assignment_to_delete = f"""
CREATE OR REPLACE TEMP VIEW tmp_rule_assignment_delete AS
SELECT ra.rule_assignment_id
FROM {catalog}.{schema}.rule_assignment ra
JOIN {catalog}.{schema_metadata}.`table` t
    ON ra.table_id = t.table_id
WHERE t.table_nk = CONCAT('{users_catalog}', '.', '{users_schema}', '.', '{table}')
  AND ra.is_active = TRUE
"""

users_soft_delete_rule_assignment = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE rule_assignment_id IN (
    SELECT rule_assignment_id FROM tmp_rule_assignment_delete
)
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema_metadata}.`table`
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE table_nk = CONCAT('{users_catalog}', '.', '{users_schema}', '.', '{table}')
  AND is_active = TRUE
"""

if action == "insert/update":
    spark.sql(users_input_view)
    spark.sql(users_update)
    spark.sql(users_insert)
    print("SCD2 insert/update completed.")

elif action == "delete":
    spark.sql(rule_assignment_to_delete)
    spark.sql(users_soft_delete_rule_assignment)
    spark.sql(users_soft_delete)
    print("SCD2 soft delete completed.")