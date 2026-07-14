# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("rule_dimension", "")
dbutils.widgets.text("description", "")
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
rule_dimension = dbutils.widgets.get("rule_dimension")
description = dbutils.widgets.get("description")
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
############################     USER INPUTS RULE_DIMENSION      #############################
##############################################################################################

user_rule_dimensions = [
    {
        "rule_dimension": rule_dimension,
        "description": description,
        "added_by": added_by
    }
]
##############################################################################################\

##### rule_dimension ######

df = spark.createDataFrame(user_rule_dimensions)
df.createOrReplaceTempView("rule_dimension_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_dimension AS
SELECT
    src.rule_dimension,
    src.description,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.description AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM rule_dimension_inputs src
"""

users_update = f"""
MERGE INTO {catalog}.{schema_metadata}.rule_dimension AS t
USING prepared_rule_dimension AS s
ON t.rule_dimension = s.rule_dimension
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.description AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.rule_dimension (
    rule_dimension,
    description,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.rule_dimension,
    s.description,
    s.added_by,
    true,
    current_timestamp()
FROM prepared_rule_dimension s
LEFT ANTI JOIN {catalog}.{schema_metadata}.rule_dimension t
    ON  t.rule_dimension = s.rule_dimension
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.description AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema_metadata}.rule_dimension
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE rule_dimension_nk = '{rule_dimension}'
  AND is_active = TRUE
"""

users_soft_delete_rule_template = f"""
UPDATE {catalog}.{schema}.rule_template
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE rule_dimension_id IN (
    SELECT rule_dimension_id 
    FROM {catalog}.{schema_metadata}.rule_dimension
    WHERE rule_dimension_nk = '{rule_dimension}'
)
AND is_active = TRUE
"""

rule_assignment_to_delete = f"""
CREATE OR REPLACE TEMP VIEW tmp_rule_assignment_delete AS
SELECT ra.rule_assignment_id
FROM {catalog}.{schema}.rule_assignment ra
JOIN {catalog}.{schema}.rule_template rt
    ON ra.rule_template_id = rt.rule_template_id
JOIN {catalog}.{schema_metadata}.rule_dimension rd
    ON rt.rule_dimension_id = rd.rule_dimension_id
WHERE rd.rule_dimension_nk = '{rule_dimension}'
  AND ra.is_active = TRUE
"""

users_soft_delete_rule_assignment = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = current_timestamp()
WHERE rule_assignment_id IN (
    SELECT rule_assignment_id FROM tmp_rule_assignment_delete
)
"""

if action == "insert/update":
    spark.sql(users_input_view)
    spark.sql(users_update)
    spark.sql(users_insert)
    print("SCD2 insert/update completed.")

elif action == "delete":
    spark.sql(users_soft_delete)
    spark.sql(users_soft_delete_rule_template)
    spark.sql(rule_assignment_to_delete)
    spark.sql(users_soft_delete_rule_assignment)
    print("SCD2 soft delete completed.")