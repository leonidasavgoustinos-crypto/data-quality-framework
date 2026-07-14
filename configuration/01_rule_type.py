# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("rule_type", "")
dbutils.widgets.text("description", "")
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
rule_type = dbutils.widgets.get("rule_type")
description = dbutils.widgets.get("description")
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
###############################     USER INPUTS RULE_TYPE     ################################
##############################################################################################

user_rule_types = [
    {
    "rule_type": rule_type,
    "description": description, 
    "added_by": added_by 
    }
]
##############################################################################################

##### rule_type ######

df = spark.createDataFrame(user_rule_types)
df.createOrReplaceTempView("rule_type_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_type AS
SELECT
    src.rule_type,
    src.description,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.description AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM rule_type_inputs src
"""

users_update = f"""
MERGE INTO {catalog}.{schema_metadata}.rule_type AS t
USING prepared_rule_type AS s
ON t.rule_type = s.rule_type
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
INSERT INTO {catalog}.{schema_metadata}.rule_type (
    rule_type,
    description,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.rule_type,
    s.description,
    s.added_by,
    true,
    current_timestamp()
FROM prepared_rule_type s
LEFT ANTI JOIN {catalog}.{schema_metadata}.rule_type t
    ON  t.rule_type = s.rule_type
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.description AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema_metadata}.rule_type
SET 
    is_active = false,
    valid_to = current_timestamp()
WHERE rule_type_nk = '{rule_type}'
  AND is_active = true
"""

users_soft_delete_rule_template = f"""
UPDATE {catalog}.{schema}.rule_template
SET 
    is_active = false,
    valid_to = current_timestamp()
WHERE rule_type_id IN (
    SELECT rule_type_id 
    FROM {catalog}.{schema_metadata}.rule_type
    WHERE rule_type_nk = '{rule_type}'
)
AND is_active = true
"""

rule_assignment_to_delete = f"""
CREATE OR REPLACE TEMP VIEW tmp_rule_assignment_delete AS
SELECT ra.rule_assignment_id
FROM {catalog}.{schema}.rule_assignment ra
JOIN {catalog}.{schema}.rule_template rt
    ON ra.rule_template_id = rt.rule_template_id
JOIN {catalog}.{schema_metadata}.rule_type rty
    ON rt.rule_type_id = rty.rule_type_id
WHERE rty.rule_type_nk = '{rule_type}'
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