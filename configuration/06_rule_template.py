# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("name", "")
dbutils.widgets.text("description", "")
dbutils.widgets.text("rule_type", "")
dbutils.widgets.text("rule_dimension", "")
dbutils.widgets.text("scope", "")
dbutils.widgets.dropdown("is_reusable", "true", ["true", "false"])
dbutils.widgets.text("engine_type", "")
dbutils.widgets.text("statement", "")
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
users_name = dbutils.widgets.get("name")
description = dbutils.widgets.get("description")
rule_type_nk = dbutils.widgets.get("rule_type")
rule_dimension_nk = dbutils.widgets.get("rule_dimension")
scope = dbutils.widgets.get("scope")
is_reusable = dbutils.widgets.get("is_reusable").strip().lower() == "true"
engine_type = dbutils.widgets.get("engine_type")
statement = dbutils.widgets.get("statement")
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
#############################     USER INPUTS RULE_TEMPLATE     ##############################
##############################################################################################

user_values_rule_templates = [
    {
        "name": users_name,
        "description": description,
        "rule_type_nk": rule_type_nk,
        "rule_dimension_nk": rule_dimension_nk,
        "scope": scope,
        "is_reusable": is_reusable,
        "engine_type": engine_type,
        "statement": statement,
        "added_by": added_by
    }
]
##############################################################################################


##### rule_template ######

df_templates = spark.createDataFrame(user_values_rule_templates)
df_templates.createOrReplaceTempView("templates_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_templates AS
SELECT
    src.name AS template_nk,
    src.name AS name,
    src.description,
    rt.rule_type_id,
    rd.rule_dimension_id,
    src.scope,
    src.is_reusable,
    src.engine_type,
    src.statement,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(rt.rule_type_id AS string), '_NULL_'),
            coalesce(cast(rd.rule_dimension_id AS string), '_NULL_'),
            coalesce(cast(src.scope AS string), '_NULL_'),
            coalesce(cast(src.is_reusable AS string), '_NULL_')
        ), 256
    ) AS new_hash

FROM templates_inputs src
JOIN {catalog}.{schema_metadata}.rule_type rt
    ON rt.rule_type_nk = src.rule_type_nk
    AND rt.is_active = TRUE
JOIN {catalog}.{schema_metadata}.rule_dimension rd
    ON rd.rule_dimension_nk = src.rule_dimension_nk
    AND rd.is_active = TRUE
"""

users_update = f"""
MERGE INTO {catalog}.{schema}.rule_template AS target
USING prepared_rule_templates AS source
ON target.template_nk = source.template_nk
AND target.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(target.rule_type_id AS string), '_NULL_'),
            coalesce(cast(target.rule_dimension_id AS string), '_NULL_'),
            coalesce(cast(target.scope AS string), '_NULL_'),
            coalesce(cast(target.is_reusable AS string), '_NULL_')
        ), 256
    ) <> source.new_hash

WHEN MATCHED THEN
UPDATE SET
    target.is_active = FALSE,
    target.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema}.rule_template (
    name,
    description,
    rule_type_id,
    rule_dimension_id,
    is_reusable,
    scope,
    engine_type,
    statement,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.name,
    s.description,
    s.rule_type_id,
    s.rule_dimension_id,
    s.is_reusable,
    s.scope,
    s.engine_type,
    s.statement,
    s.added_by,
    TRUE,
    current_timestamp()
FROM prepared_rule_templates s
LEFT ANTI JOIN {catalog}.{schema}.rule_template t
    ON  t.template_nk = s.template_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.rule_type_id AS string), '_NULL_'),
                coalesce(cast(t.rule_dimension_id AS string), '_NULL_'),
                coalesce(cast(t.scope AS string), '_NULL_'),
                coalesce(cast(t.is_reusable AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""


users_soft_delete = f"""
UPDATE {catalog}.{schema}.rule_template
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE template_nk = '{users_name}'
  AND is_active = TRUE
"""

users_soft_delete_rule_assignment = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE rule_template_id IN (
    SELECT rule_template_id
    FROM {catalog}.{schema}.rule_template
    WHERE template_nk = '{users_name}'
)
AND is_active = TRUE
"""

if action == "insert/update":
    spark.sql(users_input_view)
    spark.sql(users_update)
    spark.sql(users_insert)
    print("SCD2 insert/update completed.")

elif action == "delete":
    spark.sql(users_soft_delete)
    spark.sql(users_soft_delete_rule_assignment)
    print("SCD2 soft delete completed.")