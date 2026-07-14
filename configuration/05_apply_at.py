# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("apply_at", "")
dbutils.widgets.dropdown("is_project_specific", "true", ["true", "false"])
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
apply_at = dbutils.widgets.get("apply_at")
is_project_specific = dbutils.widgets.get("is_project_specific").strip().lower() == "true"
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
################################     USER INPUTS APPLY_AT     ################################
##############################################################################################

user_values_apply_at = [
    {
        "apply_at": apply_at,
        "is_project_specific": is_project_specific,
        "added_by": added_by
    }
]
##############################################################################################


##### apply_at ######

df_apply_at = spark.createDataFrame(user_values_apply_at)
df_apply_at.createOrReplaceTempView("apply_at_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_apply_at AS
SELECT
    src.apply_at,
    src.is_project_specific,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.apply_at AS string), '_NULL_'),
            coalesce(cast(src.is_project_specific AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM apply_at_inputs src
"""

users_update = f"""
MERGE INTO {catalog}.{schema}.apply_at AS target
USING prepared_apply_at AS source
ON target.apply_at = source.apply_at
AND target.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(target.apply_at AS string), '_NULL_'),
            coalesce(cast(target.is_project_specific AS string), '_NULL_')
        ), 256
    ) <> source.new_hash

WHEN MATCHED THEN
UPDATE SET
    target.is_active = FALSE,
    target.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema}.apply_at (
    apply_at,
    is_project_specific,
    is_active,
    added_by,
    valid_from
)
SELECT
    s.apply_at,
    s.is_project_specific,
    TRUE,
    s.added_by,
    current_timestamp()
FROM prepared_apply_at s
LEFT ANTI JOIN {catalog}.{schema}.apply_at t
    ON  t.apply_at = s.apply_at
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.apply_at AS string), '_NULL_'),
                coalesce(cast(t.is_project_specific AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema}.apply_at
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE apply_at_nk = CONCAT(
        '{apply_at}', '.', 
        CASE WHEN '{is_project_specific}' = 'True' 
             THEN 'project_specific' 
             ELSE 'project_agnostic' 
        END
    )
  AND is_active = TRUE
"""

users_soft_delete_rule_assignment = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE apply_at_id IN (
    SELECT apply_at_id
    FROM {catalog}.{schema}.apply_at
    WHERE apply_at_nk = CONCAT(
        '{apply_at}', '.', 
        CASE WHEN '{is_project_specific}' = 'True' 
             THEN 'project_specific' 
             ELSE 'project_agnostic' 
        END
    )
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