# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("criticality", "")
dbutils.widgets.text("category", "")
dbutils.widgets.dropdown("quarantine", "true", ["true", "false"])
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
criticality = dbutils.widgets.get("criticality")
category = dbutils.widgets.get("category")
quarantine = dbutils.widgets.get("quarantine").strip().lower() == "true"
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
###############################     USER INPUTS RUN_POLICY     ###############################
##############################################################################################

user_values_run_policy = [
    {
        "criticality": criticality,
        "category": category,
        "quarantine": quarantine,
        "added_by": added_by
    }
]
##############################################################################################


##### run_policy ######

df_policy = spark.createDataFrame(user_values_run_policy)
df_policy.createOrReplaceTempView("policy_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_run_policy AS
SELECT
    CONCAT(
        src.category, '.', 
        src.criticality, '.', 
        CASE WHEN src.quarantine THEN 'quarantine' ELSE 'no_quarantine' END
    ) AS policy_nk,

    src.category,
    src.criticality,
    src.quarantine,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.category AS string), '_NULL_'),
            coalesce(cast(src.criticality AS string), '_NULL_'),
            coalesce(cast(src.quarantine AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM policy_inputs src
"""

users_update = f"""
MERGE INTO {catalog}.{schema}.run_policy AS t
USING prepared_run_policy AS s
ON t.policy_nk = s.policy_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.category AS string), '_NULL_'),
            coalesce(cast(t.criticality AS string), '_NULL_'),
            coalesce(cast(t.quarantine AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema}.run_policy (
    category,
    criticality,
    quarantine,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.category,
    s.criticality,
    s.quarantine,
    s.added_by,
    TRUE,
    current_timestamp()
FROM prepared_run_policy s
LEFT ANTI JOIN {catalog}.{schema}.run_policy t
    ON  t.policy_nk = s.policy_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.category AS string), '_NULL_'),
                coalesce(cast(t.criticality AS string), '_NULL_'),
                coalesce(cast(t.quarantine AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema}.run_policy
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE policy_nk = CONCAT(
        '{category}', '.', 
        '{criticality}', '.', 
        CASE WHEN '{quarantine}' = 'True' THEN 'quarantine' ELSE 'no_quarantine' END
    )
  AND is_active = TRUE
"""

users_soft_delete_rule_assignment = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE policy_id IN (
    SELECT policy_id
    FROM {catalog}.{schema}.run_policy
    WHERE policy_nk = CONCAT(
        '{category}', '.', 
        '{criticality}', '.', 
        CASE WHEN '{quarantine}' = 'True' THEN 'quarantine' ELSE 'no_quarantine' END
    )
)
AND is_active = TRUE;
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