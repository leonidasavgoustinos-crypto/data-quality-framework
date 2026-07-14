# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("project", "")
dbutils.widgets.text("project_description", "")
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
project = dbutils.widgets.get("project")
project_description = dbutils.widgets.get("project_description")
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
################################     USER INPUTS PROJECT     #################################
##############################################################################################

user_projects = [
    {
        "project": project,
        "project_description": project_description,
        "added_by": added_by
    }
]
##############################################################################################a

##### project ######

df = spark.createDataFrame(user_projects)
df.createOrReplaceTempView("project_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_project AS
SELECT
    src.project,
    src.project_description,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.project_description AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM project_inputs src
"""

users_update = f"""
MERGE INTO {catalog}.{schema_metadata}.project AS t
USING prepared_project AS s
ON t.project = s.project
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.project_description AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.project (
    project,
    project_description,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.project,
    s.project_description,
    s.added_by,
    true,
    current_timestamp()
FROM prepared_project s
LEFT ANTI JOIN {catalog}.{schema_metadata}.project t
    ON  t.project_nk = s.project_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.project_description AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema_metadata}.project
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE project_nk = '{project}'
  AND is_active = TRUE
"""

users_soft_delete_rule_assignment = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE project_id IN (
    SELECT project_id
    FROM {catalog}.{schema_metadata}.project
    WHERE project_nk = '{project}'
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