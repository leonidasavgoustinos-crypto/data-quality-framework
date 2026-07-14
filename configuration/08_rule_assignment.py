# Databricks notebook source
dbutils.widgets.text("target_environment", "")
dbutils.widgets.text("table", "")
dbutils.widgets.text("template", "")
dbutils.widgets.text("policy", "")
dbutils.widgets.text("apply_at", "")
dbutils.widgets.text("parameters_identifiers", "")
dbutils.widgets.text("parameters_values", "")
dbutils.widgets.text("project", "")
dbutils.widgets.text("added_by", "")
dbutils.widgets.dropdown("action", "no_action", ["no_action","insert/update", "delete"])

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col

environment = dbutils.widgets.get("target_environment")
table_nk = dbutils.widgets.get("table")
template_nk = dbutils.widgets.get("template")
policy_nk = dbutils.widgets.get("policy")
apply_at_nk = dbutils.widgets.get("apply_at")
parameters_identifiers = dbutils.widgets.get("parameters_identifiers")
parameters_values = dbutils.widgets.get("parameters_values")
project_nk = dbutils.widgets.get("project")
added_by = dbutils.widgets.get("added_by")
action = dbutils.widgets.get("action")

name = "analytics_dq_"
catalog = f"{name}{environment}"
schema_metadata = "metadata"
schema = "configuration"

##############################################################################################
############################     USER INPUTS RULE_ASSIGNMENT     #############################
##############################################################################################

user_values_rule_assignment = [
    {
        "table_nk": table_nk,
        "template_nk": template_nk,
        "policy_nk": policy_nk,
        "apply_at_nk": apply_at_nk,
        "parameters_identifiers": parameters_identifiers,
        "parameters_values": parameters_values,
        "project_nk": project_nk,
        "added_by": added_by
    }
]
##############################################################################################

##### rule_assingment ######

df_rules = spark.createDataFrame(user_values_rule_assignment)
df_rules.createOrReplaceTempView("rules_assignment_inputs")

users_input_view = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_assignment AS
SELECT
    b.rule_template_id,
    src.template_nk,
    c.policy_id,
    a.table_id,
    src.table_nk,
    proj.project_id,
    NULLIF(src.project_nk, '') AS project_nk,
    e.apply_at_id,
    src.apply_at_nk,

    NULLIF(src.parameters_identifiers, '') AS parameters_identifiers,
    NULLIF(src.parameters_values, '') AS parameters_values,

    CONCAT(
        src.template_nk, '.',
        COALESCE(NULLIF(src.project_nk, ''), '_NULL_'), '.',
        src.table_nk, '.',
        src.apply_at_nk, '.',
        COALESCE(
            to_json(
                from_json(
                    NULLIF(src.parameters_identifiers, ''),
                    'MAP<STRING, STRING>'
                )
            ),
            '_NULL_'
        )
    ) AS rule_assignment_nk,

    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(b.rule_template_id AS string), '_NULL_'),
            coalesce(cast(c.policy_id AS string), '_NULL_'),
            coalesce(cast(a.table_id AS string), '_NULL_'),
            coalesce(cast(proj.project_id AS string), '_NULL_'),
            coalesce(cast(e.apply_at_id AS string), '_NULL_'),
            coalesce(cast(NULLIF(src.parameters_values, '') AS string), '_NULL_')
        ), 256
    ) AS new_hash

FROM rules_assignment_inputs src
JOIN {catalog}.{schema_metadata}.table a
    ON a.table_nk = src.table_nk 
    AND a.is_active = TRUE
JOIN {catalog}.{schema}.rule_template b
    ON b.template_nk = src.template_nk 
    AND b.is_active = TRUE
JOIN {catalog}.{schema}.run_policy c
    ON c.policy_nk = src.policy_nk 
    AND c.is_active = TRUE
JOIN {catalog}.{schema}.apply_at e
    ON e.apply_at_nk = src.apply_at_nk 
    AND e.is_active = TRUE
LEFT JOIN {catalog}.{schema_metadata}.project proj
    ON proj.project_nk = src.project_nk
    AND proj.is_active = TRUE
"""

users_update = f"""
MERGE INTO {catalog}.{schema}.rule_assignment AS t
USING prepared_rule_assignment AS s
ON t.rule_assignment_nk = s.rule_assignment_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.rule_template_id AS string), '_NULL_'),
            coalesce(cast(t.policy_id AS string), '_NULL_'),
            coalesce(cast(t.table_id AS string), '_NULL_'),
            coalesce(cast(t.project_id AS string), '_NULL_'),
            coalesce(cast(t.apply_at_id AS string), '_NULL_'),
            coalesce(cast(NULLIF(t.parameters_values, '') AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

users_insert = f"""
INSERT INTO {catalog}.{schema}.rule_assignment (
    rule_template_id,
    template_nk,
    policy_id,
    table_id,
    table_nk,
    project_id,
    project_nk,
    apply_at_id,
    apply_at_nk,
    parameters_identifiers,
    parameters_values,
    added_by,
    is_active,
    valid_from
)
SELECT
    s.rule_template_id,
    s.template_nk,
    s.policy_id,
    s.table_id,
    s.table_nk,
    s.project_id,
    s.project_nk,
    s.apply_at_id,
    s.apply_at_nk,
    s.parameters_identifiers,
    s.parameters_values,
    s.added_by,
    TRUE,
    current_timestamp()
FROM prepared_rule_assignment s
LEFT ANTI JOIN {catalog}.{schema}.rule_assignment t
    ON  t.rule_assignment_nk = s.rule_assignment_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.rule_template_id AS string), '_NULL_'),
                coalesce(cast(t.policy_id AS string), '_NULL_'),
                coalesce(cast(t.table_id AS string), '_NULL_'),
                coalesce(cast(t.project_id AS string), '_NULL_'),
                coalesce(cast(t.apply_at_id AS string), '_NULL_'),
                coalesce(cast(NULLIF(t.parameters_values, '') AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

users_soft_delete = f"""
UPDATE {catalog}.{schema}.rule_assignment
SET 
    is_active = FALSE,
    valid_to = CURRENT_TIMESTAMP()
WHERE rule_assignment_nk = CONCAT(
        '{template_nk}', '.',
        COALESCE(NULLIF('{project_nk}', ''), '_NULL_'), '.',
        '{table_nk}', '.',
        '{apply_at_nk}', '.',
        COALESCE(
            to_json(
                from_json(
                    NULLIF('{parameters_identifiers}', ''),
                    'MAP<STRING, STRING>'
                )
            ),
            '_NULL_'
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
    print("SCD2 soft delete completed.")