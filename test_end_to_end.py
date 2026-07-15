# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Framework — End-to-End Test (SQL Engine)
# MAGIC
# MAGIC **Dataset:** `samples.nyctaxi.trips` (built-in Databricks sample)
# MAGIC
# MAGIC **Test:** Check that column `passenger_count` has no NULL values.
# MAGIC
# MAGIC **Engine:** `sql` (Returns rows that fail the condition, i.e., rows where passenger_count IS NULL)
# MAGIC
# MAGIC **Run cells in order — top to bottom.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Restart Python
# MAGIC Clears the state to ensure a clean run.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Setup Environment
# MAGIC Adds the repository to the Python path and defines the catalog where the framework tables reside.

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/leonidas.avgoustinos@ms.d-one.ai/data-quality-framework')

CATALOG = "analytics_dq_dev"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Verify sample dataset exists
# MAGIC Just a quick check to make sure we can read the NYC Taxi sample data.

# COMMAND ----------

df_sample = spark.table("samples.nyctaxi.trips")
display(df_sample.limit(5))
print(f"Total rows: {df_sample.count()}")
print(f"Columns: {df_sample.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Clear all tables (Fresh start)
# MAGIC Truncates all metadata, configuration, results, and audit tables so we can start from scratch.

# COMMAND ----------

tables_to_clear = [
    f"{CATALOG}.configuration.rule_assignment",
    f"{CATALOG}.configuration.rule_template",
    f"{CATALOG}.configuration.run_policy",
    f"{CATALOG}.configuration.apply_at",
    f"{CATALOG}.metadata.`table`", # Notice the backticks for reserved keyword
    f"{CATALOG}.metadata.project",
    f"{CATALOG}.metadata.rule_type",
    f"{CATALOG}.metadata.rule_dimension",
    f"{CATALOG}.results.result",
    f"{CATALOG}.results.result_table",
    f"{CATALOG}.results.result_rule",
]

for t in tables_to_clear:
    spark.sql(f"TRUNCATE TABLE {t}")
    print(f"Cleared: {t}")

spark.sql("TRUNCATE TABLE analytics_audit.dev.processing_job_status")
print("Cleared: analytics_audit.dev.processing_job_status")
print("All tables cleared!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Load minimal configuration
# MAGIC Populates the necessary lookup tables: rule types, dimensions, projects, run policies, and application layers. 

# COMMAND ----------

# 4a. RULE TYPE — category of rule (technical or business)
spark.sql(f"""
    INSERT INTO {CATALOG}.metadata.rule_type
        (rule_type, description, is_active, added_by, valid_from, valid_to)
    VALUES
        ('technical', 'Technical data profiling quality rules', true, 'test_user', current_timestamp(), NULL)
""")
print("rule_type: technical")

# 4b. RULE DIMENSION — which DQ dimension (completeness, validity, uniqueness, consistency)
spark.sql(f"""
    INSERT INTO {CATALOG}.metadata.rule_dimension
        (rule_dimension, description, is_active, added_by, valid_from, valid_to)
    VALUES
        ('completeness', 'All required data must be present (no NULLs)', true, 'test_user', current_timestamp(), NULL)
""")
print("rule_dimension: completeness")

# 4c. PROJECT — logical grouping of tables and rules
spark.sql(f"""
    INSERT INTO {CATALOG}.metadata.project
        (project, project_description, is_active, added_by, valid_from, valid_to)
    VALUES
        ('test_project', 'Simple test with NYC Taxi sample data', true, 'test_user', current_timestamp(), NULL)
""")
print("project: test_project")

# 4d. RUN POLICY — what happens on failure (error vs warning, quarantine or not)
spark.sql(f"""
    INSERT INTO {CATALOG}.configuration.run_policy
        (criticality, category, quarantine, is_active, added_by, valid_from, valid_to)
    VALUES
        ('high', 'error', false, true, 'test_user', current_timestamp(), NULL)
""")
print("run_policy: error / high / no quarantine")

# 4e. APPLY AT — which Medallion layer to check
# apply_at_nk is auto-generated as "gold.project_agnostic" (is_project_specific=false)
spark.sql(f"""
    INSERT INTO {CATALOG}.configuration.apply_at
        (apply_at, is_project_specific, is_active, added_by, valid_from, valid_to)
    VALUES
        ('gold', false, true, 'test_user', current_timestamp(), NULL)
""")
print("apply_at: gold")
print("Minimal config loaded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Create rule template (SQL Engine)
# MAGIC Defines the generic `is_not_null` rule using pure SQL. The framework expects the SQL statement to return the *failing* records.

# COMMAND ----------

rule_type_id = spark.sql(f"SELECT rule_type_id FROM {CATALOG}.metadata.rule_type WHERE rule_type='technical'").collect()[0][0]
rule_dim_id  = spark.sql(f"SELECT rule_dimension_id FROM {CATALOG}.metadata.rule_dimension WHERE rule_dimension='completeness'").collect()[0][0]

# engine_type = "sql"
# statement: SELECT * FROM ${table} WHERE ${i:column} IS NULL
spark.sql(f"""
    INSERT INTO {CATALOG}.configuration.rule_template
        (name, description, rule_type_id, rule_dimension_id,
         scope, is_reusable, engine_type, statement, is_active, added_by, valid_from, valid_to)
    VALUES (
        'is_not_null',
        'Column must not contain NULL values',
        {rule_type_id}, {rule_dim_id},
        'column', true, 'sql',
        'SELECT * FROM ${{table}} WHERE ${{i:column}} IS NULL',
        true, 'test_user', current_timestamp(), NULL
    )
""")
print("rule_template: is_not_null (engine=sql)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Register the table to check
# MAGIC Tells the framework about the `samples.nyctaxi.trips` table. `is_active=true` is required.

# COMMAND ----------

# filter_field: tpep_pickup_datetime. The framework will test the latest partition by default based on this field.
spark.sql(f"""
    INSERT INTO {CATALOG}.metadata.`table`
        (environment, `catalog`, `schema`, `table`, layer,
         primary_key, filter_field, filter_field_type,
         is_active, added_by, valid_from, valid_to)
    VALUES (
        'dev', 'samples', 'nyctaxi', 'trips', 'gold',
        array('tpep_pickup_datetime'),
        'tpep_pickup_datetime', 'date',
        true, 'test_user', current_timestamp(), NULL
    )
""")
print("table registered: samples.nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Assign rule to table
# MAGIC Links the rule template, the table, the policy, and the project together, while providing the specific column name (`passenger_count`) to check.

# COMMAND ----------

template_id = spark.sql(f"SELECT rule_template_id FROM {CATALOG}.configuration.rule_template WHERE name='is_not_null'").collect()[0][0]
table_id    = spark.sql(f"SELECT table_id FROM {CATALOG}.metadata.`table` WHERE `table`='trips'").collect()[0][0]
policy_id   = spark.sql(f"SELECT policy_id FROM {CATALOG}.configuration.run_policy WHERE category='error' AND criticality='high'").collect()[0][0]
apply_at_id = spark.sql(f"SELECT apply_at_id FROM {CATALOG}.configuration.apply_at WHERE apply_at='gold'").collect()[0][0]
project_id  = spark.sql(f"SELECT project_id FROM {CATALOG}.metadata.project WHERE project='test_project'").collect()[0][0]

print(f"IDs: template={template_id}, table={table_id}, policy={policy_id}, apply_at={apply_at_id}, project={project_id}")

# parameters_identifiers: maps ${i:column} placeholder → 'passenger_count'
spark.sql(f"""
    INSERT INTO {CATALOG}.configuration.rule_assignment
        (rule_template_id, template_nk, policy_id, project_id, project_nk,
         table_id, table_nk, apply_at_id, apply_at_nk,
         parameters_identifiers, parameters_values,
         is_active, added_by, valid_from, valid_to)
    VALUES (
        {template_id}, 'is_not_null',
        {policy_id},   {project_id}, 'test_project',
        {table_id},    'samples.nyctaxi.trips',
        {apply_at_id}, 'gold.project_agnostic',
        '{{"column": "passenger_count"}}',
        '{{}}',
        true, 'test_user', current_timestamp(), NULL
    )
""")
print("rule_assignment: is_not_null -> passenger_count -> samples.nyctaxi.trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Verify config (sanity check)
# MAGIC Confirms that the assignment is active and all joined entities are active.

# COMMAND ----------

# All rows must show true in every _active column
spark.sql(f"""
    SELECT
        t.table_nk,
        rt.name             AS rule,
        rt.engine_type,
        aa.apply_at_nk,
        rp.category,
        t.is_active         AS table_active,
        rt.is_active        AS template_active,
        rp.is_active        AS policy_active,
        aa.is_active        AS apply_at_active,
        ra.is_active        AS assignment_active
    FROM {CATALOG}.configuration.rule_assignment ra
    JOIN {CATALOG}.metadata.`table`              t  ON t.table_id          = ra.table_id
    JOIN {CATALOG}.configuration.rule_template   rt ON rt.rule_template_id = ra.rule_template_id
    JOIN {CATALOG}.configuration.run_policy      rp ON rp.policy_id        = ra.policy_id
    JOIN {CATALOG}.configuration.apply_at        aa ON aa.apply_at_id      = ra.apply_at_id
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Run the framework!
# MAGIC Invokes the core framework execution logic. We pass `project_name` to ensure it targets our setup.

# COMMAND ----------

from framework.dq_framework import run_data_quality

job_id = run_data_quality(
    apply_at='gold',
    run_mode='all',
    full_table_name='samples.nyctaxi.trips',
    project_name='test_project',
    display_logs=True
)

print(f"Done! job_id = {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — View results
# MAGIC Queries the results and audit tables to inspect the outcome.

# COMMAND ----------

print("=== EXECUTION RESULT ===")
spark.sql(f"""
    SELECT status, tables_evaluated, tables_pass, tables_error, tables_warning,
           start_timestamp, end_timestamp
    FROM {CATALOG}.results.result
    ORDER BY start_timestamp DESC LIMIT 3
""").show(truncate=False)

print("=== TABLE RESULT ===")
spark.sql(f"""
    SELECT status, rules_evaluated, rules_pass, rules_error, start_timestamp
    FROM {CATALOG}.results.result_table
    ORDER BY start_timestamp DESC LIMIT 3
""").show(truncate=False)

print("=== RULE RESULT ===")
spark.sql(f"""
    SELECT status, rows_evaluated, rows_pass, rows_failed, start_timestamp
    FROM {CATALOG}.results.result_rule
    ORDER BY start_timestamp DESC LIMIT 3
""").show(truncate=False)

print("=== AUDIT LOG ===")
spark.sql("""
    SELECT job_id, status, start_time, end_time
    FROM analytics_audit.dev.processing_job_status
    ORDER BY start_time DESC LIMIT 3
""").show(truncate=False)
