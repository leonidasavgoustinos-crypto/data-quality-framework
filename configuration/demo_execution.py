# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Data Quality Framework - Demo & Exploration
# MAGIC Use this notebook to explore the underlying tables of the DQ framework, trigger executions, and analyze the final results and quarantine records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Metadata Tables
# MAGIC Explore the foundational metadata that defines projects, tables, and rule types.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Project

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.project;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.table ORDER BY table_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Rule Type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.rule_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Rule Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.rule_dimension;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚙️ 2. Configuration Tables
# MAGIC Explore how rules are templated, parameterized, and assigned to specific tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Rule Template

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.rule_template ORDER BY rule_template_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Rule Assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.rule_assignment ORDER BY rule_assignment_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Apply At

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.apply_at;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Run Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.run_policy;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execution
# MAGIC Examples of how to trigger the Data Quality framework programmatically.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/leonidas.avgoustinos@ms.d-one.ai/data-quality-framework')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3.1 - Single Layer Checks
# MAGIC Trigger Data Quality checks for all tables in a specific layer.

# COMMAND ----------

from framework.dq_framework import run_data_quality

# Run all checks for the 'bronze' layer
result_id = run_data_quality(
    apply_at="gold", 
    run_mode="all", 
    project_name="demo_project",  
    full_table_name=None,           
    display_logs=True
)

print(f"Result ID = {result_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3.1.1 - Runtime Filter Execution
# MAGIC Execute checks dynamically by passing a specific filter value (e.g., a timestamp or date) at runtime.

# COMMAND ----------

# Run checks for the 'gold' layer with a specific filter value
result_id = run_data_quality(
    apply_at="gold", 
    run_mode="all", 
    project_name="demo_project",  
    full_table_name=None,
    filter_values={"date": ["2099-12-31"]},  # <--- PASSING THE FILTER VALUE HERE!
    display_logs=True
)

print(f"Result ID = {result_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3.2 - Code Failure Handling
# MAGIC Test how the framework handles exceptions and errors gracefully.

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/leonidas.avgoustinos@ms.d-one.ai/data-quality-framework')
from framework.dq_framework import run_data_quality

# Purposely trigger a failure scenario to demonstrate error logging
result_id = run_data_quality(
    project_name='demo_project', 
    apply_at='gold', 
    run_mode='error',
    display_logs=True
)

print(f"Result ID = {result_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality directly on the in memory DataFrame

# COMMAND ----------

# Create an in-memory DataFrame (e.g. fresh data coming from an API or transformation)
df_custom = spark.sql("SELECT * FROM samples.nyctaxi.trips LIMIT 50")

# Run Data Quality directly on the DataFrame
# We tell it to apply the rules that are configured for 'test_trips_clean'
result_id = run_data_quality(
    apply_at="gold", 
    run_mode="all", 
    project_name="demo_project",         
    full_table_name="analytics_dq_dev.metadata.test_trips_clean",
    df_input=df_custom,        # <--- PASSING THE DATAFRAME HERE!
    display_logs=True
)

# Notice in the logs that it evaluates exactly 50 rows (the size of our dataframe),

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine Tables
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review quarantined records
# MAGIC SELECT * FROM analytics_dq_dev.quarantine.metadata_test_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Results and Logs
# MAGIC Check the rule evaluation outcomes.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High-level framework execution results
# MAGIC SELECT * FROM analytics_dq_dev.results.result ORDER BY start_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table-level evaluation summaries
# MAGIC SELECT * FROM analytics_dq_dev.results.result_table ORDER BY start_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detailed Rule-level evaluation outcomes
# MAGIC SELECT * FROM analytics_dq_dev.results.result_rule ORDER BY result_rule_id DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demonstrating Historization (SCD Type 2)
# MAGIC One of the most powerful features of our Framework is that it maintains a complete historical record for every change we make to our rules or table configurations.
# MAGIC
# MAGIC Let's see what the rule `check_is_not_null_sql` looks like right now in our database.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM analytics_dq_dev.configuration.rule_template 
# MAGIC WHERE name = 'primary_key_violation'

# COMMAND ----------

# MAGIC %md
# MAGIC As we can see, there is only **one active record**. 
# MAGIC
# MAGIC Now, acting as a Business User without programming knowledge, I will attempt to change the description of this rule through the Configuration layer, essentially updating our metadata.

# COMMAND ----------

dbutils.notebook.run("./06_rule_template", 0, 
                     {"target_environment": "dev", 
                      "name": "primary_key_violation", 
                      "description": "UPDATED: This rule checks for Primary Key Violation and was updated during the Live Demo!", 
                      "rule_type": "technical", 
                      "rule_dimension": "uniqueness", 
                      "scope": "table", 
                      "is_reusable": "true", 
                      "engine_type": "sql", 
                      "statement": "select ${i:columns} from ${table} where 1=1 group by ${i:columns} having count(*) > 1", 
                      "added_by": "Demo", 
                      "action": "insert/update"})

# COMMAND ----------

# MAGIC %md
# MAGIC What just happened in the background? The framework detected that the `description` has changed. Instead of doing a simple "overwrite" (which would delete our history), it closed the old record and opened a new one.
# MAGIC
# MAGIC Let's verify this by running the exact same Query as before:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM analytics_dq_dev.configuration.rule_template 
# MAGIC WHERE name = 'primary_key_violation'

# COMMAND ----------

# MAGIC %md
# MAGIC **Result:**
# MAGIC As you can see, we now have **two rows**:
# MAGIC 1. The **old row** (`is_active = false`) was automatically closed at the exact moment we ran the update.
# MAGIC 2. The **new row** (`is_active = true`) was created with our new description and is now the currently active rule!
# MAGIC
# MAGIC This enables complete "Time Travel". If we look at the Data Quality results from 2 months ago, we know exactly how the rule was defined at that specific point in time.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE analytics_dq_dev.metadata.demo_customers AS 
# MAGIC SELECT * FROM samples.tpch.customer LIMIT 1000;

# COMMAND ----------

# MAGIC %md
# MAGIC - catalog: analytics_dq_dev
# MAGIC - schema: metadata
# MAGIC - table: demo_customers
# MAGIC - layer: silver
# MAGIC - primary_keys: c_custkey
# MAGIC - filter_field:
# MAGIC - filter_field_type:
# MAGIC - added_by: Live Demo
# MAGIC - action: insert/update

# COMMAND ----------

# MAGIC %md
# MAGIC - target_environment: dev
# MAGIC - table_nk: analytics_dq_dev.metadata.demo_customers
# MAGIC - template_nk: check_is_not_null_sql
# MAGIC - policy_nk: warning
# MAGIC - parameters: {"column": "c_phone"}
# MAGIC - added_by: Live Demo
# MAGIC - action: insert/update

# COMMAND ----------

from framework.dq_framework import run_data_quality

result_id = run_data_quality(
    apply_at="silver", 
    run_mode="all", 
    project_name="demo_project",  
    full_table_name="analytics_dq_dev.metadata.demo_customers",  
    display_logs=True
)

