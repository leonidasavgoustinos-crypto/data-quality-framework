# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Data Quality Framework - Demo & Exploration
# MAGIC Use this notebook to explore the underlying tables of the DQ framework, trigger executions, and analyze the final results and quarantine records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration and Metadata Tables
# MAGIC Explore the framework tables in the same order used to create the configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Rule Type

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.rule_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Rule Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.rule_dimension;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Project

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.project;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Run Policy

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.run_policy;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Apply At

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.apply_at;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 Rule Template

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.rule_template ORDER BY rule_template_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.table ORDER BY table_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 Rule Assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.rule_assignment ORDER BY rule_assignment_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Execution
# MAGIC Examples of how to trigger the Data Quality framework programmatically.

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/leonidas.avgoustinos@ms.d-one.ai/data-quality-framework')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2.1 - Single Layer Checks
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
# MAGIC ### Example 2.1.1 - Runtime Filter Execution
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
# MAGIC ### Example 2.2 - Code Failure Handling
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
# MAGIC ### Example 2.3 - Data Quality directly on an in-memory DataFrame

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
# MAGIC ## 3. Quarantine Tables
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review quarantined records
# MAGIC SELECT * FROM analytics_dq_dev.quarantine.metadata_test_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Results and Logs
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
# MAGIC ## 5. Reporting Views
# MAGIC Explore consolidated configuration, execution results, and performance metrics.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consolidated configuration and metadata
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_consolidated_configuration_metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consolidated execution, table, and rule results
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_dq_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rule-level execution performance
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_dq_rule_execution_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table-level execution performance
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_dq_table_execution_performance;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall execution performance
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_dq_execution_performance;

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Registering a New Table
# MAGIC **Goal:** Demonstrate how effortlessly a Data Engineer can onboard a new table into the Data Quality framework.
# MAGIC **What we are doing:** We are registering the `samples.tpch.orders` table into our metadata layer. We define its exact location (catalog/schema/table), the logical layer it belongs to (gold), its primary key, and the field used for incremental filtering. No code changes are required in the core DQ engine to support this new dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1. Assigning DQ Rules to the New Table
# MAGIC **Goal:** Showcase the reusability of our Rule Templates and the flexibility of Rule Assignments.
# MAGIC **What we are doing:** Now that the `orders` table is registered, we are dynamically assigning two existing Data Quality rules to it:
# MAGIC 1. **`is_not_null`**: Applied to the `o_totalprice` column to ensure no missing values.

# COMMAND ----------

dbutils.notebook.run("./07_table", 0, {
    "target_environment": "dev", 
    "catalog": "samples", 
    "schema": "tpch", 
    "table": "orders", 
    "layer": "gold", 
    "primary_keys": "o_orderkey", 
    "filter_field": "o_orderdate", 
    "filter_field_type": "date", 
    "added_by": "Demo", 
    "action": "insert/update"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2. Assigning DQ Rules to the New Table
# MAGIC **Goal:** Showcase the reusability of our Rule Templates and the flexibility of Rule Assignments.
# MAGIC **What we are doing:** Now that the `orders` table is registered, we are dynamically assigning two existing Data Quality rule to it:
# MAGIC 1. **`is_not_null`**: Applied to the `o_totalprice` column to ensure no missing values.

# COMMAND ----------

dbutils.notebook.run("./08_rule_assignment", 0, {
    "target_environment": "dev", 
    "table": "samples.tpch.orders", 
    "template": "is_not_null", 
    "policy": "error.high.quarantine", 
    "apply_at": "gold.project_agnostic", 
    "parameters_identifiers": '{"column": "o_totalprice"}', 
    "parameters_values": "", 
    "project": "demo_project", 
    "added_by": "Demo", 
    "action": "insert/update"
})

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/leonidas.avgoustinos@ms.d-one.ai/data-quality-framework')
from framework.dq_framework import run_data_quality

result_id = run_data_quality(
    apply_at="gold", 
    run_mode="all", 
    project_name="demo_project",  
    full_table_name="samples.tpch.orders",
    display_logs=True
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Adding New Dimension
# MAGIC **What we are doing:** We are introducing a new Rule Dimension called `completeness`. This dimension will be used to logically group all rules that check for missing or incomplete data, providing better categorization for our final DQ Dashboards.

# COMMAND ----------

dbutils.notebook.run("./02_rule_dimension", 0, {
    "target_environment": "dev", 
    "rule_dimension": "completeness", 
    "description": "Checks for missing data", 
    "added_by": "Demo", 
    "action": "insert/update"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1. Updating a Rule Template (SCD2 in Action)
# MAGIC **Goal:** Highlight the framework's native Slowly Changing Dimension Type 2 (SCD2) capabilities and historical tracking.
# MAGIC **What we are doing:** We are modifying the existing `is_not_null` rule template. Previously, it was categorized under the `validity` dimension. We are now updating it to map to our newly created `completeness` dimension.
# MAGIC **Behind the scenes:** The framework calculates a hash of the input parameters, detects the change, and automatically performs an SCD2 operation: it "soft-deletes" (deactivates) the old record and inserts a new active record. This ensures we never lose historical tracking of how the rule was configured in the past!

# COMMAND ----------

dbutils.notebook.run("./06_rule_template", 0, {
    "target_environment": "dev", 
    "name": "is_not_null", 
    "description": "value is not null.", 
    "rule_type": "technical", 
    "rule_dimension": "completeness", # <-
    "scope": "column", 
    "is_reusable": "true", 
    "engine_type": "dqx", 
    "statement": "is_not_null", 
    "added_by": "Demo", 
    "action": "insert/update"
})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.metadata.rule_dimension;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics_dq_dev.configuration.rule_template;
