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
job_id = run_data_quality(
    apply_at="gold", 
    run_mode="all", 
    project_name="demo_project",         
    full_table_name=None,  #none = maximum value   
    display_logs=True
)

# Alternative Execution Modes (Uncomment to test):
# job_id = run_data_quality(apply_at='bronze', run_mode='all', filter_values='20250101', display_logs=True)
# job_id = run_data_quality(apply_at='fs', run_mode='all', filter_values=['2012-03-30', '2017-09-29'], display_logs=True)
# job_id = run_data_quality(apply_at='fs', run_mode='warning', display_logs=True)

print(f"Job ID = {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3.2 - Code Failure Handling
# MAGIC Test how the framework handles exceptions and errors gracefully.

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/leonidas.avgoustinos@ms.d-one.ai/data-quality-framework')
from framework.dq_framework import run_data_quality

# Purposely trigger a failure scenario to demonstrate error logging
job_id = run_data_quality(
    project_name='demo_project', 
    apply_at='gold', 
    run_mode='error',
    display_logs=True
)

print(f"Job ID = {job_id}")

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
# MAGIC ## 📊 6. Reporting
# MAGIC Consolidated views designed for Dashboards and Business Intelligence reporting.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consolidated configuration metadata view
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_consolidated_configuration_metadata;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consolidated DQ execution results view
# MAGIC SELECT * FROM analytics_dq_dev.reporting.v_dq_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary of rules checked across all projects, grouped by status and dimension
# MAGIC SELECT 
# MAGIC     Project, 
# MAGIC     catalog, 
# MAGIC     schema, 
# MAGIC     rule_type, 
# MAGIC     rule_dimension, 
# MAGIC     status, 
# MAGIC     COUNT(*) AS rules_checked 
# MAGIC FROM analytics_dq_dev.reporting.v_dq_results 
# MAGIC GROUP BY Project, catalog, schema, rule_type, rule_dimension, status 
# MAGIC ORDER BY Project, catalog, schema, rule_type, rule_dimension, status;
