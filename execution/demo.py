# Databricks notebook source
# MAGIC %md
# MAGIC #Data quality framework - Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ##Metadata tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###project

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.metadata.project

# COMMAND ----------

# MAGIC %md
# MAGIC ###table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.metadata.table order by table_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ###rule_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.metadata.rule_type

# COMMAND ----------

# MAGIC %md
# MAGIC ###rule_dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.metadata.rule_dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ###rule_dqx

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.metadata.rule_dqx

# COMMAND ----------

# MAGIC %md
# MAGIC ##Configuration Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###rule_template

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.configuration.rule_template order by rule_template_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ###rule_assignment

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.configuration.rule_assignment order by rule_assignment_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.configuration.parameters order by parameters_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### apply_at

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.configuration.apply_at

# COMMAND ----------

# MAGIC %md
# MAGIC ###run_policy

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.configuration.run_policy

# COMMAND ----------

# MAGIC %md
# MAGIC ##Execution

# COMMAND ----------

# MAGIC %md
# MAGIC ###Example 1 - Single layer checks

# COMMAND ----------

#%pip install databricks-labs-dqx
dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/data_quality_framework')
from framework.dq_framework import run_data_quality

job_id = run_data_quality(apply_at='bronze', run_mode='all')
#job_id = run_data_quality(apply_at='bronze', run_mode='all', filter_values='20250101')
#job_id = run_data_quality(apply_at='fs', run_mode='all', filter_values=['2012-03-30', '2017-09-29'])
#job_id = run_data_quality(apply_at='fs', run_mode='warning')


print(f"job_id = {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Example 2 - Between layers checks

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/data_quality_framework/framework')
from framework.dq_framework import run_data_quality

table_name = 'analytics_fs_dev.risk.fs_global_customer_quarterly'
df_input = spark.table(table_name)
job_id = run_data_quality(apply_at='bronze_to_fs', run_mode='warning', full_table_name=table_name, df_input=df_input)

print(f"job_id = {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Example 3 - Code failure

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/data_quality_framework/framework')
from framework.dq_framework import run_data_quality

job_id = run_data_quality(project_name='project_2', apply_at='gold', run_mode='error')

print(f"job_id = {job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarantine Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.quarantine.risk_fs_global_customer_quarterly;

# COMMAND ----------

# MAGIC %sql
# MAGIC select result_rule_id, category, count(*) from analytics_dq_dev.quarantine.risk_fs_global_customer_quarterly group by result_rule_id, category

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.quarantine.risk_fs_global_exposure_quarterly;

# COMMAND ----------

# MAGIC %sql
# MAGIC select result_rule_id, category, count(*) from analytics_dq_dev.quarantine.risk_fs_global_exposure_quarterly group by result_rule_id, category

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.quarantine.sas_test_customers_pk_violation_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC select result_rule_id, category, count(*) from analytics_dq_dev.quarantine.sas_test_customers_pk_violation_test group by result_rule_id, category

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results and logs

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_audit.dev.processing_job_status order by end_time desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.results.result order by start_timestamp desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.results.result_table order by start_timestamp desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.results.result_rule order by result_rule_id desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reporting

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reporting.v_consolidated_configuration_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reporting.v_dq_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select Project, catalog, schema, rule_type, rule_dimension, status, count(*) as rules_checked from analytics_dq_dev.reporting.v_dq_results group by Project, catalog, schema, rule_type, rule_dimension, status order by Project, catalog, schema, rule_type, rule_dimension, status