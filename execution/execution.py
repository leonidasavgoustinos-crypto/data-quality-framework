# Databricks notebook source
#%pip install databricks-labs-dqx

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Untitled
import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/data_quality_framework/framework')
from framework.dq_framework import run_data_quality

job_id = run_data_quality(apply_at='bronze', run_mode='all')
#job_id = run_data_quality(apply_at='bronze_to_gold', run_mode='all', project_name='sas2py')
#job_id = run_data_quality(apply_at='bronze', run_mode='all', filter_values='20250101')
#job_id = run_data_quality(apply_at='fs', run_mode='all', filter_values=['2012-03-30', '2017-09-29'])
#job_id = run_data_quality(apply_at='fs', run_mode='warning')


table_name = 'analytics_fs_dev.risk.fs_global_customer_quarterly'
df_input = spark.table(table_name)
#job_id = run_data_quality(apply_at='bronze_to_fs', run_mode='all', full_table_name=table_name, df_input=df_input)