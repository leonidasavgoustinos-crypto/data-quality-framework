# Databricks notebook source
# MAGIC %md
# MAGIC # Mapping Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reconciliation.mapping

# COMMAND ----------

# MAGIC %md
# MAGIC # Execution 

# COMMAND ----------

dbutils.library.restartPython()

import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/reconciliation')
from framework.reconciliation_framework import reconciliation

#reconciliation("rec_demo_dif_type")
#reconciliation("rec_demo_extra_col")
#reconciliation("rec_demo_extra_row")
reconciliation("rec_demo_dif_hash")


# COMMAND ----------

# MAGIC %md
# MAGIC # Results

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reconciliation.results
# MAGIC where 1=1
# MAGIC --and reconciliation_type = 'row_count'
# MAGIC --and reconciliation_type = 'data_types'
# MAGIC --and reconciliation_type = 'column_names'
# MAGIC and reconciliation_type = 'row_values'
# MAGIC --and result_status = 'fail'
# MAGIC and table in 
# MAGIC (
# MAGIC 'rec_demo_dif_type',
# MAGIC 'rec_demo_extra_col',
# MAGIC 'rec_demo_extra_row',
# MAGIC 'rec_demo_dif_hash'
# MAGIC )
# MAGIC and execution_timestamp > '2026-04-05'
# MAGIC order by execution_timestamp desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouped

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC select execution_timestamp, table, reconciliation_type, result_status, count(*)
# MAGIC from analytics_dq_dev.reconciliation.results 
# MAGIC where execution_timestamp = (select max(execution_timestamp) from analytics_dq_dev.reconciliation.results)
# MAGIC group by execution_timestamp, table, reconciliation_type, result_status
# MAGIC order by execution_timestamp desc, reconciliation_type;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deep dive

# COMMAND ----------

# MAGIC %md
# MAGIC ## Old Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reconciliation_sas.rec_demo_dif_type_old;
# MAGIC select * from analytics_dq_dev.reconciliation_sas.rec_demo_extra_col_old;
# MAGIC select * from analytics_dq_dev.reconciliation_sas.rec_demo_extra_row_old;
# MAGIC select * from analytics_dq_dev.reconciliation_sas.rec_demo_dif_hash_old;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## New Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reconciliation_databricks.rec_demo_dif_type_new;
# MAGIC select * from analytics_dq_dev.reconciliation_databricks.rec_demo_extra_col_new;
# MAGIC select * from analytics_dq_dev.reconciliation_databricks.rec_demo_extra_row_new;
# MAGIC select * from analytics_dq_dev.reconciliation_databricks.rec_demo_dif_hash_new;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row differences

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing in old

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reconciliation.rec_demo_extra_row_missing_in_old;
# MAGIC select * from analytics_dq_dev.reconciliation.rec_demo_dif_hash_missing_in_old;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing in new

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analytics_dq_dev.reconciliation.rec_demo_dif_hash_missing_in_new;