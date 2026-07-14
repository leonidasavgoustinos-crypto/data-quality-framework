# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS analytics_dq_dev.reconciliation.mapping (
# MAGIC     table_name STRING NOT NULL,
# MAGIC     old        STRING NOT NULL,
# MAGIC     new        STRING NOT NULL,
# MAGIC     CONSTRAINT pk_table_name PRIMARY KEY (table_name) 
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table analytics_dq_dev.reconciliation.results;
# MAGIC CREATE TABLE IF NOT EXISTS analytics_dq_dev.reconciliation.results (
# MAGIC     execution_timestamp        TIMESTAMP,
# MAGIC     reconciliation_type        STRING ,
# MAGIC     table                      STRING,
# MAGIC     old_table                  STRING,
# MAGIC     new_table                  STRING,
# MAGIC     result_old                 STRING,
# MAGIC     result_new                 STRING,
# MAGIC     additional_info            STRING,
# MAGIC     result_status              STRING
# MAGIC )
# MAGIC USING DELTA;