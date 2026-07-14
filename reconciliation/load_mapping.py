# Databricks notebook source
# MAGIC %sql
# MAGIC insert into analytics_dq_dev.reconciliation.mapping
# MAGIC (   table_name, 
# MAGIC     old, 
# MAGIC     new)
# MAGIC values(
# MAGIC     'vintagesegment_b4y', 
# MAGIC     'analytics_dq_dev.reconciliation_sas.vintagesegment_b4y_sas', 
# MAGIC     'analytics_dq_dev.reconciliation_databricks.vintagesegment_b4y');