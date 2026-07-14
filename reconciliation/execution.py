# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/reconciliation')
from framework.reconciliation_framework import reconciliation

reconciliation("vintagesegment_b4y")