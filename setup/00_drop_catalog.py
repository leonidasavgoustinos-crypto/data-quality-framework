# Databricks notebook source
dbutils.widgets.text("target_environment", "dev")
environment = dbutils.widgets.get("target_environment")

catalog = f"analytics_dq_{environment}"

print(f"Dropping catalog {catalog} and all its contents...")
try:
    spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
    print(f"Successfully dropped catalog {catalog}")
except Exception as e:
    print(f"Error dropping catalog {catalog}: {e}")
