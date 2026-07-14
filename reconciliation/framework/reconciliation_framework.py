from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from .utils import *
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

 
spark = SparkSession.builder.getOrCreate()

def reconciliation(table_name):
    log_section("STARTING RECONCILIATION")
    print(f"🔍 Table key: {table_name}")

    mapping_df = spark.table("analytics_dq_dev.reconciliation.mapping")
    row = mapping_df.filter(f"table_name = '{table_name}'").select("old", "new").first()

    if not row:
        raise ValueError(f"No mapping found for table_name = {table_name}")

    old_table = row.old
    new_table = row.new

    print(f"📂 Old table: {old_table}")
    print(f"📂 New table: {new_table}")

    results_table = "analytics_dq_dev.reconciliation.results"

    results_row_count = check_row_count(table_name, old_table, new_table)
    results_data_types = check_data_types(table_name, old_table, new_table)
    results_column_names = check_column_names(table_name, old_table, new_table)
    results_row_values = check_row_values(table_name, old_table, new_table)

    execution_ts = datetime.now()

    write_results_to_delta(results_row_count, results_table, execution_ts)
    write_results_to_delta(results_data_types, results_table, execution_ts)
    write_results_to_delta(results_column_names, results_table, execution_ts)
    write_results_to_delta(results_row_values, results_table, execution_ts)

    log_section("RECONCILIATION COMPLETED")
    log_section("RESULTS LOCATION")
    log_kv("Catalog", "analytics_dq_dev")
    log_kv("Schema", "reconciliation")
