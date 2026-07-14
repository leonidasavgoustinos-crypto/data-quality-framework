from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

 
spark = SparkSession.builder.getOrCreate()

def log_section(title):
    print("\n" + "="*80)
    print(f"📌 {title}")
    print("="*80 + "\n")

def log_kv(key, value, indent=2):
    print(" " * indent + f"- {key}: {value}")

def normalize_column(c):
    c = F.coalesce(c.cast("string"), F.lit("__NULL__"))
 
    c = F.when(
        c.cast("double").isNotNull(),
        F.round(c.cast("double"), 8)
    ).otherwise(c)
 
    return c.cast("string")

def build_concat_expr(cols):
    expr = []
    for c in cols:
        expr.append(c)
        expr.append(F.lit("||"))
    return F.concat(*expr[:-1])



def check_row_count(table_name, old_table, new_table):
    old_df = spark.table(old_table)
    new_df = spark.table(new_table)

    old_cnt = old_df.count()
    new_cnt = new_df.count()
    diff = abs(old_cnt - new_cnt)

    log_section("ROW COUNT CHECK")
    log_kv("Table", table_name)
    log_kv("Old table", old_table)
    log_kv("New table", new_table)
    log_kv("Old count", old_cnt)
    log_kv("New count", new_cnt)
    log_kv("Difference", diff)
    log_kv("Status", "PASS" if diff == 0 else "FAIL")

    return [{
        "reconciliation_type": "row_count",
        "table": table_name,
        "old_table": old_table,
        "new_table": new_table,
        "result_old": old_cnt,
        "result_new": new_cnt,
        "additional_info": None
    }]



def check_data_types(table_name, old_table, new_table):
    old_df = spark.table(old_table)
    new_df = spark.table(new_table)

    common_cols = sorted(set(old_df.columns).intersection(new_df.columns))

    log_section("DATA TYPE CHECK")
    log_kv("Table", table_name)
    log_kv("Columns checked", len(common_cols))

    mismatches = 0
    results = []

    for col in common_cols:
        dtype_old = str(old_df.schema[col].dataType)
        dtype_new = str(new_df.schema[col].dataType)

        if dtype_old != dtype_new:
            mismatches += 1

        results.append({
            "reconciliation_type": "data_types",
            "table": table_name,
            "old_table": old_table,
            "new_table": new_table,
            "result_old": dtype_old,
            "result_new": dtype_new,
            "additional_info": col,

        })

    log_kv("Mismatches", mismatches)
    log_kv("Status", "PASS" if mismatches == 0 else "FAIL")

    return results



def check_column_names(table_name, old_table, new_table):
    old_df = spark.table(old_table)
    new_df = spark.table(new_table)

    old_cols = set(old_df.columns)
    new_cols = set(new_df.columns)

    missing_in_new = sorted(old_cols - new_cols)
    missing_in_old = sorted(new_cols - old_cols)

    log_section("COLUMN NAME CHECK")
    log_kv("Table", table_name)
    log_kv("Columns in old", len(old_cols))
    log_kv("Columns in new", len(new_cols))
    log_kv("Missing in NEW table", missing_in_new or "None")
    log_kv("Missing in OLD table", missing_in_old or "None")
    log_kv("Status", "PASS" if not missing_in_new and not missing_in_old else "FAIL")

    results = []

    all_cols = sorted(old_cols.union(new_cols))
    for col in all_cols:
        results.append({
            "reconciliation_type": "column_names",
            "table": table_name,
            "old_table": old_table,
            "new_table": new_table,
            "result_old": col if col in old_cols else None,
            "result_new": col if col in new_cols else None,
            "additional_info": None
        })

    return results


def check_row_values(table_name, old_table, new_table):
    old_df = spark.table(old_table)
    new_df = spark.table(new_table)

    common_cols = sorted(set(old_df.columns).intersection(new_df.columns))

    log_section("ROW VALUE CHECK")
    log_kv("Table", table_name)
    log_kv("Columns compared", len(common_cols))

    old_norm = old_df.select([normalize_column(F.col(c)).alias(c) for c in common_cols])
    new_norm = new_df.select([normalize_column(F.col(c)).alias(c) for c in common_cols])

    old_hashed = old_norm.withColumn("_row_hash",F.sha2(build_concat_expr([F.col(c) for c in common_cols]), 256)).select("_row_hash", *common_cols)
    new_hashed = new_norm.withColumn("_row_hash",F.sha2(build_concat_expr([F.col(c) for c in common_cols]), 256)).select("_row_hash", *common_cols)

    old_hash_only = old_hashed.select("_row_hash")
    new_hash_only = new_hashed.select("_row_hash")

    missing_in_new = old_hash_only.exceptAll(new_hash_only)
    missing_in_old = new_hash_only.exceptAll(old_hash_only)

    missing_in_new_full = (missing_in_new.join(old_hashed, "_row_hash")).drop("_row_hash")
    missing_in_old_full = (missing_in_old.join(new_hashed, "_row_hash")).drop("_row_hash")

    cnt_missing_new = missing_in_new.count()
    cnt_missing_old = missing_in_old.count()

    log_kv("Status", "PASS" if cnt_missing_new == 0 and cnt_missing_old == 0 else "FAIL")

    if cnt_missing_new > 0:
        missing_in_new_table = f"analytics_dq_dev.reconciliation.{table_name}_missing_in_new"
        missing_in_new_full.write.mode("overwrite").saveAsTable(missing_in_new_table)
        log_section("MISSING ROWS FOUND (EXIST IN OLD → NOT IN NEW)")
        log_kv("Description", "Rows present in OLD table but missing in NEW table")
        log_kv("Row count", cnt_missing_new)
        log_kv("Catalog", "analytics_dq_dev")
        log_kv("Schema", "reconciliation")
        log_kv("Created table", missing_in_new_table)

    if cnt_missing_old > 0:
        missing_in_old_table = f"analytics_dq_dev.reconciliation.{table_name}_missing_in_old"
        missing_in_old_full.write.mode("overwrite").saveAsTable(missing_in_old_table)
        log_section("MISSING ROWS FOUND (EXIST IN NEW → NOT IN OLD)")
        log_kv("Description", "Rows present in NEW table but missing in OLD table")
        log_kv("Row count", cnt_missing_old)
        log_kv("Catalog", "analytics_dq_dev")
        log_kv("Schema", "reconciliation")
        log_kv("Created table", missing_in_old_table)


    results = []

    if cnt_missing_new == 0 and cnt_missing_old == 0:
        results.append({
            "reconciliation_type": "row_values",
            "table": table_name,
            "old_table": old_table,
            "new_table": new_table,
            "result_old": "same_row_values",
            "result_new": "same_row_values",
            "additional_info": None
        })
    else:
        results.append({
            "reconciliation_type": "row_values",
            "table": table_name,
            "old_table": old_table,
            "new_table": new_table,
            "result_old": cnt_missing_new,
            "result_new": cnt_missing_old,
            "additional_info": None
        })

    return results


def write_results_to_delta(result_items, results_table, execution_ts):

    result_schema = StructType([
        StructField("execution_timestamp", TimestampType(), True),
        StructField("reconciliation_type", StringType(), True),
        StructField("table", StringType(), True),
        StructField("old_table", StringType(), True),
        StructField("new_table", StringType(), True),
        StructField("result_old", StringType(), True),
        StructField("result_new", StringType(), True),
        StructField("additional_info", StringType(), True),
        StructField("result_status", StringType(), True),
    ])

    if isinstance(result_items, dict):
        result_items = [result_items]

    rows = []

    for r in result_items:
        if r["reconciliation_type"] == "row_values":
            status = "pass" if str(r["result_old"]) == "same_row_values" else "fail"
        else:
            status = "pass" if str(r["result_old"]) == str(r["result_new"]) else "fail"


        rows.append((execution_ts,
                     r["reconciliation_type"],
                     r["table"],
                     r["old_table"],
                     r["new_table"],
                     str(r["result_old"]) if r["result_old"] else None,
                     str(r["result_new"]) if r["result_new"] else None,
                     r["additional_info"],
                     status))

    df = spark.createDataFrame(rows, schema=result_schema)
    df.write.mode("append").saveAsTable(results_table)