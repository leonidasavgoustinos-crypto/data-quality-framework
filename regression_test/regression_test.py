# Databricks notebook source
#%pip install databricks-labs-dqx

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append('/Workspace/Users/zeakisko@piraeusbank.gr/AIProjects/data_quality_framework/framework')
from framework.dq_framework import run_data_quality

job_id = run_data_quality(apply_at='test', run_mode='all',display_logs= True)
#job_id = run_data_quality(apply_at='test_test', run_mode='all',display_logs= True)


table_name = 'analytics_fs_dev.risk.fs_global_customer_quarterly'
df_input = spark.table(table_name)
#job_id = run_data_quality(apply_at='bronze_to_fs', run_mode='all', full_table_name=table_name, df_input=df_input)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE GLOBAL TEMP VIEW latest_result_ids AS
WITH latest_results AS (
    SELECT c.result_id
    FROM analytics_dq_dev.metadata.table a
    JOIN analytics_dq_dev.results.result_table b
        ON a.table_id = b.table_id
    JOIN analytics_dq_dev.results.result c
        ON b.result_id = c.result_id
    WHERE a.table_nk IN (
        'analytics_dq_dev.test_cases.test_case_1',
        'analytics_dq_dev.test_cases.test_case_2'
    )
    ORDER BY c.start_timestamp DESC
    LIMIT 2
)
SELECT DISTINCT result_id
FROM latest_results
""")

COMPARISONS = [
    {
        "name": "RESULT",
        "prod": "analytics_dq_dev.results.result",
        "test": "analytics_dq_dev.test_cases.test_cases_results",
        "ignore": ["result_id", "start_timestamp", "end_timestamp"]
    },
    {
        "name": "TABLE",
        "prod": "analytics_dq_dev.results.result_table",
        "test": "analytics_dq_dev.test_cases.test_cases_table_results",
        "ignore": ["result_id", "result_table_id", "start_timestamp", "end_timestamp"]
    },
    {
        "name": "RULE",
        "prod": "analytics_dq_dev.results.result_rule",
        "test": "analytics_dq_dev.test_cases.test_cases_rule_results",
        "ignore": [
            "result_id",
            "result_table_id",
            "result_rule_id",
            "start_timestamp",
            "end_timestamp"
        ]
    }
]

def selectable_columns_from_prod_only(prod_table, ignore_columns):
    cols = spark.table(prod_table).columns
    cols = [c for c in cols if c not in ignore_columns]
    return ", ".join(sorted(cols))


for c in COMPARISONS:
    print(f"\n🔍 CHECKING: {c['name']}")

    cols = selectable_columns_from_prod_only(c["prod"], c["ignore"])

    spark.sql(f"""
        CREATE OR REPLACE GLOBAL TEMP VIEW prod_view AS
        SELECT {cols}
        FROM {c['prod']}
        WHERE result_id IN (
            SELECT result_id FROM global_temp.latest_result_ids
        )
    """)

    spark.sql(f"""
        CREATE OR REPLACE GLOBAL TEMP VIEW test_view AS
        SELECT {cols}
        FROM {c['test']}
        WHERE result_id IN (
            SELECT result_id FROM global_temp.latest_result_ids
        )
    """)

    diff_df = spark.sql("""
        SELECT * FROM global_temp.prod_view
        EXCEPT
        SELECT * FROM global_temp.test_view

        UNION ALL

        SELECT * FROM global_temp.test_view
        EXCEPT
        SELECT * FROM global_temp.prod_view
    """)

    diff_count = diff_df.count()

    if diff_count == 0:
        print(f"✅ [{c['name']}] EQUAL")
    else:
        print(f"❌ [{c['name']}] NOT EQUAL")
        print(f"👉 DIFFERENT ROWS: {diff_count}")
        display(diff_df)

IGNORE_QUARANTINE_COLUMNS = [
    "result_id",
    "result_table_id",
    "result_rule_id"
]
print(f"\n🔍 CHECKING: QUARANTINE")
def selectable_columns(table, ignore_columns):
    cols = spark.table(table).columns
    return ", ".join([c for c in cols if c not in ignore_columns])
 
prod_table = "analytics_dq_dev.quarantine.test_cases_test_case_1"
test_table = "analytics_dq_dev.test_cases.test_cases_quarantine"
 
prod_cols = selectable_columns(prod_table, IGNORE_QUARANTINE_COLUMNS)
test_cols = selectable_columns(test_table, IGNORE_QUARANTINE_COLUMNS)
 
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW quarantine_prod_view AS
    SELECT {prod_cols}
    FROM {prod_table}
""")
 
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW quarantine_test_view AS
    SELECT {test_cols}
    FROM {test_table}
""")

diff_df = spark.sql("""
    SELECT * FROM quarantine_prod_view
    EXCEPT ALL
    SELECT * FROM quarantine_test_view
 
    UNION ALL
 
    SELECT * FROM quarantine_test_view
    EXCEPT ALL
    SELECT * FROM quarantine_prod_view
""")
 
diff_count = diff_df.count()
 
if diff_count == 0:
    print("✅ [QUARANTINE] EQUAL")
else:
    print("❌ [QUARANTINE] NOT EQUAL")
    print(f"👉 DIFFERENT ROWS: {diff_count}")
    display(diff_df)

 