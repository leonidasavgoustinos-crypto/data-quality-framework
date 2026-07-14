import sys
import os

cwd = os.getcwd()
sys.path.append(os.path.abspath(os.path.join(cwd, '..', '..')))

from databricks.sdk.runtime import dbutils 
from framework.dq_framework import run_data_quality


def get_apply_at_value_for_table(spark, env, target_table):
    
    apply_at_value = None

    sqlstatement = f"""SELECT DISTINCT apply_at FROM analytics_dq_{env}.reporting.v_consolidated_configuration_metadata
        WHERE table_nk='{target_table}'
        AND policy_category='error'
        AND is_active = true
        AND apply_at like '%_to_%'"""

    df = spark.sql(sqlstatement)

    if df.count()>0:

        apply_at_value = df.collect()[0]['apply_at']

    return apply_at_value


def get_project_value_for_table(spark, env, target_table):
    
    project_value = None

    sqlstatement = f"""SELECT DISTINCT project FROM analytics_dq_{env}.reporting.v_consolidated_configuration_metadata
        WHERE table_nk='{target_table}'
        AND policy_category='error'
        AND is_active = true"""

    df = spark.sql(sqlstatement)

    if df.count()>0:

        project_value = df.collect()[0]['project']

    return project_value


def run_dq_and_insert(
    spark,
    df_input,
    full_table_name: str,
    env: str = "dev",
    run_mode: str = "error",
    display_logs: bool = True
):
    
    apply_at_value = get_apply_at_value_for_table(
        spark=spark,
        env=env,
        target_table=full_table_name
    )

    project_value = get_project_value_for_table(
        spark=spark,
        env=env,
        target_table=full_table_name
    )

    job_id = run_data_quality(
        apply_at=apply_at_value,
        run_mode=run_mode,
        full_table_name=full_table_name,
        project_name=project_value,
        df_input=df_input,
        display_logs=display_logs
    )
    dq_status = (
        spark.table(f"analytics_audit.{env}.processing_job_status")
             .select("status")
             .where(f"job_id = '{job_id}'")
             .collect()
    )

    if dq_status and dq_status[0]["status"] == "failed":
        dbutils.notebook.exit(f"❌ Data Quality failed for table {full_table_name}. Insert skipped.")

    print(f"✅ Data Quality passed for table {full_table_name}. Proceeding with insert.")

    (
        df_input.write
            .mode("append")
            .insertInto(full_table_name)
    )

 