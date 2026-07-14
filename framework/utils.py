from .constants import *
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import uuid
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from delta.tables import DeltaTable
import json
 
spark = SparkSession.builder.getOrCreate()
dq_engine = DQEngine(WorkspaceClient())
 
def update_audit_log_failure(params):

    audit_log_failure_delta_table = DeltaTable.forName(spark, AUDIT_LOG)

    audit_log_failure_delta_table.update(
        condition = (
            (F.col("job_id") == params.meta.job_id)
        ),
        set = {
            "status": F.lit(FAILED),
            "end_time": F.current_timestamp(),
            "error_message": F.lit(params.result.error_message)
        }
    )
 
def update_result_execution_failure(params):

    result_execution_failure_delta_table = DeltaTable.forName(spark, RESULT)

    result_execution_failure_delta_table.update(
        condition = (
            (F.col("result_id") == params.result.result_id)
        ),
        set = {
            "status": F.lit(CODE_ERROR),
            "end_timestamp": F.current_timestamp()
        }
    )

def update_result_table_execution_failure(params):

    result_table_execution_failure_delta_table = DeltaTable.forName(spark, RESULT_TABLE)

    result_table_execution_failure_delta_table.update(
        condition = (
            (F.col("result_table_id") == params.result.result_table_id)
        ),
        set = {
            "status": F.lit(CODE_ERROR),
            "end_timestamp": F.current_timestamp()
        }
    )
 
def update_result_rule_execution_failure(params):

    result_rule_execution_failure_delta_table = DeltaTable.forName(spark, RESULT_RULE)

    result_rule_execution_failure_delta_table.update(
        condition = (
            (F.col("result_rule_id") == params.result.result_rule_id)
        ),
        set = {
            "status": F.lit(CODE_ERROR),
            "end_timestamp": F.current_timestamp()
        }
    ) 
 
def open_audit_log(params):

    params.meta.job_id = str(uuid.uuid4())

    audit_log_delta_table = DeltaTable.forName(spark, AUDIT_LOG)

    audit_log_delta_table.update(
        condition = (
            (F.col("status") == RUNNING) &
            (F.col("job_id") != params.meta.job_id) &
            (F.col("job_type") == LOG_JOB_TYPE) &
            F.col("end_time").isNull()
        ),
        set = {
            "status": F.lit(FAILED),
            "end_time": F.current_timestamp()
        }
    )

    spark.sql(f"""INSERT INTO {AUDIT_LOG} 
              (job_id, 
              job_type, 
              target_table, 
              source_table, 
              status, 
              start_time)
            VALUES (
            '{params.meta.job_id}', 
            '{LOG_JOB_TYPE}', 
            'project_id:{params.meta.project_id}', 
            'result_id:{params.result.result_id}', 
            '{RUNNING}', 
            current_timestamp())""")


def close_audit_log(params):

    audit_log_delta_table = DeltaTable.forName(spark, AUDIT_LOG)
    result_df = spark.table(RESULT)

    result_status = result_df \
        .filter(F.col("result_id") == params.result.result_id) \
        .select("status") \
        .collect()[0]["status"]
        
    log_status = COMPLETED if result_status == PASS else FAILED

    audit_log_delta_table.update(
        condition = (
            (F.col("job_id") == params.meta.job_id)
        ),
        set = {
            "status": F.lit(log_status),
            "end_time": F.current_timestamp(),
            "job_type": F.lit(LOG_JOB_TYPE),
            "target_table": F.lit(f"project_id:{params.meta.project_id}"),
            "source_table": F.lit(f"result_id:{params.result.result_id}"),
        }
    )


def open_result(params):

    params.result.result_id = str(uuid.uuid4())

    result_delta_table = DeltaTable.forName(spark, RESULT)

    result_delta_table.update(
        condition = (
            (F.col("status") == RUNNING) &
            (F.col("result_id") != params.result.result_id) &
            F.col("end_timestamp").isNull()
        ),
        set = {
            "status": F.lit(CODE_ERROR),
            "end_timestamp": F.current_timestamp()
        }
    )

    spark.sql(f"""INSERT INTO {RESULT} (result_id, status, start_timestamp)
                  VALUES ('{params.result.result_id}', '{RUNNING}', current_timestamp())""")

def close_result(params):

    result_table_df = spark.table(RESULT_TABLE)
    result_delta_table = DeltaTable.forName(spark, RESULT)

    counters = result_table_df.filter(
        F.col("result_id") == params.result.result_id
    ).select(
        F.count("*").alias("tables_evaluated"),
        F.count_if(F.col("status") == ERROR).alias("tables_error"),
        F.count_if(F.col("status") == WARNING).alias("tables_warning"),
        F.count_if(F.col("status") == PASS).alias("tables_pass")
    ).collect()[0]

    params.result.tables_evaluated = counters["tables_evaluated"]
    params.result.tables_error = counters["tables_error"]
    params.result.tables_warning = counters["tables_warning"]
    params.result.tables_pass = counters["tables_pass"]

    if params.result.tables_error > 0:
        final_status = ERROR
    elif params.result.tables_warning > 0:
        final_status = WARNING
    else:
        final_status = PASS

    result_delta_table.update(
        condition = (F.col("result_id") == params.result.result_id),
        set = {
            "status": F.lit(final_status),
            "end_timestamp": F.current_timestamp(),
            "tables_evaluated": F.lit(params.result.tables_evaluated),
            "tables_error": F.lit(params.result.tables_error),
            "tables_warning": F.lit(params.result.tables_warning),
            "tables_pass": F.lit(params.result.tables_pass),
            "project_id": F.lit(params.meta.project_id)
        }
    )
        

def open_result_table(params):

    params.result.result_table_id = str(uuid.uuid4())

    result_delta_table = DeltaTable.forName(spark, RESULT_TABLE)

    result_delta_table.update(
        condition = (
            (F.col("status") == RUNNING) &
            (F.col("result_id") != params.result.result_id) &
            F.col("end_timestamp").isNull()
        ),
        set = {
            "status": F.lit(CODE_ERROR),
            "end_timestamp": F.current_timestamp()
        }
    )

    spark.sql(f"""INSERT INTO {RESULT_TABLE} (result_table_id, status, start_timestamp)
                  VALUES ('{params.result.result_table_id}', '{RUNNING}', current_timestamp())""")


def close_result_table(params):

    result_table_delta_table = DeltaTable.forName(spark, RESULT_TABLE)

    if params.result.rules_error > 0:
        final_status = ERROR
    elif params.result.rules_warning > 0:
        final_status = WARNING
    else:
        final_status = PASS

    result_table_delta_table.update(
        condition = (
            (F.col("result_table_id") == params.result.result_table_id)
        ),
        set = {
            "status": F.lit(final_status),
            "end_timestamp": F.current_timestamp(),
            "rules_evaluated": F.lit(params.result.rules_evaluated),
            "rules_pass": F.lit(params.result.rules_pass),
            "rules_warning": F.lit(params.result.rules_warning),
            "rules_error": F.lit(params.result.rules_error),
            "result_id": F.lit(params.result.result_id),
            "project_id": F.lit(params.meta.project_id),
            "table_id": F.lit(params.meta.table_id),
        }
    )


def open_result_rule(params):

    params.result.result_rule_id = str(uuid.uuid4())

    result_rule_delta_table = DeltaTable.forName(spark, RESULT_RULE)

    result_rule_delta_table.update(
        condition = (
            (F.col("status") == RUNNING) &
            (F.col("result_id") != params.result.result_id) &
            F.col("end_timestamp").isNull()
        ),
        set = {
            "status": F.lit(CODE_ERROR),
            "end_timestamp": F.current_timestamp()
        }
    )

    spark.sql(f"""INSERT INTO {RESULT_RULE} (result_rule_id, status, start_timestamp)
                  VALUES ('{params.result.result_rule_id}', '{RUNNING}', current_timestamp())""")


def close_result_rule(params):

    result_rule_delta_table = DeltaTable.forName(spark, RESULT_RULE)

    status = params.config.category if params.result.rows_failed > 0 else PASS

    result_rule_delta_table.update(
        condition = (
            (F.col("result_rule_id") == params.result.result_rule_id)
        ),
        set = {
            "status": F.lit(status),
            "end_timestamp": F.current_timestamp(),
            "rows_evaluated": F.lit(params.result.rows_evaluated),
            "rows_failed": F.lit(params.result.rows_failed),
            "rows_pass": F.lit(params.result.rows_pass),
            "statement": F.lit(params.config.statement),
            "result_id": F.lit(params.result.result_id),
            "result_table_id": F.lit(params.result.result_table_id),
            "project_id": F.lit(params.meta.project_id),
            "rule_assignment_id": F.lit(params.config.rule_assignment_id),
            "rule_type_id": F.lit(params.meta.rule_type_id),
            "policy_id": F.lit(params.config.policy_id),
            "rule_type_id": F.lit(params.meta.rule_type_id)
        }
    )



def filter_active_rules_by_mode(active_rules_df, params):
    if params.config.run_mode != ALL:

        active_rules_df = active_rules_df.filter(F.col("category") == params.config.run_mode)
    
    return active_rules_df


def get_active_rules(params):

    meta_table_df = spark.table(META_TABLE)
    run_policy_df = spark.table(RUN_POLICY)
    config_rule_template_df = spark.table(CONFIG_RULE_TEMPLATE)
    apply_at_df = spark.table(APPLY_AT)
    config_rule_assignment_df = spark.table(CONFIG_RULE_ASSIGNMENT)

    meta_table_active_df = meta_table_df.filter(F.col("is_active")).select("*")
    config_run_policy_active_df = run_policy_df.filter(F.col("is_active")).select("*")
    config_rule_template_active_df = config_rule_template_df.filter(F.col('is_active')).select("*")
    apply_at_active_df = apply_at_df.filter((F.col("is_active")) & (F.col('apply_at') == params.config.apply_at)).select("*")

    config_rule_assignment_active_df = (config_rule_assignment_df
                                        .filter((F.col('is_active')) & 
                                                (F.col("project_id").eqNullSafe(F.lit(params.meta.project_id))))
                                        .select("*"))

    if params.meta.full_table_name is not None:
        meta_table_active_df = meta_table_active_df.filter(F.col("table_nk") == params.meta.full_table_name)

    active_rules_df = (
            meta_table_active_df.alias('tbl')
            .join(config_rule_assignment_active_df.alias('assign'), F.col("assign.table_id") ==  F.col("tbl.table_id"), "inner")
            .join(config_rule_template_active_df.alias('templ'), F.col("assign.rule_template_id") ==  F.col("templ.rule_template_id"), "inner")
            .join(config_run_policy_active_df.alias('policy'), F.col("policy.policy_id") == F.col("assign.policy_id"), "inner")
            .join(apply_at_active_df.alias('apply'), F.col("apply.apply_at_id") == F.col("assign.apply_at_id"), "inner")
            .select(F.col("assign.rule_assignment_id"),\
                    F.col("apply.apply_at"),
                    F.col("templ.rule_template_id"),
                    F.col("templ.rule_dimension_id"),
                    F.col("templ.rule_type_id"),
                    F.col("templ.statement"),
                    F.col("templ.name"),
                    F.col("templ.engine_type"),
                    F.col("templ.is_reusable"),
                    F.col("assign.table_id"),
                    F.col("tbl.catalog"),
                    F.col("tbl.schema"),
                    F.col("tbl.table"),
                    F.col("tbl.primary_key"),
                    F.col("tbl.filter_field"),
                    F.col("tbl.filter_field_type"),
                    F.col("assign.policy_id"),
                    F.col("assign.parameters_identifiers"),
                    F.col("assign.parameters_values"),
                    F.col("tbl.quarantine"),
                    F.col("policy.category"),
                    F.col("policy.quarantine").alias("quarantine_flag")
            ))
    active_rules_df = filter_active_rules_by_mode(active_rules_df, params)

    return active_rules_df

def sql_ident(name):
    parts = str(name).split(".")
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)

def sql_literal(v):
    s = str(v).replace("'", "''")
    return f"'{s}'"

def pass_parameters_to_template(params):

    params.config.statement = normalize_template_simple(params.config.statement)

    template = params.config.statement.replace("${table}", "`df_filtered_by_filter_field`")

    parameters_dict = json.loads(params.config.parameters)

    for key, value in parameters_dict.items():
            ph = f"${{{'i:' + key}}}"
            if ph in template:
                if isinstance(value, list):
                    template = template.replace(ph, ", ".join(sql_ident(v) for v in value))
                else:
                    template = template.replace(ph, sql_ident(value))

    for key, value in parameters_dict.items():
        ph = f"${{{'v:' + key}}}"
        if ph in template:
            if isinstance(value, list):
                template = template.replace(ph, ", ".join(sql_literal(v) for v in value))
            else:
                template = template.replace(ph, sql_literal(value))

    params.config.statement = template

def get_project_id(params):

    if params.meta.project_name is not None:

        project_df = spark.table(META_PROJECT)

        params.meta.project_id = project_df.filter(
            (F.col("is_active")) & 
            (F.col("project_nk") == params.meta.project_name)
        ).select("project_id").collect()[0]["project_id"]
 
def filter_by_filter_field(params, df_input):
   
    if df_input is not None:
        return df_input
 
    table_path = f"{params.meta.catalog}.{params.meta.schema}.{params.meta.table}"
    df = spark.table(table_path)

    field = params.meta.filter_field
    field_type = params.meta.filter_field_type
    values_dict = params.config.filter_values or {}
 
    values = values_dict.get(field_type) 
 
    if values is None:
        max_value = df.select(F.max(F.col(field))).first()[0]
        df = df.filter(F.col(field) == max_value)
    else:
        df = df.filter(F.col(field).isin(values))
 
    return df

def run_sql( params, df_filtered_by_filter_field):

    if params.config.is_reusable:
        pass_parameters_to_template(params)
        df_filtered_by_filter_field.createOrReplaceTempView("df_filtered_by_filter_field")

    df_errors = spark.sql(params.config.statement)
    
    return df_errors
 
def run_dqx(params, df_filtered_by_filter_field):

 
    params_dict = json.loads(params.config.parameters)
    col_list = params_dict["column"]
 
    if "column" in params_dict and isinstance(params_dict["column"], list):
        params_dict["column"] = params_dict["column"][0]
 
    checks = [{
        "criticality": "error",
        "check": {
            "function": params.config.statement,
            "arguments": params_dict
        }
    }]
 
    _, df_errors = dq_engine.apply_checks_by_metadata_and_split(df_filtered_by_filter_field, checks)
 
    return df_errors
 
def normalize_template_simple(s: str) -> str:
    return s.replace("\\${", "${") if s else s
 

def drop_quarantine(params):

    table = params.meta.quarantine
    current_project = params.meta.project_id

    if spark.catalog.tableExists(table):

        delta_table = DeltaTable.forName(spark, table)
        df_existing = spark.table(table)
        
        existing_projects = [
            row.project_id for row in df_existing.select("project_id").distinct().collect()
        ]

        only_current = (set(existing_projects) == {current_project})
        mixed = (current_project in existing_projects and len(existing_projects) > 1)

        if only_current:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        elif mixed:
            delta_table.delete(F.col("project_id").eqNullSafe(F.lit(current_project)))

def merge_and_load_quarantine(rule_results, params, load_quarantine):
 
    quarantine_list = []

    for _, df_q in rule_results:
        if df_q is not None:
            quarantine_list.append(df_q)

    if not quarantine_list:
        return None

    df_final = quarantine_list[0]
    for df in quarantine_list[1:]:
        df_final = df_final.unionByName(df)

    load_quarantine(params, df_final)

    return df_final


def prepare_quarantine_rows(params, df_errors):

    if not params.config.quarantine_flag:
        return None

    if (params.result.rows_pass != params.result.rows_evaluated) and (params.result.rows_evaluated != params.result.rows_failed):

        requested_cols = params.meta.primary_key
    
        if isinstance(requested_cols, str):
            requested_cols = [c.strip() for c in requested_cols.split(",")]
        else:
            requested_cols = [str(c).strip() for c in requested_cols]
    
        df_quarantine = df_errors.select([F.col(c) for c in requested_cols])
    
        df_quarantine = (
            df_quarantine
                .withColumn("result_id", F.lit(params.result.result_id))
                .withColumn("result_table_id", F.lit(params.result.result_table_id))
                .withColumn("result_rule_id", F.lit(params.result.result_rule_id))
                .withColumn("project_id", F.lit(params.meta.project_id).cast("long"))
                .withColumn("category", F.lit(params.config.category))
        )
        return df_quarantine

    return None

        
def load_quarantine(params, df_quarantine):

    table = params.meta.quarantine

    if not spark.catalog.tableExists(table):
        df_quarantine.write.format("delta").mode("overwrite").saveAsTable(table)
    else:
        df_quarantine.write.format("delta").mode("append").saveAsTable(table)


def update_rule_counters(params):
    params.result.rules_evaluated += 1

    if params.result.rows_failed == 0:
        params.result.rules_pass += 1
    else:
        if params.config.category == ERROR:
            params.result.rules_error += 1
        elif params.config.category == WARNING:
            params.result.rules_warning += 1


def load_params_table_info(params, row_table, assignment_pairs):

    params.meta.table_id = row_table["table_id"]
    params.meta.catalog = row_table["catalog"]
    params.meta.schema = row_table["schema"]
    params.meta.table = row_table["table"]
    params.meta.primary_key = row_table["primary_key"]
    params.meta.filter_field = row_table["filter_field"]
    params.meta.quarantine = row_table["quarantine"]
    params.meta.filter_field_type = row_table["filter_field_type"]
    params.result.rules_evaluated = 0
    params.result.rules_error = 0
    params.result.rules_warning = 0
    params.result.rules_pass = 0

def load_params_rule_info(params, row_rule):

    params.config.rule_assignment_id = row_rule["rule_assignment_id"]
    params.config.rule_template_id = row_rule["rule_template_id"]
    params.config.statement = row_rule["statement"]
    params.config.engine_type = row_rule["engine_type"]
    params.config.policy_id = row_rule["policy_id"]
    params.meta.rule_type_id = row_rule["rule_type_id"]
    params.meta.rule_dimension_id = row_rule["rule_dimension_id"]
    params.config.parameters = json.dumps(
        {
            **(json.loads(row_rule["parameters_identifiers"]) if row_rule["parameters_identifiers"] not in (None, "") else {}),
            **(json.loads(row_rule["parameters_values"]) if row_rule["parameters_values"] not in (None, "") else {})
        }
    )
    params.config.category = row_rule["category"]
    params.config.is_reusable = row_rule["is_reusable"]
    params.config.rule_name = row_rule["name"]
    params.config.quarantine_flag = row_rule["quarantine_flag"]

def load_params_rows_info(params, df_filtered_by_filter_field, df_errors):
    params.result.rows_evaluated = df_filtered_by_filter_field.count()
    params.result.rows_failed = df_errors.count() if df_errors is not None else 0
    params.result.rows_pass = params.result.rows_evaluated - params.result.rows_failed 


def load_params_user_input(params, apply_at, filter_values, project_name, full_table_name, run_mode, display_logs):

    params.config.apply_at = apply_at
    params.meta.full_table_name = full_table_name
    params.config.run_mode = run_mode
    params.config.filter_values = filter_values
    params.result.display_logs = bool(display_logs) if display_logs is not None else False
    params.meta.project_name = project_name


def run_statement(params, df_filtered_by_filter_field):

    if params.config.engine_type == "sql":
        
        df_errors = run_sql(params, df_filtered_by_filter_field) 

    elif params.config.engine_type == "dqx":
                            
        df_errors = run_dqx(params, df_filtered_by_filter_field)

    else:
        print(f"Engine type {params.config.engine_type} not supported")

    return df_errors