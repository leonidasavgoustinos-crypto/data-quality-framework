from pyspark.sql import functions as F
from .constants import *
from .run_time_params import RunTimeParams
from .utils import *
from .logging import *


def run_data_quality(apply_at, run_mode, filter_values=None, project_name=None, full_table_name=None, df_input=None, display_logs=None):

    try:
        params = RunTimeParams()

        load_params_user_input(params, apply_at, filter_values, project_name, full_table_name, run_mode, display_logs)

        log_start(params)

        log_user_input(params)

        get_project_id(params)
        open_result(params)
        open_audit_log(params)

        df_active_rules = get_active_rules(params)

        log_active_rules(df_active_rules, params)

        table_ids = [
            r["table_id"]
            for r in df_active_rules.select("table_id").distinct().collect()
        ]

        for table_id in table_ids:
            _run_dq_for_single_table(
                table_id,
                df_active_rules,
                params,      
                df_input
            )

        close_result(params)
        close_audit_log(params)

        log_execution_summary(params)

        log_end(params)

        return params.meta.job_id

    except Exception as e:
        print("Code failure")
        params.result.error_message = str(e).replace("'", "''").replace("\n", " ")
        update_result_execution_failure(params)
        update_result_table_execution_failure(params)
        update_result_rule_execution_failure(params)
        update_audit_log_failure(params)
        raise




def _run_dq_for_single_table(table_id, df_active_rules, params, df_input):
    
    row_table = df_active_rules.filter(
        F.col("table_id") == table_id
    ).limit(1).collect()[0]

    rule_assignment_ids = [
        r["rule_assignment_id"]
        for r in df_active_rules
            .filter(F.col("table_id") == table_id)
            .select("rule_assignment_id")
            .distinct()
            .collect()
    ]

    load_params_table_info(params, row_table, rule_assignment_ids)

    log_table_header(table_id, params, len(rule_assignment_ids))

    df_filtered = filter_by_filter_field(params, df_input)

    df_filtered.cache()
    
    df_filtered.count()   

    open_result_table(params)

    drop_quarantine(params)

    rule_results = []

    for rule_assignment_id in rule_assignment_ids:
        result = _run_single_rule(
            rule_assignment_id,
            df_active_rules,
            params,
            df_filtered
        )
        rule_results.append(result)

    merge_and_load_quarantine(rule_results, params, load_quarantine)

    close_result_table(params)

    log_table_results(params)


def _run_single_rule(rule_assignment_id, df_active_rules, params, df_filtered):

    row_rule = df_active_rules.filter(
        F.col("rule_assignment_id") == rule_assignment_id
    ).limit(1).collect()[0]

    load_params_rule_info(params, row_rule)

    log_rule_start(params, rule_assignment_id)

    open_result_rule(params)

    df_errors = run_statement(params, df_filtered)

    load_params_rows_info(params, df_filtered, df_errors)

    close_result_rule(params)

    log_rule_results(params, rule_assignment_id)

    update_rule_counters(params) 

    df_quarantine = prepare_quarantine_rows(params, df_errors)

    return (rule_assignment_id, df_quarantine)