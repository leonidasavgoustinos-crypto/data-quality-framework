from pyspark.sql import functions as F
from .constants import *
from .run_time_params import RunTimeParams
from .utils import *
from .logging import *


def run_data_quality(apply_at, run_mode, filter_values=None, project_name=None, full_table_name=None, df_input=None, display_logs=None):

    params = RunTimeParams()

    try:
        load_params_user_input(params, apply_at, filter_values, project_name, full_table_name, run_mode, display_logs)

        log_start(params)

        log_user_input(params)

        get_project_id(params)
        open_result(params)
        # open_audit_log(params)

        df_active_rules = get_active_rules(params)

        log_active_rules(df_active_rules, params)

        rules_by_table = group_rules_by_table(df_active_rules.collect())

        for table_rows in rules_by_table.values():
            _run_dq_for_single_table(table_rows, params, df_input)

        close_result(params)
        # close_audit_log(params)

        log_execution_summary(params)

        log_end(params)

        return params.result.result_id

    except Exception as e:
        print("Code failure")
        handle_execution_failure(params, e)
        # update_audit_log_failure(params)
        raise


def _run_dq_for_single_table(table_rows, params, df_input):

    load_params_table_info(params, table_rows[0])
    params.result.table_in_progress = True
    load_params_table_rules(params, table_rows, df_input)

    log_table_header(params.meta.table_id, params, len(params.config.rules))

    df_filtered = filter_by_filter_field(params, df_input)

    df_filtered.cache()
    if df_input is None:
        params.meta.df_filtered = df_filtered

    params.result.rows_evaluated = df_filtered.count()

    df_filtered.createOrReplaceTempView("df_filtered_by_filter_field")

    log_table_rules(params)

    evaluate_table(params)

    write_quarantine(params)
    write_result_rules(params)
    write_result_table(params)

    for rule in params.config.rules:
        log_rule_results(params, rule)
    log_table_results(params)

    cleanup_table(params)
    params.result.table_in_progress = False
    