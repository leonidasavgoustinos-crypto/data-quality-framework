from pyspark.sql import functions as F
from .constants import *
from .run_time_params import RunTimeParams
from .utils import *
from .logging import *


def run_data_quality(apply_at, run_mode, filter_values=None, project_name=None, full_table_name=None, df_input=None, display_logs=None, on_rule_error="raise"):
    # on_rule_error="raise" (default): the first failing rule/table aborts the run and the exception is re-raised
    #   (callers such as combined_dq_and_df_load.run_dq_and_insert rely on this to stop the pipeline).
    # on_rule_error="continue": a failing rule/table is recorded as code_error (with error_message) and the
    #   remaining rules/tables are still evaluated; the execution ends with status code_error and returns normally.
    if on_rule_error not in ("raise", "continue"):
        raise ValueError(f"on_rule_error must be 'raise' or 'continue', got {on_rule_error!r}")

    try:
        params = RunTimeParams()

        load_params_user_input(params, apply_at, filter_values, project_name, full_table_name, run_mode, display_logs)
        params.config.on_rule_error = on_rule_error

        log_start(params)

        log_user_input(params)

        get_project_id(params)
        open_result(params)
        # open_audit_log(params)

        df_active_rules = get_active_rules(params)

        log_active_rules(df_active_rules, params)

        table_ids = [
            r["table_id"]
            for r in df_active_rules.select("table_id").distinct().collect()
        ]

        for table_id in table_ids:
            try:
                _run_dq_for_single_table(
                    table_id,
                    df_active_rules,
                    params,
                    df_input
                )
            except Exception as e:
                if params.config.on_rule_error != "continue":
                    raise
                _record_table_failure(params, table_id, e)

        close_result(params)
        # close_audit_log(params)

        log_execution_summary(params)

        log_end(params)

        return params.result.result_id

    except Exception as e:
        print("Code failure")
        params.result.error_message = str(e).replace("'", "''").replace("\n", " ")
        update_result_execution_failure(params)
        update_result_table_execution_failure(params)
        update_result_rule_execution_failure(params)
        # update_audit_log_failure(params)
        raise




def _run_dq_for_single_table(table_id, df_active_rules, params, df_input):

    # ids of the previous (already closed) table/rule must not be reused by the failure path
    params.result.result_table_id = None
    params.result.result_rule_id = None
    
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

    df_filtered = cache_if_supported(df_filtered)
    
    df_filtered.count()   

    open_result_table(params)

    drop_quarantine(params)

    rule_results = []

    for rule_assignment_id in rule_assignment_ids:
        try:
            result = _run_single_rule(
                rule_assignment_id,
                df_active_rules,
                params,
                df_filtered
            )
        except Exception as e:
            if params.config.on_rule_error != "continue":
                raise
            params.result.error_message = str(e).replace("'", "''").replace("\n", " ")
            update_result_rule_execution_failure(params)
            params.result.rules_code_error += 1
            continue
        rule_results.append(result)

    merge_and_load_quarantine(rule_results, params, load_quarantine)

    close_result_table(params)

    log_table_results(params)


def _run_single_rule(rule_assignment_id, df_active_rules, params, df_filtered):

    params.result.result_rule_id = None

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


def _record_table_failure(params, table_id, e):
    # on_rule_error="continue": a table-level failure (e.g. reading the source) becomes a code_error table row
    params.result.error_message = str(e).replace("'", "''").replace("\n", " ")
    if params.result.result_table_id is None:
        params.meta.table_id = table_id
        open_result_table(params)
    update_result_table_execution_failure(params)
