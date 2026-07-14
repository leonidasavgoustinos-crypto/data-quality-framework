def log_start(params):
    if params.result.display_logs == False:
        return
    print("#######################################################################################")
    print("#############################    Start DQ Framework   #################################")
    print("#######################################################################################\n\n")

def log_end(params):
    if params.result.display_logs == False:
        return
    print("\n#######################################################################################")
    print("#############################    End DQ Framework   ###################################")
    print("#######################################################################################\n\n")


def log_active_rules(df_active_rules, params):
    if params.result.display_logs == False:
        return
    print("\n" + "=" * 55)
    print("                   ACTIVE RULES FOUND")
    print("-" * 55)

    df_active_rules.select(
        "catalog", 
        "schema", 
        "table", 
        "name", 
        "category",
        "engine_type", 
        "is_reusable"
    ).show(truncate=False)

    print("=" * 55 + "\n")


def log_execution_summary(params):
    if params.result.display_logs == False:
        return
    print("\n" + "=" * 55)
    print("                  EXECUTION SUMMARY")
    print("-" * 55)
    print(f"Tables Evaluated : {params.result.tables_evaluated}")
    print(f"Tables Passed    : {params.result.tables_pass}")
    print(f"Tables Warning   : {params.result.tables_warning}")
    print(f"Tables Error     : {params.result.tables_error}")
    print("=" * 55 + "\n")

def log_user_input(params):
    if params.result.display_logs == False:
        return
    print("\n" + "=" * 55)
    print("                 USER INPUT PARAMETERS")
    print("-" * 55)
    print(f"Project Name     : {params.meta.project_name}")
    print(f"Apply At         : {params.config.apply_at}")
    print(f"Run Mode         : {params.config.run_mode}")
    print(f"Filter Values    : {params.config.filter_values}")
    print(f"Full Table Name  : {params.meta.full_table_name}")
    print("=" * 55 + "\n")

def log_table_header(table_id, params, assignment_count):
    if params.result.display_logs == False:
        return
    print("\n" + "=" * 55)
    print("                    TABLE START")
    print("-" * 55)
    print(f"Table Name       : {params.meta.table}")
    print(f"Table ID         : {table_id}")
    print(f"Primary Key      : {params.meta.primary_key}")
    print(f"Filter Field     : {params.meta.filter_field}")
    print(f"Rules Detected   : {assignment_count}")
    print("=" * 55 + "\n")


def log_rule_start(params, assignment_id):
    if params.result.display_logs == False:
        return
    print("=" * 55)
    print("                     RULE START")
    print("-" * 55)
    print(f"Rule Assignment ID : {assignment_id}")
    print(f"Rule Template ID   : {params.config.rule_template_id}")
    print(f"Rule Name          : {params.config.rule_name}")
    print(f"Category           : {params.config.category}")
    print(f"Engine Type        : {params.config.engine_type}")
    print(f"Parameters         : {params.config.parameters}")
    print(f"Statement          : {params.config.statement}")
    print("=" * 55)


def log_rule_results(params, assignment_id):
    if params.result.display_logs == False:
        return
    print("-------------------- RULE RESULTS ---------------------")
    print(f"Table Name     : {params.meta.table}")
    print(f"Rule Name      : {params.config.rule_name}")
    print(f"Rows Evaluated : {params.result.rows_evaluated}")
    print(f"Rows Passed    : {params.result.rows_pass}")
    print(f"Rows Failed    : {params.result.rows_failed}")
    print("======================================================\n")


def log_table_results(params):
    if params.result.display_logs == False:
        return
    print("\n------------------- TABLE SUMMARY ----------------------")
    print(f"Table Name      : {params.meta.table}")
    print(f"Rules Evaluated : {params.result.rules_evaluated}")
    print(f"Rules Passed    : {params.result.rules_pass}")
    print(f"Rules Warning   : {params.result.rules_warning}")
    print(f"Rules Error     : {params.result.rules_error}")
    print("==========================================================\n")