from .constants import *
from .run_time_params import Rule
from .logging import log_warning
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType, TimestampType
)
from datetime import datetime, timezone
from functools import reduce
import uuid
from delta.tables import DeltaTable
import json

spark = SparkSession.builder.getOrCreate()


class DQCodeError(Exception):
    pass


RESULT_RULE_SCHEMA = StructType([
    StructField("result_rule_id", StringType()),
    StructField("result_table_id", StringType()),
    StructField("result_id", StringType()),
    StructField("project_id", LongType()),
    StructField("rule_assignment_id", LongType()),
    StructField("policy_id", LongType()),
    StructField("rows_evaluated", IntegerType()),
    StructField("rows_failed", IntegerType()),
    StructField("rows_pass", IntegerType()),
    StructField("error_message", StringType()),
    StructField("status", StringType()),
    StructField("rule_type_id", LongType()),
    StructField("rule_dimension_id", LongType()),
    StructField("statement", StringType()),
    StructField("alert", BooleanType()),
    StructField("alert_id", LongType()),
    StructField("start_timestamp", TimestampType()),
    StructField("end_timestamp", TimestampType()),
])

RESULT_TABLE_SCHEMA = StructType([
    StructField("result_table_id", StringType()),
    StructField("result_id", StringType()),
    StructField("table_id", LongType()),
    StructField("project_id", LongType()),
    StructField("rules_error", IntegerType()),
    StructField("rules_warning", IntegerType()),
    StructField("rules_pass", IntegerType()),
    StructField("rules_evaluated", IntegerType()),
    StructField("status", StringType()),
    StructField("start_timestamp", TimestampType()),
    StructField("end_timestamp", TimestampType()),
])


def utc_now():
    return datetime.now(timezone.utc)


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


def handle_execution_failure(params, exc):

    params.result.error_message = str(exc).replace("\n", " ")

    if params.result.table_in_progress:
        params.result.table_status = CODE_ERROR
        params.result.table_end_timestamp = params.result.table_end_timestamp or utc_now()
        for write_step in (write_result_rules, write_result_table):
            try:
                write_step(params)
            except Exception as audit_exc:
                print(f"Could not write failure audit ({write_step.__name__}): {audit_exc}")
        cleanup_table(params)
        params.result.table_in_progress = False

    if params.result.result_id is not None:
        update_result_execution_failure(params)


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


def get_project_id(params):

    if params.meta.project_name is not None:

        project_df = spark.table(META_PROJECT)

        rows = project_df.filter(
            (F.col("is_active")) &
            (F.col("project_nk") == params.meta.project_name)
        ).select("project_id").collect()

        if not rows:
            raise ValueError(f"Project '{params.meta.project_name}' not found or not active in {META_PROJECT}")

        params.meta.project_id = rows[0]["project_id"]


def group_rules_by_table(active_rule_rows):
    by_table = {}
    seen = set()
    for row in active_rule_rows:
        if row["rule_assignment_id"] in seen:
            continue
        seen.add(row["rule_assignment_id"])
        by_table.setdefault(row["table_id"], []).append(row)
    return by_table


def sql_ident(name):
    parts = str(name).split(".")
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)

def sql_literal(v):
    s = str(v).replace("'", "''")
    return f"'{s}'"

def quote_col(name):
    return "`" + str(name).replace("`", "``") + "`"

def normalize_template_simple(s: str) -> str:
    return s.replace("\\${", "${") if s else s


def resolve_sql_template(template, parameters):
    template = normalize_template_simple(template).replace("${table}", "`df_filtered_by_filter_field`")

    for key, value in parameters.items():
        ph = f"${{{'i:' + key}}}"
        if ph in template:
            if isinstance(value, list):
                template = template.replace(ph, ", ".join(sql_ident(v) for v in value))
            else:
                template = template.replace(ph, sql_ident(value))

    for key, value in parameters.items():
        ph = f"${{{'v:' + key}}}"
        if ph in template:
            if isinstance(value, list):
                template = template.replace(ph, ", ".join(sql_literal(v) for v in value))
            else:
                template = template.replace(ph, sql_literal(value))

    return template


def _parse_primary_key(primary_key):
    if primary_key is None:
        return []
    if isinstance(primary_key, str):
        return [c.strip() for c in primary_key.split(",") if c.strip()]
    return [str(c).strip() for c in primary_key]


def build_rule_plan(params, row, df_input):

    rule = Rule(
        result_rule_id=str(uuid.uuid4()),
        rule_assignment_id=row["rule_assignment_id"],
        rule_template_id=row["rule_template_id"],
        rule_name=row["name"],
        engine_type=row["engine_type"],
        template=row["statement"],
        parameters={},
        is_reusable=row["is_reusable"],
        category=row["category"],
        quarantine_flag=row["quarantine_flag"],
        policy_id=row["policy_id"],
        rule_type_id=row["rule_type_id"],
        rule_dimension_id=row["rule_dimension_id"],
        statement=row["statement"],
    )
    params.config.rules.append(rule)

    if rule.engine_type != "sql":
        _fail_rule(rule, f"Engine type {rule.engine_type} not supported")

    try:
        rule.parameters = {
            **(json.loads(row["parameters_identifiers"]) if row["parameters_identifiers"] not in (None, "") else {}),
            **(json.loads(row["parameters_values"]) if row["parameters_values"] not in (None, "") else {})
        }
        if rule.is_reusable:
            rule.statement = resolve_sql_template(rule.template, rule.parameters)
    except Exception as e:
        _fail_rule(rule, f"Invalid rule parameters: {e}")

    if not rule.is_reusable and df_input is not None:
        log_warning(f"Rule assignment {rule.rule_assignment_id} ({rule.rule_name}) is a non-reusable "
                    f"SQL rule: it reads the tables in its statement, not the provided df_input.")

    return rule


def load_params_table_info(params, row_table):
    params.meta.table_id = row_table["table_id"]
    params.meta.catalog = row_table["catalog"]
    params.meta.schema = row_table["schema"]
    params.meta.table = row_table["table"]
    params.meta.primary_key = row_table["primary_key"]
    params.meta.filter_field = row_table["filter_field"]
    params.meta.quarantine = row_table["quarantine"]
    params.meta.filter_field_type = row_table["filter_field_type"]
    params.meta.pk_columns = _parse_primary_key(row_table["primary_key"])
    params.meta.df_filtered = None
    params.meta.df_failures = None

    params.result.result_table_id = str(uuid.uuid4())
    params.result.rules_evaluated = 0
    params.result.rules_error = 0
    params.result.rules_warning = 0
    params.result.rules_pass = 0
    params.result.rows_evaluated = 0
    params.result.table_status = None
    params.result.table_start_timestamp = utc_now()
    params.result.table_end_timestamp = None
    params.result.rules_written = False
    params.result.table_written = False


def load_params_table_rules(params, table_rows, df_input):
    params.config.rules = []
    for row in sorted(table_rows, key=lambda r: r["rule_assignment_id"]):
        build_rule_plan(params, row, df_input)


def load_params_user_input(params, apply_at, filter_values, project_name, full_table_name, run_mode, display_logs, full_scan=False):

    params.config.full_scan = bool(full_scan)
    params.config.apply_at = apply_at
    params.meta.full_table_name = full_table_name
    params.config.run_mode = run_mode
    params.config.filter_values = filter_values
    params.result.display_logs = bool(display_logs) if display_logs is not None else False
    params.meta.project_name = project_name


def filter_by_filter_field(params, df_input):

    if df_input is not None:
        return df_input

    table_path = f"{params.meta.catalog}.{params.meta.schema}.{params.meta.table}"
    df = spark.table(table_path)

    field = params.meta.filter_field
    field_type = params.meta.filter_field_type
    values_dict = params.config.filter_values or {}

    values = values_dict.get(field_type)

    if params.config.full_scan:
        return df

    if values is None:
        max_value = df.select(F.max(F.col(field))).first()[0]
        df = df.filter(F.col(field) == max_value)
    else:
        df = df.filter(F.col(field).isin(values))

    return df


def _fail_rule(rule, error):
    rule.status = CODE_ERROR
    rule.error_message = str(error).replace("\n", " ")
    raise DQCodeError(
        f"Rule assignment {rule.rule_assignment_id} ({rule.rule_name}) failed: {rule.error_message}"
    ) from (error if isinstance(error, BaseException) else None)


def _pk_select(params, available_columns):

    pk = params.meta.pk_columns
    lower = {c.lower(): c for c in available_columns}
    if pk and all(c.lower() in lower for c in pk):
        return [F.col(quote_col(lower[c.lower()])).alias(c) for c in pk], True
    return [F.lit(None).alias(c) for c in pk], False


def _build_sql_branch(params, rule):
    try:
        df_errors = spark.sql(rule.statement)
        pk_cols, rule.quarantinable = _pk_select(params, df_errors.columns)
        return df_errors.select(*pk_cols, F.lit(rule.result_rule_id).alias("result_rule_id"))
    except DQCodeError:
        raise
    except Exception as e:
        _fail_rule(rule, e)


def _build_rule_branches(params, rules):
    return [_build_sql_branch(params, r) for r in rules]


def _diagnose_failure(params, error):
    for rule in params.config.rules:
        try:
            branches = _build_rule_branches(params, [rule])
            if branches:
                reduce(lambda a, b: a.unionByName(b), branches).count()
        except DQCodeError:
            raise
        except Exception as e:
            _fail_rule(rule, e)
    raise error


def evaluate_table(params):
    rules = params.config.rules

    counts = {}
    try:
        branches = _build_rule_branches(params, rules)
        if branches:
            params.meta.df_failures = reduce(lambda a, b: a.unionByName(b), branches).cache()
            counts = {
                row["result_rule_id"]: row["count"]
                for row in params.meta.df_failures.groupBy("result_rule_id").count().collect()
            }
    except DQCodeError:
        raise
    except Exception as e:
        _diagnose_failure(params, e)

    params.result.table_end_timestamp = utc_now()

    for rule in rules:
        rule.rows_evaluated = params.result.rows_evaluated
        rule.rows_failed = counts.get(rule.result_rule_id, 0)
        rule.rows_pass = rule.rows_evaluated - rule.rows_failed
        rule.status = rule.category if rule.rows_failed > 0 else PASS

    for rule in rules:
        if rule.quarantine_flag and rule.rows_failed > 0 and not rule.quarantinable:
            _fail_rule(rule, f"Primary key columns {params.meta.pk_columns} not found in the rule output; "
                             f"rows cannot be quarantined")

    summarize_table(params)


def summarize_table(params):
    result = params.result
    result.rules_evaluated = result.rules_pass = result.rules_error = result.rules_warning = 0

    for rule in params.config.rules:
        result.rules_evaluated += 1
        if rule.rows_failed == 0:
            result.rules_pass += 1
        elif rule.category == ERROR:
            result.rules_error += 1
        elif rule.category == WARNING:
            result.rules_warning += 1

    if result.rules_error > 0:
        result.table_status = ERROR
    elif result.rules_warning > 0:
        result.table_status = WARNING
    else:
        result.table_status = PASS


def cleanup_table(params):
    for df in (params.meta.df_failures, params.meta.df_filtered):
        if df is not None:
            try:
                df.unpersist()
            except Exception:
                pass
    params.meta.df_failures = None
    params.meta.df_filtered = None


def _quarantine_df(params, quarantine_rules):
    pk = params.meta.pk_columns

    rule_meta = spark.createDataFrame(
        [(r.result_rule_id, r.category) for r in quarantine_rules],
        "result_rule_id string, category string"
    )
    base = params.meta.df_failures.join(F.broadcast(rule_meta), "result_rule_id", "inner")

    return base.select(
        *[F.col(quote_col(c)) for c in pk],
        F.lit(params.result.result_id).alias("result_id"),
        F.lit(params.result.result_table_id).alias("result_table_id"),
        F.col("result_rule_id"),
        F.lit(params.meta.project_id).cast("long").alias("project_id"),
        F.col("category"),
    )


def write_quarantine(params):

    table = params.meta.quarantine
    current_project = params.meta.project_id

    quarantine_rules = [
        r for r in params.config.rules
        if r.quarantine_flag and r.quarantinable and r.rows_failed
    ]

    if not spark.catalog.tableExists(table):
        if quarantine_rules:
            _quarantine_df(params, quarantine_rules).write.format("delta").mode("overwrite").saveAsTable(table)
        return

    existing_projects = {
        row["project_id"] for row in spark.table(table).select("project_id").distinct().collect()
    }

    if not quarantine_rules:
        if existing_projects == {current_project}:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        elif current_project in existing_projects:
            DeltaTable.forName(spark, table).delete(F.col("project_id").eqNullSafe(F.lit(current_project)))
        return

    writer = _quarantine_df(params, quarantine_rules).write.format("delta").mode("overwrite")

    if existing_projects <= {current_project}:
        writer.option("overwriteSchema", "true").saveAsTable(table)
    else:
        predicate = ("project_id IS NULL" if current_project is None
                     else f"project_id = {int(current_project)}")
        writer.option("replaceWhere", predicate).saveAsTable(table)


def write_result_rules(params):
    result = params.result
    if result.rules_written:
        return

    rules = [r for r in params.config.rules if r.status is not None]
    if not rules:
        return

    end_timestamp = result.table_end_timestamp or utc_now()
    rows = [(
        r.result_rule_id,
        result.result_table_id,
        params.result.result_id,
        params.meta.project_id,
        r.rule_assignment_id,
        r.policy_id,
        r.rows_evaluated,
        r.rows_failed,
        r.rows_pass,
        r.error_message if r.status == CODE_ERROR else None,
        r.status,
        r.rule_type_id,
        r.rule_dimension_id,
        r.statement,
        None,
        None,
        result.table_start_timestamp,
        end_timestamp,
    ) for r in rules]

    spark.createDataFrame(rows, RESULT_RULE_SCHEMA) \
        .write.format("delta").mode("append").saveAsTable(RESULT_RULE)
    result.rules_written = True


def write_result_table(params):
    result = params.result
    if result.table_written:
        return

    failed = result.table_status == CODE_ERROR
    row = (
        result.result_table_id,
        result.result_id,
        params.meta.table_id,
        params.meta.project_id,
        None if failed else result.rules_error,
        None if failed else result.rules_warning,
        None if failed else result.rules_pass,
        None if failed else result.rules_evaluated,
        result.table_status,
        result.table_start_timestamp,
        result.table_end_timestamp or utc_now(),
    )

    spark.createDataFrame([row], RESULT_TABLE_SCHEMA) \
        .write.format("delta").mode("append").saveAsTable(RESULT_TABLE)
    result.table_written = True
