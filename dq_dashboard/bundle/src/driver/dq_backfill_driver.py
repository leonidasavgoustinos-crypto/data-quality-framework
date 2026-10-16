"""dq_dashboard driver (spark_python_task), deployed by bundle/resources/dq_driver.job.yml.

Runs the REAL DQ framework (uploaded copy, unmodified) and records what the framework does not
store into the dashboard schema (capture tables, all OPTIONAL: skipped if absent):
  run_calendar, run_errors, rule_trace (+ trace_vol), quarantine_snapshot, columns_snapshot,
  config_history_snapshot, sandbox_log; load_log (sandbox only).

Environment: every catalog/schema/flag comes from env_config (bundle target values passed as task parameters).
The connected workspace is checked against expected_host BEFORE any write.
Framework: installed from the patched wheel built OUTSIDE the repo by scripts/build_patched.sh and attached by the
bundle; wheel version / sha256 / patches are logged in sandbox_log for every run.

Modes
  backfill  (sandbox only)  loads generated partitions for simulated days and runs the framework with
                            filter_values={'date': [day]}; resumable (skips (day, apply_at, project)
                            already in run_calendar); optional fault injection (sandbox only).
  run                       real run on current data (no generated data, no simulated dates).

Guarantees
  * single-threaded (dq_trace asserts; job max_concurrent_runs = 1);
  * run_calendar.result_id = return value of run_data_quality(), or dq_trace.last_result_id(trace)
    when it raises. Never read from results.result;
  * framework imported as a module (no %run / exec) so the runtime patches apply; framework files unchanged;
  * cache shim (cache_mode) only inside this process; usage logged per call in sandbox_log.
"""
import argparse
import glob
import hashlib
import inspect
import json
import os
import random
import sys
import time
from datetime import date, datetime, timedelta, timezone

# Serverless spark_python_task runs this file via exec(compile(src, filename)): __file__ is undefined there,
# but the code object still carries the real filename.
HERE = os.path.dirname(os.path.abspath(globals().get("__file__") or inspect.currentframe().f_code.co_filename))
sys.path.insert(0, HERE)
import env_config  # noqa: E402

ENV, REST = env_config.from_args(sys.argv[1:])
ENV.assert_host(env_config.runtime_host())          # abort before any write on the wrong workspace

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

spark = SparkSession.builder.getOrCreate()

import cache_shim  # noqa: E402
import dq_trace  # noqa: E402
import framework.constants as dq_constants  # noqa: E402  from the patched wheel (see build_manifest.json)
import framework.dq_framework as dqf  # noqa: E402  module import so the runtime patches apply

WHEEL_DIST = "data_quality_framework"

if dq_constants.CATALOG != ENV.dq_catalog:
    raise env_config.GuardError(f"framework/constants.py CATALOG={dq_constants.CATALOG!r} but target "
                                f"dq_catalog={ENV.dq_catalog!r}; the framework would write elsewhere. Aborting.")

SB = ENV.sb
SUPPORTS_ON_RULE_ERROR = "on_rule_error" in inspect.signature(dqf.run_data_quality).parameters
ON_RULE_ERROR = "raise"
FAULT_MARK = "DQ_DASHBOARD_FAULT_INJECTION"
ROWS_PER_PARTITION = 2000
CONFIG_TABLES = [ENV.fq(s, t) for s, t in (
    ("metadata", "project"), ("metadata", "`table`"), ("metadata", "rule_type"), ("metadata", "rule_dimension"),
    ("configuration", "apply_at"), ("configuration", "run_policy"), ("configuration", "rule_template"),
    ("configuration", "rule_assignment"))]
TRACE_SCHEMA = T.StructType([
    T.StructField("result_id", T.StringType()), T.StructField("result_table_id", T.StringType()),
    T.StructField("result_rule_id", T.StringType()), T.StructField("table_id", T.LongType()),
    T.StructField("rule_assignment_id", T.LongType()), T.StructField("call_id", T.StringType()),
    T.StructField("seq", T.IntegerType()), T.StructField("func_name", T.StringType()),
    T.StructField("traced_at", T.StringType()),
])
_exists = {}


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def has(table):
    """Capture tables are optional: write only if the table exists in the dashboard schema."""
    if table not in _exists:
        _exists[table] = spark.catalog.tableExists(f"{SB}.{table}")
        if not _exists[table]:
            print(f"[capture] {SB}.{table} not found: capture skipped")
    return _exists[table]


def append(table, rows, schema):
    if rows and has(table):
        spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(f"{SB}.{table}")


def log(phase, note):
    print(f"[sandbox_log] {phase}: {note}")
    append("sandbox_log", [(now(), phase, note)], "backfill_started_at timestamp, phase string, note string")


def code_hashes():
    """sha256 per driver-side module + combined (logged every run)."""
    files = [os.path.join(HERE, f) for f in ("dq_backfill_driver.py", "env_config.py", "dq_trace.py", "cache_shim.py")]
    per = {os.path.basename(f): hashlib.sha256(open(f, "rb").read()).hexdigest() for f in files}
    return {"combined": hashlib.sha256("".join(per[k] for k in sorted(per)).encode()).hexdigest(), "files": per}


def build_info():
    """Wheel version, sha256 and applied patches (logged every run). Aborts if the installed framework is not the
    wheel described by build_manifest.json (written by scripts/build_patched.sh)."""
    from importlib import metadata
    manifest = json.load(open(os.path.join(HERE, "build_manifest.json")))
    installed = metadata.version(WHEEL_DIST)
    try:
        from framework._build_info import BUILD_INFO as embedded
    except ImportError:
        embedded = None
    if installed != manifest["version"] or (embedded and embedded["version"] != installed):
        raise env_config.GuardError(f"installed {WHEEL_DIST}=={installed} does not match build_manifest "
                                    f"{manifest['version']} (embedded {embedded and embedded['version']}). Aborting.")
    return {"installed_version": installed, "wheel": manifest["wheel"], "wheel_sha256": manifest["sha256"],
            "patches": manifest["patches"], "source_commit": manifest["source_commit"]}


# ---------------------------------------------------------------- trace loading
def load_trace_files(pattern):
    if not dq_trace.TRACE_ENABLED or not has("rule_trace") or not glob.glob(pattern):
        return 0
    df = (spark.read.schema(TRACE_SCHEMA).json(pattern)
          .withColumn("traced_at", F.to_timestamp("traced_at")))
    df.createOrReplaceTempView("_trace_new")
    spark.sql(f"""
        MERGE INTO {SB}.rule_trace t USING _trace_new s
        ON t.call_id = s.call_id AND t.seq = s.seq
        WHEN NOT MATCHED THEN INSERT
          (result_id, result_table_id, result_rule_id, table_id, rule_assignment_id, traced_at, call_id, seq, func_name)
        VALUES (s.result_id, s.result_table_id, s.result_rule_id, s.table_id, s.rule_assignment_id, s.traced_at,
                s.call_id, s.seq, s.func_name)""")
    return df.count()


# ---------------------------------------------------------------- snapshots
def snapshot_config_history():
    if not has("config_history_snapshot"):
        return
    for t in CONFIG_TABLES:
        h = (spark.sql(f"DESCRIBE HISTORY {t}")
             .select(F.lit(t.replace("`", "")).alias("config_table"), "version", "timestamp",
                     F.col("userName").alias("user_name"), "operation",
                     F.to_json("operationParameters").alias("operation_parameters"),
                     F.current_timestamp().alias("captured_at")))
        h.createOrReplaceTempView("_hist")
        spark.sql(f"""MERGE INTO {SB}.config_history_snapshot t USING _hist s
                      ON t.config_table = s.config_table AND t.version = s.version
                      WHEN NOT MATCHED THEN INSERT *""")


def snapshot_columns(d, tables):
    if not tables or not has("columns_snapshot"):
        return
    if spark.sql(f"SELECT 1 FROM {SB}.columns_snapshot WHERE simulated_date <=> {sql_date(d)} LIMIT 1").count():
        return
    conds = " OR ".join(
        f"(table_catalog = '{t['catalog']}' AND table_schema = '{t['schema']}' AND lower(table_name) = lower('{t['table']}'))"
        for t in tables)
    spark.sql(f"""INSERT INTO {SB}.columns_snapshot
        SELECT {sql_date(d)}, table_catalog, table_schema, table_name, column_name, full_data_type, ordinal_position
        FROM system.information_schema.columns WHERE {conds}""")


def snapshot_quarantine(result_id, d, tables):
    if not result_id or not has("quarantine_snapshot"):
        return
    for t in tables:
        q = t["quarantine"]
        if spark.catalog.tableExists(q):
            spark.sql(f"""INSERT INTO {SB}.quarantine_snapshot
                SELECT '{result_id}', {sql_date(d)}, '{q}', result_rule_id, category, count(*)
                FROM {q} WHERE result_id = '{result_id}' GROUP BY result_rule_id, category""")


def sql_date(d):
    return f"DATE'{d}'" if d else "CAST(NULL AS DATE)"


# ---------------------------------------------------------------- source data (sandbox only)
def rule_columns(table_id):
    rows = spark.sql(f"""
        SELECT rt.name, ra.parameters_identifiers FROM {ENV.fq('configuration', 'rule_assignment')} ra
        JOIN {ENV.fq('configuration', 'rule_template')} rt USING (rule_template_id)
        WHERE ra.table_id = {int(table_id)} AND ra.is_active""").collect()
    not_null, not_empty = set(), set()
    for r in rows:
        col = json.loads(r.parameters_identifiers or "{}").get("column")
        if not col:
            continue
        if r.name in ("is_not_null", "is_not_null_and_not_empty"):
            not_null.add(col)
        if r.name in ("is_not_empty", "is_not_null_and_not_empty"):
            not_empty.add(col)
    return sorted(not_null), sorted(not_empty)


def defect_plan(t, d):
    """Deterministic per (table, day): realistic mix of clean days and defects."""
    rnd = random.Random(f"{t['table_id']}|{d}")
    plan = []
    if t["not_null"] and rnd.random() < 0.30:
        plan.append(["null", rnd.choice(t["not_null"]), rnd.randint(1, 25)])
    if t["not_empty"] and rnd.random() < 0.20:
        plan.append(["empty", rnd.choice(t["not_empty"]), rnd.randint(1, 15)])
    if rnd.random() < 0.15:
        plan.append(["dup", rnd.randint(1, 4)])
    if rnd.random() < 0.05:
        plan.append(["volume", 0.5])
    return plan


def load_source(t, d, day_index):
    ENV.require_sandbox("data generation (load_source)")
    nk = t["table_nk"]
    if spark.sql(f"SELECT 1 FROM {SB}.load_log WHERE table_nk = '{nk}' AND simulated_date = DATE'{d}' LIMIT 1").count():
        return
    plan = defect_plan(t, d)
    n = ROWS_PER_PARTITION
    for p in plan:
        if p[0] == "volume":
            n = int(n * p[1])
    ds = d.strftime("%Y%m%d")
    exprs = []
    for f in spark.table(nk).schema.fields:
        c, typ, lc = f.name, f.dataType.simpleString(), f.name.lower()
        if typ == "date":
            e = F.lit(d)
        elif lc == "year":
            e = F.lit(d.year)
        elif lc == "month":
            e = F.lit(d.month)
        elif lc == "year_month":
            e = F.lit(d.strftime("%Y-%m"))
        elif lc == "crsid":
            e = (F.lit(day_index * 100000) + F.col("id")).cast("int")
        elif typ in ("int", "bigint"):
            e = F.col("id").cast(typ)
        elif typ == "boolean":
            e = (F.col("id") % 7 == 0)
        elif typ == "double":
            e = F.round(F.rand(seed=day_index * 1000 + int(t["table_id"])) * 1000, 2)
        elif lc == "segment":
            e = F.element_at(F.array(*[F.lit(s) for s in ("retail", "sme", "corporate", "private")]),
                             (F.col("id") % 4 + 1).cast("int"))
        else:
            e = F.concat(F.lit(f"{c}_{ds}_"), F.col("id").cast("string"))
        for p in plan:
            if p[0] == "null" and p[1] == c:
                e = F.when(F.col("id") < p[2], F.lit(None)).otherwise(e)
            if p[0] == "empty" and p[1] == c:
                e = F.when((F.col("id") >= 100) & (F.col("id") < 100 + p[2]), F.lit("")).otherwise(e)
        exprs.append(e.cast(typ).alias(c))
    df = spark.range(n).select(*exprs, F.col("id").alias("_id"))
    for p in plan:
        if p[0] == "dup":   # duplicate the LAST rows (also hits max-filter tables such as table 3, C-04)
            df = df.unionByName(df.where(F.col("_id") >= n - p[1]))
    df.drop("_id").write.mode("append").saveAsTable(nk)
    append("load_log", [(nk, d, n + sum(p[1] for p in plan if p[0] == "dup"), json.dumps(plan), now())],
           "table_nk string, simulated_date date, rows bigint, defects string, loaded_at timestamp")


# ---------------------------------------------------------------- fault injection (sandbox-only test wrapper)
_fault = {"spec": None, "key": None, "n": 0, "fired": None}


def install_fault(spec):
    """spec = (day, apply_at, project, nth). Raises inside filter_by_filter_field for the nth table of
    that call, i.e. BEFORE open_result_table (reproduces finding F-20). Test wrapper, not tracing."""
    ENV.require_sandbox("fault injection")
    _fault["spec"] = spec
    orig = dqf.filter_by_filter_field
    if getattr(orig, "__dq_fault__", False):
        return

    def faulty_filter(params, df_input):
        _fault["n"] += 1
        s = _fault["spec"]
        if s and _fault["key"] == s[:3] and _fault["n"] == s[3]:
            # remembered: in on_rule_error="continue" the framework records this as code_error and does not re-raise
            _fault["fired"] = f"{FAULT_MARK}: deliberate crash before first rule, table_id={params.meta.table_id}"
            raise RuntimeError(_fault["fired"])
        return orig(params, df_input)
    faulty_filter.__dq_fault__ = True
    faulty_filter.__wrapped__ = orig
    dqf.filter_by_filter_field = faulty_filter


# ---------------------------------------------------------------- one framework call
def result_status(result_id):
    """The framework's own verdict for this run: status of its row in <dq_catalog>.results.result."""
    if not result_id:
        return None
    rows = spark.sql(f"SELECT status FROM {ENV.fq('results', 'result')} WHERE result_id = '{result_id}'").collect()
    return rows[0]["status"] if rows else None


def run_call(d, apply_at, project, tables, phase):
    """run_calendar.call_status:
         call raised (on_rule_error="raise")  -> 'fault_injected' (driver test wrapper) or 'code_error'
         call returned (incl. "continue" mode) -> status of this run's results.result row
                                                  (pass / warning / error / code_error / ...; 'missing' if no row)"""
    _fault["key"], _fault["n"], _fault["fired"] = ((d.isoformat() if d else None), apply_at, project), 0, None
    cache_shim.take_used()
    call_id = dq_trace.begin_call()
    t0, status, err, rid = now(), "ok", None, None
    try:
        kwargs = {"on_rule_error": ON_RULE_ERROR} if SUPPORTS_ON_RULE_ERROR else {}
        rid = dqf.run_data_quality(apply_at=apply_at, run_mode="all", project_name=project or None,
                                   filter_values={"date": [d.isoformat()]} if d else None, display_logs=False, **kwargs)
    except Exception as e:  # the framework re-raises after marking code_error
        err = f"{type(e).__name__}: {e}"[:4000]
        status = "fault_injected" if FAULT_MARK in str(e) else "code_error"
    finally:
        _, rows, trace_errors = dq_trace.end_call()
    t1 = now()
    shim_used = cache_shim.take_used()
    if rid is None:
        rid = dq_trace.last_result_id(rows)
    if err is None:                    # returned normally: in continue mode failures live in results.result
        status = result_status(rid) or "missing"
    loaded = load_trace_files(os.path.join(ENV.trace_dir, call_id, "*.json"))
    append("run_calendar", [(rid, d, d.isoformat() if d else None, apply_at, project or None, status, t0, t1, call_id)],
           "result_id string, simulated_date date, partition_value string, apply_at string, project string, "
           "call_status string, real_start timestamp, real_end timestamp, call_id string")
    if err:
        append("run_errors", [(rid, d, apply_at, project or None, err, call_id)],
               "result_id string, simulated_date date, apply_at string, project string, error_message string, call_id string")
    log(phase, f"cache_shim mode={ENV.cache_mode} used={shim_used} call_id={call_id} result_id={rid}")
    if status == "fault_injected" or _fault["fired"]:
        log(phase, f"fault_injected call_id={call_id} result_id={rid} call_status={status} {(err or _fault['fired'])[:200]}")
    if trace_errors:
        log(phase, f"trace_incomplete call_id={call_id} result_id={rid} trace_errors={trace_errors}")
        print(f"WARNING: trace incomplete for call {call_id} ({trace_errors} errors)")
    snapshot_quarantine(rid, d, tables)
    print(f"{d or 'today'} {apply_at:<16} {project or '-':<10} {status:<15} {(t1 - t0).total_seconds():7.1f}s "
          f"result_id={rid} trace_rows={len(rows)} loaded={loaded} cache_shim_used={shim_used}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("backfill", "run"), required=True)
    ap.add_argument("--start", default="")
    ap.add_argument("--ndays", default="1")
    ap.add_argument("--scopes", required=True)
    ap.add_argument("--tables", default="")
    ap.add_argument("--fault", default="")
    ap.add_argument("--on_rule_error", choices=("raise", "continue"), default="continue")
    a = ap.parse_args(REST)
    global ON_RULE_ERROR
    ON_RULE_ERROR = a.on_rule_error
    if ON_RULE_ERROR != "raise" and not SUPPORTS_ON_RULE_ERROR:
        raise env_config.GuardError("on_rule_error requested but the installed framework has no such parameter (patch 04)")
    phase = f"{ENV.target}:{a.mode}"

    if a.mode == "backfill":
        ENV.require_sandbox("simulated backfill")
        ENV.require_simulated_dates("simulated backfill")
    if a.fault:
        ENV.require_sandbox("fault injection")

    t_start = time.time()
    dq_trace.TRACE_ENABLED = ENV.trace_enabled
    if ENV.trace_enabled and not os.path.isdir(ENV.trace_dir):
        raise env_config.GuardError(f"trace_enabled but volume {ENV.trace_dir} does not exist (run ddl/02_capture.sql)")
    log(phase, f"driver start env={json.dumps(ENV.summary())} args={json.dumps(vars(a))}")
    log(phase, f"build {json.dumps(build_info())}")
    log(phase, f"driver_hash {json.dumps(code_hashes())}")
    print("recovered trace rows merged:", load_trace_files(os.path.join(ENV.trace_dir, "*", "*.json")))
    snapshot_config_history()

    tables = []
    if a.tables:
        ids = ",".join(str(int(x)) for x in a.tables.split(","))
        tables = [r.asDict() for r in spark.sql(f"""
            SELECT table_id, table_nk, catalog, schema, `table`, quarantine
            FROM {ENV.fq('metadata', '`table`')} WHERE table_id IN ({ids}) ORDER BY table_id""").collect()]
        for t in tables:
            t["not_null"], t["not_empty"] = rule_columns(t["table_id"])

    scopes = [tuple(s.split("|", 1)) for s in a.scopes.split(",") if s]
    print("trace installed:", dq_trace.install(dqf, ENV.trace_dir))
    print("cache shim:", ENV.cache_mode, cache_shim.install(ENV.cache_mode))
    if a.fault:
        fd, fa, fp, fn = a.fault.split("|")
        install_fault((fd, fa, fp, int(fn)))

    if a.mode == "run":
        for apply_at, project in scopes:
            run_call(None, apply_at, project, tables, phase)
    else:
        done = set()
        if has("run_calendar"):
            done = {(r.simulated_date, r.apply_at, r.project or "") for r in
                    spark.table(f"{SB}.run_calendar").select("simulated_date", "apply_at", "project").collect()}
        start = date.fromisoformat(a.start)
        for i in range(int(a.ndays)):
            d = start + timedelta(days=i)
            for t in tables:
                load_source(t, d, (d - date(2026, 1, 1)).days)
            snapshot_columns(d, tables)
            for apply_at, project in scopes:
                if (d, apply_at, project) in done:
                    print(f"{d} {apply_at} {project or '-'}: already in run_calendar, skipped")
                    continue
                run_call(d, apply_at, project, tables, phase)

    log(phase, f"driver end wall_clock_s={time.time() - t_start:.0f}")


if __name__ == "__main__":
    main()
