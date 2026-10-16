"""Local test (no Databricks) for the driver's run_calendar.call_status.

  python3 -I test_driver_call_status.py --driver <path/to/dq_backfill_driver.py> --expect old|new

Loads the given driver file with fake pyspark / databricks.sdk / framework modules (helpers env_config, dq_trace,
cache_shim are taken from bundle/src/driver) and calls the REAL run_call() for each scenario. The rows the driver
would append to run_calendar / sandbox_log are captured instead of written.
Checks marked "fix" reproduce the bug on the old driver (expected FAIL with --expect old) and must pass on the new one.
"""
import argparse
import importlib.util
import os
import shutil
import sys
import tempfile
import types
from datetime import date
from types import SimpleNamespace as NS

HERE = os.path.dirname(os.path.abspath(__file__))
DRIVER_DIR = os.path.normpath(os.path.join(HERE, "..", "bundle", "src", "driver"))
HOST = "https://example.cloud.databricks.com"
FAULT = "DQ_DASHBOARD_FAULT_INJECTION"


# ------------------------------------------------------------------ fakes
class FakeSpark:
    statuses = {}

    def sql(self, q):
        rid = q.split("result_id = '")[1].split("'")[0] if "result_id = '" in q else None
        rows = [{"status": self.statuses[rid]}] if rid in self.statuses else []
        return NS(collect=lambda: rows, count=lambda: len(rows))


SPARK = FakeSpark()


def install_fakes(behaviour):
    def mod(name, **attrs):
        m = types.ModuleType(name)
        m.__dict__.update(attrs)
        sys.modules[name] = m
        return m
    any_fn = lambda *a, **k: NS()
    t = mod("pyspark.sql.types", StructType=any_fn, StructField=any_fn, StringType=any_fn, LongType=any_fn, IntegerType=any_fn)
    f = mod("pyspark.sql.functions")
    sql = mod("pyspark.sql", SparkSession=NS(builder=NS(getOrCreate=lambda: SPARK)), functions=f, types=t)
    mod("pyspark", sql=sql)
    mod("databricks")
    mod("databricks.sdk", WorkspaceClient=lambda: NS(config=NS(host=HOST)))
    mod("framework")
    mod("framework.constants", CATALOG="analytics_dq_dev")

    def run_data_quality(apply_at, run_mode, filter_values=None, project_name=None, full_table_name=None,
                         df_input=None, display_logs=None, on_rule_error="raise"):
        return behaviour(sys.modules["framework.dq_framework"])
    mod("framework.dq_framework", run_data_quality=run_data_quality,
        filter_by_filter_field=lambda params, df_input: "df")


def load_driver(driver_path, behaviour):
    install_fakes(behaviour)
    work = tempfile.mkdtemp()
    for f in ("env_config.py", "dq_trace.py", "cache_shim.py"):
        shutil.copy(os.path.join(DRIVER_DIR, f), work)
    shutil.copy(driver_path, os.path.join(work, "dq_backfill_driver.py"))
    for m in ("env_config", "dq_trace", "cache_shim", "driver_under_test"):
        sys.modules.pop(m, None)
    sys.argv = ["driver", "--target", "free", "--expected_host", HOST, "--dq_catalog", "analytics_dq_dev",
                "--dashboard_schema", "dq_dashboard", "--warehouse_id", "w", "--sandbox_enabled", "true",
                "--trace_enabled", "false", "--synthetic_enabled", "true", "--simulated_dates_enabled", "true",
                "--cache_mode", "off"]
    spec = importlib.util.spec_from_file_location("driver_under_test", os.path.join(work, "dq_backfill_driver.py"))
    drv = importlib.util.module_from_spec(spec)
    sys.modules["driver_under_test"] = drv
    spec.loader.exec_module(drv)
    captured = {"run_calendar": [], "sandbox_log": []}
    drv.append = lambda table, rows, schema: captured.setdefault(table, []).extend(rows)
    drv.log = lambda phase, note: captured["sandbox_log"].append(note)
    drv.load_trace_files = lambda pattern: 0
    drv.snapshot_quarantine = lambda *a, **k: None
    return drv, captured


def scenario(driver_path, behaviour, *, on_rule_error, statuses, fault=None):
    FakeSpark.statuses = statuses
    drv, cap = load_driver(driver_path, behaviour)
    drv.ON_RULE_ERROR = on_rule_error
    d = date(2026, 10, 3)
    if fault:
        drv.install_fault((d.isoformat(), "bronze", "", 1))
    drv.run_call(d, "bronze", "", [], "test")
    return cap["run_calendar"][0][5], cap["sandbox_log"]          # (call_status, notes)


def returns(rid):
    return lambda dqf: rid


def raises(msg):
    def b(dqf):
        raise RuntimeError(msg)
    return b


def continue_swallows_fault(rid):
    def b(dqf):          # like the patched framework in continue mode: table failure recorded, not re-raised
        try:
            dqf.filter_by_filter_field(NS(meta=NS(table_id=4)), None)
        except RuntimeError:
            pass
        return rid
    return b


# (fix?, name, behaviour, kwargs, expected call_status, expect fault note)
CASES = [
    (True,  "continue mode, run ends code_error -> call_status code_error", returns("R1"), dict(on_rule_error="continue", statuses={"R1": "code_error"}), "code_error", False),
    (True,  "continue mode, run ends pass -> pass",                         returns("R2"), dict(on_rule_error="continue", statuses={"R2": "pass"}), "pass", False),
    (True,  "continue mode, run ends warning -> warning",                   returns("R3"), dict(on_rule_error="continue", statuses={"R3": "warning"}), "warning", False),
    (True,  "returned id without results row -> missing",                   returns("R7"), dict(on_rule_error="continue", statuses={}), "missing", False),
    (True,  "continue mode swallows injected fault -> code_error + fault note", continue_swallows_fault("R6"),
            dict(on_rule_error="continue", statuses={"R6": "code_error"}, fault=True), "code_error", True),
    (False, "raise mode, exception -> code_error (unchanged)",              raises("[PARSE_SYNTAX_ERROR] x"), dict(on_rule_error="raise", statuses={}), "code_error", False),
    (False, "raise mode, injected fault -> fault_injected (unchanged)",     raises(f"{FAULT}: crash"), dict(on_rule_error="raise", statuses={}), "fault_injected", True),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--driver", required=True)
    ap.add_argument("--expect", required=True, choices=("old", "new"))
    a = ap.parse_args()
    deviations = 0
    for fix, name, behaviour, kw, want_status, want_note in CASES:
        try:
            status, notes = scenario(a.driver, behaviour, **kw)
            has_note = any("fault_injected" in n for n in notes)
            ok = status == want_status and has_note == want_note
            detail = f"call_status={status!r} fault_note={has_note}"
        except Exception as e:  # noqa: BLE001
            ok, detail = False, f"{type(e).__name__}: {e}"[:150]
        expected = not (fix and a.expect == "old")
        deviations += ok != expected
        print(f"[{a.expect}] {'PASS' if ok else 'FAIL':<4} {'as expected' if ok == expected else 'UNEXPECTED':<11} "
              f"{'(fix) ' if fix else ''}{name}  -- {detail}")
    print(f"[{a.expect}] {'OK' if not deviations else f'{deviations} deviation(s)'}")
    sys.exit(1 if deviations else 0)


if __name__ == "__main__":
    main()
