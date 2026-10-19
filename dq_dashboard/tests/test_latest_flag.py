"""Warehouse test (read-only) for the is_latest_for_day expression of v_results_ext and v_results_all.

  python3 -I test_latest_flag.py --target free [--print]

Takes the expression between the `latest_flag:begin` / `latest_flag:end` markers of ddl/04_v_results_ext.sql and
ddl/06_dashboard_views.sql (they must be identical), applies it to inline rows shaped like the real results (same
columns, scopes and timestamps as a real rerun day) and compares the flag with the expected value per row.
Only a SELECT is sent; nothing is created or written.
"""
import argparse
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DDL = os.path.normpath(os.path.join(HERE, "..", "ddl"))
sys.path.insert(0, os.path.normpath(os.path.join(HERE, "..", "bundle", "src", "driver")))
import env_config  # noqa: E402

# case, row_level, run_date, table_id, apply_at, project, rule_assignment_id, call (result_id), call start, row start,
# expected is_latest_for_day
C1, C2, C3, C4 = ("8f0c6c1e-0001-4a51-9d1e-3c0f00000001", "8f0c6c1e-0002-4a51-9d1e-3c0f00000002",
                  "8f0c6c1e-0003-4a51-9d1e-3c0f00000003", "8f0c6c1e-0004-4a51-9d1e-3c0f00000004")
ROWS = [
    ("A rerun after a table fault: fault row superseded", "table", "2026-10-03", 4, "bronze_to_gold", "sas2py", None, C1, "09:00:00", "09:00:05", False),
    ("A rerun after a table fault: rerun rule 1", "rule", "2026-10-03", 4, "bronze_to_gold", "sas2py", 101, C2, "12:00:00", "12:00:20", True),
    ("A rerun after a table fault: rerun rule 2", "rule", "2026-10-03", 4, "bronze_to_gold", "sas2py", 102, C2, "12:00:00", "12:00:40", True),
    ("B fault, table not in the rerun: stays latest", "table", "2026-10-03", 5, "bronze_to_gold", "sas2py", None, C1, "09:00:00", "09:01:00", True),
    ("C rule rerun: first result", "rule", "2026-10-03", 6, "bronze_to_gold", "sas2py", 201, C1, "09:00:00", "09:02:00", False),
    ("C rule rerun: rerun result", "rule", "2026-10-03", 6, "bronze_to_gold", "sas2py", 201, C2, "12:00:00", "12:01:00", True),
    ("D fault in both calls: first fault", "table", "2026-10-03", 7, "bronze_to_gold", "sas2py", None, C1, "09:00:00", "09:03:00", False),
    ("D fault in both calls: rerun fault", "table", "2026-10-03", 7, "bronze_to_gold", "sas2py", None, C2, "12:00:00", "12:02:00", True),
    ("E rules first, fault in the rerun: rule keeps its latest result", "rule", "2026-10-03", 8, "bronze_to_gold", "sas2py", 301, C1, "09:00:00", "09:04:00", True),
    ("E rules first, fault in the rerun: rerun fault", "table", "2026-10-03", 8, "bronze_to_gold", "sas2py", None, C2, "12:00:00", "12:03:00", True),
    ("F same table and day, other scope: own fault stays latest", "table", "2026-10-03", 4, "bronze", None, None, C3, "13:00:00", "13:00:05", True),
    ("G next day: rule result", "rule", "2026-10-04", 4, "bronze_to_gold", "sas2py", 101, C4, "09:00:00", "09:00:20", True),
]


def expression(path):
    text = open(path).read()
    m = re.search(r"--\s*latest_flag:begin[^\n]*\n(.*?)\n\s*--\s*latest_flag:end", text, re.S)
    if not m:
        raise SystemExit(f"latest_flag markers not found in {path}")
    return re.sub(r"\s+", " ", m.group(1)).strip()


def build_sql(expr):
    lit = lambda v: "NULL" if v is None else (f"'{v}'" if isinstance(v, str) else str(v))
    values = ",\n  ".join(
        f"({lit(case)}, {lit(level)}, DATE'{day}', {tid}, {lit(apply_at)}, {lit(project)}, "
        f"CAST({lit(ra)} AS BIGINT), {lit(call)}, TIMESTAMP'{day} {call_ts}', TIMESTAMP'{day} {row_ts}', {str(exp).upper()})"
        for case, level, day, tid, apply_at, project, ra, call, call_ts, row_ts, exp in ROWS)
    return (
        "WITH t (case_name, row_level, run_date, table_id, apply_at, project, rule_assignment_id, result_id, "
        "start_timestamp_execution, start_timestamp, expected) AS (VALUES\n  " + values + "\n)\n"
        f"SELECT case_name, expected, {expr.replace(' AS is_latest_for_day', '')} AS actual FROM t ORDER BY case_name")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--print", action="store_true", help="print the SQL instead of running it")
    a = ap.parse_args()
    e04 = expression(os.path.join(DDL, "04_v_results_ext.sql"))
    e06 = expression(os.path.join(DDL, "06_dashboard_views.sql"))
    assert e04 == e06, "latest_flag expressions differ between 04 and 06"
    sql = build_sql(e04)
    if a.print:
        print(sql)
        return
    env = env_config.connect_local(a.target)
    rows = json.loads(env_config.sql(env, sql))
    failed = [r for r in rows if r["expected"] != r["actual"]]
    for r in rows:
        print(f"{'PASS' if r['expected'] == r['actual'] else 'FAIL'} {r['case_name']}: expected {r['expected']}, got {r['actual']}")
    assert len(rows) == len(ROWS), f"{len(rows)} rows returned, {len(ROWS)} expected"
    if failed:
        raise SystemExit(f"{len(failed)} of {len(rows)} cases failed")
    print(f"all {len(rows)} cases passed (04 and 06 use the same expression)")


if __name__ == "__main__":
    main()
