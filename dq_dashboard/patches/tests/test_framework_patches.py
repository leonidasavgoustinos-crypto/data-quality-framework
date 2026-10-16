"""Local tests (no Databricks) for the framework patches in ../NN_*.diff.

Runs the REAL framework/dq_framework.py + framework/utils.py against in-memory fakes (fake_env.py) and checks
the rows written to results.result / result_table / result_rule.

  python3 -I test_framework_patches.py --framework <dir containing framework/> [--stage NAME]

Each check states for which patch stages it must PASS. Run against the unpatched repo (stage "base") to see the
bugs reproduced, and against the patched tree to see them fixed; the run fails if any check deviates from the
expected matrix (so a check that unexpectedly passes on "base" is also reported).

Stages (cumulative): base, 01 (F-21), 02 (+F-20), 03 (+F-01/F-19/F-02), 04 (+F-03 on_rule_error).
"""
import argparse
import importlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fake_env as fe  # noqa: E402

STAGES = ["base", "01", "02", "03", "04"]
RESULT, RESULT_TABLE, RESULT_RULE = ("analytics_dq_dev.results.result", "analytics_dq_dev.results.result_table",
                                     "analytics_dq_dev.results.result_rule")

# two tables: 10 (rules 1, 2) and 20 (rule 3); rule 2 finds 4 failing rows (policy category = error)
ACTIVE = [
    dict(rule_assignment_id=1, table_id=10, rule_template_id=1, rule_type_id=1, rule_dimension_id=2, policy_id=1, name="is_not_null"),
    dict(rule_assignment_id=2, table_id=10, rule_template_id=6, rule_type_id=1, rule_dimension_id=4, policy_id=1, name="primary_key_violation"),
    dict(rule_assignment_id=3, table_id=20, rule_template_id=1, rule_type_id=1, rule_dimension_id=2, policy_id=2, name="is_not_null"),
]
for r in ACTIVE:
    r.update(catalog="c", schema="s", table=f"t{r['table_id']}", primary_key=["id"], filter_field="d", filter_field_type="date",
             quarantine=f"q.t{r['table_id']}", statement="select 1", engine_type="sql", is_reusable=False, category="error",
             quarantine_flag=False, parameters_identifiers='{"column": "id"}', parameters_values=None, apply_at="bronze")
ROWS_FAILED = {1: 0, 2: 4, 3: 0}


def load_framework(path):
    for m in [m for m in sys.modules if m == "framework" or m.startswith("framework.")]:
        del sys.modules[m]
    fe.install()
    sys.path.insert(0, path)
    try:
        return importlib.import_module("framework.dq_framework")
    finally:
        sys.path.remove(path)


def run(path, *, rule_crash=None, table_crash=None, cache_error=None, on_rule_error=None):
    """Run run_data_quality once; returns (dqf, raised_exception_or_None)."""
    fe.reset()
    fe.CACHE_ERROR["msg"] = cache_error
    dqf = load_framework(path)
    dqf.get_active_rules = lambda params: fe.FakeDF(ACTIVE)

    def get_project_id(params):
        params.meta.project_id = 7
    dqf.get_project_id = get_project_id

    def filter_by_filter_field(params, df_input):
        if table_crash == params.meta.table_id:
            raise RuntimeError(f"[TABLE_OR_VIEW_NOT_FOUND] source of table {table_crash}")
        return fe.FakeDF([{"id": i} for i in range(100)])
    dqf.filter_by_filter_field = filter_by_filter_field

    def run_statement(params, df):
        if rule_crash == params.config.rule_assignment_id:
            raise RuntimeError("[PARSE_SYNTAX_ERROR] Syntax error at or near 'x'")
        return fe.FakeDF([{"id": i} for i in range(ROWS_FAILED[params.config.rule_assignment_id])])
    dqf.run_statement = run_statement

    kwargs = dict(apply_at="bronze", run_mode="all", project_name="p")
    if on_rule_error:
        kwargs["on_rule_error"] = on_rule_error
    try:
        dqf.run_data_quality(**kwargs)
        return dqf, None
    except Exception as e:  # noqa: BLE001
        return dqf, e


def rows(t):
    return fe.STORE.get(t, [])


def by(t, key, value):
    return [r for r in rows(t) if r.get(key) == value]


# ------------------------------------------------------------------ checks: (name, stages that must PASS, fn)
def chk_clean_run(path):
    _, e = run(path)
    res, tabs, rules = rows(RESULT), rows(RESULT_TABLE), rows(RESULT_RULE)
    assert e is None, e
    assert [r["status"] for r in res] == ["error"], res
    assert sorted(r["status"] for r in tabs) == ["error", "pass"], tabs
    assert sorted(r["status"] for r in rules) == ["error", "pass", "pass"], rules
    assert all(r["rows_evaluated"] == 100 for r in rules)
    assert by(RESULT_TABLE, "table_id", 10)[0]["rules_error"] == 1


def chk_clean_run_ids(path):
    run(path)
    for r in rows(RESULT_RULE):
        assert r["rule_dimension_id"] in (2, 4), f"rule_dimension_id not written: {r}"


def chk_cache_serverless(path):
    _, e = run(path, cache_error="[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.")
    assert e is None, f"run aborted: {e}"
    assert len(rows(RESULT_RULE)) == 3


def chk_cache_other_error_still_raises(path):
    _, e = run(path, cache_error="some other cache failure")
    assert e is not None and "some other cache failure" in str(e)


def chk_table_crash_does_not_overwrite_previous(path):
    """F-20: crash in table 20 before open_result_table must not touch table 10's closed rows."""
    _, e = run(path, table_crash=20)
    assert e is not None
    t10 = by(RESULT_TABLE, "table_id", 10)[0]
    assert t10["status"] == "error", f"closed table row overwritten: {t10['status']}"
    last_rule = by(RESULT_RULE, "rule_assignment_id", 2)[0]
    assert last_rule["status"] == "error", f"closed rule row overwritten: {last_rule['status']}"
    assert rows(RESULT)[0]["status"] == "code_error"


def chk_rule_crash_marks_only_that_rule(path):
    _, e = run(path, rule_crash=2)
    assert e is not None
    statuses = {r["result_rule_id"]: r["status"] for r in rows(RESULT_RULE)}
    assert sorted(statuses.values()) == ["code_error", "pass"], statuses
    assert [r["status"] for r in rows(RESULT_TABLE)] == ["code_error"]
    assert rows(RESULT)[0]["status"] == "code_error"


def chk_rule_crash_row_has_ids_and_message(path):
    """F-01/F-19/F-02: the failed rule row carries its ids and error_message; table/result rows their ids."""
    _, e = run(path, rule_crash=2)
    failed = [r for r in rows(RESULT_RULE) if r["status"] == "code_error"][0]
    for k in ("result_id", "result_table_id", "rule_assignment_id", "project_id", "policy_id", "rule_type_id", "rule_dimension_id"):
        assert failed.get(k) is not None, f"{k} missing on failed rule row: {failed}"
    assert failed["rule_assignment_id"] == 2
    assert "PARSE_SYNTAX_ERROR" in (failed.get("error_message") or ""), failed.get("error_message")
    tab = rows(RESULT_TABLE)[0]
    assert tab.get("result_id") and tab.get("table_id") == 10 and tab.get("project_id") == 7, tab
    assert rows(RESULT)[0].get("project_id") == 7, rows(RESULT)[0]


def chk_default_still_raises_and_stops(path):
    """Default behaviour preserved: the exception propagates and later tables are not evaluated."""
    _, e = run(path, rule_crash=1)
    assert e is not None and "PARSE_SYNTAX_ERROR" in str(e)
    assert not by(RESULT_RULE, "rule_assignment_id", 3), "table 20 was evaluated although the run must abort"


def chk_continue_evaluates_rest(path):
    dqf, e = run(path, rule_crash=1, table_crash=None, on_rule_error="continue")
    assert e is None, e
    st = {r["rule_assignment_id"]: r["status"] for r in rows(RESULT_RULE)}
    assert st == {1: "code_error", 2: "error", 3: "pass"}, st
    t = {r["table_id"]: r["status"] for r in rows(RESULT_TABLE)}
    assert t == {10: "code_error", 20: "pass"}, t
    assert rows(RESULT)[0]["status"] == "code_error"
    assert "PARSE_SYNTAX_ERROR" in by(RESULT_RULE, "rule_assignment_id", 1)[0]["error_message"]


def chk_continue_table_crash_gets_row(path):
    _, e = run(path, table_crash=10, on_rule_error="continue")
    assert e is None, e
    t = {r["table_id"]: r["status"] for r in rows(RESULT_TABLE)}
    assert t == {10: "code_error", 20: "pass"}, t
    assert rows(RESULT)[0]["status"] == "code_error"


def chk_invalid_on_rule_error(path):
    _, e = run(path, on_rule_error="ignore")
    assert isinstance(e, ValueError), e


CHECKS = [
    ("clean run: statuses/counters unchanged",              ["base", "01", "02", "03", "04"], chk_clean_run),
    ("clean run: rule_dimension_id written (F-02)",         ["03", "04"],                     chk_clean_run_ids),
    ("F-21 cache unsupported on serverless -> uncached",    ["01", "02", "03", "04"],         chk_cache_serverless),
    ("F-21 other cache errors still raise",                 ["base", "01", "02", "03", "04"], chk_cache_other_error_still_raises),
    ("F-20 table crash keeps previous table/rule rows",     ["02", "03", "04"],               chk_table_crash_does_not_overwrite_previous),
    ("rule crash marks only that rule/table/run",           ["base", "01", "02", "03", "04"], chk_rule_crash_marks_only_that_rule),
    ("F-01/F-19 failed rule row has ids + error_message",   ["03", "04"],                     chk_rule_crash_row_has_ids_and_message),
    ("default on error: raise and stop (unchanged)",        ["base", "01", "02", "03", "04"], chk_default_still_raises_and_stops),
    ("F-03 on_rule_error=continue evaluates the rest",      ["04"],                           chk_continue_evaluates_rest),
    ("F-03 continue: crashed table gets a code_error row",  ["04"],                           chk_continue_table_crash_gets_row),
    ("F-03 invalid on_rule_error rejected",                 ["04"],                           chk_invalid_on_rule_error),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--framework", required=True, help="directory that contains framework/")
    ap.add_argument("--stage", required=True, choices=STAGES)
    a = ap.parse_args()
    deviations = 0
    for name, expected_stages, fn in CHECKS:
        try:
            fn(a.framework)
            ok, detail = True, ""
        except Exception as e:  # noqa: BLE001
            ok, detail = False, f"{type(e).__name__}: {e}"[:160]
        expected = a.stage in expected_stages
        mark = "PASS" if ok else "FAIL"
        verdict = "as expected" if ok == expected else "UNEXPECTED"
        deviations += ok != expected
        print(f"[{a.stage}] {mark:<4} {verdict:<11} {name}" + (f"  -- {detail}" if not ok else ""))
    print(f"[{a.stage}] {'OK' if not deviations else f'{deviations} deviation(s)'}")
    sys.exit(1 if deviations else 0)


if __name__ == "__main__":
    main()
