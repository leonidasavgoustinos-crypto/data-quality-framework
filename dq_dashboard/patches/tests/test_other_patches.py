"""Local tests (no Databricks) for patches 05-08.

  python3 -I test_other_patches.py --root <repo-or-copy> --applied 05,06,07,08

Each check names the patch it verifies; it must FAIL when that patch is not in --applied (bug reproduced) and PASS
when it is (unless marked "all": must pass regardless). Exits non-zero on any deviation from that expectation.
  05 F-13  setup/01_ddl.py keeps tables by default, explicit reset_tables=true recreates them
  06 C-01  configuration/dataset_creation.ipynb refuses to TRUNCATE without confirmation
  07 F-22  combined_dq_and_df_load.run_dq_and_insert inserts only when the run's status is 'pass'
  08 V-02/V-04  reporting views: additive id + numeric duration columns, existing lines unchanged
"""
import argparse
import json
import os
import re
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fake_env as fe  # noqa: E402

CONFIRM = "TRUNCATE analytics_dq_dev.configuration"


# ------------------------------------------------------------------ fakes for notebooks
class Widgets:
    def __init__(self, values):
        self.values, self.defaults = dict(values), {}

    def text(self, name, default="", *a):
        self.defaults.setdefault(name, default)

    def dropdown(self, name, default, choices, *a):
        self.defaults.setdefault(name, default)

    def get(self, name):
        return self.values.get(name, self.defaults.get(name, ""))


class NotebookExit(Exception):
    pass


class Notebook:
    def exit(self, msg):
        raise NotebookExit(msg)


class Dbutils:
    def __init__(self, values=None):
        self.widgets, self.notebook = Widgets(values or {}), Notebook()


class RecordingSpark:
    def __init__(self):
        self.statements = []

    def sql(self, s):
        self.statements.append(s)


# ------------------------------------------------------------------ 05 F-13
def run_ddl(root, values):
    spark, dbu = RecordingSpark(), Dbutils(values)
    src = open(os.path.join(root, "setup", "01_ddl.py")).read()
    exec(compile(src, "01_ddl.py", "exec"), {"spark": spark, "dbutils": dbu, "__name__": "nb"})
    return [s for s in spark.statements if re.search(r"(?i)\bTABLE\b", s.split("(")[0])]


def chk_05_default_keeps_tables(root):
    stmts = run_ddl(root, {})
    assert len(stmts) == 11, len(stmts)
    bad = [s.split("(")[0].strip() for s in stmts if not s.lstrip().startswith("CREATE TABLE IF NOT EXISTS")]
    assert not bad, f"default run would replace tables: {bad[:3]}"


def chk_05_reset_flag_recreates(root):
    stmts = run_ddl(root, {"reset_tables": "true"})
    assert len(stmts) == 11 and all(s.lstrip().startswith("CREATE OR REPLACE TABLE") for s in stmts)


# ------------------------------------------------------------------ 06 C-01
def notebook(root):
    return json.load(open(os.path.join(root, "configuration", "dataset_creation.ipynb")))


def guard_cell(root):
    for c in notebook(root)["cells"]:
        src = "".join(c["source"])
        if src.startswith("%sql"):
            return None                       # reached the TRUNCATE cell without a guard
        if "confirm_truncate" in src:
            return src
    return None


def chk_06_guard_refuses_without_confirmation(root):
    src = guard_cell(root)
    assert src, "no guard cell before the TRUNCATE cell"
    try:
        exec(compile(src, "guard", "exec"), {"dbutils": Dbutils()})
    except Exception as e:  # noqa: BLE001
        assert "Refusing" in str(e)
        return
    raise AssertionError("guard did not refuse")


def chk_06_guard_allows_with_confirmation(root):
    src = guard_cell(root)
    assert src, "no guard cell"
    exec(compile(src, "guard", "exec"), {"dbutils": Dbutils({"confirm_truncate": CONFIRM})})


def chk_06_sql_cell_asserts_before_truncate(root):
    sql = next("".join(c["source"]) for c in notebook(root)["cells"] if "".join(c["source"]).startswith("%sql"))
    a, t = sql.find("assert_true(:confirm_truncate"), sql.find("TRUNCATE TABLE")
    assert a != -1 and a < t, "SQL cell does not assert the confirmation before its first TRUNCATE"
    assert sql.count("TRUNCATE TABLE") == 3, "TRUNCATE statements must be unchanged"


# ------------------------------------------------------------------ 07 F-22
class GateSpark:
    def __init__(self, status):
        self.status = status

    def table(self, name):
        rows = []
        if name.endswith(".results.result") and self.status is not None:
            rows = [{"result_id": "R1", "status": self.status}]
        return GateDF(rows)                    # audit table: always empty (never written)


class GateDF:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *cols):
        return GateDF([{c: r[c] for c in cols} | {"result_id": r.get("result_id")} for r in self.rows])

    def where(self, cond):
        m = re.match(r"\s*(\w+)\s*=\s*'([^']*)'", cond)
        return GateDF([r for r in self.rows if str(r.get(m.group(1))) == m.group(2)])

    def collect(self):
        return [fe.Row(r) for r in self.rows]


class InsertDF:
    def __init__(self):
        self.inserted = False
        self.write = self

    def mode(self, m):
        return self

    def insertInto(self, name):
        self.inserted = True


def gate(root, status, raise_code_error=False):
    """Returns (inserted, exited, raised)."""
    fe.install()
    runtime = types.ModuleType("databricks.sdk.runtime")
    runtime.dbutils = Dbutils()
    sys.modules["databricks.sdk.runtime"] = runtime
    dqf = types.ModuleType("framework.dq_framework")

    def run_data_quality(**kw):
        if raise_code_error:
            raise RuntimeError("[PARSE_SYNTAX_ERROR] rule failed")
        return "R1"
    dqf.run_data_quality = run_data_quality
    sys.modules["framework"] = types.ModuleType("framework")
    sys.modules["framework.dq_framework"] = dqf
    src = open(os.path.join(root, "framework", "combined_dq_and_df_load.py")).read()
    g = {"__name__": "combined"}
    exec(compile(src, "combined_dq_and_df_load.py", "exec"), g)
    g["get_apply_at_value_for_table"] = lambda **k: "bronze_to_gold"
    g["get_project_value_for_table"] = lambda **k: "sas2py"
    df = InsertDF()
    exited = raised = False
    try:
        g["run_dq_and_insert"](spark=GateSpark(status), df_input=df, full_table_name="c.s.t", env="dev")
    except NotebookExit:
        exited = True
    except RuntimeError:
        raised = True
    return df.inserted, exited, raised


def chk_07_pass_inserts(root):
    inserted, exited, _ = gate(root, "pass")
    assert inserted and not exited


def chk_07_warning_blocks(root):
    inserted, exited, _ = gate(root, "warning")
    assert not inserted and exited, f"inserted={inserted}"


def chk_07_error_blocks(root):
    inserted, exited, _ = gate(root, "error")
    assert not inserted and exited, f"error-level DQ failure but inserted={inserted}"


def chk_07_code_error_status_blocks(root):
    inserted, exited, _ = gate(root, "code_error")
    assert not inserted and exited, f"inserted={inserted}"


def chk_07_code_error_exception_blocks(root):
    inserted, _, raised = gate(root, None, raise_code_error=True)
    assert raised and not inserted


def chk_07_missing_result_row_blocks(root):
    inserted, exited, _ = gate(root, None)
    assert not inserted and exited, f"no result row but inserted={inserted}"


# ------------------------------------------------------------------ 08 views (additive)
def views_file(root):
    return open(os.path.join(root, "setup", "03_reporting_views.py")).read()


ORIGINAL_VIEWS = None


def chk_08_existing_lines_unchanged(root):
    """Every line of the original file is still present, in order (a trailing comma may be appended)."""
    new = views_file(root).splitlines()
    i = 0
    for line in ORIGINAL_VIEWS.splitlines():
        while i < len(new) and new[i] != line and new[i] != line + ",":
            i += 1
        assert i < len(new), f"original line missing or reordered: {line!r}"
        i += 1
    assert views_file(root).count("CREATE OR REPLACE VIEW") == ORIGINAL_VIEWS.count("CREATE OR REPLACE VIEW")


def chk_08_new_columns_present(root):
    s = views_file(root)
    for c in ("rr.result_id", "t.result_table_id", "ru.result_rule_id", "ru.rule_assignment_id", "ru.error_message",
              "AS avg_seconds_per_rule", "AS duration_seconds_for_execution", "AS avg_seconds_per_table"):
        assert c in s, f"missing {c}"


CHECKS = [
    ("05", "setup default keeps existing tables (IF NOT EXISTS)", chk_05_default_keeps_tables),
    ("all", "setup reset_tables=true (or original) recreates tables", chk_05_reset_flag_recreates),
    ("06", "guard refuses TRUNCATE without confirmation", chk_06_guard_refuses_without_confirmation),
    ("06", "guard allows with explicit confirmation", chk_06_guard_allows_with_confirmation),
    ("06", "SQL cell asserts confirmation before TRUNCATE", chk_06_sql_cell_asserts_before_truncate),
    ("all", "gate: status pass -> insert", chk_07_pass_inserts),
    ("07", "gate: status warning -> no insert", chk_07_warning_blocks),
    ("07", "gate: status error -> no insert", chk_07_error_blocks),
    ("07", "gate: status code_error -> no insert", chk_07_code_error_status_blocks),
    ("all", "gate: run_data_quality raises -> no insert", chk_07_code_error_exception_blocks),
    ("07", "gate: missing result row -> no insert", chk_07_missing_result_row_blocks),
    ("08", "views: every existing line unchanged/in order", chk_08_existing_lines_unchanged),
    ("08", "views: new id + numeric duration columns", chk_08_new_columns_present),
]


def main():
    global ORIGINAL_VIEWS
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="repo root or patched copy (contains setup/, configuration/, framework/)")
    ap.add_argument("--original", required=True, help="unpatched repo root (reference for the additive-views check)")
    ap.add_argument("--applied", default="", help="comma-separated patch numbers applied to --root, e.g. 05,06,07,08")
    a = ap.parse_args()
    ORIGINAL_VIEWS = open(os.path.join(a.original, "setup", "03_reporting_views.py")).read()
    applied = {x.strip() for x in a.applied.split(",") if x.strip()}
    deviations = 0
    for patch, name, fn in CHECKS:
        try:
            fn(a.root)
            ok, detail = True, ""
        except Exception as e:  # noqa: BLE001
            ok, detail = False, f"{type(e).__name__}: {e}"[:150]
        expected = patch == "all" or patch in applied
        if patch == "08" and name.startswith("views: every") and "08" not in applied:
            expected = True                  # trivially true on the original file itself
        deviations += ok != expected
        print(f"[{patch}] {'PASS' if ok else 'FAIL':<4} {'as expected' if ok == expected else 'UNEXPECTED':<11} {name}"
              + (f"  -- {detail}" if not ok else ""))
    print("OK" if not deviations else f"{deviations} deviation(s)")
    sys.exit(1 if deviations else 0)


if __name__ == "__main__":
    main()
