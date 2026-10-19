"""Offline guard tests: simulate target 'company' (sandbox off) and a wrong host, and check that every
sandbox-only entrypoint refuses BEFORE sending any SQL. No workspace calls are made.

  python3 -I scripts/test_guards.py
"""
import builtins
import dataclasses
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config as E  # noqa: E402

DDL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ddl")

COMPANY = E.Env(target="company", expected_host="https://company.example.cloud.databricks.com",
                dq_catalog="analytics_dq_prd", dashboard_schema="dq_dashboard", warehouse_id="w123",
                cache_mode="auto", sandbox_enabled=False, trace_enabled=False,
                synthetic_enabled=False, simulated_dates_enabled=False, profile="COMPANY")
sent = []
existing = []      # rows returned for the information_schema lookup of --replace-views


def fake_sql(env, s):
    sent.append(s)
    return json.dumps(existing) if "information_schema.tables" in s else "[]"


E.sql = fake_sql
E.connect_local = lambda target: COMPANY


def expect_refusal(name, fn):
    sent.clear()
    try:
        fn()
    except E.GuardError as e:
        assert not sent, f"{name}: SQL was sent before refusal: {sent}"
        print(f"PASS {name}: {str(e)[:110]}")
        return
    except SystemExit as e:
        raise AssertionError(f"{name}: exited {e}") from None
    raise AssertionError(f"{name}: NOT refused (sent={sent})")


def run_main(module, argv):
    sys.argv = [module] + argv
    mod = __import__(module)
    mod.env_config.sql = E.sql
    mod.env_config.connect_local = E.connect_local
    mod.main()


expect_refusal("gen_ddl (gen_sources)", lambda: run_main("gen_ddl", ["--target", "company"]))
expect_refusal("run_ddl 01_sources.sql", lambda: run_main("run_ddl", ["--target", "company", os.path.join(DDL, "01_sources.sql")]))
expect_refusal("run_ddl 03_sandbox.sql", lambda: run_main("run_ddl", ["--target", "company", os.path.join(DDL, "03_sandbox.sql")]))
expect_refusal("teardown --reset-capture without --execute",
               lambda: run_main("teardown", ["--target", "company", "--step", "1", "--reset-capture"]))
expect_refusal("teardown --reset-capture without explicit --call-ids",
               lambda: run_main("teardown", ["--target", "company", "--step", "1", "--execute", "--reset-capture"]))
expect_refusal("teardown --reset-capture on company (sandbox off)",
               lambda: run_main("teardown", ["--target", "company", "--step", "1", "--execute", "--reset-capture",
                                             "--call-ids", "00000000-0000-4000-8000-000000000001"]))
for step in (2, 3, 4, 5):
    expect_refusal(f"teardown step {step}", lambda s=step: run_main("teardown", ["--target", "company", "--step", str(s), "--execute"]))
expect_refusal("driver backfill guard", lambda: COMPANY.require_sandbox("simulated backfill"))
expect_refusal("driver data generation", lambda: COMPANY.require_sandbox("data generation (load_source)"))
expect_refusal("driver fault injection", lambda: COMPANY.require_sandbox("fault injection"))
expect_refusal("driver simulated dates", lambda: COMPANY.require_simulated_dates("simulated backfill"))
expect_refusal("host mismatch", lambda: COMPANY.assert_host("https://example.cloud.databricks.com"))
expect_refusal("company with sandbox on", lambda: E._build({**{k: getattr(COMPANY, k) for k in E.STR_KEYS},
                                                         **{k: "true" for k in E.BOOL_KEYS}}))
expect_refusal("placeholder values", lambda: E._build({**{k: getattr(COMPANY, k) for k in E.STR_KEYS},
                                                     "dq_catalog": "<SET: x>", **{k: "false" for k in E.BOOL_KEYS}}))

# step 1 on company is allowed: preview only sends SELECTs, never DELETE without --execute
sent.clear()
run_main("teardown", ["--target", "company", "--step", "1"])
assert sent and all(s.lstrip().upper().startswith("SELECT") for s in sent), sent
print(f"PASS teardown step 1 preview on company: {len(sent)} SELECT(s), 0 DELETE")

# ---- run_ddl --replace-views
MARK = "dq_dashboard sandbox - generated"
TMP = tempfile.mkdtemp()


def ddl_file(name, body):
    path = os.path.join(TMP, name)
    with open(path, "w") as f:
        f.write("-- @targets: free, company\n" + body)
    return path


def view(fq, comment=MARK):
    return f"CREATE VIEW IF NOT EXISTS {fq}\nCOMMENT '{comment}'\nAS SELECT 1 AS x;\n"


def expect_refusal_no_write(name, fn):
    """Refused, and at most read-only lookups were sent before the refusal."""
    sent.clear()
    try:
        fn()
    except E.GuardError as e:
        assert all(s.lstrip().upper().startswith("SELECT") for s in sent), f"{name}: write sent before refusal: {sent}"
        print(f"PASS {name}: {str(e)[:110]}")
        return
    raise AssertionError(f"{name}: NOT refused (sent={sent})")


SB = f"{COMPANY.dq_catalog}.{COMPANY.dashboard_schema}"
replace = lambda path, *extra: run_main("run_ddl", ["--target", "company", path, "--replace-views", *extra])

existing[:] = [{"table_name": "results_synthetic", "table_type": "MANAGED", "comment": MARK}]
expect_refusal_no_write("replace-views: table target", lambda: replace(ddl_file("t.sql", view(f"{SB}.results_synthetic")), "--yes"))
existing[:] = []
for schema in ("reporting", "results", "configuration", "metadata", "quarantine"):
    expect_refusal(f"replace-views: view in {schema}",
                   lambda sc=schema: replace(ddl_file("o.sql", view(f"{COMPANY.dq_catalog}.{sc}.v_dq_results")), "--yes"))
expect_refusal("replace-views: view in another catalog", lambda: replace(ddl_file("c.sql", view(f"other_catalog.{COMPANY.dashboard_schema}.v_x")), "--yes"))
existing[:] = [{"table_name": "v_x", "table_type": "VIEW", "comment": None}]
expect_refusal_no_write("replace-views: existing view without marker", lambda: replace(ddl_file("m.sql", view(f"{SB}.v_x")), "--yes"))
existing[:] = []
expect_refusal("replace-views: statement without marker", lambda: replace(ddl_file("n.sql", view(f"{SB}.v_x", "other")), "--yes"))
for stmt in (f"CREATE TABLE IF NOT EXISTS {SB}.t (x INT)", f"DROP VIEW {SB}.v_x", f"ALTER VIEW {SB}.v_x AS SELECT 2",
             f"INSERT INTO {SB}.t VALUES (1)", f"DELETE FROM {SB}.t", f"TRUNCATE TABLE {SB}.t",
             f"MERGE INTO {SB}.t USING {SB}.t s ON true WHEN MATCHED THEN DELETE", f"UPDATE {SB}.t SET x = 1",
             f"CREATE OR REPLACE TABLE {SB}.t AS SELECT 1"):
    expect_refusal(f"replace-views: non-view statement in file ({stmt.split()[0]} {stmt.split()[1]})",
                   lambda st=stmt: replace(ddl_file("x.sql", view(f"{SB}.v_x") + st + ";\n"), "--yes"))
expect_refusal("default mode: CREATE OR REPLACE VIEW", lambda: run_main(
    "run_ddl", ["--target", "company", ddl_file("r.sql", f"CREATE OR REPLACE VIEW {SB}.v_x COMMENT '{MARK}' AS SELECT 1;\n")]))
expect_refusal("default mode: marked view file is not replaced", lambda: run_main(
    "run_ddl", ["--target", "company", ddl_file("d.sql", view(f"{SB}.v_x").replace("IF NOT EXISTS ", ""))]))

# not confirmed: list only, no write
existing[:] = [{"table_name": "v_x", "table_type": "VIEW", "comment": MARK}]
sent.clear()
builtins.input = lambda prompt="": "no"
replace(ddl_file("ok.sql", view(f"{SB}.v_x") + view(f"{SB}.v_new")))
assert all(s.lstrip().upper().startswith("SELECT") for s in sent), sent
print("PASS replace-views without --yes: views listed, 0 statements executed")
# confirmed: exactly one CREATE OR REPLACE VIEW per marked view of the dashboard schema
sent.clear()
replace(ddl_file("ok.sql", view(f"{SB}.v_x") + view(f"{SB}.v_new")), "--yes")
writes = [s for s in sent if not s.lstrip().upper().startswith("SELECT")]
assert [w.split("\n")[0] for w in writes] == [f"CREATE OR REPLACE VIEW {SB}.v_x", f"CREATE OR REPLACE VIEW {SB}.v_new"], writes
assert all(f"COMMENT '{MARK}'" in w for w in writes), writes
print(f"PASS replace-views with --yes: {len(writes)} CREATE OR REPLACE VIEW, nothing else")
# the shipped view files pass the replace-mode checks
existing[:] = []
for f in ("04_v_results_ext.sql", "06_dashboard_views.sql", "07_rule_daily.sql"):
    sent.clear()
    replace(os.path.join(DDL, f), "--dry-run")
    assert all(s.lstrip().upper().startswith("SELECT") for s in sent), sent
print("PASS shipped view files accepted by --replace-views (dry run)")
print("all guard tests passed")
