"""Offline guard tests: simulate target 'company' (sandbox off) and a wrong host, and check that every
sandbox-only entrypoint refuses BEFORE sending any SQL. No workspace calls are made.

  python3 -I scripts/test_guards.py
"""
import dataclasses
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config as E  # noqa: E402

DDL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ddl")

COMPANY = E.Env(target="company", expected_host="https://company.example.cloud.databricks.com",
                dq_catalog="analytics_dq_prd", dashboard_schema="dq_dashboard", warehouse_id="w123",
                cache_mode="auto", sandbox_enabled=False, trace_enabled=False,
                synthetic_enabled=False, simulated_dates_enabled=False, profile="COMPANY")
sent = []
E.sql = lambda env, s: sent.append(s) or "[]"
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
print("all guard tests passed")
