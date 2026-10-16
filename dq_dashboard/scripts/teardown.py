"""Target-aware teardown runner for ../ddl/teardown.sql (template).

  python3 -I scripts/teardown.py --target free --step 1             # preview (default, read-only)
  python3 -I scripts/teardown.py --target free --step 1 --execute   # delete matched rows, then re-preview
  python3 -I scripts/teardown.py --target free --step 1 --execute --reset-capture --call-ids a,b
                                                                    # sandbox only: + capture rows of those calls
  python3 -I scripts/teardown.py --target free --step 2 --execute   # sandbox only (2-5)
  --objects <path>   sandbox_objects.json written by gen_ddl.py (or set DQ_SANDBOX_OBJECTS); required by
                     steps 2-5 on the sandbox, no default

Rules
  * argument combinations are checked first, then the CLI profile must point at expected_host;
  * a step runs only if the target is listed in its `-- @targets:` line;
  * `-- @sandbox_only` steps (2-5: DROPs, bundle destroy) and --reset-capture require sandbox_enabled=true,
    so they are always refused in company (company never calls bundle destroy);
  * step 1 deletes ONLY rows matched by recorded ids; unmatched rows are reported, never deleted;
  * --reset-capture works on an explicit, verified list of call_ids only (never "everything") and only after
    the step-1 deletes left 0 unmatched rows;
  * step 2 aborts if the quarantine schema holds any table this work did not create;
  * steps 3/4 drop a schema/catalog only if its COMMENT is the sandbox marker; never <dq_catalog> itself.
Without --execute nothing is changed.
"""
import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config  # noqa: E402

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
BUNDLE_DIR = os.path.join(ROOT, "bundle")
MARK = "dq_dashboard sandbox - generated"
_UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def q(env, s):
    return json.loads(env_config.sql(env, s) or "[]")


def show(rows):
    for r in rows:
        print("   ", r)


def sections(text):
    out = {}
    for block in re.split(r"(?m)^-- @step ", text)[1:]:
        out[int(block.split("\n", 1)[0].strip())] = block
    return out


def statements(block, tag, split=";"):
    m = re.search(rf"-- @{tag}\n(.*?)(?=\n-- @\w+\n|\Z)", block, re.S)
    if not m:
        return []
    body = re.sub(r"(?m)^\s*--[^\n]*", "", m.group(1))      # whole-line comments only (keeps --flags)
    return [s.strip() for s in body.split(split) if s.strip()]


def sandbox_objects(env, path_arg):
    p = env_config.objects_path(path_arg)
    if not os.path.isfile(p):
        raise env_config.GuardError(f"sandbox objects file not found: {p}")
    o = json.load(open(p))
    if o.get("target") != env.target:
        raise env_config.GuardError(f"{p} was written for target {o.get('target')!r}, not {env.target!r}")
    return o


def fill(env, text, reset_calls, reset_results, objects_path):
    lit = lambda xs: ", ".join(f"'{x}'" for x in xs) if xs else "NULL"
    text = text.replace("{{reset_call_ids}}", lit(reset_calls)).replace("{{reset_result_ids}}", lit(reset_results))
    keys = ("quarantine_table_list", "quarantine_drops", "schema_drops", "catalog_drops", "legacy_code_root_delete")
    if not any(f"{{{{{k}}}}}" in text for k in keys):
        return text
    if not env.sandbox_enabled:            # sandbox-only steps are refused anyway; keep placeholders harmless
        for k in keys:
            text = text.replace(f"{{{{{k}}}}}", "''" if k == "quarantine_table_list" else "")
        return text
    o = sandbox_objects(env, objects_path)
    qt = o["quarantine_tables"]
    schemas = [f"{env.dq_catalog}.{env.dashboard_schema}"] + [f"{env.dq_catalog}.{s}" for s in o["schemas_created_in_dq_catalog"]]
    schemas += [f"{env.dq_catalog}.{s}" for s in o.get("reconciliation_schemas", [])]
    legacy = o.get("legacy_code_root")
    return (text
            .replace("{{quarantine_table_list}}", ", ".join(f"'{t}'" for t in qt))
            .replace("{{quarantine_drops}}", "\n".join(f"DROP TABLE IF EXISTS {env.dq_catalog}.quarantine.`{t}`;" for t in qt))
            .replace("{{schema_drops}}", "\n".join(f"DROP SCHEMA IF EXISTS {s} CASCADE;" for s in schemas))
            .replace("{{catalog_drops}}", "\n".join(f"DROP CATALOG IF EXISTS {c} CASCADE;" for c in o["catalogs_created"]))
            .replace("{{legacy_code_root_delete}}", f"databricks workspace delete {legacy} --recursive" if legacy else ""))


def check_marker(env, stmt):
    m = re.match(r"(?i)drop\s+(schema|catalog)\s+if\s+exists\s+([\w.`]+)", stmt)
    if not m:
        return True
    kind, name = m.group(1).lower(), m.group(2).replace("`", "")
    if kind == "catalog":
        if name == env.dq_catalog:
            raise env_config.GuardError(f"REFUSED: will never drop the DQ catalog {name}")
        rows = q(env, f"SELECT comment FROM system.information_schema.catalogs WHERE catalog_name = '{name}'")
    else:
        cat, sch = name.split(".")
        rows = q(env, f"SELECT comment FROM {cat}.information_schema.schemata WHERE schema_name = '{sch}'")
    if not rows:
        print(f"    skip {kind} {name}: does not exist")
        return False
    if rows[0].get("comment") != MARK:
        raise env_config.GuardError(f"REFUSED: {kind} {name} lacks the sandbox COMMENT ({rows[0].get('comment')!r})")
    return True


def reset_candidates(env):
    return q(env, f"""SELECT c.call_id, c.result_id, c.simulated_date, c.apply_at, c.project, c.call_status,
                             (SELECT count(*) FROM {env.sb}.rule_trace t WHERE t.call_id = c.call_id) AS trace_rows
                      FROM {env.sb}.run_calendar c ORDER BY c.real_start""")


def verified_reset(env, call_ids):
    cand = {r["call_id"]: r for r in reset_candidates(env)}
    unknown = [c for c in call_ids if c not in cand]
    if unknown:
        raise env_config.GuardError(f"REFUSED: call_ids not in run_calendar: {unknown}")
    return call_ids, sorted({cand[c]["result_id"] for c in call_ids if cand[c]["result_id"]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--step", type=int, required=True, choices=(1, 2, 3, 4, 5))
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--reset-capture", action="store_true", help="step 1, sandbox only: clear capture rows of --call-ids")
    ap.add_argument("--call-ids", default="", help="explicit comma-separated call_ids for --reset-capture")
    ap.add_argument("--objects", help="sandbox_objects.json for steps 2-5 (else $DQ_SANDBOX_OBJECTS)")
    a = ap.parse_args()
    call_ids = [c.strip() for c in a.call_ids.split(",") if c.strip()]
    if a.reset_capture and (a.step != 1 or not a.execute or not call_ids):
        raise env_config.GuardError("--reset-capture requires --step 1 --execute --call-ids <explicit list>")
    if call_ids and not a.reset_capture:
        raise env_config.GuardError("--call-ids is only used with --reset-capture")
    bad = [c for c in call_ids if not _UUID.match(c)]
    if bad:
        raise env_config.GuardError(f"not call_ids: {bad}")

    env = env_config.connect_local(a.target)               # host guard first
    block = sections(open(os.path.join(ROOT, "ddl", "teardown.sql")).read())[a.step]
    targets = [t.strip() for t in re.search(r"-- @targets:\s*([^\n]+)", block).group(1).split(",")]
    if env.target not in targets:
        raise env_config.GuardError(f"REFUSED: teardown step {a.step} is not allowed on target '{env.target}' (allowed: {targets})")
    if "-- @sandbox_only" in block:
        env.require_sandbox(f"teardown step {a.step}")
    if a.reset_capture:
        env.require_sandbox("teardown --reset-capture")

    print(f"== teardown step {a.step} on target {env.target} ({'EXECUTE' if a.execute else 'preview only'})")
    reset_calls, reset_results = verified_reset(env, call_ids) if a.reset_capture else ([], [])
    block = env.render(fill(env, block, reset_calls, reset_results, a.objects))

    for s in statements(block, "preview"):
        print("-- preview"); show(q(env, s))
    if a.step == 1 and env.sandbox_enabled:
        print("-- capture-reset candidates (run_calendar; pass the ones to reset via --call-ids)")
        show(reset_candidates(env))
    for s in statements(block, "guard"):
        rows = q(env, s)
        print(f"-- guard: {len(rows)} unexpected rows"); show(rows)
        if rows:
            raise env_config.GuardError("REFUSED: guard query returned rows; nothing was dropped")
    shell = statements(block, "shell", split="\n")
    if not a.execute:
        for s in statements(block, "execute"):
            print("-- would run:", s.splitlines()[0][:140])
        for s in shell:
            print("-- would run (shell):", s)
        return

    for s in statements(block, "execute"):
        if check_marker(env, s):
            print("-- run:", s.splitlines()[0][:140]); show(q(env, s))
    for s in shell:
        print("-- run (shell):", s)
        args = s.split() + ["-p", env.profile]
        print(env_config._run(args, cwd=BUNDLE_DIR if " bundle " in f" {s} " else None)[-400:])
    for s in statements(block, "preview"):
        print("-- preview after"); show(q(env, s))

    if a.reset_capture:
        unmatched = [r for r in q(env, statements(block, "preview")[0])
                     if r.get("unmatched_keep_and_report") not in ("0", "", None)]
        if unmatched:
            raise env_config.GuardError(f"REFUSED capture reset: unmatched rows remain {unmatched}; the ids are still needed")
        print(f"-- capture reset for call_ids={reset_calls} result_ids={reset_results}")
        for s in statements(block, "capture_reset"):
            print("-- run:", s.splitlines()[0][:160]); show(q(env, s))
        for cid in reset_calls:
            path = f"dbfs:{env.trace_dir}/{cid}"
            env_config._run(["databricks", "fs", "rm", "-r", path, "-p", env.profile])
            print("-- removed trace folder", path)
        print("-- capture rows left for these calls:")
        ids = ", ".join(f"'{c}'" for c in reset_calls)
        show(q(env, f"SELECT 'run_calendar' t, count(*) n FROM {env.sb}.run_calendar WHERE call_id IN ({ids}) UNION ALL "
                    f"SELECT 'rule_trace', count(*) FROM {env.sb}.rule_trace WHERE call_id IN ({ids}) UNION ALL "
                    f"SELECT 'run_errors', count(*) FROM {env.sb}.run_errors WHERE call_id IN ({ids})"))


if __name__ == "__main__":
    main()
