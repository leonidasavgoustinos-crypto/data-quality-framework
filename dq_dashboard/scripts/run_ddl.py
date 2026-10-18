"""Run a DDL template statement by statement on the target's warehouse.

  python3 -I scripts/run_ddl.py --target free ddl/02_capture.sql [--dry-run]

Guards (abort before the first statement):
  * the CLI profile must point at the target's expected_host;
  * a file marked `-- @sandbox_only` requires sandbox_enabled=true;
  * a file whose `-- @targets:` line does not list the target is refused;
  * only CREATE <CATALOG|SCHEMA|TABLE|VOLUME|VIEW> ... IF NOT EXISTS is executed. Anything else is refused,
    so this runner can never REPLACE, DROP or ALTER an existing object (an existing view is left as it is).
Placeholders ({{dq_catalog}}, ...) are rendered by env_config; unresolved ones abort.
"""
import argparse
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config  # noqa: E402

ALLOWED = re.compile(r"(?is)^create\s+(catalog|schema|table|volume|view)\s+if\s+not\s+exists\s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("file")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    env = env_config.connect_local(a.target)
    text = open(a.file).read()

    m = re.search(r"--\s*@targets:\s*([^\n]+)", text)
    if not m or env.target not in [t.strip() for t in m.group(1).split(",")]:
        raise env_config.GuardError(f"{a.file} is not allowed on target '{env.target}' (@targets: {m.group(1) if m else 'missing'})")
    if "@sandbox_only" in text:
        env.require_sandbox(os.path.basename(a.file))

    sql = re.sub(r"--[^\n]*", "", env.render(text))
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    bad = [s[:80] for s in stmts if not ALLOWED.match(s)]
    if bad:
        raise env_config.GuardError(f"REFUSED non-additive statements: {bad}")
    ok = 0
    for s in stmts:
        if a.dry_run:
            print(s.splitlines()[0][:120])
            continue
        env_config.sql(env, s)
        ok += 1
    print(f"{a.file} on {env.target}: {len(stmts)} statements, {ok} executed{' (dry run)' if a.dry_run else ''}")


if __name__ == "__main__":
    main()
