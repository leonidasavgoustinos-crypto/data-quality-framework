"""Run a DDL template statement by statement on the target's warehouse.

  python3 -I scripts/run_ddl.py --target free ddl/02_capture.sql [--dry-run]

Guards (abort before the first statement):
  * the CLI profile must point at the target's expected_host;
  * a file marked `-- @sandbox_only` requires sandbox_enabled=true;
  * a file whose `-- @targets:` line does not list the target is refused;
  * only CREATE <CATALOG|SCHEMA|TABLE|VOLUME|VIEW> ... IF NOT EXISTS is executed. Anything else is refused,
    so this runner can never REPLACE, DROP or ALTER an existing object (an existing view is left as it is).
Placeholders ({{dq_catalog}}, ...) are rendered by env_config; unresolved ones abort.

  python3 -I scripts/run_ddl.py --target free ddl/06_dashboard_views.sql --replace-views [--yes]

--replace-views (explicit, separate mode) rolls out changed view definitions. Every statement of the file must be
CREATE VIEW IF NOT EXISTS <dq_catalog>.<dashboard_schema>.<name> COMMENT '<MARKER>' AS ...; it is then sent as
CREATE OR REPLACE VIEW. Refused, before anything is executed, when:
  * any statement of the file is not such a view statement (tables, DROP, ALTER, INSERT, ...);
  * a view lies outside the dashboard schema, or the dashboard schema is one of the framework schemas;
  * the existing object is not a VIEW, or does not carry the MARKER comment (new views are created).
The list of views is printed first; nothing is executed without --yes (or an interactive "yes").
"""
import argparse
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config  # noqa: E402

ALLOWED = re.compile(r"(?is)^create\s+(catalog|schema|table|volume|view)\s+if\s+not\s+exists\s")

MARKER = "dq_dashboard sandbox - generated"
FRAMEWORK_SCHEMAS = {"results", "configuration", "metadata", "quarantine", "reporting", "information_schema"}
VIEW_STMT = re.compile(r"(?is)^create\s+view\s+if\s+not\s+exists\s+`?(\w+)`?\.`?(\w+)`?\.`?(\w+)`?\s+"
                       r"comment\s+'([^']*)'\s+as\s")


def replace_plan(env, stmts):
    """Validate a --replace-views file; return [(view, action, statement to send)] or raise GuardError."""
    if env.dashboard_schema.lower() in FRAMEWORK_SCHEMAS:
        raise env_config.GuardError(f"REFUSED: dashboard schema '{env.dashboard_schema}' is a framework schema")
    parsed, bad = [], []
    for s in stmts:
        m = VIEW_STMT.match(s)
        if not m:
            bad.append(f"not a CREATE VIEW IF NOT EXISTS statement: {s[:80]}")
        elif (m.group(1).lower(), m.group(2).lower()) != (env.dq_catalog.lower(), env.dashboard_schema.lower()):
            bad.append(f"{m.group(1)}.{m.group(2)}.{m.group(3)} is outside {env.sb}")
        elif m.group(4) != MARKER:
            bad.append(f"{m.group(3)} does not carry the marker comment")
        else:
            parsed.append((m.group(3).lower(), s[m.end(0):]))
    if bad:
        raise env_config.GuardError(f"REFUSED --replace-views: {bad}")
    names = ", ".join(f"'{n}'" for n, _ in parsed)
    existing = {r["table_name"].lower(): r for r in json.loads(env_config.sql(env, (
        f"SELECT table_name, table_type, comment FROM {env.dq_catalog}.information_schema.tables "
        f"WHERE lower(table_schema) = '{env.dashboard_schema.lower()}' AND lower(table_name) IN ({names})")))}
    plan = []
    for name, body in parsed:
        cur = existing.get(name)
        if cur and cur["table_type"] != "VIEW":
            bad.append(f"{name} exists as {cur['table_type']}, not a VIEW")
        elif cur and cur["comment"] != MARKER:
            bad.append(f"{name} exists without the marker comment")
        else:
            plan.append((name, "replace" if cur else "create",
                         f"CREATE OR REPLACE VIEW {env.sb}.{name}\nCOMMENT '{MARKER}'\nAS {body}"))
    if bad:
        raise env_config.GuardError(f"REFUSED --replace-views: {bad}")
    return plan


def run_replace(env, stmts, a):
    plan = replace_plan(env, stmts)
    for name, action, _ in plan:
        print(f"  {action:7} view {env.sb}.{name}")
    if a.dry_run:
        print(f"{a.file} on {env.target}: {len(plan)} views, 0 executed (dry run)")
        return
    if not a.yes:
        answer = input("type 'yes' to run these statements: ") if sys.stdin.isatty() else ""
        if answer.strip() != "yes":
            print(f"{a.file} on {env.target}: {len(plan)} views, 0 executed (not confirmed; pass --yes)")
            return
    for _, _, s in plan:
        env_config.sql(env, s)
    print(f"{a.file} on {env.target}: {len(plan)} views, {len(plan)} executed (replace mode)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("file")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--replace-views", action="store_true", help="replace marked views of the dashboard schema")
    ap.add_argument("--yes", action="store_true", help="confirm --replace-views without a prompt")
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
    if a.replace_views:
        return run_replace(env, stmts, a)
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
