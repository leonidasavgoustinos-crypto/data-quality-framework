"""Generate the synthetic result history for the dashboard (sandbox only, deterministic).

  python3 -I scripts/gen_synthetic.py --target free [--seed 20261001] [--days 60] [--dry-run]

What it writes: ONLY {{dq_catalog}}.{{dashboard_schema}}.results_synthetic (full regenerate: the table is emptied,
then refilled). It never writes to results.*, configuration.*, metadata.* or quarantine.*.

How the rows are built:
  * Template = the real runs in v_results_ext: the same scopes (apply_at, project), tables, rule assignments and
    config columns (rule, dimension, criticality, policy, layer, ...), so every filter value exists in real config.
  * The history covers the --days days before the first real run_date, one call per scope and day.
  * Per assignment: a behaviour profile (stable, noisy, chronic, flapping, broken) derived from its real failure
    rate, plus a few scheduled events (incident streaks, a partial load, reruns, missed runs, a slow week).
  * Volumes, durations and failure ratios come from the real per-assignment medians, with small noise and a slow
    volume growth; failing rows are quarantined (rows_quarantined = rows_failed), as in the real runs.
  * random.Random(seed) and uuid5 ids: the same seed gives byte-identical rows.

Guards: sandbox_enabled and synthetic_enabled must be true; the host guard of env_config applies.
"""
import argparse
import base64
import datetime as dt
import hashlib
import json
import math
import os
import random
import statistics
import sys
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config  # noqa: E402

TABLE = "results_synthetic"
NS = uuid.UUID("6f1c2a52-3d4e-4b8a-9c0d-5e7f81a2b3c4")
MAX_PAYLOAD = 450_000                    # bytes of base64 per INSERT (the statement travels as a CLI argument)

# Scheduled start (UTC hour, minute) per scope; the real runs keep their own times.
SCHEDULE = {"bronze|": (1, 0), "bronze_to_fs|": (1, 30), "fs|": (2, 10), "gold|project_2": (2, 20),
            "bronze_to_gold|sas2py": (2, 30), "fs_to_gold|sas2py": (2, 45), "test|": (3, 0), "test_test|": (3, 10)}
# Events, as day offsets from the first synthetic day (0-based).
MISSED = {(21, "test_test|"), (35, "test_test|"), (42, "fs|")}         # no call that day
RERUNS = {(31, "bronze_to_gold|sas2py"), (48, "bronze|")}               # first call has a table fault, rerun +3h
FAULTS = {(25, "bronze|"), (45, "fs_to_gold|sas2py")}                   # one table fails before its rules run
PARTIAL_LOAD = (17, "bronze_to_fs|")                                     # one table loads ~50 % of its rows
SLOW = (range(49, 54), "bronze_to_fs|", 1.6)                             # duration regression


def scope_of(apply_at, project):
    return f"{apply_at}|{project or ''}"


def fetch(env, sql):
    return json.loads(env_config.sql(env, sql))


def num(v, cast=float):
    return None if v is None else cast(v)


def coerce(rows, types):
    """The Statement Execution API returns every value as a string: cast back by column type."""
    conv = {"bigint": int, "int": int, "double": float, "boolean": lambda v: v == "true"}
    for r in rows:
        for k, v in r.items():
            if v is not None and types.get(k) in conv:
                r[k] = conv[types[k]](v)
    return rows


def worst(statuses):
    """Table/execution status as the framework reports it: code_error > error > warning > pass."""
    for s in ("code_error", "error", "warning"):
        if s in statuses:
            return s
    return "pass"


class Profile:
    def __init__(self, kind, p_fail=0.0, stick=0.0):
        self.kind, self.p_fail, self.stick, self.failing = kind, p_fail, stick, False

    def next(self, rng, factor):
        if self.kind == "broken":
            return "code_error"
        if self.kind == "flapping":
            self.failing = rng.random() < 0.45
        elif self.failing:
            self.failing = rng.random() < self.stick
        else:
            self.failing = rng.random() < self.p_fail * factor
        return "fail" if self.failing else "pass"


def profiles_for(rng, assignments):
    """Behaviour per rule assignment, from its real failure rate (fails / real days)."""
    out, candidates = {}, []
    for aid, a in sorted(assignments.items()):
        f = a["fail_rate"]
        if a["code_error"]:
            out[aid] = Profile("broken")
        elif f >= 0.7:
            out[aid] = Profile("chronic", 0.30, 0.90)
        elif f >= 0.2:
            out[aid] = Profile("noisy", 0.12, 0.55)
            candidates.append(aid)
        elif f > 0:
            out[aid] = Profile("noisy", 0.05, 0.40)
            candidates.append(aid)
        else:
            out[aid] = Profile("stable", 0.0 if rng.random() < 0.65 else 0.012, 0.3)
    for aid in rng.sample(candidates, min(5, len(candidates))):
        out[aid] = Profile("flapping")
    return out


def load_template(env):
    sb = env.sb
    types = {c["column_name"]: c["full_data_type"] for c in fetch(env, (
        f"SELECT column_name, full_data_type FROM {env.dq_catalog}.information_schema.columns "
        f"WHERE table_schema = '{env.dashboard_schema}' AND table_name = 'v_results_ext'"))}
    rows = coerce(fetch(env, f"SELECT * FROM {sb}.v_results_ext ORDER BY run_date, start_timestamp, result_rule_id"), types)
    if not rows:
        raise env_config.GuardError("v_results_ext is empty: the template needs real runs")
    days = sorted({r["run_date"] for r in rows})
    rule_rows = [r for r in rows if r["row_level"] == "rule"]
    latest = {}                               # newest real rule row per assignment = config template
    for r in rule_rows:
        latest[int(r["rule_assignment_id"])] = r
    assignments = {}
    for aid, tmpl in latest.items():
        mine = [r for r in rule_rows if int(r["rule_assignment_id"]) == aid]
        fails = [r for r in mine if r["status"] in ("error", "warning")]
        ratios = [int(r["rows_failed"]) / int(r["rows_evaluated"]) for r in fails if num(r["rows_evaluated"])]
        assignments[aid] = {
            "tmpl": tmpl,
            "scope": scope_of(tmpl["apply_at"], tmpl["project"]),
            "fail_rate": len(fails) / len(days),
            "code_error": all(r["status"] == "code_error" for r in mine),
            "dur": statistics.median(float(r["duration_s"]) for r in mine),
            "rows": statistics.median(int(r["rows_evaluated"]) for r in mine if num(r["rows_evaluated"])) if any(num(r["rows_evaluated"]) for r in mine) else None,
            "ratio": statistics.median(ratios) if ratios else None,
            "error_message": next((r["error_message"] for r in mine if r["error_message"]), None),
        }
    tables = {}                               # (scope, table_id) -> overhead seconds, quarantine table name
    for r in rule_rows:
        key = (scope_of(r["apply_at"], r["project"]), int(r["table_id"]))
        t = tables.setdefault(key, {"over": [], "qt": None, "tmpl": r})
        if r["quarantine_table"]:
            t["qt"] = r["quarantine_table"]
    per_table_call = {}
    for r in rule_rows:
        k = (r["result_table_id"],)
        e = per_table_call.setdefault(k, [r, 0.0])
        e[1] += float(r["duration_s"])
    for (rt_id,), (r, rule_sum) in per_table_call.items():
        key = (scope_of(r["apply_at"], r["project"]), int(r["table_id"]))
        tables[key]["over"].append(max(1.0, float(r["table_duration_s"]) - rule_sum))
    for t in tables.values():
        t["over"] = statistics.median(t["over"])
    return days, assignments, tables


def rule_row(base, tmpl, a, status, rng, day_i, vol_factor, dur_factor, start):
    """One rule result. base = call/table ids and dates; tmpl = real row with the config columns."""
    r = {k: tmpl.get(k) for k in tmpl if k != "is_latest_for_day"}
    rows = None if a["rows"] is None or status == "code_error" else max(1, round(a["rows"] * vol_factor))
    failed = 0
    if status in ("error", "warning") and rows:
        ratio = a["ratio"] or math.exp(rng.gauss(math.log(0.012 if status == "error" else 0.006), 0.6))
        failed = max(1, min(rows, round(rows * ratio * math.exp(rng.gauss(0, 0.35)))))
    dur = round(a["dur"] * dur_factor * math.exp(rng.gauss(0, 0.15)), 3)
    r.update(base)
    r.update({
        "row_level": "rule", "status": status,
        "result_rule_id": str(uuid.uuid5(NS, f"{base['result_table_id']}|{tmpl['rule_assignment_id']}")),
        "start_timestamp": start, "end_timestamp": start + dt.timedelta(seconds=dur), "duration_s": dur,
        "rows_evaluated": rows, "rows_failed": None if rows is None else failed,
        "rows_pass": None if rows is None else rows - failed,
        "rows_quarantined": failed if failed else None,
        "quarantine_table": base["_qt"] if failed else None,
        "error_message": a["error_message"] if status == "code_error" else None,
        "error_summary": (a["error_message"].split("JVM stacktrace:", 1)[0].strip() or None)
                         if status == "code_error" and a["error_message"] else None,
        "is_pass": status == "pass", "is_scored": status in ("pass", "warning", "error"),
    })
    return r


def build(days_real, assignments, tables, seed, ndays):
    rng = random.Random(seed)
    first_real = dt.date.fromisoformat(min(days_real))
    days = [first_real - dt.timedelta(days=ndays - i) for i in range(ndays)]
    profiles = profiles_for(rng, assignments)
    by_scope = {}
    for aid, a in sorted(assignments.items()):
        by_scope.setdefault(a["scope"], {}).setdefault(int(a["tmpl"]["table_id"]), []).append(aid)

    # incident streaks: (start day, length, scope, table): every rule of that table fails for the window
    incidents, multi = [], [(s, t) for s, ts in sorted(by_scope.items()) for t, aids in sorted(ts.items())
                            if len(aids) >= 3 and not any(assignments[x]["code_error"] for x in aids)]
    for start in (4, 12, 23, 33, 40, 52):
        s, t = rng.choice(multi)
        incidents.append((start, rng.randint(2, 7), s, t))

    out = []
    for i, day in enumerate(days):
        factor = 1.5 - 0.5 * i / (ndays - 1)          # teams fix issues over time: fewer new failures
        growth = 0.88 + 0.12 * i / (ndays - 1)
        statuses = {aid: profiles[aid].next(rng, factor) for aid in sorted(assignments)}
        for scope in sorted(by_scope):
            if (i, scope) in MISSED:
                continue
            calls = [("first", 0), ("rerun", 3)] if (i, scope) in RERUNS else [("only", 0)]
            for label, plus_h in calls:
                h, m = SCHEDULE.get(scope, (4, 0))
                t0 = dt.datetime.combine(day, dt.time(h, m), dt.timezone.utc) + dt.timedelta(
                    hours=plus_h, seconds=rng.randint(-240, 240))
                result_id = str(uuid.uuid5(NS, f"{seed}|{day}|{scope}|{label}"))
                fault_table = None
                if (i, scope) in FAULTS or label == "first":
                    fault_table = rng.choice(sorted(by_scope[scope]))
                call_rows, cursor, table_summ = [], t0 + dt.timedelta(seconds=3), []
                for table_id in sorted(by_scope[scope]):
                    tinfo = tables[(scope, table_id)]
                    rtid = str(uuid.uuid5(NS, f"{result_id}|{table_id}"))
                    base = {"result_id": result_id, "result_table_id": rtid, "run_date": day,
                            "is_simulated_date": True, "partition_value": day.isoformat(),
                            "_qt": tinfo["qt"] or tinfo["tmpl"]["quarantine"]}   # metadata.table.quarantine
                    t_start = cursor
                    if table_id == fault_table:           # table-level row, no rule rows (as in the real fault)
                        dur_t = round(tinfo["over"] * 0.4 * math.exp(rng.gauss(0, 0.1)), 3)
                        tm = tinfo["tmpl"]
                        r = {k: None for k in tm if k != "is_latest_for_day"}
                        for k in ("table_id", "project_id", "project", "data_layer", "catalog", "apply_at", "schema",
                                  "table_name", "table_nk", "quarantine", "filter_field", "filter_field_type",
                                  "project_description"):
                            r[k] = tm[k]
                        r.update(base)
                        r.update({"row_level": "table", "result_rule_id": None, "status": "code_error",
                                  "start_timestamp": t_start, "end_timestamp": t_start + dt.timedelta(seconds=dur_t),
                                  "duration_s": dur_t, "is_pass": False, "is_scored": False})
                        trows = [r]
                        cursor = t_start + dt.timedelta(seconds=dur_t + 2)
                    else:
                        vol = growth * math.exp(rng.gauss(0, 0.02))
                        if (i, scope) == PARTIAL_LOAD and table_id == sorted(by_scope[scope])[0]:
                            vol *= 0.5
                        dur_f = SLOW[2] if (i in SLOW[0] and scope == SLOW[1]) else 1.0
                        rcursor, trows = t_start + dt.timedelta(seconds=tinfo["over"] * 0.6), []
                        for aid in by_scope[scope][table_id]:
                            a = assignments[aid]
                            st = statuses[aid]
                            if any(s0 <= i < s0 + ln and s == scope and t == table_id for s0, ln, s, t in incidents):
                                st = "fail"
                            if (i, scope) == PARTIAL_LOAD and table_id == sorted(by_scope[scope])[0] \
                                    and a["tmpl"]["rule_dimension"] == "completeness":
                                st = "fail"
                            if label == "rerun" and st == "fail" and rng.random() < 0.5:
                                st = "pass"                    # some issues were fixed before the rerun
                            if st == "fail":
                                st = "warning" if a["tmpl"]["policy_category"] == "warning" else "error"
                            r = rule_row(base, a["tmpl"], a, st, rng, i, vol, dur_f, rcursor)
                            rcursor = r["end_timestamp"] + dt.timedelta(seconds=0.4)
                            trows.append(r)
                        cursor = rcursor + dt.timedelta(seconds=tinfo["over"] * 0.4)
                    t_end = cursor
                    rs = [r["status"] for r in trows if r["row_level"] == "rule"]
                    tstat = "code_error" if trows[0]["row_level"] == "table" else (
                        "code_error" if rs and all(s == "code_error" for s in rs) else worst([s for s in rs if s != "code_error"] or ["pass"]))
                    counts = None if trows[0]["row_level"] == "table" else {
                        "rules_evaluated": sum(s != "code_error" for s in rs), "rules_pass": rs.count("pass"),
                        "rules_warning": rs.count("warning"), "rules_error": rs.count("error")}
                    for r in trows:
                        r.update({"table_status": tstat, "start_timestamp_table": t_start, "end_timestamp_table": t_end,
                                  "table_duration_s": round((t_end - t_start).total_seconds(), 3)})
                        r.update(counts or {"rules_evaluated": None, "rules_pass": None, "rules_warning": None,
                                            "rules_error": None})
                    table_summ.append(tstat)
                    call_rows += trows
                    cursor += dt.timedelta(seconds=2)
                t_end = cursor + dt.timedelta(seconds=2)
                ex = {"execution_status": worst(table_summ), "tables_evaluated": len(table_summ),
                      "tables_pass": table_summ.count("pass"), "tables_warning": table_summ.count("warning"),
                      "tables_error": table_summ.count("error"), "start_timestamp_execution": t0,
                      "end_timestamp_execution": t_end, "execution_duration_s": round((t_end - t0).total_seconds(), 3),
                      "run_ts": t0}
                for r in call_rows:
                    r.update(ex)
                    r["is_synthetic"] = True
                    r.pop("_qt", None)
                out += call_rows
    return out


def to_json(v):
    if isinstance(v, dt.datetime):
        return v.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(v, dt.date):
        return v.isoformat()
    return v


SPARK_TYPES = {"string": "STRING", "date": "DATE", "timestamp": "TIMESTAMP", "boolean": "BOOLEAN",
               "bigint": "BIGINT", "int": "INT", "double": "DOUBLE"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--seed", type=int, default=20261001)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    env = env_config.connect_local(a.target)
    env.require_sandbox("generate synthetic history")
    if not env.synthetic_enabled:
        raise env_config.GuardError(f"REFUSED on target '{env.target}': synthetic_enabled is false")
    fq = f"{env.sb}.{TABLE}"
    cols = fetch(env, f"SELECT column_name, full_data_type FROM {env.dq_catalog}.information_schema.columns "
                      f"WHERE table_schema = '{env.dashboard_schema}' AND table_name = '{TABLE}' ORDER BY ordinal_position")
    if not cols:
        raise env_config.GuardError(f"{fq} does not exist: run scripts/run_ddl.py ddl/05_synthetic_tables.sql first")

    days, assignments, tables = load_template(env)
    rows = build(days, assignments, tables, a.seed, a.days)
    names = [c["column_name"] for c in cols]
    missing = sorted(set(names) - set(rows[0]))
    if missing:
        raise env_config.GuardError(f"generator does not fill columns {missing}")
    casts = {c["column_name"]: SPARK_TYPES[c["full_data_type"]] for c in cols}
    calls = len({r["result_id"] for r in rows})
    items = [json.dumps({n: to_json(r.get(n)) for n in names}) for r in rows]
    digest = hashlib.sha256("\n".join(items).encode()).hexdigest()[:16]
    print(f"{fq}: {len(rows)} rows, {calls} calls, {len(assignments)} assignments, seed {a.seed}, "
          f"{min(r['run_date'] for r in rows)}..{max(r['run_date'] for r in rows)}, content sha256 {digest}")
    if a.dry_run:
        return
    schema = ", ".join(f"`{n}`: {casts[n]}" for n in names)
    batches, cur, size = [], [], 0
    for item in items:
        if cur and (size + len(item)) * 4 // 3 > MAX_PAYLOAD:
            batches.append(cur)
            cur, size = [], 0
        cur.append(item)
        size += len(item) + 1
    batches.append(cur)
    env_config.sql(env, f"DELETE FROM {fq}")
    for i, batch in enumerate(batches, 1):
        b64 = base64.b64encode(("[" + ",".join(batch) + "]").encode()).decode()
        env_config.sql(env, f"INSERT INTO {fq} SELECT * FROM (SELECT inline(from_json(CAST(unbase64('{b64}') AS STRING), "
                            f"'ARRAY<STRUCT<{schema}>>')))")
        print(f"  batch {i}/{len(batches)}: {len(batch)} rows", flush=True)
    n = fetch(env, f"SELECT count(*) AS n FROM {fq}")[0]["n"]
    print(f"{fq}: {n} rows after insert")
    if int(n) != len(rows):
        raise env_config.GuardError(f"row count mismatch: generated {len(rows)}, table has {n}")


if __name__ == "__main__":
    main()
