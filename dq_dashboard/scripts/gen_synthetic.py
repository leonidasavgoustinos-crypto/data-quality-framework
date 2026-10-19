"""Generate the synthetic result history for the dashboard (sandbox only, deterministic).

  python3 -I scripts/gen_synthetic.py --target free [--seed 20261001] [--days 60] [--dry-run]

What it writes: ONLY these tables of {{dq_catalog}}.{{dashboard_schema}} (full regenerate: each table is emptied,
then refilled): results_synthetic, rule_assignment_synthetic, quarantine_lifecycle_synthetic,
incident_triage_synthetic, alert_log_synthetic, config_changes_synthetic, recon_mapping_synthetic,
recon_results_synthetic, schema_changes_synthetic. It never writes to results.*, configuration.*, metadata.* or
quarantine.*.

How the rows are built:
  * Template = the real runs in v_results_ext: the same scopes (apply_at, project), tables, rule assignments and
    config columns (rule, dimension, criticality, policy, layer, ...), so every filter value exists in real config.
  * The history covers the --days days before the first real run_date, one call per scope and day.
  * Per assignment: a behaviour profile (stable, noisy, chronic, flapping, broken) derived from its real failure
    rate, plus a few scheduled events (incident streaks, a partial load, reruns, missed runs, a slow week).
  * Volumes, durations and failure ratios come from the real per-assignment medians, with small noise and a slow
    volume growth; failing rows are quarantined (rows_quarantined = rows_failed), as in the real runs.
  * Demo rules for the dimensions without real rules (completeness, consistency, timeliness): assignments in
    rule_assignment_synthetic and their results on the synthetic days and on the real run days (added to the real
    calls, is_synthetic = TRUE). Own random stream, so the history above does not change.
  * After the results are written: the quarantine lifecycle (entry / exit time and outcome per quarantined rule
    result), the triage of the derived incidents (owning team, acknowledgement) and the alerts with their outcome,
    read back from the dashboard views (v_results_all, v_incidents) so that they match what the dashboard derives.
  * Also: configuration change history of the demo rules (3 of them disabled, so their results stop), a
    reconciliation of 8 migrated tables (checks of the reconciliation module, converging over the runs), and schema
    change events consistent with today's schemas (columns_snapshot).
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
TABLES = ("results_synthetic", "rule_assignment_synthetic", "quarantine_lifecycle_synthetic", "config_changes_synthetic",
          "recon_mapping_synthetic", "recon_results_synthetic", "schema_changes_synthetic",
          "incident_triage_synthetic", "alert_log_synthetic")
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
    return out, incidents, days


# Demo rules for the dimensions without real rules: dimension -> template, policy and behaviour.
# share = part of the (scope, table) pairs that get the rule; table checks have no failed rows (nothing quarantined).
DEMO_RULES = {
    "completeness": {"template_id": 9101, "rule_name": "row_count_check", "dimension_id": 1, "policy_id": 9201,
                     "criticality": "high", "category": "error", "scope": "table", "quarantine": False,
                     "params": lambda t: (None, '{"min_ratio_vs_previous": 0.9}'), "share": 0.75,
                     "profile": ("stable", 0.02, 0.4), "table_check": True,
                     "description": "Rows loaded are at least 90 % of the previous load"},
    "consistency": {"template_id": 9102, "rule_name": "referential_integrity", "dimension_id": 3, "policy_id": 9202,
                    "criticality": "medium", "category": "warning", "scope": "column", "quarantine": True,
                    "params": lambda t: (json.dumps({"column": t["pk"][0]}) if t["pk"] else None, '{"max_orphans": 5}'),
                    "share": 0.45, "profile": ("noisy", 0.08, 0.5), "table_check": False,
                    "description": "Every key exists in the reference table"},
    "timeliness": {"template_id": 9103, "rule_name": "freshness_check", "dimension_id": 9105, "policy_id": 9203,
                   "criticality": "medium", "category": "error", "scope": "table", "quarantine": False,
                   "params": lambda t: (json.dumps({"column": t["filter_field"]}) if t["filter_field"] else None,
                                        '{"max_age_hours": 30}'), "share": 0.55,
                   "profile": ("noisy", 0.04, 0.6), "table_check": True,
                   "description": "The newest partition is at most 30 hours old"},
}
DEMO_ID0 = 900000                         # demo assignment ids start here (real ids are far below)
DISABLE_DAY = 45                          # day index on which 3 demo rules are disabled (no results afterwards)
CRIT_WEIGHT = {"high": 3, "medium": 2, "low": 1}


def demo_assignments(rng, assignments, tables):
    """One demo assignment per (dimension, table) for a share of the tables, on the table's first scope."""
    first_scope = {}
    for scope, table_id in sorted(tables):
        first_scope.setdefault(table_id, scope)
    pk = {}
    for a in assignments.values():
        t = a["tmpl"]
        if t["rule_dimension"] == "uniqueness" and t.get("parameters_identifiers"):
            cols = json.loads(t["parameters_identifiers"]).get("columns") or []
            pk.setdefault(int(t["table_id"]), cols)
    out, n = {}, 0
    for dim, d in DEMO_RULES.items():
        for table_id, scope in sorted(first_scope.items()):
            if rng.random() >= d["share"]:
                continue
            n += 1
            aid = DEMO_ID0 + n
            tm = tables[(scope, table_id)]["tmpl"]
            info = {"pk": pk.get(table_id, []), "filter_field": tm["filter_field"]}
            ident, values = d["params"](info)
            out[aid] = {"dim": dim, "d": d, "scope": scope, "table_id": table_id, "tmpl": tm,
                        "ident": ident, "values": values, "profile": Profile(*d["profile"])}
    return out


def assignment_rows(demo, first_day):
    rows = []
    for aid, a in sorted(demo.items()):
        t, d = a["tmpl"], a["d"]
        off = a.get("disabled_day")
        valid_to = dt.datetime.combine(first_day + dt.timedelta(days=off), dt.time(9, 0), dt.timezone.utc) if off else None
        rows.append({
            "rule_assignment_id": aid, "table_id": a["table_id"], "project_id": t["project_id"],
            "rule_template_id": d["template_id"], "policy_id": d["policy_id"], "apply_at_id": None,
            "project": t["project"], "data_layer": t["data_layer"], "catalog": t["catalog"],
            "criticality": d["criticality"], "rule_dimension": a["dim"], "schema": t["schema"],
            "table_name": t["table_name"], "table_nk": t["table_nk"], "apply_at": t["apply_at"],
            "rule_name": d["rule_name"], "rule_type": "technical", "engine_type": "sql", "rule_scope": d["scope"],
            "policy_category": d["category"], "quarantine_status": d["quarantine"],
            "parameters_identifiers": a["ident"], "parameters_values": a["values"],
            "assignment_is_active": off is None, "template_is_active": True, "policy_is_active": True,
            "table_is_active": True, "apply_at_is_active": True, "is_effective_active": off is None,
            "valid_from": dt.datetime.combine(first_day, dt.time(0, 0), dt.timezone.utc), "valid_to": valid_to,
            "added_by": "demo data", "is_synthetic": True})
    return rows


def demo_rule_row(ctx, a, status, rng):
    """A demo rule result inside an existing table call (ctx = one rule row of that call)."""
    d = a["d"]
    r = {k: ctx.get(k) for k in ctx if k != "is_latest_for_day"}
    rows = max(int(x) for x in [ctx.get("rows_evaluated") or 0]) or None
    failed = 0
    if status in ("error", "warning") and rows and not d["table_check"]:
        failed = max(1, round(rows * math.exp(rng.gauss(math.log(0.004), 0.5))))
    dur = round(0.6 + rng.random() * 1.8, 3)
    end_t = ctx["end_timestamp_table"] or ctx["end_timestamp"]
    start = end_t - dt.timedelta(seconds=dur + 0.3)
    qt = ctx.get("quarantine_table") or ctx.get("quarantine")
    r.update({
        "row_level": "rule", "status": status, "is_synthetic": True,
        "result_rule_id": str(uuid.uuid5(NS, f"{ctx['result_table_id']}|{a['_id']}")),
        "rule_assignment_id": a["_id"], "policy_id": d["policy_id"], "rule_template_id": d["template_id"],
        "rule_type_id": None, "rule_dimension_id": d["dimension_id"], "criticality": d["criticality"],
        "rule_dimension": a["dim"], "rule_name": d["rule_name"], "rule_description": d["description"],
        "rule_type": "technical", "rule_scope": d["scope"], "is_reusable": True, "engine_type": "sql",
        "statement": None, "parameters_identifiers": a["ident"], "parameters_values": a["values"],
        "policy_category": d["category"], "quarantine_status": d["quarantine"], "is_project_specific": False,
        "start_timestamp": start, "end_timestamp": start + dt.timedelta(seconds=dur), "duration_s": dur,
        "rows_evaluated": rows, "rows_failed": None if rows is None else failed,
        "rows_pass": None if rows is None else rows - failed,
        "rows_quarantined": failed if failed and d["quarantine"] else None,
        "quarantine_table": qt if failed and d["quarantine"] else None,
        "error_message": None, "error_summary": None, "crit_weight": CRIT_WEIGHT[d["criticality"]],
        "is_pass": status == "pass", "is_scored": True,
    })
    return r


def build_demo_rules(seed, assignments, tables, synth_rows, real_rows, incidents, ndays):
    """Demo assignments and their results in every table call of the synthetic history and of the real runs."""
    rng = random.Random(seed + 7)
    demo = demo_assignments(rng, assignments, tables)
    for aid, a in demo.items():
        a["_id"] = aid
    for aid in random.Random(seed + 17).sample(sorted(demo), 3):
        demo[aid]["disabled_day"] = DISABLE_DAY
    by_table = {}
    for aid, a in sorted(demo.items()):
        by_table.setdefault((a["scope"], a["table_id"]), []).append(aid)
    # table calls: (day index, scope, table_id, call label, context row)
    calls = {}
    first = min(r["run_date"] for r in synth_rows)
    for r in synth_rows:
        if r["row_level"] == "rule":
            calls.setdefault(r["result_table_id"], ((r["run_date"] - first).days, r))
    real_days = sorted({r["run_date"] for r in real_rows})
    for r in real_rows:
        if r["row_level"] == "rule":
            day = dt.date.fromisoformat(r["run_date"]) if isinstance(r["run_date"], str) else r["run_date"]
            calls.setdefault(r["result_table_id"], (ndays + real_days.index(r["run_date"]), dict(r)))
    for _, ctx in calls.values():         # real rows come as strings from the API
        for k in ("end_timestamp_table", "end_timestamp", "start_timestamp_execution", "run_ts"):
            if isinstance(ctx.get(k), str):
                ctx[k] = dt.datetime.fromisoformat(ctx[k].replace("Z", "+00:00"))
        if isinstance(ctx.get("run_date"), str):
            ctx["run_date"] = dt.date.fromisoformat(ctx["run_date"])
    status_of_day = {}
    out = []
    for rtid, (i, ctx) in sorted(calls.items(), key=lambda kv: (kv[1][0], str(kv[1][1]["run_ts"]), kv[0])):
        scope = scope_of(ctx["apply_at"], ctx["project"])
        for aid in by_table.get((scope, int(ctx["table_id"])), []):
            a = demo[aid]
            if i >= a.get("disabled_day", 10 ** 6):
                continue
            if (aid, i) not in status_of_day:
                st = a["profile"].next(rng, 1.0)
                if any(s0 <= i < s0 + ln and s == scope and t == a["table_id"] for s0, ln, s, t in incidents):
                    st = "fail"
                if (i, scope) == PARTIAL_LOAD and a["dim"] == "completeness" and \
                        a["table_id"] == sorted(t for (sc, t) in tables if sc == scope)[0]:
                    st = "fail"
                status_of_day[(aid, i)] = st
            st = status_of_day[(aid, i)]
            if st == "fail":
                st = "warning" if a["d"]["category"] == "warning" else "error"
            out.append(demo_rule_row(ctx, a, st, rng))
    return demo, out


TEAMS = {"risk": "risk data team", "pd": "pd models team", "retail": "retail models team",
         "masterscale": "rating models team", "corporate": "rating models team", "master": "rating models team",
         "ifrs9": "ifrs9 team", "sas": "platform team"}


def team_of(schema):
    s = (schema or "").lower()
    for k, v in TEAMS.items():
        if s.startswith(k):
            return v
    return "data platform team"


def build_lifecycle(seed, batches, as_of):
    """Exit time and outcome per quarantined batch: most rows are reprocessed by the next run or released within
    days, some are discarded, a few stay in quarantine."""
    rng = random.Random(seed + 11)
    out = []
    for b in batches:
        u = rng.random()
        if u < 0.50:
            outcome, delay = "reprocessed", dt.timedelta(hours=rng.uniform(20, 30))
        elif u < 0.75:
            outcome, delay = "released", dt.timedelta(days=rng.uniform(1, 6))
        elif u < 0.85:
            outcome, delay = "released", dt.timedelta(days=rng.uniform(7, 25))
        elif u < 0.91:
            outcome, delay = "discarded", dt.timedelta(days=rng.uniform(2, 10))
        else:
            outcome, delay = None, None
        entered = b["entered_ts"]
        left = entered + delay if delay else None
        if left and left > as_of:
            outcome, left = None, None
        out.append({"batch_id": str(uuid.uuid5(NS, f"q|{b['result_rule_id']}")), "result_rule_id": b["result_rule_id"],
                    "rule_assignment_id": b["rule_assignment_id"], "table_id": b["table_id"],
                    "quarantine_table": b["quarantine_table"], "rows_quarantined": b["rows_quarantined"],
                    "entered_ts": entered, "left_ts": left, "outcome": outcome, "is_synthetic": True})
    return out


def build_triage_alerts(seed, incidents, as_of):
    """Owner and acknowledgement per incident; one alert per incident (plus a reminder for long high-criticality
    incidents) with its outcome: noise for one-day incidents that pass again (often), accepted risk for long ones,
    fixed otherwise; open incidents younger than a week are not assessed yet."""
    rng = random.Random(seed + 13)
    triage, alerts = [], []
    for inc in incidents:
        det, resolved = inc["detected_ts"], inc["resolved_ts"]
        ack = det + dt.timedelta(hours=rng.uniform(1, 20)) if rng.random() < (0.85 if resolved else 0.6) else None
        if ack and (ack > as_of or (resolved and ack > resolved)):
            ack = None
        triage.append({"incident_key": inc["incident_key"], "owner": team_of(inc["schema"]), "acknowledged_ts": ack,
                       "is_synthetic": True})
        runs = inc["failing_runs"]
        if resolved:
            if runs == 1 and rng.random() < 0.45:
                outcome = "noise"
            elif runs >= 6 and rng.random() < 0.35:
                outcome = "accepted risk"
            else:
                outcome = "fixed"
        else:
            outcome = ("accepted risk" if rng.random() < 0.6 else None) if (as_of - det).days >= 7 else None
        kinds = [("new incident", det + dt.timedelta(minutes=rng.uniform(1, 8)))]
        if inc["criticality"] == "high" and runs >= 3:
            kinds.append(("reminder", det + dt.timedelta(days=2, minutes=rng.uniform(1, 8))))
        for kind, ts in kinds:
            if ts > as_of:
                continue
            alerts.append({"alert_id": str(uuid.uuid5(NS, f"a|{inc['incident_key']}|{kind}")),
                           "incident_key": inc["incident_key"], "rule_assignment_id": inc["rule_assignment_id"],
                           "sent_ts": ts, "channel": "email" if inc["criticality"] == "high" else "chat",
                           "alert_kind": kind, "outcome": outcome, "is_synthetic": True})
    return triage, alerts


def build_config_changes(seed, demo, first_day, incidents, as_of, columns):
    """Configuration change history of the demo rules, consistent with their results: added on the first day,
    3 disabled on DISABLE_DAY (no results afterwards), thresholds loosened a few hours before the passing run that
    ended a failing streak of 2+ days (old value = tighter, new value = current), referential-integrity column
    changed (old = another column, new = current)."""
    rng = random.Random(seed + 19)
    out = []
    def add(ts, by, kind, aid, field=None, old=None, new=None):
        out.append({"change_id": str(uuid.uuid5(NS, f"c|{aid}|{kind}|{ts.isoformat()}")), "change_ts": ts,
                    "changed_by": by, "change_type": kind, "rule_assignment_id": aid,
                    "table_id": demo[aid]["table_id"], "field_name": field, "old_value": old, "new_value": new,
                    "is_synthetic": True})
    for aid, a in sorted(demo.items()):
        by = team_of(a["tmpl"]["schema"])
        add(dt.datetime.combine(first_day, dt.time(8, rng.randint(0, 59)), dt.timezone.utc), by, "rule added", aid,
            "rule_template", None, a["d"]["rule_name"])
        if a.get("disabled_day"):
            add(dt.datetime.combine(first_day + dt.timedelta(days=a["disabled_day"]), dt.time(9, 0), dt.timezone.utc),
                by, "rule disabled", aid, "is_active", "true", "false")
        t = a["tmpl"]
        others = [c for c in columns.get((t["catalog"], t["schema"], t["table_name"]), {})
                  if a["ident"] and json.dumps({"column": c}) != a["ident"]]
        if a["dim"] == "consistency" and others and rng.random() < 0.3:
            ts = dt.datetime.combine(first_day + dt.timedelta(days=rng.randint(5, 40)), dt.time(10, 15), dt.timezone.utc)
            add(ts, by, "rule changed", aid, "parameters_identifiers", json.dumps({"column": rng.choice(others)}),
                a["ident"])
    tighter = {"completeness": '{"min_ratio_vs_previous": 0.95}', "consistency": '{"max_orphans": 0}',
               "timeliness": '{"max_age_hours": 26}'}           # previous (tighter) value; the current one is new
    changed = set()
    for inc in sorted(incidents, key=lambda x: (x["resolved_ts"] or as_of, x["incident_key"]), reverse=True):
        a = demo.get(inc["rule_assignment_id"])
        if not a or a["_id"] in changed or not inc["resolved_ts"] or inc["failing_runs"] < 2 or rng.random() > 0.6:
            continue
        changed.add(a["_id"])
        add(inc["resolved_ts"] - dt.timedelta(hours=rng.uniform(2, 6)), team_of(a["tmpl"]["schema"]),
            "threshold changed", inc["rule_assignment_id"], "parameters_values", tighter[a["dim"]], a["values"])
    return [r for r in out if r["change_ts"] <= as_of]


SPARK_TYPE = {"int": "IntegerType()", "bigint": "LongType()", "string": "StringType()", "date": "DateType()",
              "double": "DoubleType()", "boolean": "BooleanType()", "timestamp": "TimestampType()"}
LEGACY_TYPE = {"IntegerType()": "StringType()", "DateType()": "StringType()", "DoubleType()": "DecimalType(18,2)",
               "StringType()": "VarcharType(50)", "LongType()": "IntegerType()", "BooleanType()": "StringType()"}


def build_recon(seed, columns, volumes, first_day, ndays_total, as_of):
    """A migration of 8 tables (legacy copy -> registered table) reconciled every 3 days with the checks of the
    reconciliation module; mismatches converge: type and name mismatches are fixed at a run, missing rows decay.
    Final states: most mappings match, some partial, one or two still mismatch."""
    rng = random.Random(seed + 23)
    cands = sorted(k for k in columns if len(columns[k]) >= 3 and volumes.get(k))
    picked = rng.sample(cands, min(8, len(cands)))
    mapping, results = [], []
    finals = ["match", "match", "match", "match", "partial", "partial", "mismatch", "mismatch"]
    for n, key in enumerate(picked):
        cat, sch, tbl = key
        name, old_t, new_t = f"{sch}.{tbl}", f"legacy_sas.{sch}_{tbl}", f"{cat}.{sch}.{tbl}"
        mapping.append({"table_name": name, "old": old_t, "new": new_t, "is_synthetic": True})
        cols = columns[key]
        final = finals[n % len(finals)]
        names = list(cols)
        bad_types = rng.sample(names, min(len(names), rng.randint(1, 3)))
        bad_names = rng.sample([c for c in names if c not in bad_types], min(2, len(names) - len(bad_types)))
        fix_types = rng.randint(4, 12) if final != "mismatch" else 10 ** 6
        fix_names = rng.randint(2, 8) if final != "mismatch" or rng.random() < 0.5 else 10 ** 6
        miss0, rows0 = rng.uniform(0.02, 0.08), volumes[key]
        residual = {"match": 0.0, "partial": rng.uniform(0.001, 0.006), "mismatch": rng.uniform(0.015, 0.03)}[final]
        for k, day in enumerate(range(0, ndays_total, 3)):
            ts = dt.datetime.combine(first_day + dt.timedelta(days=day), dt.time(5, 0), dt.timezone.utc) + \
                dt.timedelta(minutes=10 * n)
            if ts > as_of:
                break
            rows_old = round(rows0 * (0.9 + 0.1 * day / ndays_total))
            share = max(residual, miss0 * math.exp(-k / 3.0)) if final != "match" or k < 12 else 0.0
            in_new = round(rows_old * share * rng.uniform(0.5, 0.8))
            in_old = round(rows_old * share * rng.uniform(0.1, 0.4))
            rows_new = rows_old - in_new + in_old
            base = {"execution_timestamp": ts, "table": name, "old_table": old_t, "new_table": new_t,
                    "is_synthetic": True}
            results.append({**base, "reconciliation_type": "row_count", "result_old": str(rows_old),
                            "result_new": str(rows_new), "additional_info": None,
                            "result_status": "pass" if rows_old == rows_new else "fail"})
            for c, typ in cols.items():
                new_type = SPARK_TYPE.get(typ.lower(), "StringType()")
                old_type = LEGACY_TYPE.get(new_type, new_type) if c in bad_types and k < fix_types else new_type
                if c in bad_names and k < fix_names:
                    continue
                results.append({**base, "reconciliation_type": "data_types", "result_old": old_type,
                                "result_new": new_type, "additional_info": c,
                                "result_status": "pass" if old_type == new_type else "fail"})
            for c in cols:
                if c in bad_names and k < fix_names:
                    for o, nw in ((c.upper(), None), (None, c)):
                        results.append({**base, "reconciliation_type": "column_names", "result_old": o,
                                        "result_new": nw, "additional_info": None, "result_status": "fail"})
                else:
                    results.append({**base, "reconciliation_type": "column_names", "result_old": c, "result_new": c,
                                    "additional_info": None, "result_status": "pass"})
            same = in_new == 0 and in_old == 0
            results.append({**base, "reconciliation_type": "row_values",
                            "result_old": "same_row_values" if same else str(in_new),
                            "result_new": "same_row_values" if same else str(in_old), "additional_info": None,
                            "result_status": "pass" if same else "fail"})
    return mapping, results


def build_schema_changes(seed, columns, first_day, ndays):
    """Column added / type changed / removed events in the synthetic history, consistent with today's schemas
    (columns_snapshot): added and type-changed columns exist today with the new type; removed columns do not."""
    rng = random.Random(seed + 29)
    old_of = {"int": "string", "bigint": "int", "date": "string", "double": "decimal(18,2)", "string": "varchar(50)",
              "boolean": "string", "timestamp": "string"}
    out = []
    for key in rng.sample(sorted(columns), min(18, len(columns))):
        cat, sch, tbl = key
        cols = list(columns[key].items())
        for _ in range(rng.randint(1, 2)):
            u = rng.random()
            col, typ = rng.choice(cols)
            ts = dt.datetime.combine(first_day + dt.timedelta(days=rng.randint(2, ndays - 2)),
                                     dt.time(rng.randint(0, 5), rng.randint(0, 59)), dt.timezone.utc)
            if u < 0.4:
                ev = ("column added", None, typ)
            elif u < 0.75:
                ev = ("type changed", old_of.get(typ.lower(), "string"), typ)
            else:
                col, ev = f"{col}_old", ("column removed", typ, None)
            out.append({"event_ts": ts, "table_catalog": cat, "table_schema": sch, "table_name": tbl,
                        "column_name": col, "change_type": ev[0], "old_type": ev[1], "new_type": ev[2],
                        "is_synthetic": True})
    return sorted(out, key=lambda r: (r["event_ts"], r["table_name"], r["column_name"]))


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
    rows, incidents, syn_days = build(days, assignments, tables, a.seed, a.days)
    types = {c["column_name"]: c["full_data_type"] for c in fetch(env, (
        f"SELECT column_name, full_data_type FROM {env.dq_catalog}.information_schema.columns "
        f"WHERE table_schema = '{env.dashboard_schema}' AND table_name = 'v_results_ext'"))}
    real_rows = coerce(fetch(env, f"SELECT * FROM {env.sb}.v_results_ext WHERE row_level = 'rule' "
                                  f"ORDER BY run_date, start_timestamp, result_rule_id"), types)
    demo, demo_rows = build_demo_rules(a.seed, assignments, tables, rows, real_rows, incidents, a.days)
    rows += demo_rows
    print(f"{fq}: demo rules {len(demo)} assignments, {len(demo_rows)} results")
    write(env, TABLE, rows, a.dry_run, extra=f"{len({r['result_id'] for r in rows})} calls, seed {a.seed}, "
          f"{min(r['run_date'] for r in rows)}..{max(r['run_date'] for r in rows)}")
    write(env, "rule_assignment_synthetic", assignment_rows(demo, syn_days[0]), a.dry_run)
    if a.dry_run:
        print("dry run: lifecycle, triage and alerts are derived from the written results, not built")
        return
    # derived from the dashboard views, in recorded (unshifted) time
    shift = int(fetch(env, f"SELECT date_shift_days AS d FROM {env.sb}.v_date_shift")[0]["d"])
    # as of = the dashboard's reference time (v_data_mode.as_of_ts: end of the newest run), in recorded time
    as_of = dt.datetime.fromisoformat(fetch(env, f"SELECT CAST(timestampadd(DAY, -{shift}, as_of_ts) AS STRING) AS t "
                                                 f"FROM {env.sb}.v_data_mode")[0]["t"].replace(" ", "T") + "+00:00")
    ts = lambda v: None if v is None else dt.datetime.fromisoformat(v.replace("Z", "+00:00")) - dt.timedelta(days=shift)
    batches = fetch(env, f"""SELECT result_rule_id, rule_assignment_id, table_id,
            coalesce(quarantine_table, quarantine) AS quarantine_table, rows_quarantined, run_ts
        FROM {env.sb}.v_results_all
        WHERE is_latest_for_day AND row_level = 'rule' AND rows_quarantined > 0 ORDER BY result_rule_id""")
    for b0 in batches:
        b0.update({"rule_assignment_id": int(b0["rule_assignment_id"]), "table_id": int(b0["table_id"]),
                   "rows_quarantined": int(b0["rows_quarantined"]),
                   "entered_ts": ts(b0["run_ts"]) + dt.timedelta(minutes=10)})
    write(env, "quarantine_lifecycle_synthetic", build_lifecycle(a.seed, batches, as_of), False)
    incs = fetch(env, f"""SELECT incident_key, rule_assignment_id, `schema`, criticality, failing_runs,
            CAST(detected_ts AS STRING) AS detected_ts, CAST(resolved_ts AS STRING) AS resolved_ts
        FROM {env.sb}.v_incidents ORDER BY incident_key""")
    for i0 in incs:
        i0.update({"rule_assignment_id": int(i0["rule_assignment_id"]), "failing_runs": int(i0["failing_runs"]),
                   "detected_ts": ts(i0["detected_ts"].replace(" ", "T") + "Z"),
                   "resolved_ts": ts(i0["resolved_ts"].replace(" ", "T") + "Z") if i0["resolved_ts"] else None})
    triage, alerts = build_triage_alerts(a.seed, incs, as_of)
    write(env, "incident_triage_synthetic", triage, False)
    write(env, "alert_log_synthetic", alerts, False)
    # current schemas of the registered tables (newest columns_snapshot day) and their row volumes
    snap = fetch(env, f"""SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.data_type
        FROM {env.sb}.columns_snapshot c
        WHERE c.simulated_date = (SELECT MAX(simulated_date) FROM {env.sb}.columns_snapshot)
        ORDER BY 1, 2, 3, c.ordinal_position""")
    columns = {}
    for r in snap:
        columns.setdefault((r["table_catalog"], r["table_schema"], r["table_name"]), {})[r["column_name"]] = r["data_type"]
    vols = {(r["catalog"], r["schema"], r["table_name"]): int(float(r["v"])) for r in fetch(env, f"""
        SELECT catalog, `schema`, table_name, percentile_approx(rows_evaluated, 0.5) AS v
        FROM {env.sb}.v_results_ext WHERE row_level = 'rule' AND rows_evaluated IS NOT NULL GROUP BY 1, 2, 3""")}
    write(env, "config_changes_synthetic", build_config_changes(a.seed, demo, syn_days[0], incs, as_of, columns), False)
    ndays_total = (last_recorded_day(env) - syn_days[0]).days + 1
    mapping, recon = build_recon(a.seed, columns, vols, syn_days[0], ndays_total, as_of)
    write(env, "recon_mapping_synthetic", mapping, False)
    write(env, "recon_results_synthetic", recon, False)
    write(env, "schema_changes_synthetic", build_schema_changes(a.seed, columns, syn_days[0], a.days), False)


def last_recorded_day(env):
    return dt.date.fromisoformat(fetch(env, f"SELECT CAST(recorded_last_day AS STRING) AS d FROM {env.sb}.v_date_shift")[0]["d"])


def write(env, table, rows, dry_run, extra=""):
    """Full regenerate of one synthetic table of the dashboard schema: DELETE, then INSERT in batches."""
    if table not in TABLES:
        raise env_config.GuardError(f"REFUSED: {table} is not a synthetic table of this generator")
    fq = f"{env.sb}.{table}"
    cols = fetch(env, f"SELECT column_name, full_data_type FROM {env.dq_catalog}.information_schema.columns "
                      f"WHERE table_schema = '{env.dashboard_schema}' AND table_name = '{table}' ORDER BY ordinal_position")
    if not cols:
        raise env_config.GuardError(f"{fq} does not exist: run scripts/run_ddl.py ddl/05_synthetic_tables.sql first")
    names = [c["column_name"] for c in cols]
    missing = sorted(set(names) - set(rows[0])) if rows else []
    if missing:
        raise env_config.GuardError(f"generator does not fill columns {missing} of {table}")
    casts = {c["column_name"]: SPARK_TYPES[c["full_data_type"]] for c in cols}
    items = [json.dumps({n: to_json(r.get(n)) for n in names}) for r in rows]
    digest = hashlib.sha256("\n".join(items).encode()).hexdigest()[:16]
    print(f"{fq}: {len(rows)} rows{', ' + extra if extra else ''}, content sha256 {digest}")
    if dry_run:
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
        if not batch:
            continue
        b64 = base64.b64encode(("[" + ",".join(batch) + "]").encode()).decode()
        env_config.sql(env, f"INSERT INTO {fq} SELECT * FROM (SELECT inline(from_json(CAST(unbase64('{b64}') AS STRING), "
                            f"'ARRAY<STRUCT<{schema}>>')))")
    n = fetch(env, f"SELECT count(*) AS n FROM {fq}")[0]["n"]
    print(f"  {fq}: {n} rows after insert ({len(batches)} batches)")
    if int(n) != len(rows):
        raise env_config.GuardError(f"row count mismatch in {table}: generated {len(rows)}, table has {n}")


if __name__ == "__main__":
    main()
