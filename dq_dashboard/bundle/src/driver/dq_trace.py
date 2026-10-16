"""Observation-only trace for the DQ framework (used by dq_backfill_driver.py).

Why: code_error rows in results.* carry no result_id / table_id / rule_assignment_id
(framework_findings.md F-01), so they cannot be attributed or deleted by id.

How: at runtime, inside the driver process only, the three open_* functions that
framework.dq_framework bound via `from .utils import *` are replaced *in the
dq_framework module namespace* by pass-through wrappers. The uploaded framework
files are never modified. Each wrapper:
  * calls the original with the same arguments and returns its result unchanged;
  * lets any exception from the original propagate unchanged (no except around it);
  * only AFTER a successful call records the ids THAT FUNCTION just set (all other
    fields NULL) plus the function name;
  * persists every record immediately as its own small JSON file in the UC Volume
    TRACE_DIR (one complete file per event: UC Volumes do not support appends), so
    the ids survive a driver crash. The driver loads them into dq_dashboard.rule_trace
    after each run_data_quality() call and at start-up (recovery).
  * swallows and counts failures while recording, so tracing can never change the
    framework's behaviour. The driver marks such calls `trace_incomplete`.

SINGLE-THREADED ONLY: the trace keeps per-call state in module globals. begin_call()
asserts that no other call is active and that it runs on the main thread; the
driver must never call run_data_quality() in parallel.
"""
import functools
import json
import os
import threading
import uuid
from datetime import datetime, timezone

TRACE_ENABLED = True          # set False in the driver to compare runs without the trace
TRACE_DIR = None              # set by install() from env_config (env.trace_dir); never hard-coded

# fields each wrapped function is responsible for (everything else is stored as NULL)
FIELDS = {
    "open_result":       ("result_id",),
    "open_result_table": ("result_id", "result_table_id", "table_id"),
    "open_result_rule":  ("result_id", "result_table_id", "result_rule_id", "table_id", "rule_assignment_id"),
}
ALL_FIELDS = ("result_id", "result_table_id", "result_rule_id", "table_id", "rule_assignment_id")

_state = {"call_id": None, "seq": 0, "errors": 0, "rows": []}


def _value(params, field):
    r, m, c = params.result, params.meta, params.config
    return {
        "result_id": r.result_id, "result_table_id": r.result_table_id,
        "result_rule_id": r.result_rule_id, "table_id": m.table_id,
        "rule_assignment_id": c.rule_assignment_id,
    }[field]


def _record(func_name, params):
    try:
        if _state["call_id"] is None:
            raise RuntimeError("traced function called outside begin_call()/end_call()")
        _state["seq"] += 1
        row = {f: None for f in ALL_FIELDS}
        for f in FIELDS[func_name]:
            v = _value(params, f)
            row[f] = None if v is None else (int(v) if f in ("table_id", "rule_assignment_id") else str(v))
        row.update(call_id=_state["call_id"], seq=_state["seq"], func_name=func_name,
                   traced_at=datetime.now(timezone.utc).isoformat())
        path = os.path.join(TRACE_DIR, _state["call_id"], f"{_state['seq']:06d}_{func_name}.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:                    # one complete file per event
            fh.write(json.dumps(row))
        _state["rows"].append(row)
    except Exception:                                  # never let tracing affect the run
        _state["errors"] += 1


def _wrap(name, original):
    @functools.wraps(original)                         # also sets wrapper.__wrapped__ = original
    def wrapper(params, *args, **kwargs):
        result = original(params, *args, **kwargs)     # exceptions propagate unchanged
        _record(name, params)
        return result
    wrapper.__dq_trace__ = True
    return wrapper


def install(dq_framework_module, trace_dir):
    """Patch framework.dq_framework in memory. Idempotent. No-op if TRACE_ENABLED is False."""
    global TRACE_DIR
    if not TRACE_ENABLED:
        return False
    if not trace_dir:
        raise ValueError("trace_dir is required when TRACE_ENABLED")
    TRACE_DIR = trace_dir
    for name in FIELDS:
        current = getattr(dq_framework_module, name)
        if not getattr(current, "__dq_trace__", False):
            setattr(dq_framework_module, name, _wrap(name, current))
    return True


def uninstall(dq_framework_module):
    """Restore the real functions (unwraps any number of trace layers)."""
    for name in FIELDS:
        fn = getattr(dq_framework_module, name)
        while getattr(fn, "__dq_trace__", False):
            fn = fn.__wrapped__
        setattr(dq_framework_module, name, fn)


def begin_call():
    """Start tracing one run_data_quality() call. Returns its call_id."""
    assert threading.current_thread() is threading.main_thread(), "dq_trace is single-threaded only"
    assert _state["call_id"] is None, "run_data_quality() must never be called in parallel"
    _state.update(call_id=str(uuid.uuid4()), seq=0, errors=0, rows=[])
    return _state["call_id"]


def end_call():
    """Finish the call. Returns (call_id, rows, trace_errors)."""
    out = (_state["call_id"], list(_state["rows"]), _state["errors"])
    _state.update(call_id=None, seq=0, errors=0, rows=[])
    return out


def last_result_id(rows):
    """result_id of the execution opened during the call (used when the call raised)."""
    ids = [r["result_id"] for r in rows if r["func_name"] == "open_result" and r["result_id"]]
    return ids[-1] if ids else None
