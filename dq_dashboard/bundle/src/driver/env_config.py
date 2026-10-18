"""Single source of environment configuration for every dq_dashboard script.

The values live in the bundle (bundle/databricks.yml, one target per workspace). This module ships with the
driver in bundle/src/driver/; the local scripts in scripts/ import it from there.
Nothing else may hard-code a catalog, schema, warehouse, host or profile.

Loaders
  from_args(argv)      inside a job: values arrive as task parameters (--dq_catalog ... ),
                       see bundle/resources/dq_driver.job.yml. Returns (env, remaining_argv).
  from_widgets(dbu)    inside a notebook: same keys as widgets.
  from_bundle(target)  locally: `databricks bundle validate -t <target> -o json`.
  connect_local(target) from_bundle + host guard against the target's CLI profile.

Guards (raise GuardError, never warn-and-continue)
  env.require_sandbox(action)       sandbox_enabled must be true
  env.require_simulated_dates(a)    simulated_dates_enabled must be true
  env.assert_host(actual_host)      connected workspace must equal expected_host
Templates
  env.render(text)                  replaces {{dq_catalog}}, {{dashboard_schema}}, {{warehouse_id}}, {{target}}
                                    and the flags ({{simulated_dates_enabled}}, ...) as true/false;
                                    fails on any unresolved {{...}}
"""
import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass, fields

HERE = os.path.dirname(os.path.abspath(__file__))
BUNDLE_DIR = os.path.normpath(os.path.join(HERE, "..", ".."))       # bundle/src/driver -> bundle

STR_KEYS = ("target", "expected_host", "dq_catalog", "dashboard_schema", "warehouse_id", "cache_mode")
BOOL_KEYS = ("sandbox_enabled", "trace_enabled", "synthetic_enabled", "simulated_dates_enabled")
CACHE_MODES = ("auto", "off", "force_noop")
_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


class GuardError(RuntimeError):
    """Raised when an action is not allowed on the current target. Never catch-and-continue."""


@dataclass(frozen=True)
class Env:
    target: str
    expected_host: str
    dq_catalog: str
    dashboard_schema: str
    warehouse_id: str
    cache_mode: str
    sandbox_enabled: bool
    trace_enabled: bool
    synthetic_enabled: bool
    simulated_dates_enabled: bool
    profile: str = ""            # local CLI profile (empty inside jobs)

    # ---- derived names
    @property
    def sb(self):                # dashboard schema, fully qualified
        return f"{self.dq_catalog}.{self.dashboard_schema}"

    @property
    def trace_dir(self):
        return f"/Volumes/{self.dq_catalog}/{self.dashboard_schema}/trace_vol"

    def fq(self, schema, table):
        return f"{self.dq_catalog}.{schema}.{table}"

    # ---- guards
    def require_sandbox(self, action):
        if not self.sandbox_enabled:
            raise GuardError(f"REFUSED on target '{self.target}': '{action}' requires sandbox_enabled=true "
                             f"(sandbox-only operation: generated data, simulated runs, fault injection, DROP).")

    def require_simulated_dates(self, action):
        if not self.simulated_dates_enabled:
            raise GuardError(f"REFUSED on target '{self.target}': '{action}' requires simulated_dates_enabled=true.")

    def assert_host(self, actual_host):
        norm = lambda h: (h or "").strip().rstrip("/").lower().replace("http://", "https://")
        if norm(actual_host) != norm(self.expected_host):
            raise GuardError(f"REFUSED: connected workspace '{actual_host}' != expected '{self.expected_host}' "
                             f"for target '{self.target}'. Aborting before any write.")

    # ---- templates
    def render(self, text):
        values = {k: getattr(self, k) for k in ("dq_catalog", "dashboard_schema", "warehouse_id", "target")}
        values.update({k: "true" if getattr(self, k) else "false" for k in BOOL_KEYS})   # SQL boolean literals
        out = re.sub(r"\{\{(\w+)\}\}", lambda m: values.get(m.group(1), m.group(0)), text)
        left = sorted(set(re.findall(r"\{\{\w+\}\}", out)))
        if left:
            raise GuardError(f"unresolved template placeholders: {left}")
        return out

    def summary(self):
        return {f.name: getattr(self, f.name) for f in fields(self)}


def _bool(v):
    s = str(v).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    raise GuardError(f"not a boolean: {v!r}")


def _build(values, profile=""):
    missing = [k for k in STR_KEYS + BOOL_KEYS if values.get(k) in (None, "")]
    if missing:
        raise GuardError(f"missing environment values: {missing}")
    placeholders = [k for k in STR_KEYS if "<" in str(values[k]) or "${" in str(values[k])]
    if placeholders:
        raise GuardError(f"target '{values['target']}' still has placeholder values for {placeholders}; "
                         f"set them in bundle/databricks.yml (see MIGRATION.md)")
    for k in ("dq_catalog", "dashboard_schema"):
        if not _IDENT.match(values[k]):
            raise GuardError(f"{k}={values[k]!r} is not a plain identifier")
    if values["cache_mode"] not in CACHE_MODES:
        raise GuardError(f"cache_mode must be one of {CACHE_MODES}, got {values['cache_mode']!r}")
    if not str(values["expected_host"]).startswith("https://"):
        raise GuardError("expected_host must start with https://")
    env = Env(**{k: str(values[k]) for k in STR_KEYS},
              **{k: _bool(values[k]) for k in BOOL_KEYS}, profile=profile)
    if env.target == "company" and env.sandbox_enabled:
        raise GuardError("target 'company' must never have sandbox_enabled=true")
    return env


def from_args(argv=None):
    ap = argparse.ArgumentParser(add_help=False)
    for k in STR_KEYS + BOOL_KEYS:
        ap.add_argument(f"--{k}", required=True)
    known, rest = ap.parse_known_args(argv)
    return _build(vars(known)), rest


def from_widgets(dbutils):
    return _build({k: dbutils.widgets.get(k) for k in STR_KEYS + BOOL_KEYS})


def _run(cmd, cwd=None):
    r = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    if r.returncode != 0:
        raise GuardError(f"command failed: {' '.join(cmd)}\n{(r.stderr or r.stdout).strip()[:800]}")
    return r.stdout


def from_bundle(target):
    raw = _run(["databricks", "bundle", "validate", "-t", target, "-o", "json"], cwd=BUNDLE_DIR)
    cfg = json.loads(raw[raw.index("{"):])
    values = {k: v.get("value") for k, v in cfg.get("variables", {}).items()}
    values["target"] = cfg["bundle"]["target"]
    return _build(values, profile=cfg["workspace"].get("profile", ""))


def objects_path(arg=None):
    """Path of sandbox_objects.json: the --objects argument, else $DQ_SANDBOX_OBJECTS. No default."""
    p = arg or os.environ.get("DQ_SANDBOX_OBJECTS", "")
    if not p:
        raise GuardError("sandbox_objects.json path not given: pass --objects <path> or set DQ_SANDBOX_OBJECTS")
    return os.path.abspath(os.path.expanduser(p))


def local_host(profile):
    d = json.loads(_run(["databricks", "auth", "describe", "-p", profile, "-o", "json"]))
    return d["details"]["host"]


def connect_local(target):
    """Load the target and verify the CLI profile points at the expected workspace."""
    env = from_bundle(target)
    env.assert_host(local_host(env.profile))
    return env


def runtime_host():
    """Host of the workspace the current job/notebook runs in."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient().config.host


_SQL_API = "/api/2.0/sql/statements"
SQL_WAIT_TIMEOUT = "30s"          # server-side wait per request (5s..50s); longer statements are polled
SQL_POLL_SECONDS = 2
SQL_MAX_SECONDS = 900             # give up (and cancel) after this


def _api(method, path, body=None, profile=""):
    cmd = ["databricks", "api", method, path] + (["--json", json.dumps(body)] if body is not None else [])
    cmd += ["-p", profile] if profile else []
    return json.loads(_run(cmd) or "{}")


def sql(env, statement):
    """Run one SQL statement on the target warehouse via the Statement Execution API (local scripts only).

    Returns a JSON array of row objects ({column: value}); values are strings or null, as returned by the API.
    Raises GuardError on any final state other than SUCCEEDED, with the server's error message.
    """
    import time
    body = {"warehouse_id": env.warehouse_id, "statement": statement, "wait_timeout": SQL_WAIT_TIMEOUT,
            "on_wait_timeout": "CONTINUE", "disposition": "INLINE", "format": "JSON_ARRAY"}
    r = _api("post", _SQL_API, body, env.profile)
    sid, started = r.get("statement_id"), time.monotonic()
    while r.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if time.monotonic() - started > SQL_MAX_SECONDS:
            _api("post", f"{_SQL_API}/{sid}/cancel", {}, env.profile)
            raise GuardError(f"statement {sid} did not finish within {SQL_MAX_SECONDS}s; cancelled")
        time.sleep(SQL_POLL_SECONDS)
        r = _api("get", f"{_SQL_API}/{sid}", profile=env.profile)
    status = r.get("status", {})
    if status.get("state") != "SUCCEEDED":
        err = status.get("error", {})
        raise GuardError(f"statement {sid} {status.get('state')}: {err.get('error_code', '')} "
                         f"{err.get('message', '(no message)')}\n  SQL: {statement.strip()[:300]}")
    cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
    result = r.get("result") or {}
    rows = list(result.get("data_array") or [])
    while result.get("next_chunk_index") is not None:
        result = _api("get", f"{_SQL_API}/{sid}/result/chunks/{result['next_chunk_index']}", profile=env.profile)
        rows += result.get("data_array") or []
    return json.dumps([dict(zip(cols, row)) for row in rows])
