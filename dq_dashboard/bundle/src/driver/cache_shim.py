"""DataFrame.cache()/persist() compatibility shim for serverless (framework_findings.md F-21).

The framework calls `df_filtered.cache()` (framework/dq_framework.py:83). On serverless this raises
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE, so no rule is ever evaluated. This shim is installed
ONLY inside the driver process (runtime patch of the pyspark DataFrame classes). The framework files are
never modified.

cache_mode (bundle variable):
  off         no patch; behaviour exactly as the framework (fails on serverless)
  auto        call the real cache()/persist(); ONLY if it raises NOT_SUPPORTED_WITH_SERVERLESS, return the
              DataFrame unchanged (= uncached; results identical, inputs are re-read per rule).
              Any other exception propagates unchanged. Logged once per process.
  force_noop  never cache (returns the DataFrame unchanged); for comparison runs only

`take_used()` returns how often the fallback/no-op path was taken since the last call; the driver writes
it to sandbox_log for every run_data_quality() call.
"""
import functools

MODES = ("auto", "off", "force_noop")
MARK = "NOT_SUPPORTED_WITH_SERVERLESS"
_state = {"mode": "off", "used": 0, "logged": False}


def _classes():
    out = []
    try:
        from pyspark.sql import DataFrame
        out.append(DataFrame)
    except Exception:
        pass
    try:                                    # Spark Connect DataFrame (serverless)
        from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
        if ConnectDataFrame not in out:
            out.append(ConnectDataFrame)
    except Exception:
        pass
    return out


def _wrap(original, name):
    @functools.wraps(original)
    def shim(self, *args, **kwargs):
        if _state["mode"] == "force_noop":
            _state["used"] += 1
            return self
        try:
            return original(self, *args, **kwargs)
        except Exception as e:
            if MARK not in str(e):
                raise                       # unrelated errors propagate unchanged
            _state["used"] += 1
            if not _state["logged"]:
                _state["logged"] = True
                print(f"[cache_shim] DataFrame.{name}() not supported on this compute ({MARK}); "
                      f"continuing UNCACHED (cache_mode=auto). Logged once.")
            return self
    shim.__dq_cache_shim__ = True
    return shim


def install(mode):
    if mode not in MODES:
        raise ValueError(f"cache_mode must be one of {MODES}")
    _state["mode"] = mode
    if mode == "off":
        return []
    patched = []
    for cls in _classes():
        for name in ("cache", "persist"):
            fn = getattr(cls, name, None)
            if fn is not None and not getattr(fn, "__dq_cache_shim__", False):
                setattr(cls, name, _wrap(fn, name))
                patched.append(f"{cls.__module__}.{cls.__name__}.{name}")
    return patched


def uninstall():
    for cls in _classes():
        for name in ("cache", "persist"):
            fn = getattr(cls, name, None)
            while getattr(fn, "__dq_cache_shim__", False):
                fn = fn.__wrapped__
            if fn is not None:
                setattr(cls, name, fn)
    _state["mode"] = "off"


def take_used():
    n = _state["used"]
    _state["used"] = 0
    return n
