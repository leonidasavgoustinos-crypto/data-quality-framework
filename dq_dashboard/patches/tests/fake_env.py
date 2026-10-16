"""Minimal in-memory stand-ins for pyspark / delta / databricks so framework/*.py can be imported and its
bookkeeping (INSERT on open, DeltaTable.update on close/failure) can be tested without Databricks.

Only what framework/dq_framework.py + framework/utils.py touch on the paths under test is implemented:
  F.col / lit / current_timestamp / count / count_if, Column ==, !=, &, eqNullSafe, isNull, isin, alias,
  DataFrame.filter/select/distinct/limit/collect/count/cache, spark.sql("INSERT INTO t (cols) VALUES (...)"),
  spark.table, spark.catalog.tableExists, DeltaTable.forName(...).update(condition, set).
Source reading, rule execution and rule discovery are replaced per scenario by the test (monkeypatch).
"""
import itertools
import re
import sys
import types

STORE = {}                      # table name -> list[dict]
SQL_LOG = []
_clock = itertools.count(1)     # monotonically increasing fake timestamps
CACHE_ERROR = {"msg": None}     # message raised by DataFrame.cache() when set


def reset():
    STORE.clear()
    SQL_LOG.clear()
    CACHE_ERROR["msg"] = None


# ------------------------------------------------------------------ Column expressions
def _v(x, row):
    return x.ev(row) if isinstance(x, Col) else x


class Col:
    def __init__(self, fn, name=None):
        self.fn, self.name = fn, name

    def ev(self, row):
        return self.fn(row)

    def alias(self, name):
        return Col(self.fn, name)

    def __eq__(self, o):   # SQL semantics: NULL = x is not true
        return Col(lambda r: (lambda a, b: a is not None and b is not None and a == b)(_v(self, r), _v(o, r)))

    def __ne__(self, o):
        return Col(lambda r: (lambda a, b: a is not None and b is not None and a != b)(_v(self, r), _v(o, r)))

    def __and__(self, o):
        return Col(lambda r: bool(_v(self, r)) and bool(_v(o, r)))

    def __or__(self, o):
        return Col(lambda r: bool(_v(self, r)) or bool(_v(o, r)))

    def __invert__(self):
        return Col(lambda r: not _v(self, r))

    def eqNullSafe(self, o):
        return Col(lambda r: _v(self, r) == _v(o, r))

    def isNull(self):
        return Col(lambda r: _v(self, r) is None)

    def isin(self, values):
        return Col(lambda r: _v(self, r) in values)

    __hash__ = object.__hash__


class Agg:
    def __init__(self, kind, cond=None, name=None):
        self.kind, self.cond, self.name = kind, cond, name

    def alias(self, name):
        return Agg(self.kind, self.cond, name)

    def compute(self, rows):
        if self.kind == "count":
            return len(rows)
        return sum(1 for r in rows if self.cond.ev(r))


functions = types.ModuleType("pyspark.sql.functions")
functions.col = lambda name: Col(lambda r: r.get(name), name)
functions.lit = lambda v: Col(lambda r: v)
functions.current_timestamp = lambda: Col(lambda r: next(_clock))
functions.count = lambda _="*": Agg("count")
functions.count_if = lambda cond: Agg("count_if", cond)
functions.max = lambda c: Agg("max", c)


# ------------------------------------------------------------------ DataFrame / Row
class Row(dict):
    def __getattr__(self, k):
        try:
            return self[k]
        except KeyError:
            raise AttributeError(k)


class FakeDF:
    def __init__(self, rows):
        self.rows = [Row(r) for r in rows]

    def filter(self, cond):
        return FakeDF([r for r in self.rows if cond.ev(r)])

    where = filter

    def select(self, *cols):
        if any(isinstance(c, Agg) for c in cols):
            return FakeDF([{c.name: c.compute(self.rows) for c in cols}])
        out = []
        for r in self.rows:
            out.append({(c if isinstance(c, str) else c.name): (r.get(c) if isinstance(c, str) else c.ev(r)) for c in cols})
        return FakeDF(out)

    def distinct(self):
        seen, out = set(), []
        for r in self.rows:
            k = tuple(sorted(r.items()))
            if k not in seen:
                seen.add(k)
                out.append(r)
        return FakeDF(out)

    def limit(self, n):
        return FakeDF(self.rows[:n])

    def collect(self):
        return list(self.rows)

    def count(self):
        return len(self.rows)

    def cache(self):
        if CACHE_ERROR["msg"]:
            raise RuntimeError(CACHE_ERROR["msg"])
        return self

    def first(self):
        return self.rows[0] if self.rows else None


# ------------------------------------------------------------------ spark.sql INSERT parser
_INSERT = re.compile(r"(?is)^\s*insert\s+into\s+([\w.`]+)\s*\((.*?)\)\s*values\s*\((.*)\)\s*$")


def _split_values(s):
    out, cur, q = [], "", False
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == "'":
            if q and i + 1 < len(s) and s[i + 1] == "'":
                cur += "''"
                i += 2
                continue
            q = not q
        if ch == "," and not q:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    out.append(cur.strip())
    return out


def _parse_value(tok):
    t = tok.strip()
    if t.upper() == "NULL":
        return None
    if t.lower() == "current_timestamp()":
        return next(_clock)
    if t.startswith("'") and t.endswith("'"):
        return t[1:-1].replace("''", "'")
    try:
        return int(t)
    except ValueError:
        return float(t)


class _Catalog:
    def tableExists(self, name):
        return False


class FakeSpark:
    catalog = _Catalog()

    def sql(self, stmt):
        SQL_LOG.append(stmt)
        m = _INSERT.match(stmt)
        if not m:
            return FakeDF([])
        table = m.group(1).replace("`", "")
        cols = [c.strip() for c in m.group(2).split(",")]
        vals = [_parse_value(v) for v in _split_values(m.group(3))]
        assert len(cols) == len(vals), (cols, vals)
        STORE.setdefault(table, []).append(dict(zip(cols, vals)))
        return FakeDF([])

    def table(self, name):
        return FakeDF(STORE.get(name.replace("`", ""), []))


SPARK = FakeSpark()


class _Builder:
    def getOrCreate(self):
        return SPARK


class SparkSession:
    builder = _Builder()


class DeltaTable:
    def __init__(self, name):
        self.name = name

    @classmethod
    def forName(cls, spark, name):
        return cls(name.replace("`", ""))

    def update(self, condition, set):
        for row in STORE.get(self.name, []):
            if condition.ev(row):
                for k, v in set.items():
                    row[k] = _v(v, row)


# ------------------------------------------------------------------ module installation
def install():
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession, sql.DataFrame, sql.functions = SparkSession, FakeDF, functions
    pyspark.sql = sql
    delta = types.ModuleType("delta")
    tables = types.ModuleType("delta.tables")
    tables.DeltaTable = DeltaTable
    delta.tables = tables
    dbx = types.ModuleType("databricks")
    sdk = types.ModuleType("databricks.sdk")
    sdk.WorkspaceClient = lambda *a, **k: object()
    labs = types.ModuleType("databricks.labs")
    dqx = types.ModuleType("databricks.labs.dqx")
    engine = types.ModuleType("databricks.labs.dqx.engine")
    engine.DQEngine = lambda *a, **k: object()
    for name, mod in {"pyspark": pyspark, "pyspark.sql": sql, "pyspark.sql.functions": functions,
                      "delta": delta, "delta.tables": tables, "databricks": dbx, "databricks.sdk": sdk,
                      "databricks.labs": labs, "databricks.labs.dqx": dqx, "databricks.labs.dqx.engine": engine}.items():
        sys.modules[name] = mod
