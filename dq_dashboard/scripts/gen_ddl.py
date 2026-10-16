"""Generate ddl/01_sources.sql (SANDBOX ONLY) from the registered tables in <dq_catalog>.metadata.table.

  python3 -I scripts/gen_ddl.py --target free --objects <path/to/sandbox_objects.json>
  (or set DQ_SANDBOX_OBJECTS=<path> instead of --objects)

Refuses unless the target has sandbox_enabled=true and the CLI profile points at expected_host.
For every registered table it creates an EMPTY Delta table holding exactly the columns its rules reference,
plus two neutral profiling columns (amount, segment). Catalog/schema names of the SOURCE tables come from
the metadata itself (data-driven), the DQ catalog from env_config. Also writes sandbox_objects.json (--objects),
used by teardown.py to know which catalogs/schemas this work created. That file names the workspace user, so keep
it outside dq_dashboard/. There is no default: --objects, else $DQ_SANDBOX_OBJECTS, else the script stops.
"""
import argparse
import json
import os
import pathlib
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "bundle", "src", "driver"))
import env_config  # noqa: E402

HERE = pathlib.Path(__file__).parent
DDL = HERE.parent / "ddl"
OUT = DDL / "01_sources.sql"
COMMENT = "dq_dashboard sandbox - generated"

# Extra columns referenced by non-parameterised template SQL (setup/02_initial_load.py:715-735)
EXTRA = {
    "fs_global_customer_quarterly": ["EBAStatusRuleCode_Cust", "IRBFlag_Cust", "NPEFlag_Cust_Ori", "PreviousEBAStatus_Cust"],
    "fs_global_exposure_quarterly": ["CustomerCode", "IsExclusionGlobalWOF"],
}
TYPES = {
    "crsid": "INT", "year": "INT", "month": "INT", "date": "DATE",
    "ReferenceDate": "DATE", "AssessmentDate": "DATE",
    "IRBFlag_Cust": "BOOLEAN", "IsExclusionGlobalWOF": "BOOLEAN",
}
FILTER_TYPES = {"date": "DATE", "int": "INT"}


def export_registered(env):
    rows = json.loads(env_config.sql(env, f"""
        SELECT t.table_id, t.catalog, t.schema, t.`table` AS tbl, t.layer, to_json(t.primary_key) pk,
               t.filter_field, t.filter_field_type, to_json(collect_set(ra.parameters_identifiers)) params
        FROM {env.fq('metadata', '`table`')} t
        LEFT JOIN {env.fq('configuration', 'rule_assignment')} ra ON ra.table_id = t.table_id
        GROUP BY ALL ORDER BY t.table_id"""))
    (DDL / "registered_tables.json").write_text(json.dumps(rows, indent=2))
    return rows


def columns_for(t):
    cols = list(json.loads(t["pk"]))
    if t["filter_field"]:
        cols.append(t["filter_field"])
    for p in json.loads(t["params"] or "[]"):
        d = json.loads(p) if p else {}
        for k in ("column", "columns"):
            v = d.get(k)
            if v:
                cols.extend(v if isinstance(v, list) else [v])
    cols += EXTRA.get(t["tbl"], [])
    seen, out = set(), []
    for c in cols:
        if c.lower() not in seen:
            seen.add(c.lower())
            out.append(c)
    return out


def col_type(c, t):
    if c == t["filter_field"]:
        return FILTER_TYPES.get(t["filter_field_type"], "STRING")
    return TYPES.get(c, "STRING")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", required=True)
    ap.add_argument("--objects", help="sandbox_objects.json to create/update (else $DQ_SANDBOX_OBJECTS)")
    a = ap.parse_args()
    objects = pathlib.Path(env_config.objects_path(a.objects))
    env = env_config.connect_local(a.target)
    env.require_sandbox("gen_sources / 01_sources.sql")
    tables = export_registered(env)

    catalogs = sorted({t["catalog"] for t in tables} - {env.dq_catalog})
    schemas = sorted({(t["catalog"], t["schema"]) for t in tables})
    lines = [
        "-- @targets: free",
        "-- @sandbox_only",
        "-- Source tables for the registered tables (EMPTY; data comes from the backfill driver).",
        "-- Written by scripts/gen_ddl.py --target <t>. Run with scripts/run_ddl.py. Teardown: teardown.py",
        "",
    ]
    lines += [f"CREATE CATALOG IF NOT EXISTS {c} COMMENT '{COMMENT}';" for c in catalogs]
    lines += [""] + [f"CREATE SCHEMA IF NOT EXISTS {c}.{s} COMMENT '{COMMENT}';" for c, s in schemas] + [""]
    for t in tables:
        body = ",\n".join(f"  `{c}` {col_type(c, t)}" for c in columns_for(t))
        body += ",\n  `amount` DOUBLE,\n  `segment` STRING"
        lines.append(f"-- table_id {t['table_id']} · layer {t['layer']}\n"
                     f"CREATE TABLE IF NOT EXISTS {t['catalog']}.{t['schema']}.`{t['tbl']}` (\n{body}\n)"
                     f" COMMENT '{COMMENT}';\n")
    OUT.write_text("\n".join(lines) + "\n")
    previous = json.loads(objects.read_text()) if objects.exists() else {}
    objects.parent.mkdir(parents=True, exist_ok=True)
    objects.write_text(json.dumps({
        **{k: v for k, v in previous.items() if k.startswith("legacy_") or k == "reconciliation_schemas"},
        "target": env.target,
        "catalogs_created": catalogs,
        "schemas_created_in_dq_catalog": sorted(s for c, s in schemas if c == env.dq_catalog),
        "quarantine_tables": sorted(f"{t['schema']}_{t['tbl']}".lower() for t in tables),
    }, indent=2))
    print(f"{len(catalogs)} catalogs, {len(schemas)} schemas, {len(tables)} tables -> {OUT}; objects -> {objects}")


if __name__ == "__main__":
    main()
