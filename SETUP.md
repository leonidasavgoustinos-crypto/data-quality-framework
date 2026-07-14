# ⚠️  CONFIGURATION — Read Before Deploying

## What to change

Before running anything, edit `framework/constants.py`:

```python
CATALOG       = "your_dq_catalog"      # ← your DQ catalog name
CATALOG_AUDIT = "your_audit_catalog"   # ← your audit catalog name
SCHEMA_AUDIT  = "dev"                  # ← dev or prod
```

## Deployment order

Run notebooks in this exact order for a **new environment**:

```
1.  setup/01_ddl.py                     (creates tables)
2.  setup/02_initial_load.py            (loads seed data)
3.  setup/03_reporting_views.py         (creates views)
4.  configuration/07_table.py           (register your tables)
5.  configuration/08_rule_assignment.py (assign rules to tables)
6.  execution/execution.py              (run DQ checks)
```

## Notebooks 01–06 in configuration/

These are **reference/admin notebooks** — you only run them when:
- Adding a new project (`03_project.py`)
- Defining a new custom rule (`06_rule_template.py`)
- Most rule types, dimensions, policies, and apply_at values
  are already loaded by `setup/02_initial_load.py`

## Environment variables

Never hardcode credentials. Use Databricks Secrets:
```python
# ✅ Correct
password = dbutils.secrets.get("scope_name", "key_name")

# ❌ Wrong
password = "my_actual_password"
```
