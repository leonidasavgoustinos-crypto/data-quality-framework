# Databricks notebook source
# Config runner for the DQ Framework UI (dq-framework-ui).
# The app starts ONE Job that runs this notebook. This notebook calls the configuration
# notebook (01–08) named in "notebook", passing "params" as its widget values —
# exactly as if a user filled in the widgets and pressed Run. No framework logic here.

dbutils.widgets.text("notebook", "")
dbutils.widgets.text("params", "{}")
# Folder with the configuration notebooks (the Git folder of the framework)
dbutils.widgets.text("config_folder", "../configuration")

# COMMAND ----------

import json

ALLOWED = {
    "01_rule_type", "02_rule_dimension", "03_project", "04_run_policy",
    "05_apply_at", "06_rule_template", "07_table", "08_rule_assignment",
}

notebook = dbutils.widgets.get("notebook").strip()
params = json.loads(dbutils.widgets.get("params") or "{}")
folder = dbutils.widgets.get("config_folder").rstrip("/")

if notebook not in ALLOWED:
    raise ValueError(f"Notebook '{notebook}' is not allowed. Allowed: {sorted(ALLOWED)}")
if params.get("action") not in ("insert/update", "delete"):
    raise ValueError("params.action must be 'insert/update' or 'delete'")

print(f"Running {folder}/{notebook} with {params}")
result = dbutils.notebook.run(f"{folder}/{notebook}", 600, {k: str(v) for k, v in params.items()})
dbutils.notebook.exit(result or "ok")
