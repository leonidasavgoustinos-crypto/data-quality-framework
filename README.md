# Data Quality Framework

A **configuration-driven data quality framework** built for Databricks. Define rules in a database — no code changes needed to add new checks.

Includes a companion **Reconciliation Framework** for comparing old vs new table versions during migrations.

---

## 📦 Repository Structure

```
data-quality-framework/
│
├── framework/                        # ← Core Python package (the engine)
│   ├── dq_framework.py               #   Main entrypoint: run_data_quality()
│   ├── utils.py                      #   All DB operations & rule execution logic
│   ├── constants.py                  #   ⚠️  CONFIGURE THIS FIRST (catalog names)
│   ├── run_time_params.py            #   Data classes: Meta, Config, Result
│   ├── logging.py                    #   Console logging helpers
│   └── combined_dq_and_df_load.py    #   Helper: run DQ + write df in one call
│
├── reconciliation/                   # ← Reconciliation module (old vs new table)
│   ├── framework/
│   │   ├── reconciliation_framework.py
│   │   └── utils.py
│   ├── DDL.py                        #   Create reconciliation tables
│   ├── load_mapping.py               #   Register old/new table pairs
│   ├── execution.py                  #   Run reconciliation
│   └── demo.py
│
├── setup/                            # ← One-time infrastructure setup
│   ├── 01_ddl.py                     #   Create all DQ tables/schemas
│   ├── 02_initial_load.py            #   Load seed data (rule types, policies, etc.)
│   └── 03_reporting_views.py         #   Create reporting views
│
├── configuration/                    # ← Manage rules & tables (run as needed)
│   ├── 01_rule_type.py
│   ├── 02_rule_dimension.py
│   ├── 03_project.py
│   ├── 04_run_policy.py
│   ├── 05_apply_at.py
│   ├── 06_rule_template.py           #   Define SQL/DQX rules
│   ├── 07_table.py                   #   Register tables to check
│   └── 08_rule_assignment.py         #   Assign rules to tables
│
├── execution/
│   ├── execution.py                  #   Standalone run examples
│   └── demo.py                       #   Full demo notebook
│
├── regression_test/
│   ├── regression_test.py
│   └── reference_tables_ddl.py
│
├── setup.py
├── .gitignore
└── README.md
```

---

## ⚡ Quick Start

### 1. Configure catalog names

Edit [`framework/constants.py`](framework/constants.py) to match your Databricks Unity Catalog:

```python
CATALOG       = "your_dq_catalog"      # Where DQ tables live
CATALOG_AUDIT = "your_audit_catalog"   # Where audit logs live
SCHEMA_AUDIT  = "dev"
```

### 2. One-time setup (run once per environment)

Upload all files to your Databricks Workspace, then run in order:

```
setup/01_ddl.py             → Creates all schemas and tables
setup/02_initial_load.py    → Loads seed data (rule types, policies, templates)
setup/03_reporting_views.py → Creates reporting views
```

### 3. Register a table

Run [`configuration/07_table.py`](configuration/07_table.py) with your table details.

### 4. Assign a rule to the table

Run [`configuration/08_rule_assignment.py`](configuration/08_rule_assignment.py).

### 5. Execute

```python
import sys
sys.path.append('/Workspace/Users/your_email/AIProjects/data_quality_framework')
from framework.dq_framework import run_data_quality

job_id = run_data_quality(apply_at='gold', run_mode='all', display_logs=True)
print(f"job_id = {job_id}")
```

---

## 🔧 Usage

### Standalone run

```python
from framework.dq_framework import run_data_quality

# Run all rules on the gold layer
job_id = run_data_quality(apply_at='gold', run_mode='all')

# Run only warning-level rules
job_id = run_data_quality(apply_at='gold', run_mode='warning')

# Run for a specific project
job_id = run_data_quality(apply_at='gold', run_mode='all', project_name='my_project')

# Pass a DataFrame directly (between-layer checks)
df = spark.table('your_catalog.schema.table')
job_id = run_data_quality(
    apply_at='bronze_to_gold',
    run_mode='all',
    full_table_name='your_catalog.schema.table',
    df_input=df
)
```

### Embedded in a pipeline

```python
from framework.combined_dq_and_df_load import run_dq_and_insert

# Runs DQ checks → if passed, writes df → if failed, exits notebook
run_dq_and_insert(
    spark=spark,
    df_input=df,
    full_table_name='your_catalog.schema.table',
    env='dev'
)
```

### Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `apply_at` | `bronze`, `silver`, `gold`, `fs`, ... | Medallion layer to check |
| `run_mode` | `all`, `error`, `warning` | Which severity level to run |
| `project_name` | string or `None` | Filter rules by project |
| `full_table_name` | `catalog.schema.table` | Limit to a single table |
| `df_input` | Spark DataFrame | Pass data directly (skips table read) |
| `display_logs` | `True` / `False` | Print execution logs to console |

---

## 📊 Results

Results are stored at three levels:

| Table | Level | Description |
|-------|-------|-------------|
| `results.result` | Execution | One record per run — overall PASS/WARNING/ERROR |
| `results.result_table` | Table | One record per table per run |
| `results.result_rule` | Rule | One record per rule per run (rows evaluated/failed) |
| `quarantine.*` | Row-level | The actual failing records (if quarantine=True in policy) |

Query results:
```sql
SELECT * FROM your_dq_catalog.results.result        ORDER BY start_timestamp DESC;
SELECT * FROM your_dq_catalog.results.result_table  ORDER BY start_timestamp DESC;
SELECT * FROM your_dq_catalog.results.result_rule   ORDER BY start_timestamp DESC;

-- Reporting view (consolidated)
SELECT * FROM your_dq_catalog.reporting.v_dq_results;
```

---

## 📐 Rule Engines

### SQL Engine
Write a SQL statement that **returns the failing rows**. Supports placeholders:

| Placeholder | Replaced with | Example |
|-------------|---------------|---------|
| `${table}` | DataFrame temp view | auto |
| `${i:col}` | Column identifier | `${i:customer_id}` → `` `customer_id` `` |
| `${v:val}` | Literal value | `${v:status}` → `'ACTIVE'` |

```sql
-- Example: detect duplicate primary keys
SELECT ${i:columns} FROM ${table} GROUP BY ${i:columns} HAVING COUNT(*) > 1

-- Example: detect negative balances
SELECT * FROM ${table} WHERE ${i:balance} < 0

-- Example: detect invalid status values
SELECT * FROM ${table} WHERE ${i:status} NOT IN (${v:valid_statuses})
```

### DQX Engine
Uses [Databricks Labs DQX](https://github.com/databrickslabs/dqx) built-in functions:

```
is_not_null              → column has no nulls
is_not_empty             → string column is not empty
is_not_null_and_not_empty → neither null nor empty
```

---

## ⚙️ Built-in Rule Templates

Loaded automatically by `setup/02_initial_load.py`:

| Rule | Engine | Dimension | Description |
|------|--------|-----------|-------------|
| `is_not_null` | DQX | validity | Column has no nulls |
| `is_not_empty` | DQX | validity | String is not empty |
| `is_not_null_and_not_empty` | DQX | validity | Not null and not empty |
| `primary_key_violation` | SQL | uniqueness | Detects duplicate PKs |
| `negative_balances` | SQL | validity | Detects negative amounts |
| `date_sequence_error` | SQL | validity | Start date > End date |
| `vat_gr_format` | SQL | validity | Greek VAT number format |
| `check_thresholds` | SQL | validity | Custom threshold check |

---

## 🔄 Reconciliation Framework

Compare an old and new version of a table (e.g., after a system migration):

```python
from reconciliation.framework.reconciliation_framework import reconciliation

reconciliation("my_table")
# Reads mapping: my_table → old_table, new_table
```

Performs 4 checks automatically:
- **Row count** — same number of rows?
- **Column names** — same columns?
- **Data types** — same schemas?
- **Row values** — exactly the same data? (SHA-256 hash per row)

---

## 🏗️ Architecture

```
Databricks (Unity Catalog)
└── your_dq_catalog
    ├── metadata/        → project, table, rule_type, rule_dimension
    ├── configuration/   → rule_template, rule_assignment, run_policy, apply_at
    ├── results/         → result, result_table, result_rule
    ├── quarantine/      → failing records per table
    └── reporting/       → v_dq_results, v_consolidated_configuration_metadata
```

---

## 📋 Requirements

- Databricks Runtime (with PySpark)
- Unity Catalog enabled
- `databricks-labs-dqx` library (`%pip install databricks-labs-dqx`)
- `databricks-sdk`
- `delta-spark`
