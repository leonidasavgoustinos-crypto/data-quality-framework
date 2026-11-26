# Data Quality Dashboard - Deployment Bundle

This folder is a **Declarative Automation Bundle** (formerly Databricks Asset Bundle). It packages the **Data quality** AI/BI dashboard together with everything the dashboard needs: the Unity Catalog schemas, the DQ framework tables, the reporting views, two jobs and the permissions. One command installs or updates all of it in any Databricks workspace.

The bundle is meant to be copied into client projects. Everything that differs between companies, workspaces or environments is a **variable**. You change values, not code.

---

## Contents

1. [What you get](#1-what-you-get)
2. [How it works](#2-how-it-works)
3. [Prerequisites](#3-prerequisites)
4. [Folder layout](#4-folder-layout)
5. [Quick start (dev)](#5-quick-start-dev)
6. [Adding it to your project: what to change](#6-adding-it-to-your-project-what-to-change)
7. [Setting up test and prod](#7-setting-up-test-and-prod)
8. [Configuration reference](#8-configuration-reference)
9. [Extending the bundle](#9-extending-the-bundle)
10. [CI/CD](#10-cicd)
11. [Day-to-day operations](#11-day-to-day-operations)
12. [Troubleshooting](#12-troubleshooting)
13. [Known limitations](#13-known-limitations)

---

## 1. What you get

After a deploy, the target workspace contains:

| Object | Name | Purpose |
|---|---|---|
| AI/BI dashboard | `Data quality` (configurable) | Four pages: Latest run, Since the start, Area, Table |
| Job | `dq_setup` | Builds or updates every object the dashboard needs (5 tasks, safe to re-run) |
| Job | `dq_healthcheck` | Daily check that the DQ framework is still writing results; emails on failure |
| Schemas | `<catalog>.reporting`, `<source_catalog>.results / configuration / metadata / quarantine` | Created if missing |
| Tables | 10 DQ framework tables (`result`, `result_rule`, `rule_assignment`, `table`, ...) | Created **empty, only if missing**. Existing tables and data are never touched |
| Views | 8 base views `v_simple_*` + 4 extension views `v_simple_*_ext` | The only objects the dashboard reads |
| Grants | `ALL PRIVILEGES` for the owner group, `USE` / `SELECT` for the viewer group | On the reporting schema |

> The bundle creates the **structure**. The **data** comes from the DQ framework (`execution/` in this repository). Until the framework has run at least once in a workspace, the dashboard is empty and `dq_healthcheck` reports "No DQ executions found".

---

## 2. How it works

### 2.1 The deploy flow

```
  scripts/deploy.sh <target>
        |
        |-- 1. databricks bundle validate   checks the configuration, resolves variables
        |-- 2. databricks bundle deploy     uploads files, creates/updates dashboard + jobs
        '-- 3. databricks bundle run dq_setup
                     |
                     |-- bootstrap     create catalogs (optional) and schemas, apply grants
                     |-- base_tables   CREATE TABLE IF NOT EXISTS (src/sql/10_base_tables)
                     |-- base_views    CREATE OR REPLACE VIEW     (src/sql/20_base_views)
                     |-- ext_views     CREATE OR REPLACE VIEW     (src/sql/30_ext_views)
                     '-- smoke_tests   every view is queryable    (src/sql/90_smoke_tests)
```

Each task waits for the previous one. If a task fails, the next ones do not run and the notification email is sent.

### 2.2 One SQL runner for everything

All SQL lives in plain `.sql` files under `src/sql/`. One generic notebook, `src/notebooks/run_sql_folder`, runs them:

* It runs every `*.sql` file of one folder in **file name order** (`01_...`, `02_...`), one statement at a time.
* It replaces placeholders like `${catalog}` with the job task parameter of the same name. Those parameters come from the bundle variables.
* It checks **all** files before running anything. If a file uses a placeholder that has no parameter, the task stops before any SQL runs.
* An `enabled` parameter (`true` / `false`) turns a whole folder on or off.

Example: the file `src/sql/20_base_views/01_v_simple_status.sql` contains

```sql
CREATE OR REPLACE VIEW ${catalog}.${schema}.v_simple_status AS ...
```

In `dev` this runs as `analytics_dq_dev.reporting.v_simple_status`. In `prod` it runs as `analytics_dq_prod.reporting.v_simple_status`. The SQL file is the same.

### 2.3 How the dashboard finds its data

The dashboard JSON (`src/dashboards/data_quality.lvdash.json`) uses table names **without** catalog or schema (`FROM v_simple_results`). At deploy time the bundle sets the dashboard's default catalog and schema from the `catalog` and `schema` variables (`dataset_catalog` / `dataset_schema` in `resources/dashboards/data_quality.dashboard.yml`). The same JSON therefore works in every workspace.

### 2.4 Where values come from

Every value is a variable declared in `databricks.yml`. When the CLI resolves a variable, it uses the first source that has a value:

1. `--var name=value` on the command line
2. Environment variable `BUNDLE_VAR_<name>` (recommended for CI and secrets-free automation)
3. The `variables:` block of the target file (`targets/dev.yml`, `test.yml`, `prod.yml`)
4. The `default` in `databricks.yml`

### 2.5 Environments

Each environment is **a separate Databricks workspace** and has its own file in `targets/`.

| | dev | test | prod |
|---|---|---|---|
| Mode | `development` | `production` | `production` |
| Who deploys | each developer, as themselves | CI, as a service principal | CI, as a service principal, from `main` only |
| Resource names | prefixed `[dev <user>] ` (one copy per developer) | as configured | as configured |
| Files uploaded to | `~/.bundle/dq_dashboard/dev` | `/Workspace/Shared/.bundle/dq_dashboard/test` | `/Workspace/Shared/.bundle/dq_dashboard/prod` |
| Schedules | always paused | `UNPAUSED` | `UNPAUSED` |
| Base views rebuilt | **no** (`deploy_base_views: "false"`) | yes | yes |

Why base views are not rebuilt in dev: in the reference dev workspace the base views are edited by hand by their owner while the dashboard is being developed. The `dq_setup` job must not overwrite that work. In a new client dev workspace where nobody edits the views by hand, set `deploy_base_views: "true"`.

---

## 3. Prerequisites

| Requirement | Why |
|---|---|
| Databricks workspace with **Unity Catalog** | All objects are UC objects |
| **Serverless compute for jobs** enabled | Both jobs run on serverless (no cluster configuration) |
| A **SQL warehouse** (serverless or pro) | Runs the dashboard queries |
| **Databricks CLI 0.281.0 or newer** (`databricks -v`) | Needed for `dataset_catalog` / `dataset_schema` on dashboards. The legacy `pip install databricks-cli` does **not** support bundles |
| Catalog privileges for the deploying identity | `USE CATALOG` + `CREATE SCHEMA` on the catalogs, or the schemas must already exist. `CREATE CATALOG` on the metastore only if `create_catalog: "true"` |
| Grant privileges for the deploying identity | Must own (or have `MANAGE` on) the reporting schema and catalog, because `bootstrap` runs `GRANT` statements |
| Workspace groups | The groups named in `owner_group` and `viewer_group` must exist |
| test / prod only: a **service principal** | Runs the jobs (`run_as`) and is used by CI to deploy |

Install the CLI: <https://docs.databricks.com/dev-tools/cli/install.html>

---

## 4. Folder layout

```
bundles/dq_dashboard/
|-- databricks.yml                 bundle name, includes, ALL variable declarations
|-- targets/
|   |-- dev.yml                    dev workspace: host, catalog, warehouse, switches
|   |-- test.yml                   test workspace (placeholders to fill in)
|   '-- prod.yml                   prod workspace (placeholders to fill in)
|-- resources/                     one YAML file per deployed resource
|   |-- dashboards/data_quality.dashboard.yml
|   '-- jobs/
|       |-- dq_setup.job.yml
|       '-- dq_healthcheck.job.yml
|-- src/
|   |-- dashboards/data_quality.lvdash.json     the dashboard itself
|   |-- notebooks/
|   |   |-- bootstrap                catalogs, schemas, grants
|   |   '-- run_sql_folder           generic SQL runner (reusable as-is)
|   '-- sql/
|       |-- 10_base_tables/          DQ framework tables (IF NOT EXISTS)
|       |-- 20_base_views/           v_simple_* views
|       |-- 30_ext_views/            v_simple_*_ext views
|       |-- 90_smoke_tests/          checks at the end of dq_setup
|       '-- 95_health_checks/        checks run daily by dq_healthcheck
|-- scripts/deploy.sh              validate + deploy + run dq_setup in one command
|-- ci/azure-devops.yml            sample pipeline (feature -> test, main -> test + prod)
'-- README.md
```

`scripts/` and `ci/` are not uploaded to the workspace (`sync.exclude` in `databricks.yml`).

---

## 5. Quick start (dev)

```bash
# 1. Log in to the dev workspace once (opens a browser)
databricks auth login --host https://<your-dev-workspace>

# 2. Go to the bundle folder
cd bundles/dq_dashboard

# 3. Check the configuration
databricks bundle validate -t dev

# 4. Deploy and build everything
bash scripts/deploy.sh dev
```

Then open **Dashboards** in the workspace and find `[dev <your name>] Data quality`.

To see what is deployed: `databricks bundle summary -t dev`.
To remove your dev copy of the dashboard and jobs: `databricks bundle destroy -t dev` (tables and views are **not** removed).

---

## 6. Adding it to your project: what to change

### 6.1 Copy the folder

Copy `bundles/dq_dashboard/` into your repository. It does not depend on files outside its own folder. Keep one folder per bundle (`bundles/<name>/`) if the repository holds several bundles.

### 6.2 Checklist

| # | File | What to change | Required? |
|---|---|---|---|
| 1 | `databricks.yml` | `bundle.name` if you deploy more than one copy to the same workspace | Optional |
| 2 | `databricks.yml` | Defaults that are specific to the reference install: `catalog` (`analytics_dq_dev`), `owner_group` (`dq-framework-admins`), `timezone` (`Europe/Athens`) | Recommended |
| 3 | `targets/dev.yml` | `workspace.host`, `catalog`, `warehouse_id` lookup (warehouse **name**), `notification_email`, `deploy_base_views` | **Yes** |
| 4 | `targets/test.yml` | Everything marked `<...>` plus `workspace.host` | **Yes**, before the first test deploy |
| 5 | `targets/prod.yml` | Everything marked `<...>` plus `workspace.host` | **Yes**, before the first prod deploy |
| 6 | `ci/azure-devops.yml` | `BUNDLE_ROOT` if you moved the folder; pipeline secrets (section 10) | If you use CI |
| 7 | `src/sql/**` | Only if your DQ framework tables differ from the standard layout | Usually no |
| 8 | `src/dashboards/data_quality.lvdash.json` | Only to change the dashboard design (see 9.3) | Usually no |

You do **not** need to change `run_sql_folder`, `bootstrap`, `deploy.sh` or the job files.

### 6.3 Pointing to existing DQ framework tables

If the DQ framework already runs in your workspace, point the bundle at its tables. Nothing is created over them, because table creation uses `IF NOT EXISTS`:

```yaml
# targets/<env>.yml
variables:
  catalog: my_reporting_catalog       # where the views and the dashboard data live
  schema: dq_reporting
  source_catalog: my_dq_catalog       # where the framework tables live
  results_schema: results
  configuration_schema: configuration
  metadata_schema: metadata
  create_base_tables: "false"         # optional: do not even try to create tables
```

---

## 7. Setting up test and prod

Do this once per environment.

1. **Create a service principal** in the account console and add it to the test (or prod) workspace. Note its **application ID**.
2. **Give it privileges**: `USE CATALOG` and `CREATE SCHEMA` on the catalogs (or create the schemas beforehand), ownership or `MANAGE` on the reporting schema (needed for the grants), and `CAN USE` on the SQL warehouse.
3. **Create the groups** used for `owner_group` and `viewer_group`, or change the variables to groups that already exist.
4. **Fill in the target file** (`targets/test.yml` or `targets/prod.yml`):

```yaml
targets:
  test:
    workspace:
      host: https://adb-1234567890.12.azuredatabricks.net   # uncomment and set
    variables:
      catalog: analytics_dq_test
      warehouse_id:
        lookup:
          warehouse: "Shared Serverless Warehouse"           # warehouse NAME, not id
      service_principal: 00000000-0000-0000-0000-000000000000
      notification_email: dq-team@company.com
```

The host cannot be a variable, because the CLI needs it before it resolves variables. You can leave it commented out and set `DATABRICKS_HOST` instead (as the sample CI does). Keeping it in the file protects against deploying to the wrong workspace.

5. **Deploy** from CI (section 10) or by hand, authenticated as the service principal:

```bash
export DATABRICKS_HOST=https://<test-workspace>
export DATABRICKS_CLIENT_ID=<sp-application-id>
export DATABRICKS_CLIENT_SECRET=<sp-oauth-secret>
bash scripts/deploy.sh test
```

---

## 8. Configuration reference

All variables are declared, with descriptions, in `databricks.yml`. Switches are strings: `"true"` or `"false"`.

### Locations

| Variable | Default | Meaning |
|---|---|---|
| `catalog` | `analytics_dq_dev` | Catalog of the reporting schema (views + dashboard data) |
| `schema` | `reporting` | Schema of the views |
| `source_catalog` | same as `catalog` | Catalog of the DQ framework tables |
| `results_schema` | `results` | Framework result tables |
| `configuration_schema` | `configuration` | Framework configuration tables |
| `metadata_schema` | `metadata` | Framework metadata tables |
| `quarantine_schema` | `quarantine` | Where the framework writes quarantined rows |
| `warehouse_id` | none, set per target | SQL warehouse of the dashboard. Use a `lookup` by warehouse name |

### Switches

| Variable | Default | When to change |
|---|---|---|
| `create_catalog` | `"false"` | `"true"` on a fresh metastore where the deploying identity may create catalogs |
| `create_schemas` | `"true"` | `"false"` if schemas are managed by a platform team |
| `create_base_tables` | `"true"` | `"false"` if the framework tables are created elsewhere |
| `deploy_base_views` | `"true"` (dev: `"false"`) | `"false"` where someone maintains the base views by hand |
| `deploy_ext_views` | `"true"` | Rarely |
| `run_smoke_tests` | `"true"` | Rarely |

### Access and notifications

| Variable | Default | Meaning |
|---|---|---|
| `owner_group` | `dq-framework-admins` | `CAN_MANAGE` on dashboard and jobs, `ALL PRIVILEGES` on the reporting schema |
| `viewer_group` | `users` | `CAN_RUN` on the dashboard (test/prod: `CAN_VIEW` on the jobs), `USE` + `SELECT` on the reporting schema |
| `service_principal` | placeholder | Application ID that runs the jobs in test and prod |
| `notification_email` | placeholder | Receives job failure emails |

### Dashboard and schedules

| Variable | Default | Meaning |
|---|---|---|
| `dashboard_display_name` | `Data quality` | Dashboard name |
| `timezone` | `Europe/Athens` | Timezone of both schedules |
| `setup_cron` | `0 0 5 ? * MON` | `dq_setup` every Monday 05:00 (Quartz cron) |
| `healthcheck_cron` | `0 0 7 * * ?` | `dq_healthcheck` every day 07:00 |
| `schedule_status` | `PAUSED` (test/prod: `UNPAUSED`) | Dev mode pauses schedules regardless |
| `freshness_max_days` | `3` | Health check fails if the newest DQ execution is older |

### Business parameters (change the numbers on the dashboard)

These values are written into the views by `dq_setup`. Agree changes with the dashboard owner, then re-run `dq_setup`.

| Variable | Default | Meaning |
|---|---|---|
| `weight_high` / `weight_medium` / `weight_low` | `3` / `2` / `1` | Weight of a rule in the quality score, by criticality |
| `weight_default` | `2` | Weight when the criticality is unknown |
| `score_k_shrink` | `3` | How strongly tables with few rules are pulled towards the average score |
| `score_stale_factor` | `2` | A table is stale (left out of the score) when its last run is older than this factor times its usual interval |
| `score_interval_window_days` | `60` | Days used to measure a table's usual run interval |
| `score_min_days_for_interval` | `3` | Minimum run days before a table can be stale |
| `score_warning_penalty` | `0.5` | Share of a Warning's failed rows that counts against the score (0 to 1) |

---

## 9. Extending the bundle

### 9.1 Add a view or a check

1. Add a file with the next number to the right folder, for example `src/sql/30_ext_views/05_v_simple_my_view_ext.sql`.
2. Use placeholders for every catalog and schema: `${catalog}.${schema}.<view>` and `${source_catalog}.${metadata_schema}.<table>`.
3. Finish every statement with `;`. One file may hold several statements.
4. Deploy. The next `dq_setup` run picks up the file. No YAML change is needed.

For a smoke test or health check, write a `SELECT` that calls `raise_error('message')` when the condition fails (see `src/sql/95_health_checks/01_freshness.sql`).

### 9.2 Add a new placeholder

1. Declare the variable in `databricks.yml` (with a description and a default).
2. Pass it to the task in `resources/jobs/dq_setup.job.yml` under `base_parameters`, using the same name: `my_value: ${var.my_value}`.
3. Use `${my_value}` in the SQL file.

If step 2 is missed, the runner stops with "no parameter for placeholder(s) ['my_value']" before running any SQL.

### 9.3 Change the dashboard

1. Edit the deployed dashboard in the dev workspace (or a copy of it).
2. Export it back into the bundle:

```bash
databricks bundle generate dashboard --resource data_quality --force -t dev
```

3. Make sure the queries use table names **without** catalog and schema (`FROM v_simple_results`, not `FROM analytics_dq_dev.reporting.v_simple_results`). A fully qualified name ties the dashboard to one workspace.
4. Check `resources/dashboards/data_quality.dashboard.yml` with `git diff`. If the export wrote fixed values (for example a `warehouse_id`), put the `${var.*}` references and the permissions back.
5. Commit the updated files.

### 9.4 Add another dashboard or job

Add its source under `src/` and one YAML file under `resources/dashboards/` or `resources/jobs/`. The `include: resources/**/*.yml` line in `databricks.yml` picks it up automatically.

### 9.5 Add an environment

Copy `targets/test.yml` to `targets/<name>.yml`, rename the key under `targets:` and set its values. No other change is needed.

---

## 10. CI/CD

`ci/azure-devops.yml` is a ready-to-adapt Azure DevOps pipeline:

* Every push to `feature/*` or `main`: **validate**, then **deploy to test** and run `dq_setup`.
* Push to `main` only: then **deploy to prod**, through the Azure DevOps environment `production` (add an approval check there).

Pipeline variables to create (mark tokens as secret):

| Variable | Value |
|---|---|
| `DATABRICKS_HOST_TEST`, `DATABRICKS_HOST_PROD` | Workspace URLs |
| `DATABRICKS_TOKEN_TEST`, `DATABRICKS_TOKEN_PROD` | Service principal tokens |
| `DQ_NOTIFICATION_EMAIL` | Team address |

For GitHub Actions or GitLab, use the same three commands as `scripts/deploy.sh`, install the CLI with the official installer (<https://github.com/databricks/setup-cli>), and pass values as `BUNDLE_VAR_<name>` environment variables. Using a service principal with OAuth (`DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET`) is preferred over personal tokens.

---

## 11. Day-to-day operations

| Task | How |
|---|---|
| Apply a configuration change | Change the variable, then `bash scripts/deploy.sh <target>` |
| Rebuild views only | Run the `dq_setup` job in the workspace UI, or `databricks bundle run dq_setup -t <target>` |
| Check freshness now | `databricks bundle run dq_healthcheck -t <target>` |
| See what is deployed | `databricks bundle summary -t <target>` |
| Remove dashboard and jobs | `databricks bundle destroy -t <target>`. Schemas, tables and views stay |

Re-running `dq_setup` is always safe: tables are only created when missing, views are replaced with the definitions in `src/sql/`, and grants are re-applied.

---

## 12. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `validate` fails: `could not find warehouse` | The `warehouse_id` lookup needs the exact warehouse **name** of that workspace |
| `validate` / `deploy` fails on `dataset_catalog` | CLI older than 0.281.0. Update the CLI |
| `bootstrap` fails on `GRANT` | The identity running `dq_setup` does not own (or have `MANAGE` on) the reporting schema or catalog, or a group does not exist |
| `bootstrap` fails on `CREATE SCHEMA` | Missing `CREATE SCHEMA` on the catalog, or the catalog does not exist (set `create_catalog: "true"` or create it) |
| Runner: `no parameter for placeholder(s)` | A SQL file uses `${x}` but the task has no `x` parameter. See 9.2 |
| `smoke_tests` fails: table or view not found | A previous task was disabled (`enabled: false`) and the object does not exist in this workspace |
| `dq_healthcheck` fails: `No DQ executions found` | The DQ framework has not run yet in this workspace. Expected right after a new install |
| Dashboard shows `TABLE_OR_VIEW_NOT_FOUND` | `dq_setup` has not run successfully yet, or `catalog` / `schema` do not match where the views were created |
| Dashboard is empty but has no errors | The framework tables have no rows yet |
| Prod deploy fails on `run_as` | `service_principal` is still the placeholder, or the SP is not in the workspace |

---

## 13. Known limitations

* **Base view copies.** `src/sql/20_base_views` holds copies of the base views as they were in the reference dev workspace on 2026-11-26. If the views are changed by hand in dev, copy the new definitions into these files, otherwise test and prod keep the old logic.
* **Grants always run.** `bootstrap` applies the grants whenever `owner_group` / `viewer_group` are set. There is no separate switch yet.
* **Fixed time windows.** The 7-day and 30-day windows of the dashboard are written in the dashboard queries, not in variables.
* **Serverless only.** The jobs need serverless compute for jobs. For classic compute, add a `job_clusters` block to the job files.
* **Not tested end to end yet.** `bundle validate` passes for `dev`. A full `deploy` and `dq_setup` run has not yet been done in any workspace. Do this in dev before the first test deploy.

