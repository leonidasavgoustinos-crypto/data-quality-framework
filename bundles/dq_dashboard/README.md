# dq_dashboard — Declarative Automation Bundle

Deploys the **Data quality** AI/BI dashboard together with all the views and framework tables it needs. Plug-and-play: a single command sets up an empty workspace from scratch.

## Layout

```
databricks.yml          variable declarations, includes
targets/
  dev.yml               development workspace (views NOT replaced)
  test.yml              test workspace (all objects rebuilt)
  prod.yml              production workspace (all objects rebuilt)
resources/
  dashboards/data_quality.dashboard.yml
  jobs/dq_setup.job.yml          one-time (or weekly) full rebuild
  jobs/dq_healthcheck.job.yml    daily freshness check
src/
  dashboards/data_quality.lvdash.json
  notebooks/
    bootstrap.ipynb         creates catalogs, schemas, grants
    run_sql_folder.ipynb    generic runner (reusable in any project)
  sql/
    10_base_tables/     DQ framework tables (IF NOT EXISTS)
    20_base_views/      v_simple_* base views (Eva's definitions)
    30_ext_views/       v_simple_*_ext extension views
    90_smoke_tests/     structural checks after setup
    95_health_checks/   daily freshness check
scripts/deploy.sh       one-command deploy + setup
ci/azure-devops.yml     sample CI pipeline
```

## First deploy to a new workspace

1. Fill in `targets/<env>.yml` (or set env vars):
   * `workspace.host` — workspace URL
   * `warehouse_id.lookup.warehouse` — SQL warehouse name
   * `service_principal` — application ID (test / prod only)
   * `notification_email` — team address
2. Authenticate: `databricks auth login --host <host>` or set `DATABRICKS_HOST` + `DATABRICKS_TOKEN`.
3. Run:

```bash
cd bundles/dq_dashboard
bash scripts/deploy.sh dev        # or test / prod
```

This will: validate → deploy → run `dq_setup` (bootstrap → tables → views → ext views → smoke tests).

## Variables reference

All variables are declared with descriptions in `databricks.yml`. Key ones:

| Variable | Description | Dev default |
|---|---|---|
| `catalog` | Catalog with the reporting schema | `analytics_dq_dev` |
| `source_catalog` | Catalog with the DQ base tables | same as `catalog` |
| `deploy_base_views` | Rebuild Eva's base views | `false` in dev |
| `weight_high/medium/low` | Score criticality weights | `3/2/1` |
| `freshness_max_days` | Health check threshold (days) | `3` |

## Adding a new view

Create a numbered `.sql` file in `src/sql/30_ext_views/` (or `20_base_views/` for base views). Use `${catalog}.${schema}` for the view name. No other file needs changing — `run_sql_folder` picks it up automatically on the next `dq_setup` run.

## Reusing this bundle for another project

Copy `bundles/dq_dashboard/` into the new project repository. Update:
* `databricks.yml` → `bundle.name`
* `targets/*.yml` → workspace hosts, catalog names, warehouse name
* `src/sql/` → your own SQL files
* `src/dashboards/` → your dashboard JSON

The `run_sql_folder` runner, `bootstrap` notebook, CI pipeline, and `deploy.sh` require no changes.
