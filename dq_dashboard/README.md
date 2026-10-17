# dq_dashboard: Data Quality Monitor

A Databricks Asset Bundle that runs the Data Quality Framework of this repo and builds the
"Data Quality Monitor" Lakeview dashboard on top of its results.

- A **driver job** runs `run_data_quality(...)` per (apply_at, project). Against target `free`, it can also load
  sandbox source data for simulated days. It records what the framework does not store itself
  (simulated run dates, error messages, quarantine snapshots) in the dashboard schema.
- The **framework** is installed as a wheel built from a fixed repo commit plus the fixes in `patches/`.
  The repo itself is not changed by the build.
- The **dashboard** (`bundle/src/dashboards/dq_monitor.lvdash.json`) is deployed by the bundle.

Two targets: `free` (personal sandbox: serverless, sandbox data allowed) and `company` (real runs only; every
sandbox-only action is refused). See `docs/MIGRATION.md` for the switch to `company`.

## Layout
| Path | Content |
|---|---|
| `bundle/` | `databricks.yml` (targets, variables), `resources/` (job, dashboard), `src/driver/` (driver, env config, helpers, build manifest), `src/dashboards/` |
| `scripts/` | `build_patched.sh` (wheel), `check_host.sh` (deploy host check), `gen_ddl.py`, `run_ddl.py`, `teardown.py`, `test_guards.py` |
| `ddl/` | `01_sources.sql` (sandbox source tables), `02_capture.sql` (capture tables, all targets), `03_sandbox.sql`, `teardown.sql` (template), `registered_tables.json` |
| `patches/` | framework fixes: one `.diff` + `.md` each, `APPLY_AT_END.md`, `tests/` (local, no Databricks) |
| `tests/` | local test for the driver's `run_calendar.call_status` |
| `docs/` | `MIGRATION.md`, `framework_findings.md`, `framework-findings-for-colleague.md` |

## Prerequisites
- Databricks CLI with a profile for the workspace (target `free` uses profile `DEFAULT`), Python 3, `patch`, `git`.
- Workspace-specific values are kept out of git. For target `free`, create
  `bundle/.databricks/bundle/free/variable-overrides.json` (ignored by `.gitignore`):
  ```json
  {
    "expected_host": "https://<workspace>.cloud.databricks.com",
    "warehouse_id": "<sql warehouse id>"
  }
  ```
  `expected_host` must equal the host of the CLI profile. This is checked in three places, each before anything
  is written:
  - `bundle deploy` / `bundle plan`: the artifact build runs `scripts/check_host.sh` first;
  - the driver job at start-up (`env_config.assert_host`);
  - every local script (`env_config.connect_local`).
- Local scripts run SQL through the Statement Execution API (`databricks api post /api/2.0/sql/statements`) on
  `warehouse_id`. Any final state other than `SUCCEEDED` stops the script with the server's error message.

All commands below are run from the repo root unless they say otherwise.

## Build
```bash
bash dq_dashboard/scripts/build_patched.sh            # default: base commit from build_manifest.json, patches 01 02 03 04 07 08
BASE_COMMIT=9c671ac PATCHES="01 02 03 04" bash dq_dashboard/scripts/build_patched.sh
```
The script reads the base files with `git show` (no checkout), applies the patches, and runs the local patch tests.
It then writes:
- the wheel to `dq_dashboard/bundle/dist/`;
- `bundle/src/driver/build_manifest.json` (version, sha256, patches, source commit);
- scratch files to `dq_dashboard/.build/`.

The build is reproducible: the same inputs give the same wheel sha256 and the same `build_manifest.json`.
`built_at` is the BASE_COMMIT commit time, or `SOURCE_DATE_EPOCH` if set. The build fails if anything outside
`dq_dashboard/` has uncommitted changes.

Current wheel (`1.1.0+dqd.p01.p02.p03.p04.p07.p08`, base `9c671ac`): sha256 `56dfb4d085a0bda10f4c379645172cab95191327ea38041b86093aec98eea09d`
(content identical to the pilot-verified build except a comment line in _build_info.py).

## Validate and deploy
```bash
cd dq_dashboard/bundle
databricks bundle validate --strict -t free
databricks bundle plan -t free
databricks bundle deploy -t free          # runs the build above as the bundle artifact
```

## Run (optional)
```bash
databricks bundle run dq_driver -t free --params mode=run,scopes=<apply_at|project,...>
```
The job has no schedule. Backfill parameters (`mode=backfill`, `start`, `ndays`, `tables`, `fault`) are accepted
on target `free` only.

## Sandbox DDL (target `free` only)
```bash
export DQ_SANDBOX_OBJECTS=<path/to/sandbox_objects.json>     # or pass --objects <path> to each script
python3 -I dq_dashboard/scripts/gen_ddl.py --target free
python3 -I dq_dashboard/scripts/run_ddl.py --target free dq_dashboard/ddl/01_sources.sql --dry-run
python3 -I dq_dashboard/scripts/run_ddl.py --target free dq_dashboard/ddl/02_capture.sql --dry-run
```
`sandbox_objects.json` lists the catalogs, schemas and quarantine tables created for the sandbox. It names the
workspace user, so keep it outside `dq_dashboard/`. There is no default path: `gen_ddl.py` and `teardown.py`
(steps 2-5) take `--objects`, else `DQ_SANDBOX_OBJECTS`, and stop with an error if neither is set.

## Teardown
Every step previews first. Nothing changes without `--execute`.
```bash
python3 -I dq_dashboard/scripts/teardown.py --target free --step 1                    # rows written by driver runs (by recorded ids)
python3 -I dq_dashboard/scripts/teardown.py --target free --step 1 --execute
python3 -I dq_dashboard/scripts/teardown.py --target free --step 2                    # 2-5: sandbox only (DROPs, bundle destroy)
```
On `company`, only step 1 is allowed, and nothing is ever dropped.

## Local tests (no Databricks)
```bash
python3 -I dq_dashboard/scripts/test_guards.py
python3 -I dq_dashboard/tests/test_driver_call_status.py --driver dq_dashboard/bundle/src/driver/dq_backfill_driver.py --expect new
cd dq_dashboard/patches/tests && python3 -I test_framework_patches.py --framework <repo copy> --stage 04
```

## Framework fixes
All fixes in `patches/` are applied on the feature branch. `patches/APPLY_AT_END.md` lists the commits and the
checks that the branch code equals the sandbox wheel. Merging to `main` is a separate, optional step.
