# Migration to the company workspace: checklist

The whole project switches workspace by switching the bundle target: `free` → `company`.
Everything environment-specific lives in `bundle/databricks.yml`. Scripts read it through `bundle/src/driver/env_config.py`.
All paths and commands below are relative to `dq_dashboard/` unless they say otherwise.
On `company`, nothing sandbox-only can run. Guards refuse it before any SQL is sent (tested by `scripts/test_guards.py`).

## 0. Fix the framework first (in the repo, by its owners)
These must be fixed before the company relies on the dashboard; see `framework_findings.md`.

| Must fix | Why it matters for the dashboard |
|---|---|
| **F-21** cache() on serverless | Without it, serverless runs evaluate nothing. With the fix, the company needs no shim (`cache_mode` can stay `auto` as a safety net, or `off`) |
| **F-01 / F-19** ids written at open time | code_error rows become attributable; trace not needed (`trace_enabled=false`) |
| **F-02** error_message / rule_dimension_id / alert | 3.5 and 7.x widgets get real data instead of text cards |
| **F-03** per-rule failure handling | one bad rule no longer aborts the whole run |
| **F-20** stale ids in the except block | closed good results are no longer overwritten as code_error |
| **F-04** invalid `PK_violation_custom_error` SQL | otherwise permanent code_error in project_2/gold |
| **F-13** `CREATE OR REPLACE` on results | rerunning setup would wipe the history the dashboard shows |

Recommended next: F-08 (quarantine history), F-10 (store the filter value), F-14 (expected schedule), V-01..V-05.

### How the framework reaches the workspace
The framework is installed as a wheel by the bundle; it is never copied into the workspace by hand and `framework/` is not synced.
- The bundle artifact `dq_framework_patched` runs `scripts/build_patched.sh`: a clean copy of the repo's `framework/` → the selected patches → the local patch tests → a reproducible wheel in `bundle/dist/`, plus `bundle/src/driver/build_manifest.json` (version, sha256, patches, source commit).
- `databricks bundle deploy` uploads that wheel and the job installs it: target `free` as a serverless environment dependency, target `company` as a cluster library (`whl: ./dist/*.whl`), next to `databricks-labs-dqx`.
- Every run, the driver checks that the installed framework is the wheel named in `build_manifest.json` (it aborts otherwise) and logs version, sha256 and patches in `sandbox_log`.
- Sandbox default patch set: `01 02 03 04 07 08` (wheel `1.1.0+dqd.p01.p02.p03.p04.p07.p08`). 05 and 06 only touch `setup/` and `configuration/`, so they are not in the wheel.

## 1. Configure access
- [ ] `databricks auth login --host https://<company-workspace> --profile COMPANY`
- [ ] `databricks auth describe -p COMPANY`: the host is correct and you are the expected user or service principal.
- [ ] `bundle/databricks.yml` → target `company` keeps `workspace.profile: COMPANY`.

## 2. Set the target variables (bundle/databricks.yml → targets.company.variables)
| Variable | Value |
|---|---|
| `expected_host` | exact company workspace URL. Every script compares it with the connected host |
| `dq_catalog` | catalog of the DQ framework. **Must equal `framework/constants.py` `CATALOG`** (the driver aborts otherwise) |
| `dashboard_schema` | `dq_dashboard` (or the agreed name) |
| `warehouse_id` | SQL warehouse for the dashboard and SQL scripts |
| `cluster_id` | classic cluster for the driver, or switch the job to serverless as in target `free` (needs F-21 fixed or `cache_mode=auto`) |
| `sandbox_enabled`, `trace_enabled`, `synthetic_enabled`, `simulated_dates_enabled` | **false** (`env_config` refuses `sandbox_enabled=true` on `company`) |
| `cache_mode` | `auto` |

`env_config` refuses any value still containing `<SET: …>`.

## 3. Validate (read-only)
- [ ] `cd bundle && databricks bundle validate --strict -t company`
- [ ] `python3 -I scripts/test_guards.py`: every sandbox-only action is refused.
- [ ] `python3 -c "import sys; sys.path.insert(0,'bundle/src/driver'); import env_config as E; print(E.connect_local('company').summary())"`: host check passes.
- [ ] Ask the catalog owner for grants: `USE CATALOG`, `USE SCHEMA` and `SELECT` on `<dq_catalog>.results/metadata/configuration`; `CREATE SCHEMA` (or a pre-created `dashboard_schema`); `SELECT` on `system.billing`, `system.access.audit`, `system.lakeflow`, `system.information_schema`.

## 4. Create the dashboard schema objects (additive only)
- [ ] `python3 -I scripts/run_ddl.py --target company ddl/02_capture.sql --dry-run`, then without `--dry-run`. This creates the capture tables as **empty stubs** plus the trace volume. No data.
- [ ] Phase 2 view DDL: `python3 -I scripts/run_ddl.py --target company ddl/<views>.sql`. The `*_synthetic` stubs are created **empty**, never filled.
- [ ] Do **not** run `01_sources.sql` / `03_sandbox.sql`; they are refused anyway.

## 5. Deploy
- [ ] Framework wheel: once the patches are applied to the repo (`patches/APPLY_AT_END.md`), the wheel must be built from the repo as it is, with no patches on top, so set `PATCHES=""` for the build and the deploy (the default set would no longer apply). The repo working tree must be clean.
- [ ] `PATCHES="" bash scripts/build_patched.sh`, then check `bundle/src/driver/build_manifest.json`: version, sha256 and `source_commit` are the ones you expect.
- [ ] `cd bundle && PATCHES="" databricks bundle deploy -t company`. The artifact rebuilds the same wheel, deploy uploads it with the driver, and the job installs it on `cluster_id`. Production mode: no `[dev …]` prefixes; the dashboard is published as "Data Quality Monitor".
- [ ] Optional: `databricks bundle run dq_driver -t company --params mode=run,scopes=<apply_at|project,...>`. Real runs only; capture rows are observed data.

## 6. Verify
- [ ] The first driver run's `sandbox_log` shows the wheel version and sha256 from `build_manifest.json`.
- [ ] Every page renders; ★ widgets show company sources or "data not available" cards (see the widget status list, "★ widgets per target").
- [ ] No widget reads `*_synthetic` rows (all stubs are empty): `SELECT count(*)` on each stub = 0.
- [ ] Every query runs in under 10 s on the company warehouse.
- [ ] `python3 -I scripts/teardown.py --target company --step 1` (preview) shows only ids from driver runs. Steps 2–4 are refused on company by design.

## Rollback
- `databricks bundle destroy -t company` removes the job and dashboard.
- Rows written by driver runs: `scripts/teardown.py --target company --step 1 --execute` (deletes only by recorded ids).
- The dashboard schema can be dropped by the catalog owner. No script is allowed to drop it on company.
