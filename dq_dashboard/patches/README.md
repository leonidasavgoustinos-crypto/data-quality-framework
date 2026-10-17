# Framework patches

All patches are applied on the feature branch (commits listed in **APPLY_AT_END.md**). Merging to `main` is a separate,
optional step. The sandbox wheel is still built from `9c671ac` plus the patches (`../scripts/build_patched.sh`), which
gives the same framework code as the branch.

| # | Finding | Files | Default behaviour change | Sandbox build | Status |
|---|---|---|---|---|---|
| 01 | F-21 cache on serverless (Blocker) | framework/utils.py, dq_framework.py | none on classic; serverless works uncached | yes | ✅ `60348c2` |
| 02 | F-20 stale ids in failure path (High) | framework/utils.py, dq_framework.py | failure path only | yes | ✅ `d0abcb7` |
| 03 | F-01/F-19/F-02 ids at open + error_message (High) | framework/utils.py | failed rows carry ids + message | yes | ✅ `38b3051` |
| 04 | F-03 opt-in `on_rule_error` (High) | framework/dq_framework.py, utils.py, run_time_params.py | none (`raise` default) | yes | ✅ `c221144` |
| 05 | F-13 setup keeps tables (Med) | setup/01_ddl.py | **yes**: rerun no longer recreates | no | ✅ `72e3a0c` |
| 06 | C-01 guard before TRUNCATE (High) | configuration/dataset_creation.ipynb | demo needs confirmation | no | ✅ `72e3a0c` |
| 07 | F-22 insert gate reads results.result (High) | framework/combined_dq_and_df_load.py | **yes**: gate now blocks non-pass | yes | ✅ `09a098b` |
| 08 | V-02/V-04 views additive (Med) | setup/03_reporting_views.py | none (columns added at the end) | yes | ✅ `a526ee6` |

Commits and checks: **APPLY_AT_END.md**. Sandbox build: `scripts/build_patched.sh` (`PATCHES="01 02 03 04 07 08"` by default).
Current sandbox wheel (base `9c671ac`): sha256 `56dfb4d085a0bda10f4c379645172cab95191327ea38041b86093aec98eea09d` (content identical to the pilot-verified build except a comment line in _build_info.py).

## Tests (no Databricks needed)
`tests/fake_env.py` provides in-memory fakes for pyspark/delta/databricks/DQX. `tests/test_framework_patches.py` runs the real
framework code against them and checks the rows written to result / result_table / result_rule:
```bash
cd dq_dashboard/patches/tests
python3 -I test_framework_patches.py --framework /path/to/repo --stage base   # before applying: bugs reproduced
python3 -I test_framework_patches.py --framework /path/to/repo --stage 04     # after all four patches
```
Each check declares the stages in which it must pass. The script exits non-zero on any deviation, including a check that
unexpectedly passes on `base`. Result 2026-10-15: base/01/02/03/04 all "OK" (11 checks); `test_other_patches.py` unpatched and with 05–08: "OK" (13 checks).
Not covered locally (needs Databricks): real serverless errors, DQX, Delta concurrency. That is done by the pilot rerun (see each .md).

## Not patched
See `../docs/framework-findings-for-colleague.md` (🔴 items).
