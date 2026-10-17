# Applying the patches to the repo

All eight patches are applied on the feature branch and pushed. Merging to `main` is a separate,
optional step.

## Status on the feature branch (2026-10-17)
| Patch | Finding | Commit | Message |
|---|---|---|---|
| 01 | F-21 | `60348c2` | `fix(framework): continue uncached when cache() is not supported on serverless (F-21)` |
| 02 | F-20 | `d0abcb7` | `fix(framework): never overwrite closed result rows from the failure path (F-20)` |
| 03 | F-01, F-19, F-02 | `38b3051` | `fix(framework): write ids on open, store error_message and rule_dimension_id (F-01, F-19, F-02)` |
| 04 | F-03 | `c221144` | `feat(framework): opt-in on_rule_error=continue; default raise unchanged (F-03)` |
| 07 | F-22 | `09a098b` | `fix(pipeline): gate run_dq_and_insert on results.result status (F-22)` |
| 08 | V-02, V-04 | `a526ee6` | `feat(reporting): add id and numeric duration columns to views (V-02, V-04)` |
| 05 + 06 | F-13, C-01 | `72e3a0c` | `Keep existing DQ tables on setup rerun; require confirmation before TRUNCATE in dataset_creation` |

- 01–04, 07 and 08 sit on top of `e2f69c9`, in the required order (01 → 02 → 03 → 04 are cumulative; 07 and 08 are
  independent of each other).
- `72e3a0c` equals patches 05 + 06 except 3 comment lines without the finding IDs.
- None of the `.diff` files applies again on the feature branch (`git apply --check` reports "patch failed"); that is expected.

## Verified (2026-10-17, read-only)
1. Local tests against the branch itself:
   ```bash
   cd dq_dashboard/patches/tests
   python3 -I test_framework_patches.py --framework ../../.. --stage 04                                   # [04] OK
   python3 -I test_other_patches.py --root ../../.. --original ../../.build/base --applied 05,06,07,08     # OK
   ```
2. The branch code equals the sandbox wheel. `framework/*.py` (7 files) and `setup/03_reporting_views.py` on `HEAD`
   are byte-identical to `dq_dashboard/.build/src/`, and to the `framework/` files inside the deployed wheel. The
   wheel only adds `framework/_build_info.py`.
   - Sandbox wheel: `1.1.0+dqd.p01.p02.p03.p04.p07.p08`, base `9c671ac`.
   - sha256 `56dfb4d085a0bda10f4c379645172cab95191327ea38041b86093aec98eea09d` (content identical to the
     pilot-verified build except a comment line in _build_info.py).
3. Databricks (free workspace): the patch 08 views have been live in `analytics_dq_dev.reporting` since 2026-10-16.
   Original columns compared with `EXCEPT ALL` in both directions → 0 rows; mm:ss vs seconds → 0 mismatches.

## Building from the branch
The sandbox build still uses `BASE_COMMIT=9c671ac` plus the patches, which gives the same framework code as `HEAD`.
Building straight from the branch (`BASE_COMMIT=a526ee6 PATCHES=""`) is **not supported yet**: with no patches,
`build_patched.sh` runs the local tests at stage `base`, which expects the unfixed bugs, so the build would fail. It
needs a small change to the build script first. That wheel would also get a different version label and sha256.

## Next
- Optional: merge the feature branch to `main`.
- Remove the sandbox-only workarounds that the fixes make unnecessary (cache shim, trace, …) once a run without them
  is confirmed.
