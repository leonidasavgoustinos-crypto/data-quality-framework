# Applying the patches to the repo: END OF PROJECT ONLY

Until then, nothing is applied and nothing is committed: `git -C .. status --short` must stay empty. The sandbox uses a wheel
built from a copy (`scripts/build_patched.sh`).

## Status on feature/anr (2026-10-15)
- **05 (F-13) and 06 (C-01) are already on `feature/anr` as commit `72e3a0c`** (authored by Apostolos-Nikolaos Rigas,
  pushed to `origin/feature/anr`, **not merged**). Its content equals patches 05 + 06 except 3 comment lines
  without the finding IDs. Do **not** apply 05/06 again (`git apply --check` correctly fails: "patch failed").
- **Remaining to apply at the end: 01 → 02 → 03 → 04, then 07 and 08.**
- The final apply must be done **on top of `72e3a0c` or later**. The sandbox wheel is built from the pinned base `9c671ac`
  (`build_patched.sh`, `BASE_COMMIT`), whose `framework/` is identical to `72e3a0c`'s.

## Required order
| Group | Patches | Rule |
|---|---|---|
| framework core | 01 → 02 → 03 → 04 | cumulative: designed and tested only in this order |
| independent | 07, 08 | after 01–04 (07: `framework/combined_dq_and_df_load.py`, 08: `setup/03_reporting_views.py`) |
| done | 05, 06 | already committed in `72e3a0c` |

Verified 2026-10-15 with `git apply --check --whitespace=error` on plain copies of the files at `72e3a0c` (taken read-only
with `git show`; no git state touched): `01 02 03 04 07 08` all OK. On the result, `test_framework_patches.py --stage 04` → OK
and `test_other_patches.py --applied 05,06,07,08` → OK. 05/06 → "patch failed" (already in the base, as expected).

## Commands (repo root, feature/anr at 72e3a0c or later)
```bash
git status --short                      # must be empty
git merge-base --is-ancestor 72e3a0c HEAD && echo "base OK"
P=dq_dashboard/patches
for n in 01 02 03 04 07 08; do
  f=$(ls $P/${n}_*.diff); git apply --check --whitespace=error "$f" || { echo "STOP: $f"; break; }
done
git apply $P/01_F-21_cache_on_serverless.diff                         && git add framework/ && git commit -m "<msg 01>"
git apply $P/02_F-20_stale_ids_in_failure_path.diff                   && git add framework/ && git commit -m "<msg 02>"
git apply $P/03_F-01_F-19_F-02_ids_at_open_and_error_message.diff     && git add framework/ && git commit -m "<msg 03>"
git apply $P/04_F-03_on_rule_error_flag.diff                          && git add framework/ && git commit -m "<msg 04>"
git apply $P/07_F-22_insert_gate_reads_result_status.diff             && git add framework/ && git commit -m "<msg 07>"
git apply $P/08_V-02_V-04_views_additive_ids_and_seconds.diff         && git add setup/03_reporting_views.py && git commit -m "<msg 08>"
```

## Suggested commit messages (one commit per finding)
| # | Message |
|---|---|
| 01 | `fix(framework): continue uncached when cache() is not supported on serverless (F-21)` |
| 02 | `fix(framework): never overwrite closed result rows from the failure path (F-20)` |
| 03 | `fix(framework): write ids on open, store error_message and rule_dimension_id (F-01, F-19, F-02)` |
| 04 | `feat(framework): opt-in on_rule_error="continue"; default raise unchanged (F-03)` |
| 07 | `fix(pipeline): gate run_dq_and_insert on results.result status (F-22)` |
| 08 | `feat(reporting): add id and numeric duration columns to views (additive) (V-02, V-04)` |
| 05 / 06 | already committed as `72e3a0c` |

## Verify after applying
1. Local tests against the repo itself:
   ```bash
   cd dq_dashboard/patches/tests
   python3 -I test_framework_patches.py --framework ../../.. --stage 04
   python3 -I test_other_patches.py --root ../../.. --original <copy of 72e3a0c> --applied 05,06,07,08
   ```
2. The wheel built from the repo equals the sandbox wheel. Same files: compare `framework/*.py` with `dq_dashboard/.build/src/framework/*.py`
   (only `_build_info.py` differs). Sandbox wheel `1.1.0+dqd.p01.p02.p03.p04.p07.p08`, base `9c671ac`:
   sha256 `56dfb4d085a0bda10f4c379645172cab95191327ea38041b86093aec98eea09d` (content identical to the pilot-verified build except a comment line in _build_info.py).
3. Databricks: redeploy setup views (08) and compare old vs new view columns (`EXCEPT ALL` in both directions → 0 rows); one
   framework run on serverless without the driver shim (F-21).
4. Remove the sandbox-only workarounds that the patches make unnecessary (cache shim, trace, …) as planned.
