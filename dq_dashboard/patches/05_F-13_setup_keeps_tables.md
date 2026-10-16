# 05 · F-13: rerunning setup wipes results and configuration (Med) · status: needs colleague OK

**Root cause**: `setup/01_ddl.py` creates all 11 tables with `CREATE OR REPLACE TABLE` (results at :24/:45/:75, configuration and
metadata at :100–:291). Rerunning setup, e.g. to add a column, silently deletes every result (the history the dashboard shows)
and all configuration.

**Fix** (+4 lines, 11 lines changed): new widget `reset_tables` (dropdown, default `false`).
`create_table = "CREATE OR REPLACE TABLE" if reset_tables == "true" else "CREATE TABLE IF NOT EXISTS"`, used by all 11 statements.

**Behaviour change: yes, deliberately.** A default rerun no longer recreates tables. Whoever relied on "rerun setup = clean slate
/ apply schema changes" must now set `reset_tables=true` (or use `00_drop_catalog.py`). Schema changes to existing tables then
need an explicit `ALTER TABLE` or a reset. **Needs the colleague's OK.**
Not changed: the hard-coded `environment = 'dev'` at :9, which overrides the widget (separate Low issue, listed 🔴).

**Tests** (`tests/test_other_patches.py`): "setup default keeps existing tables" (fails before, passes after) and
"reset_tables=true (or original) recreates tables". 01_ddl.py is executed against a recording fake spark/dbutils.
Not in the sandbox build (no runtime effect here).
