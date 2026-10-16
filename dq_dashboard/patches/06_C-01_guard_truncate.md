# 06 · C-01: demo notebook truncates the configuration without warning (High) · status: needs colleague OK

**Root cause**: the first (and only) cell of `configuration/dataset_creation.ipynb` starts with
`TRUNCATE TABLE analytics_dq_dev.configuration.run_policy / rule_template / rule_assignment`. Running the demo notebook on a
shared workspace destroys the real configuration.

**Fix** (+30 lines, nothing removed)
- New first cell (Python): widget `confirm_truncate`. Unless it equals `TRUNCATE analytics_dq_dev.configuration`, it raises
  `Refusing to TRUNCATE…`, so "Run all" stops there.
- The SQL cell itself starts with `SELECT assert_true(:confirm_truncate = '…', '…')`, so it also refuses when run on its own.
  On runtimes without named-parameter widgets the statement errors, which also stops the cell before any TRUNCATE (fail-safe).

**Behaviour change**: the demo needs one extra, explicit confirmation. TRUNCATE statements and data generation are unchanged.

**Risk**: low. Not verified on Databricks: that `:confirm_truncate` in a `%sql` cell resolves to the widget value (Databricks
notebook named parameters). Test in the colleague's workspace first.

**Tests** (local): guard refuses without confirmation, allows with it, SQL cell asserts before its first TRUNCATE and keeps the
3 TRUNCATEs. Not in the sandbox build.
