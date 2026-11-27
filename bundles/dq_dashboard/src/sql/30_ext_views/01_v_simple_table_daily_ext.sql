-- Extension of v_simple_table_daily (read-only; does not change the base view).
-- day_rank: 1 = newest run day of the table_label in its area, 2 = the one before, ...
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_table_daily_ext AS
SELECT
  t.*,
  ROW_NUMBER() OVER (PARTITION BY t.table_label, t.project_label ORDER BY t.run_date DESC) AS day_rank
FROM `${catalog}`.`${schema}`.v_simple_table_daily t;
