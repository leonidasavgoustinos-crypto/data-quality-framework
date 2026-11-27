-- Extension of v_simple_rule_daily (read-only; does not change the base view).
-- day_rank: 1 = newest run day of the rule_key, 2 = the one before, ...
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_rule_daily_ext AS
SELECT
  d.*,
  ROW_NUMBER() OVER (PARTITION BY d.rule_key ORDER BY d.run_date DESC) AS day_rank
FROM `${catalog}`.`${schema}`.v_simple_rule_daily d;
