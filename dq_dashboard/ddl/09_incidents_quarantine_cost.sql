-- @targets: free, company
-- Views for the Quarantine, Performance & Cost and Incidents pages. Requires 05, 06, 07 and 08.
-- Rendered by scripts/run_ddl.py. Synthetic parts are read only while {{use_synthetic}} is true, and their recorded
-- times are moved by v_date_shift.date_shift_days like every other dashboard date.

-- v_incidents: one row per incident, derived from the results (works on every target): an incident is a streak of
-- consecutive failing (warning or error) run days of one rule assignment (v_rule_daily.streak_start).
--   detected_ts: the run that first failed; previous_run_ts: the run of the assignment before it.
--   resolved_ts: the first passing run after the last failing day; NULL = not resolved (open).
--   time_to_detect_h: previous_run_ts -> detected_ts (approximation: the failure happened between the two runs).
--   time_to_resolve_h: detected_ts -> resolved_ts.
--   incident_key: rule_assignment_id || ':' || recorded first failing day (stable while the date shift grows);
--   incident_id: short display id from the key.
-- owner and acknowledged_ts come from incident_triage_synthetic (demo only; the framework stores no triage).
-- state: resolved, acknowledged (open with an acknowledgement) or open.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_incidents
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH d AS (
  SELECT
    v.*,
    LAG(run_ts) OVER w                                                          AS previous_run_ts,
    MIN(IF(state = 'pass', run_ts, NULL)) OVER (
      PARTITION BY rule_assignment_id ORDER BY run_date ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    )                                                                           AS next_pass_ts,
    MAX(run_date) OVER (PARTITION BY rule_assignment_id)                        AS assignment_last_day
  FROM {{dq_catalog}}.{{dashboard_schema}}.v_rule_daily v
  WINDOW w AS (PARTITION BY rule_assignment_id ORDER BY run_date)
),
inc AS (
  SELECT
    rule_assignment_id, streak_start AS started_on,
    MIN(recorded_run_date)                                                      AS recorded_start,
    MAX(run_date)                                                               AS last_failing_day,
    COUNT(*)                                                                    AS failing_runs,
    MIN_BY(run_ts, run_date)                                                    AS detected_ts,
    MIN_BY(previous_run_ts, run_date)                                           AS previous_run_ts,
    MAX_BY(next_pass_ts, run_date)                                              AS resolved_ts,
    MAX(assignment_last_day)                                                    AS assignment_last_day,
    MAX(status = 'error')                                                       AS had_error,
    MAX_BY(status, run_date)                                                    AS last_status,
    SUM(rows_failed)                                                            AS rows_failed,
    MAX(project) AS project, MAX(data_layer) AS data_layer, MAX(catalog) AS catalog, MAX(criticality) AS criticality,
    MAX(rule_dimension) AS rule_dimension, MAX(apply_at) AS apply_at, MAX(`schema`) AS `schema`,
    MAX(table_name) AS table_name, MAX(table_label) AS table_label, MAX(rule_name) AS rule_name,
    MAX(rule_label) AS rule_label, MAX(assignment_label) AS assignment_label, MAX(is_synthetic) AS has_synthetic
  FROM d
  WHERE state = 'failing'
  GROUP BY rule_assignment_id, streak_start
),
keyed AS (
  SELECT inc.*, concat(CAST(rule_assignment_id AS STRING), ':', CAST(recorded_start AS STRING)) AS incident_key
  FROM inc
)
SELECT
  k.*,
  concat('INC-', upper(substr(sha2(k.incident_key, 256), 1, 6)))               AS incident_id,
  (unix_seconds(k.detected_ts) - unix_seconds(k.previous_run_ts)) / 3600.0      AS time_to_detect_h,
  (unix_seconds(k.resolved_ts) - unix_seconds(k.detected_ts)) / 3600.0          AS time_to_resolve_h,
  t.owner,
  timestampadd(DAY, s.date_shift_days, t.acknowledged_ts)                       AS acknowledged_ts,
  CASE WHEN k.resolved_ts IS NOT NULL THEN 'resolved'
       WHEN t.acknowledged_ts IS NOT NULL THEN 'acknowledged'
       ELSE 'open' END                                                          AS state
FROM keyed k
CROSS JOIN {{dq_catalog}}.{{dashboard_schema}}.v_date_shift s
LEFT JOIN {{dq_catalog}}.{{dashboard_schema}}.incident_triage_synthetic t
       ON t.incident_key = k.incident_key AND {{use_synthetic}};

-- v_alerts: alerts sent for the incidents, with their outcome ('not assessed' when none yet). Demo only
-- (alert_log_synthetic; the framework does not record alerts). sent_ts shifted like the other dates.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_alerts
COMMENT 'dq_dashboard sandbox - generated'
AS
SELECT
  a.alert_id, a.incident_key, i.incident_id, a.rule_assignment_id,
  timestampadd(DAY, s.date_shift_days, a.sent_ts)                               AS sent_ts,
  CAST(timestampadd(DAY, s.date_shift_days, a.sent_ts) AS DATE)                 AS sent_date,
  a.channel, a.alert_kind, COALESCE(a.outcome, 'not assessed')                   AS outcome,
  i.project, i.data_layer, i.catalog, i.criticality, i.rule_dimension, i.table_label, i.rule_label, i.state,
  a.is_synthetic
FROM {{dq_catalog}}.{{dashboard_schema}}.alert_log_synthetic a
CROSS JOIN {{dq_catalog}}.{{dashboard_schema}}.v_date_shift s
LEFT JOIN {{dq_catalog}}.{{dashboard_schema}}.v_incidents i ON i.incident_key = a.incident_key
WHERE {{use_synthetic}};

-- v_quarantine_lifecycle: batches of quarantined rows with the time they entered and left quarantine and how
-- (released, reprocessed, discarded; status 'in quarantine' while not left). Demo only
-- (quarantine_lifecycle_synthetic); the framework replaces a quarantine table on every run and stores no times.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_quarantine_lifecycle
COMMENT 'dq_dashboard sandbox - generated'
AS
SELECT
  q.batch_id, q.result_rule_id, q.rule_assignment_id, q.table_id, q.quarantine_table, q.rows_quarantined,
  timestampadd(DAY, s.date_shift_days, q.entered_ts)                            AS entered_ts,
  CAST(timestampadd(DAY, s.date_shift_days, q.entered_ts) AS DATE)              AS entered_date,
  timestampadd(DAY, s.date_shift_days, q.left_ts)                               AS left_ts,
  COALESCE(q.outcome, 'in quarantine')                                          AS status,
  concat(a.`schema`, '.', a.table_name) AS table_label, a.rule_name, a.project, a.data_layer, a.catalog,
  a.criticality, a.rule_dimension, q.is_synthetic
FROM {{dq_catalog}}.{{dashboard_schema}}.quarantine_lifecycle_synthetic q
CROSS JOIN {{dq_catalog}}.{{dashboard_schema}}.v_date_shift s
LEFT JOIN {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active a ON a.rule_assignment_id = q.rule_assignment_id
WHERE {{use_synthetic}};

-- v_dq_cost: DBUs and list cost of the jobs that run the framework checks (job name dq_dashboard_driver or
-- dq_dashboard_backfill*), per billing day and SKU, from system.billing.usage and system.billing.list_prices.
-- Billing dates are the real dates (not shifted); billing arrives with a delay of a few hours.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_dq_cost
COMMENT 'dq_dashboard sandbox - generated'
AS
SELECT
  u.usage_date, u.usage_metadata.job_name AS job_name, u.sku_name,
  SUM(u.usage_quantity)                                                         AS dbus,
  SUM(u.usage_quantity * lp.pricing.default)                                    AS list_cost,
  MAX(lp.currency_code)                                                         AS currency
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices lp
       ON lp.sku_name = u.sku_name AND u.usage_end_time >= lp.price_start_time
      AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
WHERE u.usage_metadata.job_name RLIKE 'dq_dashboard_(driver|backfill)'
GROUP BY u.usage_date, u.usage_metadata.job_name, u.sku_name;
