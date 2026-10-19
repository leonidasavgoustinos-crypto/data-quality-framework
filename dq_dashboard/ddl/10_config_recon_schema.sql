-- @targets: free, company
-- Views for the Configuration & Coverage, Reconciliation and Schema changes pages. Requires 05, 06, 07 and 08.
-- Rendered by scripts/run_ddl.py. Synthetic parts are read only while {{use_synthetic}} is true; their recorded
-- times are moved by v_date_shift.date_shift_days. Real configuration times are wall-clock times and not shifted.
-- Who: user names that are e-mail addresses are shown as 'user <6 hex>' (hash), never as the address.

-- v_config_changes: one row per configuration change event.
--   'Delta history': every write to a configuration or metadata table (config_history_snapshot, DESCRIBE HISTORY).
--   'SCD2':          rule assignments added (valid_from) or closed (valid_to), grouped per time and author.
--   'demo':          config_changes_synthetic (rule added / changed / disabled, threshold changed), per assignment.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_config_changes
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH hist AS (
  SELECT
    h.`timestamp`                                                               AS change_ts,
    h.user_name                                                                 AS changed_by_raw,
    concat('table ', lower(h.operation))                                        AS change_type,
    'configuration table'                                                       AS object_type,
    element_at(split(h.config_table, '\\.'), -1)                                AS object_label,
    CAST(NULL AS BIGINT) AS rule_assignment_id, CAST(NULL AS BIGINT) AS table_id,
    CAST(NULL AS STRING) AS field_name, CAST(NULL AS STRING) AS old_value,
    concat('version ', CAST(h.version AS STRING))                               AS new_value,
    'Delta history'                                                             AS source,
    FALSE                                                                       AS is_synthetic
  FROM {{dq_catalog}}.{{dashboard_schema}}.config_history_snapshot h
),
scd2 AS (
  SELECT ts AS change_ts, added_by AS changed_by_raw, change_type, 'rule assignments' AS object_type,
         concat(CAST(COUNT(*) AS STRING), ' rule assignments') AS object_label,
         CAST(NULL AS BIGINT) AS rule_assignment_id, CAST(NULL AS BIGINT) AS table_id,
         CAST(NULL AS STRING) AS field_name, CAST(NULL AS STRING) AS old_value, CAST(NULL AS STRING) AS new_value,
         'SCD2' AS source, FALSE AS is_synthetic
  FROM (SELECT valid_from AS ts, added_by, 'rules added' AS change_type FROM {{dq_catalog}}.configuration.rule_assignment
        UNION ALL
        SELECT valid_to, added_by, 'rules closed' FROM {{dq_catalog}}.configuration.rule_assignment WHERE valid_to IS NOT NULL)
  GROUP BY ts, added_by, change_type
),
demo AS (
  SELECT timestampadd(DAY, s.date_shift_days, c.change_ts) AS change_ts, c.changed_by AS changed_by_raw,
         c.change_type, 'rule assignment' AS object_type, CAST(NULL AS STRING) AS object_label,
         c.rule_assignment_id, c.table_id, c.field_name, c.old_value, c.new_value, 'demo' AS source, TRUE AS is_synthetic
  FROM {{dq_catalog}}.{{dashboard_schema}}.config_changes_synthetic c
  CROSS JOIN {{dq_catalog}}.{{dashboard_schema}}.v_date_shift s
  WHERE {{use_synthetic}}
),
u AS (
  SELECT * FROM hist UNION ALL SELECT * FROM scd2 UNION ALL SELECT * FROM demo
)
SELECT
  u.change_ts, CAST(u.change_ts AS DATE) AS change_date,
  CASE WHEN u.changed_by_raw LIKE '%@%' THEN concat('user ', substr(sha2(lower(u.changed_by_raw), 256), 1, 6))
       ELSE u.changed_by_raw END                                                AS changed_by,
  u.change_type, u.object_type,
  COALESCE(u.object_label, concat(a.rule_name, COALESCE(concat('(', get_json_object(a.parameters_identifiers, '$.column'), ')'), ''),
                                  ' · ', a.table_name))                         AS object_label,
  u.rule_assignment_id, COALESCE(u.table_id, a.table_id) AS table_id,
  concat(a.`schema`, '.', a.table_name)                                         AS table_label,
  u.field_name, u.old_value, u.new_value, u.source, u.is_synthetic,
  a.project, a.data_layer, a.catalog, a.criticality, a.rule_dimension
FROM u
LEFT JOIN {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active a ON a.rule_assignment_id = u.rule_assignment_id;

-- v_catalog_coverage: one row per table in the schemas that hold registered tables (system.information_schema),
-- with whether it is registered, its effective active rules (v_assignment_active) and its reads in the last 30 days
-- (system.access.audit getTable).
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_catalog_coverage
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH sch AS (
  SELECT DISTINCT lower(catalog) AS c, lower(`schema`) AS s FROM {{dq_catalog}}.metadata.`table`
),
t AS (
  SELECT i.table_catalog, i.table_schema, i.table_name, i.table_type
  FROM system.information_schema.tables i
  JOIN sch ON sch.c = lower(i.table_catalog) AND sch.s = lower(i.table_schema)
),
reads AS (
  SELECT lower(request_params.full_name_arg) AS full_name, COUNT(*) AS reads_30d
  FROM system.access.audit
  WHERE action_name = 'getTable' AND event_date > date_sub(current_date(), 30)
  GROUP BY 1
),
rules AS (
  SELECT table_id, COUNT(*) AS active_rules FROM {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active
  WHERE is_effective_active GROUP BY table_id
)
SELECT
  t.table_catalog AS catalog, t.table_schema AS `schema`, t.table_name, concat(t.table_schema, '.', t.table_name) AS table_label,
  t.table_type, m.table_id IS NOT NULL AS is_registered, m.layer AS data_layer,
  COALESCE(r.active_rules, 0) AS active_rules, COALESCE(r.active_rules, 0) > 0 AS is_covered,
  COALESCE(x.reads_30d, 0) AS reads_30d
FROM t
LEFT JOIN {{dq_catalog}}.metadata.`table` m
       ON lower(m.catalog) = lower(t.table_catalog) AND lower(m.`schema`) = lower(t.table_schema)
      AND lower(m.table) = lower(t.table_name)
LEFT JOIN rules r ON r.table_id = m.table_id
LEFT JOIN reads x ON x.full_name = lower(concat_ws('.', t.table_catalog, t.table_schema, t.table_name));

-- v_recon_results: reconciliation results (columns of the reconciliation module's results table) with the mapping,
-- dates shifted. Demo only (recon_*_synthetic): the reconciliation schema does not exist on this target. On a
-- target with reconciliation runs, the source becomes <dq_catalog>.reconciliation.results / mapping.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_recon_results
COMMENT 'dq_dashboard sandbox - generated'
AS
SELECT
  timestampadd(DAY, s.date_shift_days, r.execution_timestamp)                   AS execution_timestamp,
  CAST(timestampadd(DAY, s.date_shift_days, r.execution_timestamp) AS DATE)     AS run_date,
  r.reconciliation_type, r.`table` AS mapping, r.old_table, r.new_table, r.result_old, r.result_new,
  r.additional_info, r.result_status, r.is_synthetic
FROM {{dq_catalog}}.{{dashboard_schema}}.recon_results_synthetic r
JOIN {{dq_catalog}}.{{dashboard_schema}}.recon_mapping_synthetic m ON m.table_name = r.`table`
CROSS JOIN {{dq_catalog}}.{{dashboard_schema}}.v_date_shift s
WHERE {{use_synthetic}};

-- v_recon_runs: one row per mapping and reconciliation run.
--   rows_old / rows_new: row_count check; only_in_old / only_in_new: row_values check (rows missing in the new /
--   old table); type_mismatches: data_types checks that fail; name_mismatches: columns present on one side only.
--   status: match = every check passes; partial = the schema checks pass and row count and row values differ by at
--   most 1 % of rows_old; mismatch = otherwise.
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_recon_runs
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH g AS (
  SELECT
    mapping, execution_timestamp, run_date, MAX(old_table) AS old_table, MAX(new_table) AS new_table,
    MAX(IF(reconciliation_type = 'row_count', CAST(result_old AS BIGINT), NULL))  AS rows_old,
    MAX(IF(reconciliation_type = 'row_count', CAST(result_new AS BIGINT), NULL))  AS rows_new,
    MAX(IF(reconciliation_type = 'row_values', COALESCE(try_cast(result_old AS BIGINT), 0), NULL)) AS only_in_old,
    MAX(IF(reconciliation_type = 'row_values', COALESCE(try_cast(result_new AS BIGINT), 0), NULL)) AS only_in_new,
    COUNT_IF(reconciliation_type = 'data_types' AND result_status = 'fail')     AS type_mismatches,
    COUNT_IF(reconciliation_type = 'column_names' AND result_status = 'fail')   AS name_mismatches,
    COUNT_IF(result_status = 'fail')                                            AS failed_checks,
    COUNT(*)                                                                    AS checks,
    MAX(is_synthetic)                                                           AS is_synthetic
  FROM {{dq_catalog}}.{{dashboard_schema}}.v_recon_results
  GROUP BY mapping, execution_timestamp, run_date
)
SELECT
  g.*,
  (g.rows_new - g.rows_old) / NULLIF(g.rows_old, 0)                             AS rows_change,
  CASE WHEN g.failed_checks = 0 THEN 'match'
       WHEN g.type_mismatches = 0 AND g.name_mismatches = 0
        AND abs(g.rows_new - g.rows_old) <= 0.01 * g.rows_old
        AND g.only_in_old + g.only_in_new <= 0.01 * g.rows_old THEN 'partial'
       ELSE 'mismatch' END                                                      AS status,
  ROW_NUMBER() OVER (PARTITION BY g.mapping ORDER BY g.execution_timestamp DESC) = 1 AS is_latest
FROM g;

-- v_schema_changes: column added / removed / type changed per registered table.
--   'snapshot': difference between consecutive days of dashboard_schema.columns_snapshot (real; date shifted
--               like the run dates).
--   'demo':     schema_changes_synthetic.
-- rules_on_column: effective active rule assignments of the table whose parameters name the column (impact).
CREATE VIEW IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.v_schema_changes
COMMENT 'dq_dashboard sandbox - generated'
AS
WITH cs AS (
  SELECT * FROM {{dq_catalog}}.{{dashboard_schema}}.columns_snapshot
),
days AS (
  SELECT table_catalog, table_schema, table_name, simulated_date,
         LAG(simulated_date) OVER (PARTITION BY table_catalog, table_schema, table_name ORDER BY simulated_date) AS prev_date
  FROM (SELECT DISTINCT table_catalog, table_schema, table_name, simulated_date FROM cs)
),
cur AS (
  SELECT d.table_catalog, d.table_schema, d.table_name, d.simulated_date, c.column_name, c.data_type
  FROM days d JOIN cs c ON c.table_catalog = d.table_catalog AND c.table_schema = d.table_schema
                       AND c.table_name = d.table_name AND c.simulated_date = d.simulated_date
  WHERE d.prev_date IS NOT NULL
),
prv AS (
  SELECT d.table_catalog, d.table_schema, d.table_name, d.simulated_date, c.column_name, c.data_type
  FROM days d JOIN cs c ON c.table_catalog = d.table_catalog AND c.table_schema = d.table_schema
                       AND c.table_name = d.table_name AND c.simulated_date = d.prev_date
),
snap AS (
  SELECT
    CAST(COALESCE(cur.simulated_date, prv.simulated_date) AS TIMESTAMP)         AS event_ts,
    COALESCE(cur.table_catalog, prv.table_catalog) AS table_catalog, COALESCE(cur.table_schema, prv.table_schema) AS table_schema,
    COALESCE(cur.table_name, prv.table_name) AS table_name, COALESCE(cur.column_name, prv.column_name) AS column_name,
    CASE WHEN prv.column_name IS NULL THEN 'column added' WHEN cur.column_name IS NULL THEN 'column removed'
         ELSE 'type changed' END                                                AS change_type,
    prv.data_type AS old_type, cur.data_type AS new_type, 'snapshot' AS source, FALSE AS is_synthetic
  FROM cur FULL OUTER JOIN prv
    ON cur.table_catalog = prv.table_catalog AND cur.table_schema = prv.table_schema AND cur.table_name = prv.table_name
   AND cur.simulated_date = prv.simulated_date AND lower(cur.column_name) = lower(prv.column_name)
  WHERE prv.column_name IS NULL OR cur.column_name IS NULL OR cur.data_type <> prv.data_type
),
demo AS (
  SELECT event_ts, table_catalog, table_schema, table_name, column_name, change_type, old_type, new_type,
         'demo' AS source, TRUE AS is_synthetic
  FROM {{dq_catalog}}.{{dashboard_schema}}.schema_changes_synthetic
  WHERE {{use_synthetic}}
),
u AS (SELECT * FROM snap UNION ALL SELECT * FROM demo),
rules AS (
  SELECT table_id, parameters_identifiers FROM {{dq_catalog}}.{{dashboard_schema}}.v_assignment_active WHERE is_effective_active
),
ev AS (
  SELECT
    timestampadd(DAY, s.date_shift_days, u.event_ts)                            AS event_ts,
    CAST(timestampadd(DAY, s.date_shift_days, u.event_ts) AS DATE)              AS event_date,
    u.table_catalog AS catalog, u.table_schema AS `schema`, u.table_name, concat(u.table_schema, '.', u.table_name) AS table_label,
    m.layer AS data_layer, m.table_id, u.column_name, u.change_type, u.old_type, u.new_type, u.source, u.is_synthetic
  FROM u
  CROSS JOIN {{dq_catalog}}.{{dashboard_schema}}.v_date_shift s
  LEFT JOIN {{dq_catalog}}.metadata.`table` m
         ON lower(m.catalog) = lower(u.table_catalog) AND lower(m.`schema`) = lower(u.table_schema)
        AND lower(m.table) = lower(u.table_name)
)
SELECT ev.event_ts, ev.event_date, ev.catalog, ev.`schema`, ev.table_name, ev.table_label, ev.data_layer, ev.column_name,
       ev.change_type, ev.old_type, ev.new_type, ev.source, ev.is_synthetic, COUNT(r.table_id) AS rules_on_column
FROM ev
LEFT JOIN rules r ON r.table_id = ev.table_id
                 AND lower(r.parameters_identifiers) LIKE concat('%"', lower(ev.column_name), '"%')
GROUP BY ev.event_ts, ev.event_date, ev.catalog, ev.`schema`, ev.table_name, ev.table_label, ev.data_layer, ev.column_name,
         ev.change_type, ev.old_type, ev.new_type, ev.source, ev.is_synthetic;
