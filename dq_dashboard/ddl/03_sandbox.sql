-- @targets: free
-- @sandbox_only
-- Sandbox-only helper tables (never created in company).
CREATE TABLE IF NOT EXISTS {{dq_catalog}}.{{dashboard_schema}}.load_log (
  table_nk STRING, simulated_date DATE, rows BIGINT, defects STRING, loaded_at TIMESTAMP
) COMMENT 'dq_dashboard sandbox - generated';
