-- Base view definition, agreed on 2026-11-26. Every deploy builds its own copy in its reporting schema.
-- Grain: one row per framework status (four statuses); status_order 1 = worst.
CREATE OR REPLACE VIEW `${catalog}`.`${schema}`.v_simple_status AS
SELECT
  status,
  status_label,
  status_symbol,
  status_order,
  status_color,
  status_bg_color
FROM VALUES
  ('error',      'Failure',       '✕', 1, '#D64545', '#FBE3E1'),
  ('warning',    'Warning',       '!', 2, '#E3A033', '#FDF0D6'),
  ('code_error', 'Did not run',   '?', 3, '#8A8F98', '#ECEEF1'),
  ('pass',       'Pass',          '✓', 4, '#2E8B57', '#E2F3E8')
  AS t(status, status_label, status_symbol, status_order, status_color, status_bg_color);
