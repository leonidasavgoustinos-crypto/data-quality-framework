# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_dq_results AS
# MAGIC SELECT
# MAGIC
# MAGIC   m.catalog,
# MAGIC   m.schema,
# MAGIC   m.layer AS data_layer,
# MAGIC   p.project AS project,
# MAGIC   p.project_description,
# MAGIC   m.table AS table_name,
# MAGIC   ra.parameters_identifiers,
# MAGIC   ra.parameters_values,
# MAGIC   m.quarantine,
# MAGIC   m.table_nk,
# MAGIC   rt.name AS rule_name,
# MAGIC   rt.description AS rule_description,
# MAGIC   rule.rule_type,
# MAGIC   rule.description AS rule_type_description, 
# MAGIC   rd.rule_dimension,
# MAGIC   rd.description AS dimension_description,
# MAGIC   ru.status,
# MAGIC   rp.criticality,
# MAGIC   ap.apply_at,
# MAGIC   ap.is_project_specific,
# MAGIC   ap.is_active,
# MAGIC   m.filter_field,
# MAGIC   m.filter_field_type,
# MAGIC   t.rules_evaluated,
# MAGIC   t.rules_pass,
# MAGIC   t.rules_error,
# MAGIC   t.rules_warning,
# MAGIC   ru.statement,
# MAGIC   rr.tables_evaluated,
# MAGIC   rr.tables_error,
# MAGIC   rr.tables_warning,
# MAGIC   rr.tables_pass,
# MAGIC   ru.rows_evaluated,
# MAGIC   ru.rows_pass,
# MAGIC   ru.rows_failed,
# MAGIC   rp.quarantine AS quarantine_status,
# MAGIC   rp.category AS policy_category,
# MAGIC   rt.scope AS rule_scope,
# MAGIC   rt.is_reusable,
# MAGIC   rt.engine_type,
# MAGIC   t.start_timestamp AS start_timestamp_table,
# MAGIC   t.end_timestamp AS end_timestamp_table,
# MAGIC   ru.start_timestamp AS start_timestamp_rule,
# MAGIC   ru.end_timestamp AS end_timestamp_rule,
# MAGIC   rr.start_timestamp AS start_timestamp_execution,
# MAGIC   rr.end_timestamp AS end_timestamp_execution
# MAGIC   
# MAGIC   FROM analytics_dq_dev.results.result rr
# MAGIC   LEFT JOIN analytics_dq_dev.results.result_table t
# MAGIC     ON t.result_id = rr.result_id
# MAGIC   LEFT JOIN analytics_dq_dev.results.result_rule ru
# MAGIC     ON t.result_table_id = ru.result_table_id
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.`table` m
# MAGIC     ON t.table_id = m.table_id
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.project p
# MAGIC     ON t.project_id = p.project_id
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.rule_type rule
# MAGIC     ON ru.rule_type_id = rule.rule_type_id
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.rule_assignment ra
# MAGIC     ON ru.rule_assignment_id = ra.rule_assignment_id
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.run_policy rp
# MAGIC     ON ra.policy_id = rp.policy_id
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.rule_template rt
# MAGIC     ON ra.rule_template_id = rt.rule_template_id
# MAGIC   LEFT JOIN analytics_dq_dev.metadata.rule_dimension rd
# MAGIC     ON rt.rule_dimension_id = rd.rule_dimension_id
# MAGIC   LEFT JOIN analytics_dq_dev.configuration.apply_at ap
# MAGIC    ON ra.apply_at_id = ap.apply_at_id
# MAGIC
# MAGIC
# MAGIC ORDER BY t.start_timestamp DESC;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_consolidated_configuration_metadata AS 
# MAGIC SELECT
# MAGIC   p.project,
# MAGIC   p.project_description,
# MAGIC   m.catalog,
# MAGIC   m.schema,
# MAGIC   m.layer,
# MAGIC   m.`table`,
# MAGIC   m.quarantine,
# MAGIC   m.table_nk,
# MAGIC   ra.parameters_identifiers,
# MAGIC   ra.parameters_values,
# MAGIC   ap.apply_at,
# MAGIC   ap.is_project_specific,
# MAGIC   ap.is_active,
# MAGIC   rt.name AS rule_name,
# MAGIC   rt.statement,
# MAGIC   rt.description AS rule_description,
# MAGIC   rule.rule_type,
# MAGIC   rule.description AS rule_type_description,
# MAGIC   m.filter_field,
# MAGIC   m.filter_field_type,
# MAGIC   rd.rule_dimension,
# MAGIC   rd.description AS dimension_description,
# MAGIC   rp.criticality,
# MAGIC   rp.category AS policy_category,
# MAGIC   rp.quarantine AS quarantine_status,
# MAGIC   rt.scope,
# MAGIC   rt.engine_type
# MAGIC
# MAGIC FROM analytics_dq_dev.configuration.rule_assignment ra 
# MAGIC LEFT JOIN analytics_dq_dev.metadata.`table` m
# MAGIC    ON ra.table_id = m.table_id
# MAGIC LEFT JOIN analytics_dq_dev.metadata.project p
# MAGIC    ON ra.project_id = p.project_id
# MAGIC LEFT JOIN analytics_dq_dev.configuration.run_policy rp
# MAGIC    ON ra.policy_id = rp.policy_id
# MAGIC LEFT JOIN analytics_dq_dev.configuration.rule_template rt 
# MAGIC    ON ra.rule_template_id = rt.rule_template_id
# MAGIC LEFT JOIN analytics_dq_dev.metadata.rule_type rule
# MAGIC    ON rt.rule_type_id = rule.rule_type_id
# MAGIC LEFT JOIN analytics_dq_dev.metadata.rule_dimension rd 
# MAGIC    ON rt.rule_dimension_id = rd.rule_dimension_id
# MAGIC LEFT JOIN analytics_dq_dev.configuration.apply_at ap
# MAGIC    ON ra.apply_at_id = ap.apply_at_id
# MAGIC ORDER BY p.valid_from DESC;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_dq_rule_execution_performance AS
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rule_name,
# MAGIC     rows_evaluated,
# MAGIC     rows_pass,
# MAGIC     rows_failed,
# MAGIC     start_timestamp_rule,
# MAGIC     end_timestamp_rule,
# MAGIC     TIMESTAMPDIFF(SECOND, start_timestamp_rule, end_timestamp_rule) AS duration_seconds_for_rule
# MAGIC FROM analytics_dq_dev.reporting.v_dq_results
# MAGIC WHERE tables_evaluated IS NOT NULL
# MAGIC   AND status != 'code_error'
# MAGIC GROUP BY
# MAGIC     table_name,
# MAGIC     rule_name,
# MAGIC     rows_evaluated,
# MAGIC     rows_pass,
# MAGIC     rows_failed,
# MAGIC     start_timestamp_rule,
# MAGIC     end_timestamp_rule;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_dq_table_execution_performance AS
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rules_evaluated,
# MAGIC     rules_pass,
# MAGIC     rules_error,
# MAGIC     rules_warning,
# MAGIC     start_timestamp_table,
# MAGIC     end_timestamp_table,
# MAGIC     TIMESTAMPDIFF(second, start_timestamp_table, end_timestamp_table) AS duration_seconds_for_table,
# MAGIC     LPAD(FLOOR((TIMESTAMPDIFF(second, start_timestamp_table, end_timestamp_table) / rules_evaluated) / 60), 2, '0')
# MAGIC         || ':' ||
# MAGIC     LPAD(MOD(CAST(TIMESTAMPDIFF(second, start_timestamp_table, end_timestamp_table) / rules_evaluated AS INTEGER), 60), 2, '0')
# MAGIC         AS avg_duration_per_rule
# MAGIC FROM analytics_dq_dev.reporting.v_dq_results
# MAGIC WHERE tables_evaluated IS NOT NULL
# MAGIC   AND status != 'code_error'
# MAGIC GROUP BY
# MAGIC     table_name,
# MAGIC     rules_evaluated,
# MAGIC     rules_pass,
# MAGIC     rules_error,
# MAGIC     rules_warning,
# MAGIC     start_timestamp_table,
# MAGIC     end_timestamp_table;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW analytics_dq_dev.reporting.v_dq_execution_performance AS
# MAGIC SELECT 
# MAGIC     tables_evaluated,
# MAGIC     tables_pass,
# MAGIC     tables_error,
# MAGIC     tables_warning,
# MAGIC     start_timestamp_execution,
# MAGIC     end_timestamp_execution,
# MAGIC     LPAD(FLOOR(TIMESTAMPDIFF(SECOND, start_timestamp_execution, end_timestamp_execution) / 60), 2, '0')
# MAGIC         || ':' ||
# MAGIC     LPAD(MOD(TIMESTAMPDIFF(SECOND, start_timestamp_execution, end_timestamp_execution), 60), 2, '0')
# MAGIC         AS duration_mins_for_execution,
# MAGIC     LPAD(FLOOR((TIMESTAMPDIFF(SECOND, start_timestamp_execution, end_timestamp_execution) / tables_evaluated) / 60), 2, '0')
# MAGIC         || ':' ||
# MAGIC     LPAD(MOD(CAST(TIMESTAMPDIFF(SECOND, start_timestamp_execution, end_timestamp_execution) / tables_evaluated AS INTEGER), 60), 2, '0')
# MAGIC         AS avg_duration_per_table
# MAGIC FROM analytics_dq_dev.reporting.v_dq_results
# MAGIC WHERE tables_evaluated IS NOT NULL
# MAGIC   AND status != 'code_error'
# MAGIC GROUP BY 
# MAGIC     tables_evaluated,
# MAGIC     tables_pass,
# MAGIC     tables_error,
# MAGIC     tables_warning,
# MAGIC     start_timestamp_execution,
# MAGIC     end_timestamp_execution;