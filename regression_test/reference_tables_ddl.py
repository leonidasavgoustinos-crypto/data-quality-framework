# Databricks notebook source
# MAGIC %md
# MAGIC create test tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE TABLE analytics_dq_dev.test_cases.test_case_1(
# MAGIC     customer_id INT,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     email STRING,
# MAGIC     created_at DATE NOT NULL
# MAGIC );
# MAGIC  
# MAGIC INSERT INTO analytics_dq_dev.test_cases.test_case_1 (customer_id, first_name, last_name, email, created_at) VALUES
# MAGIC (1, 'Nikos', 'Papadopoulos', 'nikos@gmail.com', '2024-03-06'),
# MAGIC (2, 'Anna', 'Georgiou', 'anna@yahoo.com', '2024-03-06'),
# MAGIC (2, 'Anna', 'Georgiou', 'anna@yahoo.com', '2024-03-06'),
# MAGIC (3, NULL, 'Karalis', 'karalis@example.com', '2024-03-06'),
# MAGIC (NULL, 'Maria', NULL, '', '2024-03-06'),
# MAGIC (4, 'Maria', 'Karalis', NULL, '2024-03-06')
# MAGIC
# MAGIC CREATE or REPLACE TABLE analytics_dq_dev.test_cases.test_case_2 (
# MAGIC     user_id INT,
# MAGIC     username STRING,
# MAGIC     country STRING,
# MAGIC     phone_number STRING,
# MAGIC     ingestion_date DATE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC  
# MAGIC INSERT INTO analytics_dq_dev.test_cases.test_case_2 (user_id, username, country, phone_number, ingestion_date) VALUES
# MAGIC (1, NULL, 'GR', '6900000001', '2024-01-01'),
# MAGIC (2, NULL, 'GR', '6900000002', '2024-01-01'),
# MAGIC (3, NULL, 'DE', '6900000003', '2024-01-01'),
# MAGIC (4, NULL, 'DE', '6900000004', '2024-01-01'),
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC CREATE or REPLACE TABLE analytics_dq_dev.test_cases.test_cases_results (
# MAGIC     result_id        VARCHAR(36) PRIMARY KEY,
# MAGIC     project_id       INT,
# MAGIC     status           STRING,
# MAGIC     tables_evaluated INT,
# MAGIC     tables_error     INT,
# MAGIC     tables_warning   INT,
# MAGIC     tables_pass      INT,
# MAGIC     start_timestamp  TIMESTAMP NOT NULL,
# MAGIC     end_timestamp    TIMESTAMP NOT NULL
# MAGIC );
# MAGIC  
# MAGIC INSERT INTO analytics_dq_dev.test_cases.test_cases_results (
# MAGIC     result_id,
# MAGIC     project_id,
# MAGIC     status,
# MAGIC     tables_evaluated,
# MAGIC     tables_error,
# MAGIC     tables_warning,
# MAGIC     tables_pass,
# MAGIC     start_timestamp,
# MAGIC     end_timestamp
# MAGIC ) VALUES
# MAGIC (
# MAGIC     '2194bcff-1e21-4340-b344-533aa2b1f306',
# MAGIC     NULL,
# MAGIC     'error',
# MAGIC     1,
# MAGIC     1,
# MAGIC     0,
# MAGIC     0,
# MAGIC     '2026-04-16 10:23:23.440',
# MAGIC     '2026-04-16 10:25:26.792'
# MAGIC ),
# MAGIC (
# MAGIC     '23a386db-40b4-40ca-a6b9-474aba4c0da0',
# MAGIC     NULL,
# MAGIC     'error',
# MAGIC     1,
# MAGIC     1,
# MAGIC     0,
# MAGIC     0,
# MAGIC     '2026-04-15 16:09:35.915',
# MAGIC     '2026-04-15 16:11:21.986'
# MAGIC );
# MAGIC
# MAGIC %sql
# MAGIC CREATE or REPLACE TABLE analytics_dq_dev.test_cases.test_cases_quarantine (
# MAGIC     customer_id INT,
# MAGIC     email string,
# MAGIC     result_id STRING NOT NULL,
# MAGIC     result_table_id STRING NOT NULL,
# MAGIC     result_rule_id STRING NOT NULL,
# MAGIC     project_id int,
# MAGIC     category string
# MAGIC );
# MAGIC
# MAGIC INSERT INTO analytics_dq_dev.test_cases.test_cases_quarantine (
# MAGIC     customer_id,
# MAGIC     email,
# MAGIC     result_id,
# MAGIC     result_table_id,
# MAGIC     result_rule_id,
# MAGIC     project_id,
# MAGIC     category
# MAGIC ) VALUES
# MAGIC (
# MAGIC     3,
# MAGIC     'karalis@example.com',
# MAGIC     'b6d60105-f982-4f3e-b653-ee1fa1de85b4',
# MAGIC     '278b7765-5f9f-422b-85b3-0221fd755d00',
# MAGIC     '104b9670-7c4e-4818-aa04-276a872df0b2',
# MAGIC     NULL,
# MAGIC     'error'
# MAGIC ),
# MAGIC (
# MAGIC     2,
# MAGIC     'anna@yahoo.com',
# MAGIC     'b6d60105-f982-4f3e-b653-ee1fa1de85b4',
# MAGIC     '278b7765-5f9f-422b-85b3-0221fd755d00',
# MAGIC     'a2621c1d-9843-4f26-82af-6382694101d6',
# MAGIC     NULL,
# MAGIC     'error'
# MAGIC ),
# MAGIC (
# MAGIC     4,
# MAGIC     NULL,
# MAGIC     'b6d60105-f982-4f3e-b653-ee1fa1de85b4',
# MAGIC     '278b7765-5f9f-422b-85b3-0221fd755d00',
# MAGIC     '6ece459c-09a4-4b47-9473-0b16e20a898e',
# MAGIC     NULL,
# MAGIC     'error'
# MAGIC ),
# MAGIC (
# MAGIC     NULL,
# MAGIC     '',
# MAGIC     'b6d60105-f982-4f3e-b653-ee1fa1de85b4',
# MAGIC     '278b7765-5f9f-422b-85b3-0221fd755d00',
# MAGIC     '5ff287c6-1103-4c36-8f38-cd9589e39578',
# MAGIC     NULL,
# MAGIC     'warning'
# MAGIC ),
# MAGIC (
# MAGIC     NULL,
# MAGIC     '',
# MAGIC     'b6d60105-f982-4f3e-b653-ee1fa1de85b4',
# MAGIC     '278b7765-5f9f-422b-85b3-0221fd755d00',
# MAGIC     '8421af6f-7c1d-43e9-b458-91e35ec8aa4e',
# MAGIC     NULL,
# MAGIC     'error'
# MAGIC );
# MAGIC  
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE analytics_dq_dev.test_cases.test_cases_table_results (
# MAGIC     result_table_id    STRING,
# MAGIC     result_id          STRING,
# MAGIC     table_id           INT,
# MAGIC     project_id         STRING,
# MAGIC     rules_error        INT,
# MAGIC     rules_warning      INT,
# MAGIC     rules_pass         INT,
# MAGIC     rules_evaluated    INT,
# MAGIC     status             STRING,
# MAGIC     start_timestamp    TIMESTAMP,
# MAGIC     end_timestamp      TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC  
# MAGIC INSERT INTO analytics_dq_dev.test_cases.test_cases_table_results VALUES
# MAGIC (
# MAGIC '55a2c7d2-f323-42e8-be68-d8e6bd3be05b',
# MAGIC '19c26485-a787-4e0b-951e-144dc5381eed',
# MAGIC 35,
# MAGIC NULL,
# MAGIC 4,
# MAGIC 1,
# MAGIC 0,
# MAGIC 5,
# MAGIC 'error',
# MAGIC '2026-04-20T12:47:23.444+03:00',
# MAGIC '2026-04-20T12:48:49.428+03:00'
# MAGIC ),
# MAGIC (
# MAGIC '46a6c2cb-e75f-4fa8-966a-b526d6b11519',
# MAGIC 'fe5612fe-4f35-4b3d-be17-22a120b8e731',
# MAGIC 36,
# MAGIC NULL,
# MAGIC 2,
# MAGIC 0,
# MAGIC 2,
# MAGIC 4,
# MAGIC 'error',
# MAGIC '2026-04-20T12:50:03.867+03:00',
# MAGIC '2026-04-20T12:51:15.930+03:00'
# MAGIC );
# MAGIC  
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE analytics_dq_dev.test_cases.test_cases_rule_results (
# MAGIC     result_rule_id        STRING,
# MAGIC     result_table_id       STRING,
# MAGIC     result_id             STRING,
# MAGIC     project_id            STRING,
# MAGIC     rule_assignment_id    INT,
# MAGIC     policy_id             INT,
# MAGIC     rows_evaluated        INT,
# MAGIC     rows_failed           INT,
# MAGIC     rows_pass             INT,
# MAGIC     error_message         STRING,
# MAGIC     status                VARCHAR(20),
# MAGIC     rule_type_id          INT,
# MAGIC     rule_dimension_id     INT,
# MAGIC     statement             STRING,
# MAGIC     alert                 BOOLEAN,
# MAGIC     alert_id              STRING,
# MAGIC     start_timestamp       TIMESTAMP,
# MAGIC     end_timestamp         TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC INSERT INTO analytics_dq_dev.test_cases.test_cases_rule_results VALUES
# MAGIC (
# MAGIC '3db6000a-24c2-4196-a7ea-63a80413be44',
# MAGIC '46a6c2cb-e75f-4fa8-966a-b526d6b11519',
# MAGIC 'fe5612fe-4f35-4b3d-be17-22a120b8e731',
# MAGIC NULL,
# MAGIC 43, 1, 5, 1, 4,
# MAGIC NULL,
# MAGIC 'error',
# MAGIC 1, NULL,
# MAGIC 'select `country`, `phone_number` from `df_filtered_by_filter_field` where 1=1 group by `country`, `phone_number` having count(*) > 1',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:50:39.414+03:00',
# MAGIC '2026-04-20T12:50:46.240+03:00'
# MAGIC ),
# MAGIC (
# MAGIC '891de02d-3690-41a7-b5f6-fb7f75941fd8',
# MAGIC '55a2c7d2-f323-42e8-be68-d8e6bd3be05b',
# MAGIC '19c26485-a787-4e0b-951e-144dc5381eed',
# MAGIC NULL,
# MAGIC 40, 1, 6, 1, 5,
# MAGIC NULL,
# MAGIC 'error',
# MAGIC 1, NULL,
# MAGIC 'select `customer_id`, `email` from `df_filtered_by_filter_field` where 1=1 group by `customer_id`, `email` having count(*) > 1',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:48:35.291+03:00',
# MAGIC '2026-04-20T12:48:38.926+03:00'
# MAGIC ),
# MAGIC (
# MAGIC 'f2638689-593c-43e6-9006-35496daf430d',
# MAGIC '55a2c7d2-f323-42e8-be68-d8e6bd3be05b',
# MAGIC '19c26485-a787-4e0b-951e-144dc5381eed',
# MAGIC NULL,
# MAGIC 79, 5, 6, 1, 5,
# MAGIC NULL,
# MAGIC 'warning',
# MAGIC 1, NULL,
# MAGIC 'is_not_empty',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:47:50.561+03:00',
# MAGIC '2026-04-20T12:47:55.698+03:00'
# MAGIC ),
# MAGIC (
# MAGIC '8726a033-cd74-49bd-ab5e-66b30ee08eb1',
# MAGIC '55a2c7d2-f323-42e8-be68-d8e6bd3be05b',
# MAGIC '19c26485-a787-4e0b-951e-144dc5381eed',
# MAGIC NULL,
# MAGIC 39, 1, 6, 1, 5,
# MAGIC NULL,
# MAGIC 'error',
# MAGIC 1, NULL,
# MAGIC 'is_not_null',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:48:04.899+03:00',
# MAGIC '2026-04-20T12:48:10.796+03:00'
# MAGIC ),
# MAGIC (
# MAGIC '5925e709-f9eb-4f38-8bbe-101021885475',
# MAGIC '55a2c7d2-f323-42e8-be68-d8e6bd3be05b',
# MAGIC '19c26485-a787-4e0b-951e-144dc5381eed',
# MAGIC NULL,
# MAGIC 39, 1, 6, 1, 5,
# MAGIC NULL,
# MAGIC 'error',
# MAGIC 1, NULL,
# MAGIC 'is_not_null',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:47:34.143+03:00',
# MAGIC '2026-04-20T12:47:40.965+03:00'
# MAGIC ),
# MAGIC (
# MAGIC '28095a84-1563-455a-b7f0-a4c1bb13597d',
# MAGIC '46a6c2cb-e75f-4fa8-966a-b526d6b11519',
# MAGIC 'fe5612fe-4f35-4b3d-be17-22a120b8e731',
# MAGIC NULL,
# MAGIC 41, 5, 5, 0, 5,
# MAGIC NULL,
# MAGIC 'pass',
# MAGIC 1, NULL,
# MAGIC 'is_not_empty',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:50:11.578+03:00',
# MAGIC '2026-04-20T12:50:17.146+03:00'
# MAGIC ),
# MAGIC (
# MAGIC 'd6f9390a-e714-4ba7-b94c-4f78ff38658f',
# MAGIC '55a2c7d2-f323-42e8-be68-d8e6bd3be05b',
# MAGIC '19c26485-a787-4e0b-951e-144dc5381eed',
# MAGIC NULL,
# MAGIC 39, 1, 6, 1, 5,
# MAGIC NULL,
# MAGIC 'error',
# MAGIC 1, NULL,
# MAGIC 'is_not_null',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:48:20.639+03:00',
# MAGIC '2026-04-20T12:48:26.332+03:00'
# MAGIC ),
# MAGIC (
# MAGIC '63fa7f4d-1b28-49aa-a601-2513fb9c6b3b',
# MAGIC '46a6c2cb-e75f-4fa8-966a-b526d6b11519',
# MAGIC 'fe5612fe-4f35-4b3d-be17-22a120b8e731',
# MAGIC NULL,
# MAGIC 42, 1, 5, 5, 0,
# MAGIC NULL,
# MAGIC 'error',
# MAGIC 1, NULL,
# MAGIC 'is_not_null',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:50:25.722+03:00',
# MAGIC '2026-04-20T12:50:30.617+03:00'
# MAGIC ),
# MAGIC (
# MAGIC 'eab4fa3e-6f62-4876-aff3-1ae5c196262b',
# MAGIC '46a6c2cb-e75f-4fa8-966a-b526d6b11519',
# MAGIC 'fe5612fe-4f35-4b3d-be17-22a120b8e731',
# MAGIC NULL,
# MAGIC 42, 1, 5, 0, 5,
# MAGIC NULL,
# MAGIC 'pass',
# MAGIC 1, NULL,
# MAGIC 'is_not_null',
# MAGIC NULL, NULL,
# MAGIC '2026-04-20T12:50:55.089+03:00',
# MAGIC '2026-04-20T12:51:02.398+03:00'
# MAGIC );
# MAGIC