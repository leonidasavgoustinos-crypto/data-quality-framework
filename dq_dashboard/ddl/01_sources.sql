-- @targets: free
-- @sandbox_only
-- Source tables for the registered tables (EMPTY; data comes from the backfill driver).
-- Written by scripts/gen_ddl.py --target <t>. Run with scripts/run_ddl.py. Teardown: teardown.py

CREATE CATALOG IF NOT EXISTS analytics_fs_dev COMMENT 'dq_dashboard sandbox - generated';
CREATE CATALOG IF NOT EXISTS analytics_gold_cr_models_dev COMMENT 'dq_dashboard sandbox - generated';
CREATE CATALOG IF NOT EXISTS analytics_gold_cr_validation_dev COMMENT 'dq_dashboard sandbox - generated';
CREATE CATALOG IF NOT EXISTS analytics_silver_dev COMMENT 'dq_dashboard sandbox - generated';

CREATE SCHEMA IF NOT EXISTS analytics_dq_dev.test_cases COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_fs_dev.risk COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_models_dev.ifrs9_lgd_components COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_models_dev.master_scale COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_models_dev.retail_behavioral_pds COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_validation_dev.corporate_rating_corporate_monitoring COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_validation_dev.master_scale_monitoring COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_validation_dev.masterscale COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_validation_dev.pd_customer_cu COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_validation_dev.pd_induns COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_gold_cr_validation_dev.retail_behavioral_pds_monitoring COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_silver_dev.central_tables COMMENT 'dq_dashboard sandbox - generated';
CREATE SCHEMA IF NOT EXISTS analytics_silver_dev.sas_test COMMENT 'dq_dashboard sandbox - generated';

-- table_id 1 · layer test
CREATE TABLE IF NOT EXISTS analytics_dq_dev.test_cases.`test_case_2` (
  `phone_number` STRING,
  `country` STRING,
  `ingestion_date` DATE,
  `username` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 2 · layer test_test
CREATE TABLE IF NOT EXISTS analytics_dq_dev.test_cases.`test_case_1` (
  `customer_id` STRING,
  `email` STRING,
  `created_at` DATE,
  `first_name` STRING,
  `last_name` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 3 · layer silver
CREATE TABLE IF NOT EXISTS analytics_silver_dev.sas_test.`customers_pk_violation_test_2` (
  `crsid` INT,
  `date` DATE,
  `year` INT,
  `month` INT,
  `year_month` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 4 · layer silver
CREATE TABLE IF NOT EXISTS analytics_silver_dev.sas_test.`customers_pk_violation_test` (
  `crsid` INT,
  `date` DATE,
  `year` INT,
  `month` INT,
  `year_month` STRING,
  `nationality_country_code` STRING,
  `tax_identification_number` STRING,
  `credit_risk_rating` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 5 · layer silver
CREATE TABLE IF NOT EXISTS analytics_silver_dev.central_tables.`customer` (
  `crsid` INT,
  `date` DATE,
  `year` INT,
  `month` INT,
  `year_month` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 6 · layer silver
CREATE TABLE IF NOT EXISTS analytics_silver_dev.central_tables.`assets` (
  `date` DATE,
  `year` INT,
  `month` INT,
  `asset_code` STRING,
  `source_system_code` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 7 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_global_customer_quarterly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `PreviousEBAStatus_Cust` STRING,
  `EBAStatusRuleCode_Cust` STRING,
  `IRBFlag_Cust` BOOLEAN,
  `NPEFlag_Cust_Ori` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 8 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_non_performing_customer_quarterly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 9 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_performing_customer_quarterly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 10 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_global_exposure_quarterly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `CustomerCode` STRING,
  `IsExclusionGlobalWOF` BOOLEAN,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 11 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_limits_ccfs_exposure_quarterly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 12 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_non_performing_exposure_quarterly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 13 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_performing_exposure_quarterly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 14 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_staging_exposure_quarterly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 15 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_sbl_exposure_monthly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 16 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_sbl_customer_monthly` (
  `ReferenceDate` DATE,
  `CommonObligor_InternalCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 17 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_masterscale_exposure_monthly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 18 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_masterscale_customer_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 19 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_ead_qrre_exposure_monthly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 20 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_ead_qrre_customer_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 21 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_induns_exposure_monthly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 22 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_induns_customer_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 23 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_indsec_exposure_monthly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 24 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_indsec_customer_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 25 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.masterscale.`masterscale_psi` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 26 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.masterscale.`masterscale_total` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 27 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_pd_qrre_exposure_monthly` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 28 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_pd_qrre_customer_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 29 · layer fs
CREATE TABLE IF NOT EXISTS analytics_fs_dev.risk.`fs_cu_pd_customer_monthly` (
  `ReferenceDate` DATE,
  `CustomerCodeFinal` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 30 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.retail_behavioral_pds.`raw_indsec_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 31 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.retail_behavioral_pds.`raw_induns_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 32 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.ifrs9_lgd_components.`raw_ead_qrre_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 33 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.master_scale.`raw_masterscale_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 34 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.retail_behavioral_pds.`raw_sbl_monthly` (
  `ReferenceDate` DATE,
  `CommonObligor_InternalCode` STRING,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 35 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.retail_behavioral_pds.`raw_cu_pd_monthly` (
  `ReferenceDate` DATE,
  `CustomerCodeFinal` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 36 · layer fs
CREATE TABLE IF NOT EXISTS analytics_gold_cr_models_dev.retail_behavioral_pds.`raw_pd_qrre_monthly` (
  `ReferenceDate` DATE,
  `CustomerCode` STRING,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 37 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.masterscale.`masterscale_total_final` (
  `AssessmentDate` DATE,
  `ReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 38 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.masterscale.`masterscale_no_SME_final` (
  `AssessmentDate` DATE,
  `ReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 39 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.pd_customer_cu.`customer_cu_total` (
  `ReferenceDate` DATE,
  `CustomerCodeFinal` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 40 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.pd_customer_cu.`customer_cu_total_final` (
  `ReferenceDate` DATE,
  `CustomerCodeFinal` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 41 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.pd_induns.`dev_forborne` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 42 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.pd_induns.`dev_non_forborne` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 43 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.retail_behavioral_pds_monitoring.`agri_pd_final_data` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 44 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.corporate_rating_corporate_monitoring.`combined_dataset_ms` (
  `ReferenceDate` DATE,
  `InternalCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 45 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.master_scale_monitoring.`final_data` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

-- table_id 46 · layer gold
CREATE TABLE IF NOT EXISTS analytics_gold_cr_validation_dev.retail_behavioral_pds_monitoring.`sbl_pd_final_data` (
  `ReferenceDate` DATE,
  `ExposureReferenceCode` STRING,
  `amount` DOUBLE,
  `segment` STRING
) COMMENT 'dq_dashboard sandbox - generated';

