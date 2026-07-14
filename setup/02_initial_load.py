# Databricks notebook source
dbutils.widgets.text("target_environment", "")
environment = dbutils.widgets.get("target_environment")
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit, col
environment = 'dev'
name = "analytics_dq_"
catalog = f"{name}{environment}"

##############################################################################################
####################################     USER INPUTS     #####################################
##############################################################################################

##############################################################################################
###############################     USER INPUTS RULE_TYPE     ################################
##############################################################################################

user_rule_types = [
    {"rule_type": "technical",
    "description": "technical data profiling quality rules", 
    "added_by": "Groumpas"
    },
    {"rule_type": "business", 
     "description": "business rules for data quality", 
     "added_by": "Groumpas"
    }
]
##############################################################################################

##############################################################################################
############################     USER INPUTS RULE_DIMENSION      #############################
##############################################################################################

user_rule_dimensions = [
    {
        "rule_dimension": "completeness",
        "description": "Completeness measures whether all required data is present",
        "added_by": "Groumpas"
    },
    {
        "rule_dimension": "validity",
        "description": "Validity checks if the data follow formats and constraints defined by the business",
        "added_by": "Groumpas"
    },
    {
        "rule_dimension": "consistency",
        "description": "Consistency checks that data matches across fields, tables and layers",
        "added_by": "Groumpas"
    },
    {
        "rule_dimension": "uniqueness",
        "description": "Uniqueness ensures no duplicates exist",
        "added_by": "Groumpas"
    }
]
##############################################################################################

##############################################################################################
#################################     USER INPUTS TABLE     ##################################
##############################################################################################

user_values = [
    {
        "environment": "dev",
        "catalog": "analytics_dq_dev",
        "schema": "test_cases",
        "table": "test_case_2",
        "layer": "test",
        "primary_keys": ["phone_number", "country"],
        "filter_field": "ingestion_date",
        "filter_field_type": "date",
        "added_by": "staff"
    },
    {
        "environment": "dev",
        "catalog": "analytics_dq_dev",
        "schema": "test_cases",
        "table": "test_case_1",
        "layer": "test_test",
        "primary_keys": ["customer_id", "email"],
        "filter_field": "created_at",
        "filter_field_type": "date",
        "added_by": "staff"
    },
    {
        "environment": "dev",
        "catalog": "analytics_silver_dev",
        "schema": "sas_test",
        "table": "customers_pk_violation_test_2",
        "layer": "silver",
        "primary_keys": ["crsid", "date", "year", "month", "year_month"],
        "filter_field": "crsid",
        "filter_field_type": "int",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_silver_dev",
        "schema": "sas_test",
        "table": "customers_pk_violation_test",
        "layer": "silver",
        "primary_keys": ["crsid", "date", "year", "month", "year_month"],
        "filter_field": "date",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_silver_dev",
        "schema": "central_tables",
        "table": "customer",
        "layer": "silver",
        "primary_keys": ["crsid", "date", "year", "month", "year_month"],
        "filter_field": "date",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_silver_dev",
        "schema": "central_tables",
        "table": "assets",
        "layer": "silver",
        "primary_keys": ["date", "year", "month", "asset_code", "source_system_code"],
        "filter_field": "date",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_global_customer_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_non_performing_customer_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_performing_customer_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_global_exposure_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_limits_ccfs_exposure_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_non_performing_exposure_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_performing_exposure_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_staging_exposure_quarterly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_sbl_exposure_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_sbl_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CommonObligor_InternalCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_masterscale_exposure_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_masterscale_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_ead_qrre_exposure_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_ead_qrre_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_induns_exposure_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_induns_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_indsec_exposure_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_indsec_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "masterscale",
        "table": "masterscale_psi",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "masterscale",
        "table": "masterscale_total",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_pd_qrre_exposure_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_pd_qrre_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_fs_dev",
        "schema": "risk",
        "table": "fs_cu_pd_customer_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCodeFinal"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "retail_behavioral_pds",
        "table": "raw_indsec_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "retail_behavioral_pds",
        "table": "raw_induns_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "ifrs9_lgd_components",
        "table": "raw_ead_qrre_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "master_scale",
        "table": "raw_masterscale_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "retail_behavioral_pds",
        "table": "raw_sbl_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CommonObligor_InternalCode", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "retail_behavioral_pds",
        "table": "raw_cu_pd_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCodeFinal"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_models_dev",
        "schema": "retail_behavioral_pds",
        "table": "raw_pd_qrre_monthly",
        "layer": "fs",
        "primary_keys": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "masterscale",
        "table": "masterscale_total_final",
        "layer": "gold",
        "primary_keys": ["AssessmentDate", "ReferenceCode"],
        "filter_field": "AssessmentDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "masterscale",
        "table": "masterscale_no_SME_final",
        "layer": "gold",
        "primary_keys": ["AssessmentDate", "ReferenceCode"],
        "filter_field": "AssessmentDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "pd_customer_cu",
        "table": "customer_cu_total",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "CustomerCodeFinal"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "pd_customer_cu",
        "table": "customer_cu_total_final",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "CustomerCodeFinal"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "pd_induns",
        "table": "dev_forborne",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "pd_induns",
        "table": "dev_non_forborne",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "retail_behavioral_pds_monitoring",
        "table": "agri_pd_final_data",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "corporate_rating_corporate_monitoring",
        "table": "combined_dataset_ms",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "InternalCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "master_scale_monitoring",
        "table": "final_data",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    },
    {
        "environment": "dev",
        "catalog": "analytics_gold_cr_validation_dev",
        "schema": "retail_behavioral_pds_monitoring",
        "table": "sbl_pd_final_data",
        "layer": "gold",
        "primary_keys": ["ReferenceDate", "ExposureReferenceCode"],
        "filter_field": "ReferenceDate",
        "filter_field_type": "date",
        "added_by": "Groumpas"
    }
]
##############################################################################################

##############################################################################################
################################     USER INPUTS PROJECT     #################################
##############################################################################################

user_projects = [
    {
        "project": "sas2py",
        "project_description": "sas retirement and migration of risk models to cloud",
        "added_by": "Groumpas"
    },
    {
        "project": "project_2",
        "project_description": "project_2_description",
        "added_by": "Groumpas"
    }
]
##############################################################################################

##############################################################################################
###############################     USER INPUTS RUN_POLICY     ###############################
##############################################################################################

user_values_run_policy = [
    {
        "criticality": "high",
        "category": "error",
        "quarantine": True,
        "added_by": "Groumpas"
    },
    {
        "criticality": "high",
        "category": "error",
        "quarantine": False,
        "added_by": "Groumpas"
    },
    {
        "criticality": "high",
        "category": "warning",
        "quarantine": True,
        "added_by": "Groumpas"
    },
    {
        "criticality": "high",
        "category": "warning",
        "quarantine": False,
        "added_by": "Groumpas"
    },
    {
        "criticality": "medium",
        "category": "error",
        "quarantine": True,
        "added_by": "Groumpas"
    },
    {
        "criticality": "medium",
        "category": "error",
        "quarantine": False,
        "added_by": "Groumpas"
    },
    {
        "criticality": "medium",
        "category": "warning",
        "quarantine": True,
        "added_by": "Groumpas"
    },
    {
        "criticality": "medium",
        "category": "warning",
        "quarantine": False,
        "added_by": "Groumpas"
    },
    {
        "criticality": "low",
        "category": "error",
        "quarantine": True,
        "added_by": "Groumpas"
    },
    {
        "criticality": "low",
        "category": "error",
        "quarantine": False,
        "added_by": "Groumpas"
    },
    {
        "criticality": "low",
        "category": "warning",
        "quarantine": True,
        "added_by": "Groumpas"
    },
    {
        "criticality": "low",
        "category": "warning",
        "quarantine": False,
        "added_by": "Groumpas"
    }
]
##############################################################################################

##############################################################################################
#############################     USER INPUTS RULE_TEMPLATE     ##############################
##############################################################################################

user_values_rule_templates = [
    {
        "name": "is_not_null",
        "description": "value is not null.",
        "rule_type_nk": "technical",
        "rule_dimension_nk": "validity",
        "scope": "column",
        "is_reusable": True,
        "engine_type": "dqx",
        "statement": "is_not_null",
        "added_by": "Groumpas"
    },
    {
        "name": "is_not_empty",
        "description": "string is not empty (nulls allowed).",
        "rule_type_nk": "technical",
        "rule_dimension_nk": "validity",
        "scope": "column",
        "is_reusable": True,
        "engine_type": "dqx",
        "statement": "is_not_empty",
        "added_by": "Groumpas"
    },
    {
        "name": "is_not_null_and_not_empty",
        "description": "value is neither null nor empty.",
        "rule_type_nk": "technical",
        "rule_dimension_nk": "validity",
        "scope": "column",
        "is_reusable": True,
        "engine_type": "dqx",
        "statement": "is_not_null_and_not_empty",
        "added_by": "Groumpas"
    },
    {
        "name": "PK_violation_custom_error",
        "description": "Primary Key Violation",
        "rule_type_nk": "technical",
        "rule_dimension_nk": "uniqueness",
        "is_reusable": True,
        "scope": "table",
        "engine_type": "sql",
        "statement": "select ${i:columns} from_customer_error ${table} where 1=1 group by ${i:columns} having count(*) > 1",
        "added_by": "Groumpas"
    },
    {
        "name": "non_reusable_statement",
        "description": "Non Reusable Statement",
        "rule_type_nk": "business",
        "rule_dimension_nk": "validity",
        "is_reusable": False,
        "scope": "table",
        "engine_type": "sql",
        "statement": "select exp.* from analytics_fs_dev.risk.fs_global_exposure_quarterly exp left join analytics_fs_dev.risk.fs_global_customer_quarterly cust on cust.CustomerCode = exp.CustomerCode and cust.ReferenceDate = exp.ReferenceDate where exp.ReferenceDate > '2025-01-01' and exp.IsExclusionGlobalWOF = True and cust.NPEFlag_Cust_Ori = 'NPE' and cust.IRBFlag_Cust = True and cust.EBAStatusRuleCode_Cust = '0.1-' and exp.ExposureReferenceCode in ('6996161857971', '6993152439289', '6992154029456', '6996161549444')",
        "added_by": "Groumpas"
    },
    {
        "name": "check_thresholds",
        "description": "Check thresholds",
        "rule_type_nk": "business",
        "rule_dimension_nk": "validity",
        "is_reusable": True,
        "scope": "table",
        "engine_type": "sql",
        "statement": "select * from ${table} where ReferenceDate between ${v:min_date} and ${v:max_date} and EBAStatusRuleCode_Cust = ${v:eba} and IRBFlag_Cust = ${v:irb}",
        "added_by": "Groumpas"
    },
    {
        "name": "primary_key_violation",
        "description": "Primary Key Violation",
        "rule_type_nk": "technical",
        "rule_dimension_nk": "uniqueness",
        "is_reusable": True,
        "scope": "table",
        "engine_type": "sql",
        "statement": "select ${i:columns} from ${table} where 1=1 group by ${i:columns} having count(*) > 1",
        "added_by": "Groumpas"
    },
    {
        "name": "date_sequence_error",
        "description": "Temporal Inconsistency",
        "rule_type_nk": "business",
        "rule_dimension_nk": "validity",
        "is_reusable": True,
        "scope": "column",
        "engine_type": "sql",
        "statement": "select * from ${table} where 1=1 and ${eff_dt} > ${end_dt}",
        "added_by": "Groumpas"
    },
    {
        "name": "vat_gr_format",
        "description": "Greek Tax Identification Number Validation",
        "rule_type_nk": "business",
        "rule_dimension_nk": "validity",
        "is_reusable": True,
        "scope": "column",
        "engine_type": "sql",
        "statement": "select * from ${table} where 1=1 and (trim(ifnull(${afm}, '')) = ''or length(trim(${afm})) <> 9 or case when ( (try_cast(substr(trim(${afm}),1,1) as int)*256 +  try_cast(substr(trim(${afm}),2,1) as int)*128 +  try_cast(substr(trim(${afm}),3,1) as int)*64 +  try_cast(substr(trim(${afm}),4,1) as int)*32 + try_cast(substr(trim(${afm}),5,1) as int)*16 + try_cast(substr(trim(${afm}),6,1) as int)*8 + try_cast(substr(trim(${afm}),7,1) as int)*4 + try_cast(substr(trim(${afm}),8,1) as int)*2) % 11 ) = 10 then 0 else ( (try_cast(substr(trim(${afm}),1,1) as int)*256 +  try_cast(substr(trim(${afm}),2,1) as int)*128 +  try_cast(substr(trim(${afm}),3,1) as int)*64 +  try_cast(substr(trim(${afm}),4,1) as int)*32 +  try_cast(substr(trim(${afm}),5,1) as int)*16 +  try_cast(substr(trim(${afm}),6,1) as int)*8 +  try_cast(substr(trim(${afm}),7,1) as int)*4 +  try_cast(substr(trim(${afm}),8,1) as int)*2) % 11) end <> try_cast(substr(trim(${afm}),9,1) as int))",
        "added_by": "Groumpas"
    },
    {
        "name": "negative_balances",
        "description": "Negative Balances",
        "rule_type_nk": "business",
        "rule_dimension_nk": "validity",
        "is_reusable": True,
        "scope": "column",
        "engine_type": "sql",
        "statement": "select * from ${table} where 1=1 and ${balance} < 0.00",
        "added_by": "Groumpas"
    }
]
##############################################################################################


##############################################################################################
################################     USER INPUTS APPLY_AT     ################################
##############################################################################################

user_values_apply_at = [
    {
        "apply_at": "bronze",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "silver",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "gold",
        "is_project_specific": True,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "fs",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "bronze_to_silver",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "bronze_to_fs",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "silver_to_gold",
        "is_project_specific": True,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "silver_to_fs",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "fs_to_gold",
        "is_project_specific": True,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "bronze_to_gold",
        "is_project_specific": True,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "test",
        "is_project_specific": False,
        "added_by": "Groumpas"
    },
    {
        "apply_at": "test_test",
        "is_project_specific": False,
        "added_by": "Groumpas"
    }
]
##############################################################################################

##############################################################################################
############################     USER INPUTS RULE_ASSIGNMENT     #############################
##############################################################################################

user_values_rule_assignment = [
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_1",
        "template_nk": "is_not_empty",
        "policy_nk": "warning.medium.quarantine",
        "apply_at_nk": "test_test.project_agnostic",
        "parameters_identifiers": '{"column": "email"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_2",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test.project_agnostic",
        "parameters_identifiers": '{"columns": ["country", "phone_number"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_2",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test.project_agnostic",
        "parameters_identifiers": '{"column": "phone_number"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_2",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test.project_agnostic",
        "parameters_identifiers": '{"column": "username"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_2",
        "template_nk": "is_not_empty",
        "policy_nk": "warning.medium.quarantine",
        "apply_at_nk": "test.project_agnostic",
        "parameters_identifiers": '{"column": "username"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_1",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test_test.project_agnostic",
        "parameters_identifiers": '{"columns": ["customer_id", "email"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_1",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test_test.project_agnostic",
        "parameters_identifiers": '{"column": "email"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_1",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test_test.project_agnostic",
        "parameters_identifiers": '{"column": "customer_id"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_1",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test_test.project_agnostic",
        "parameters_identifiers": '{"column": "last_name"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_dq_dev.test_cases.test_case_1",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "test_test.project_agnostic",
        "parameters_identifiers": '{"column": "first_name"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_psi",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_total",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_total_final",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["AssessmentDate", "ReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_no_SME_final",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["AssessmentDate", "ReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_psi",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_psi",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_total",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}', 
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_total",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_total_final",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceCode"}', 
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_total_final",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "AssessmentDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_no_SME_final",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "AssessmentDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.masterscale.masterscale_no_SME_final",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceCode"}', 
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_indsec_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_induns_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.ifrs9_lgd_components.raw_ead_qrre_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale.raw_masterscale_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_sbl_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CommonObligor_InternalCode", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_cu_pd_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCodeFinal"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_cu_pd_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCodeFinal"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_cu_pd_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_pd_qrre_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_pd_qrre_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_pd_qrre_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_pd_qrre_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_indsec_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_indsec_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_indsec_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_induns_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_induns_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_induns_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.ifrs9_lgd_components.raw_ead_qrre_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.ifrs9_lgd_components.raw_ead_qrre_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.ifrs9_lgd_components.raw_ead_qrre_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale.raw_masterscale_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale.raw_masterscale_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale.raw_masterscale_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_sbl_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_sbl_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds.raw_sbl_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CommonObligor_InternalCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_non_performing_customer_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_non_performing_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_non_performing_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_performing_customer_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_performing_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_performing_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_exposure_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_limits_ccfs_exposure_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_limits_ccfs_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_limits_ccfs_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_non_performing_exposure_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_non_performing_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_non_performing_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_performing_exposure_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_performing_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_performing_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_staging_exposure_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_staging_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_staging_exposure_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_ead_qrre_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_ead_qrre_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_ead_qrre_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_induns_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_induns_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_induns_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_induns_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_induns_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_induns_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_indsec_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_indsec_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_indsec_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_indsec_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_indsec_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_indsec_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_pd_qrre_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_pd_qrre_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_pd_qrre_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_pd_qrre_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_pd_qrre_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_pd_qrre_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_cu_pd_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCodeFinal"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_cu_pd_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCodeFinal"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_cu_pd_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_customer_quarterly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "fs.project_agnostic",
        "parameters_identifiers": '{"column": "PreviousEBAStatus_Cust"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_customer_quarterly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test",
        "template_nk": "is_not_null",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"column": "nationality_country_code"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test",
        "template_nk": "is_not_null",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"column": "credit_risk_rating"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test",
        "template_nk": "is_not_empty",
        "policy_nk": "warning.medium.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"column": "tax_identification_number"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test",
        "template_nk": "is_not_null_and_not_empty",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"column": "tax_identification_number"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"columns":["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_masterscale_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_masterscale_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_masterscale_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_masterscale_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_masterscale_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_masterscale_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_exposure_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns":["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_exposure_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CommonObligor_InternalCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}', 
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_sbl_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CommonObligor_InternalCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_ead_qrre_customer_monthly",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCode"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_ead_qrre_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_ead_qrre_customer_monthly",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze_to_fs.project_agnostic",
        "parameters_identifiers": '{"column": "CustomerCode"}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test",
        "template_nk": "PK_violation_custom_error",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "gold.project_specific",
        "parameters_identifiers": '{"columns": ["crsid", "date", "year", "month", "year_month"]}',
        "parameters_values": '',
        "project_nk": "project_2",
        "added_by": "Mr Big Failure"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_exposure_quarterly",
        "template_nk": "non_reusable_statement",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs.project_agnostic",
        "parameters_identifiers": '',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_customer_quarterly",
        "template_nk": "check_thresholds",
        "policy_nk": "warning.high.quarantine",
        "apply_at_nk": "fs.project_agnostic",
        "parameters_identifiers": '',
        "parameters_values": '{"irb": false, "eba": "-", "min_date": "2023-03-01", "max_date": "2026-05-31"}',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test_2",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"columns":  ["crsid", "date", "year", "month", "year_month"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_fs_dev.risk.fs_global_customer_quarterly",
        "template_nk": "check_thresholds",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs.project_agnostic",
        "parameters_identifiers": '',
        "parameters_values": '{"irb": false, "eba": "-", "min_date": "2023-03-01", "max_date": "2023-05-31"}',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_silver_dev.sas_test.customers_pk_violation_test",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "bronze.project_agnostic",
        "parameters_identifiers": '{"columns": ["crsid", "date", "year", "month", "year_month"]}',
        "parameters_values": '',
        "project_nk": '',
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_customer_cu.customer_cu_total",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_customer_cu.customer_cu_total",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCodeFinal"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_customer_cu.customer_cu_total",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCodeFinal"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_customer_cu.customer_cu_total_final",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_customer_cu.customer_cu_total_final",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "CustomerCodeFinal"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_customer_cu.customer_cu_total_final",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "CustomerCodeFinal"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_induns.dev_forborne",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_induns.dev_forborne",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_induns.dev_forborne",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_induns.dev_non_forborne",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_induns.dev_non_forborne",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_validation_dev.pd_induns.dev_non_forborne",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds_monitoring.agri_pd_final_data",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds_monitoring.agri_pd_final_data",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds_monitoring.agri_pd_final_data",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.corporate_rating_corporate_monitoring.combined_dataset_ms",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.corporate_rating_corporate_monitoring.combined_dataset_ms",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "InternalCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.corporate_rating_corporate_monitoring.combined_dataset_ms",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "InternalCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale_monitoring.final_data",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale_monitoring.final_data",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.master_scale_monitoring.final_data",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds_monitoring.sbl_pd_final_data",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ReferenceDate"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds_monitoring.sbl_pd_final_data",
        "template_nk": "is_not_null",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"column": "ExposureReferenceCode"}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    },
    {
        "table_nk": "analytics_gold_cr_models_dev.retail_behavioral_pds_monitoring.sbl_pd_final_data",
        "template_nk": "primary_key_violation",
        "policy_nk": "error.high.quarantine",
        "apply_at_nk": "fs_to_gold.project_specific",
        "parameters_identifiers": '{"columns": ["ReferenceDate", "ExposureReferenceCode"]}',
        "parameters_values": '',
        "project_nk": "sas2py",
        "added_by": "Groumpas"
    }
]
##############################################################################################

schema_metadata = "metadata"

##### rule_type ######

df = spark.createDataFrame(user_rule_types)
df.createOrReplaceTempView("rule_type_inputs")

sql_prepared = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_type AS
SELECT
    src.rule_type AS rule_type_nk,
    src.rule_type,
    src.description,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.description AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM rule_type_inputs src
"""

sql_close = f"""
MERGE INTO {catalog}.{schema_metadata}.rule_type AS t
USING prepared_rule_type AS s
ON t.rule_type_nk = s.rule_type_nk
AND t.is_active = TRUE
AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.description AS string), '_NULL_')
            ), 256
        ) <> s.new_hash

WHEN MATCHED THEN 
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing = f"""
UPDATE {catalog}.{schema_metadata}.rule_type t 
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_rule_type s
    WHERE s.rule_type_nk = t.rule_type_nk
    )
"""

sql_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.rule_type (
    rule_type_nk,
    rule_type,
    description,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.rule_type_nk,
    s.rule_type,
    s.description,
    s.added_by,
    true,
    current_timestamp(),
    NULL
FROM prepared_rule_type s
LEFT ANTI JOIN {catalog}.{schema_metadata}.rule_type t
    ON  t.rule_type_nk = s.rule_type_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.description AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared)
    spark.sql(sql_close)
    spark.sql(sql_close_missing)
    spark.sql(sql_insert)
    print("SCD2 rule_type update completed successfully.")
except Exception as e:
    print("Error during SCD2 update:", e)

##### rule_dimension ######

df = spark.createDataFrame(user_rule_dimensions)
df.createOrReplaceTempView("rule_dimension_inputs")

sql_prepared = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_dimension AS
SELECT
    src.rule_dimension AS rule_dimension_nk,
    src.rule_dimension,
    src.description,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.description AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM rule_dimension_inputs src
"""

sql_close = f"""
MERGE INTO {catalog}.{schema_metadata}.rule_dimension AS t
USING prepared_rule_dimension AS s
ON t.rule_dimension_nk = s.rule_dimension_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.description AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing = f"""
UPDATE {catalog}.{schema_metadata}.rule_dimension t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_rule_dimension s
    WHERE s.rule_dimension_nk = t.rule_dimension_nk
)
"""

sql_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.rule_dimension (
    rule_dimension_nk,
    rule_dimension,
    description,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.rule_dimension_nk,
    s.rule_dimension,
    s.description,
    s.added_by,
    true,
    current_timestamp(),
    NULL
FROM prepared_rule_dimension s
LEFT ANTI JOIN {catalog}.{schema_metadata}.rule_dimension t
    ON  t.rule_dimension_nk = s.rule_dimension_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.description AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared)
    spark.sql(sql_close)
    spark.sql(sql_close_missing)
    spark.sql(sql_insert)
    print("SCD2 rule_dimension update completed successfully.")
except Exception as e:
    print("Error during SCD2 update:", e)

##### table ######

df = spark.createDataFrame(user_values)
df.createOrReplaceTempView("table_inputs")

sql_prepared = f"""
CREATE OR REPLACE TEMP VIEW prepared_table AS
SELECT
    CONCAT(src.catalog, '.', src.schema, '.', src.`table`) AS table_nk,
    src.environment,
    src.catalog,
    src.schema,
    src.`table`,
    src.layer,
    src.primary_keys,
    src.filter_field,
    src.filter_field_type,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.primary_keys AS string), '_NULL_'),
            coalesce(cast(src.filter_field AS string), '_NULL_'),
            coalesce(cast(src.filter_field_type AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM table_inputs src
"""

sql_close = f"""
MERGE INTO {catalog}.{schema_metadata}.`table` AS t
USING prepared_table AS s
ON t.table_nk = s.table_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.primary_key AS string), '_NULL_'),
            coalesce(cast(t.filter_field AS string), '_NULL_'),
            coalesce(cast(t.filter_field_type AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing = f"""
UPDATE {catalog}.{schema_metadata}.`table` t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_table s
    WHERE s.table_nk = t.table_nk
)
"""

sql_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.`table` (
    table_nk,
    environment,
    catalog,
    schema,
    `table`,
    layer,
    primary_key,
    filter_field,
    filter_field_type,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.table_nk,
    s.environment,
    s.catalog,
    s.schema,
    s.`table`,
    s.layer,
    s.primary_keys,
    s.filter_field,
    s.filter_field_type,
    s.added_by,
    true,
    current_timestamp(),
    NULL
FROM prepared_table s
LEFT ANTI JOIN {catalog}.{schema_metadata}.`table` t
    ON  t.table_nk = s.table_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.primary_key AS string), '_NULL_'),
                coalesce(cast(t.filter_field AS string), '_NULL_'),
                coalesce(cast(t.filter_field_type AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared)
    spark.sql(sql_close)
    spark.sql(sql_close_missing)
    spark.sql(sql_insert)
    print("SCD2 table update completed successfully.")
except Exception as e:
    print("Error during SCD2 update:", e)


##### project ######

df = spark.createDataFrame(user_projects)
df.createOrReplaceTempView("project_inputs")

sql_prepared = f"""
CREATE OR REPLACE TEMP VIEW prepared_project AS
SELECT
    src.project AS project_nk,
    src.project,
    src.project_description,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.project_description AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM project_inputs src
"""

sql_close = f"""
MERGE INTO {catalog}.{schema_metadata}.project AS t
USING prepared_project AS s
ON t.project_nk = s.project_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.project_description AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing = f"""
UPDATE {catalog}.{schema_metadata}.project t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_project s
    WHERE s.project_nk = t.project_nk
)
"""

sql_insert = f"""
INSERT INTO {catalog}.{schema_metadata}.project (
    project_nk,
    project,
    project_description,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.project_nk,
    s.project,
    s.project_description,
    s.added_by,
    true,
    current_timestamp(),
    NULL
FROM prepared_project s
LEFT ANTI JOIN {catalog}.{schema_metadata}.project t
    ON  t.project_nk = s.project_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.project_description AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared)
    spark.sql(sql_close)
    spark.sql(sql_close_missing)
    spark.sql(sql_insert)
    print("SCD2 project update completed successfully.")
except Exception as e:
    print("Error during SCD2 update:", e)

schema = "configuration"

##### run_policy ######

df_policy = spark.createDataFrame(user_values_run_policy)
df_policy.createOrReplaceTempView("policy_inputs")

sql_prepared_policy = f"""
CREATE OR REPLACE TEMP VIEW prepared_run_policy AS
SELECT
    CONCAT(
        src.category, '.', 
        src.criticality, '.', 
        CASE WHEN src.quarantine THEN 'quarantine' ELSE 'no_quarantine' END
    ) AS policy_nk,

    src.category,
    src.criticality,
    src.quarantine,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.category AS string), '_NULL_'),
            coalesce(cast(src.criticality AS string), '_NULL_'),
            coalesce(cast(src.quarantine AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM policy_inputs src
"""

sql_close_policy = f"""
MERGE INTO {catalog}.{schema}.run_policy AS t
USING prepared_run_policy AS s
ON t.policy_nk = s.policy_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.category AS string), '_NULL_'),
            coalesce(cast(t.criticality AS string), '_NULL_'),
            coalesce(cast(t.quarantine AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing_policy = f"""
UPDATE {catalog}.{schema}.run_policy t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_run_policy s
    WHERE s.policy_nk = t.policy_nk
)
"""

sql_insert_policy = f"""
INSERT INTO {catalog}.{schema}.run_policy (
    policy_nk,
    category,
    criticality,
    quarantine,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.policy_nk,
    s.category,
    s.criticality,
    s.quarantine,
    s.added_by,
    TRUE,
    current_timestamp(),
    NULL
FROM prepared_run_policy s
LEFT ANTI JOIN {catalog}.{schema}.run_policy t
    ON  t.policy_nk = s.policy_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.category AS string), '_NULL_'),
                coalesce(cast(t.criticality AS string), '_NULL_'),
                coalesce(cast(t.quarantine AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared_policy)
    spark.sql(sql_close_policy)
    spark.sql(sql_close_missing_policy)
    spark.sql(sql_insert_policy)
    print("SCD2 run_policy update completed successfully.")
except Exception as e:
    print("Error:", e)

##### rule_template ######

df_templates = spark.createDataFrame(user_values_rule_templates)
df_templates.createOrReplaceTempView("templates_inputs")

sql_prepared_rt = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_templates AS
SELECT
    src.name AS template_nk,
    src.name AS name,
    src.description,
    rt.rule_type_id,
    rd.rule_dimension_id,
    src.scope,
    src.is_reusable,
    src.engine_type,
    src.statement,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(rt.rule_type_id AS string), '_NULL_'),
            coalesce(cast(rd.rule_dimension_id AS string), '_NULL_'),
            coalesce(cast(src.scope AS string), '_NULL_'),
            coalesce(cast(src.is_reusable AS string), '_NULL_')
        ), 256
    ) AS new_hash

FROM templates_inputs src
JOIN {catalog}.{schema_metadata}.rule_type rt
    ON rt.rule_type_nk = src.rule_type_nk
    AND rt.is_active = TRUE
JOIN {catalog}.{schema_metadata}.rule_dimension rd
    ON rd.rule_dimension_nk = src.rule_dimension_nk
    AND rd.is_active = TRUE
"""

sql_close_previous_rt = f"""
MERGE INTO {catalog}.{schema}.rule_template AS target
USING prepared_rule_templates AS source
ON target.template_nk = source.template_nk
AND target.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(target.rule_type_id AS string), '_NULL_'),
            coalesce(cast(target.rule_dimension_id AS string), '_NULL_'),
            coalesce(cast(target.scope AS string), '_NULL_'),
            coalesce(cast(target.is_reusable AS string), '_NULL_')
        ), 256
    ) <> source.new_hash

WHEN MATCHED THEN
UPDATE SET
    target.is_active = FALSE,
    target.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing_rt = f"""
UPDATE {catalog}.{schema}.rule_template t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_rule_templates s
    WHERE s.template_nk = t.template_nk
)
"""

sql_insert_new_rt = f"""
INSERT INTO {catalog}.{schema}.rule_template (
    name,
    template_nk,
    description,
    rule_type_id,
    rule_dimension_id,
    is_reusable,
    scope,
    engine_type,
    statement,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.name,
    s.template_nk,
    s.description,
    s.rule_type_id,
    s.rule_dimension_id,
    s.is_reusable,
    s.scope,
    s.engine_type,
    s.statement,
    s.added_by,
    TRUE,
    current_timestamp(),
    NULL
FROM prepared_rule_templates s
LEFT ANTI JOIN {catalog}.{schema}.rule_template t
    ON  t.template_nk = s.template_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.rule_type_id AS string), '_NULL_'),
                coalesce(cast(t.rule_dimension_id AS string), '_NULL_'),
                coalesce(cast(t.scope AS string), '_NULL_'),
                coalesce(cast(t.is_reusable AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared_rt)
    spark.sql(sql_close_previous_rt)
    spark.sql(sql_close_missing_rt)
    spark.sql(sql_insert_new_rt)
    print("SCD2 rule_template update completed successfully.")
except Exception as e:
    print("Error:", e)

##### apply_at ######

df_apply_at = spark.createDataFrame(user_values_apply_at)
df_apply_at.createOrReplaceTempView("apply_at_inputs")

sql_prepared_apply_at = f"""
CREATE OR REPLACE TEMP VIEW prepared_apply_at AS
SELECT
    src.apply_at,
    src.is_project_specific,
    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(src.apply_at AS string), '_NULL_'),
            coalesce(cast(src.is_project_specific AS string), '_NULL_')
        ), 256
    ) AS new_hash
FROM apply_at_inputs src
"""

sql_close_previous_apply_at = f"""
MERGE INTO {catalog}.{schema}.apply_at AS target
USING prepared_apply_at AS source
ON target.apply_at = source.apply_at
AND target.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(target.apply_at AS string), '_NULL_'),
            coalesce(cast(target.is_project_specific AS string), '_NULL_')
        ), 256
    ) <> source.new_hash

WHEN MATCHED THEN
UPDATE SET
    target.is_active = FALSE,
    target.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing_apply_at = f"""
UPDATE {catalog}.{schema}.apply_at t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_apply_at s
    WHERE s.apply_at = t.apply_at
)
"""

sql_insert_new_apply_at = f"""
INSERT INTO {catalog}.{schema}.apply_at (
    apply_at,
    is_project_specific,
    is_active,
    added_by,
    valid_from,
    valid_to
)
SELECT
    s.apply_at,
    s.is_project_specific,
    TRUE,
    s.added_by,
    current_timestamp(),
    NULL
FROM prepared_apply_at s
LEFT ANTI JOIN {catalog}.{schema}.apply_at t
    ON  t.apply_at = s.apply_at
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.apply_at AS string), '_NULL_'),
                coalesce(cast(t.is_project_specific AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared_apply_at)
    spark.sql(sql_close_previous_apply_at)
    spark.sql(sql_close_missing_apply_at)
    spark.sql(sql_insert_new_apply_at)
    print("SCD2 apply_at update completed successfully.")
except Exception as e:
    print("Error:", e)

##### rule_assingment ######

df_rules = spark.createDataFrame(user_values_rule_assignment)
df_rules.createOrReplaceTempView("rules_assignment_inputs")

sql_prepared_ra = f"""
CREATE OR REPLACE TEMP VIEW prepared_rule_assignment AS
SELECT
    b.rule_template_id,
    src.template_nk,
    c.policy_id,
    a.table_id,
    src.table_nk,
    proj.project_id,
    NULLIF(src.project_nk, '') AS project_nk,
    e.apply_at_id,
    src.apply_at_nk,

    NULLIF(src.parameters_identifiers, '') AS parameters_identifiers,
    NULLIF(src.parameters_values, '') AS parameters_values,

    CONCAT(
        src.template_nk, '.',
        COALESCE(NULLIF(src.project_nk, ''), '_NULL_'), '.',
        src.table_nk, '.',
        src.apply_at_nk, '.',
        COALESCE(
            to_json(
                from_json(
                    NULLIF(src.parameters_identifiers, ''),
                    'MAP<STRING, STRING>'
                )
            ),
            '_NULL_'
        )
    ) AS rule_assignment_nk,

    src.added_by,

    sha2(
        concat_ws(
            '||',
            coalesce(cast(b.rule_template_id AS string), '_NULL_'),
            coalesce(cast(c.policy_id AS string), '_NULL_'),
            coalesce(cast(a.table_id AS string), '_NULL_'),
            coalesce(cast(proj.project_id AS string), '_NULL_'),
            coalesce(cast(e.apply_at_id AS string), '_NULL_'),
            coalesce(cast(NULLIF(src.parameters_values, '') AS string), '_NULL_')
        ), 256
    ) AS new_hash

FROM rules_assignment_inputs src
JOIN {catalog}.{schema_metadata}.table a
    ON a.table_nk = src.table_nk 
    AND a.is_active = TRUE
JOIN {catalog}.{schema}.rule_template b
    ON b.template_nk = src.template_nk 
    AND b.is_active = TRUE
JOIN {catalog}.{schema}.run_policy c
    ON c.policy_nk = src.policy_nk 
    AND c.is_active = TRUE
JOIN {catalog}.{schema}.apply_at e
    ON e.apply_at_nk = src.apply_at_nk 
    AND e.is_active = TRUE
LEFT JOIN {catalog}.{schema_metadata}.project proj
    ON proj.project_nk = src.project_nk
    AND proj.is_active = TRUE
"""

sql_close_ra = f"""
MERGE INTO {catalog}.{schema}.rule_assignment AS t
USING prepared_rule_assignment AS s
ON t.rule_assignment_nk = s.rule_assignment_nk
AND t.is_active = TRUE
AND sha2(
        concat_ws(
            '||',
            coalesce(cast(t.rule_template_id AS string), '_NULL_'),
            coalesce(cast(t.policy_id AS string), '_NULL_'),
            coalesce(cast(t.table_id AS string), '_NULL_'),
            coalesce(cast(t.project_id AS string), '_NULL_'),
            coalesce(cast(t.apply_at_id AS string), '_NULL_'),
            coalesce(cast(NULLIF(t.parameters_values, '') AS string), '_NULL_')
        ), 256
    ) <> s.new_hash

WHEN MATCHED THEN
UPDATE SET
    t.is_active = FALSE,
    t.valid_to = CURRENT_TIMESTAMP()
"""

sql_close_missing_ra = f"""
UPDATE {catalog}.{schema}.rule_assignment t
SET
    t.valid_to = current_timestamp(),
    t.is_active = false
WHERE t.valid_to IS NULL
AND NOT EXISTS (
    SELECT 1
    FROM prepared_rule_assignment s
    WHERE s.rule_assignment_nk = t.rule_assignment_nk
)
"""

sql_insert_ra = f"""
INSERT INTO {catalog}.{schema}.rule_assignment (
    rule_template_id,
    template_nk,
    policy_id,
    table_id,
    table_nk,
    project_id,
    project_nk,
    apply_at_id,
    apply_at_nk,
    parameters_identifiers,
    parameters_values,
    added_by,
    is_active,
    valid_from,
    valid_to
)
SELECT
    s.rule_template_id,
    s.template_nk,
    s.policy_id,
    s.table_id,
    s.table_nk,
    s.project_id,
    s.project_nk,
    s.apply_at_id,
    s.apply_at_nk,
    s.parameters_identifiers,
    s.parameters_values,
    s.added_by,
    TRUE,
    current_timestamp(),
    NULL
FROM prepared_rule_assignment s
LEFT ANTI JOIN {catalog}.{schema}.rule_assignment t
    ON  t.rule_assignment_nk = s.rule_assignment_nk
    AND t.is_active = TRUE
    AND sha2(
            concat_ws(
                '||',
                coalesce(cast(t.rule_template_id AS string), '_NULL_'),
                coalesce(cast(t.policy_id AS string), '_NULL_'),
                coalesce(cast(t.table_id AS string), '_NULL_'),
                coalesce(cast(t.project_id AS string), '_NULL_'),
                coalesce(cast(t.apply_at_id AS string), '_NULL_'),
                coalesce(cast(NULLIF(t.parameters_values, '') AS string), '_NULL_')
            ), 256
        ) = s.new_hash
"""

try:
    spark.sql(sql_prepared_ra)
    spark.sql(sql_close_ra)
    spark.sql(sql_close_missing_ra)
    spark.sql(sql_insert_ra)
    print("SCD2 rule_assignment update completed successfully.")
except Exception as e:
    print("Error:", e)