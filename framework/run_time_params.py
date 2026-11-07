from dataclasses import dataclass, field
from typing import Any

@dataclass
class Meta:
    table_id: Any = None
    project_id: Any = None
    project_name: Any = None
    catalog: Any = None
    schema: Any = None
    table: Any = None
    quarantine: Any = None
    primary_key: Any = None
    filter_field: Any = None
    filter_field_type: Any = None
    job_id: Any = None
    full_table_name: Any = None
    pk_columns: list = field(default_factory=list)
    df_filtered: Any = None
    df_failures: Any = None

@dataclass
class Config:
    apply_at: Any = None
    run_mode: Any = None
    filter_values: Any = None
    rules: list = field(default_factory=list)

@dataclass
class Result:
    result_id: Any = None
    result_table_id: Any = None
    error_message: Any = None
    rules_evaluated: int = 0
    rules_error: int = 0
    rules_warning: int = 0
    rules_pass: int = 0
    rows_evaluated: int = 0
    tables_evaluated: int = 0
    tables_error: int = 0
    tables_warning: int = 0
    tables_pass: int = 0
    display_logs: Any = None
    table_status: Any = None
    table_start_timestamp: Any = None
    table_end_timestamp: Any = None
    table_in_progress: bool = False
    rules_written: bool = False
    table_written: bool = False


@dataclass
class Rule:
    result_rule_id: str
    rule_assignment_id: Any
    rule_template_id: Any
    rule_name: Any
    engine_type: Any
    template: Any
    parameters: dict
    is_reusable: Any
    category: Any
    quarantine_flag: Any
    policy_id: Any
    rule_type_id: Any
    rule_dimension_id: Any
    statement: Any = None
    quarantinable: bool = True
    status: Any = None
    rows_evaluated: Any = None
    rows_failed: Any = None
    rows_pass: Any = None
    error_message: Any = None


@dataclass
class RunTimeParams:
    meta: Meta = field(default_factory=Meta)
    config: Config = field(default_factory=Config)
    result: Result = field(default_factory=Result)
