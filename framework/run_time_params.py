from dataclasses import dataclass, field
from typing import Any

@dataclass
class Meta:
    rule_assignment_id: Any = None
    template_id: Any = None
    table_id: Any = None
    project_id: Any = None
    project_name: Any = None
    catalog: Any = None
    schema: Any = None
    table: Any = None
    quarantine: Any = None
    quarantine_id: Any = None
    primary_key: Any = None
    category: Any = None
    source_column_name: Any = None
    rule_type_id: Any = None
    rule_dimension_id: Any = None
    filter_field: Any = None
    filter_field_type: Any = None
    job_id: Any = None
    full_table_name: Any = None

@dataclass
class Config:
    engine_type: Any = None
    statement: Any = None
    rule_assignment_id: Any = None
    rule_template_id: Any = None
    policy_id: Any = None
    apply_at: Any = None
    category: Any = None
    parameters: Any = None
    run_mode: Any = None
    is_reusable: Any = None
    filter_values: Any = None
    rule_name: Any = None
    quarantine_flag: bool = None

@dataclass
class Result:
    result_id: Any = None
    result_table_id: Any = None
    result_rule_id: Any = None
    error_message: Any = None
    rules_evaluated: int = 0
    rules_error: int = 0
    rules_warning: int = 0
    rules_pass: int = 0
    rows_evaluated: int = 0
    rows_failed: int = 0
    rows_pass:int = 0
    tables_evaluated: int = 0
    tables_error: int = 0
    tables_warning: int = 0
    tables_pass: int = 0
    display_logs: Any = None


@dataclass
class RunTimeParams:
    meta: Meta = field(default_factory=Meta)
    config: Config = field(default_factory=Config)
    result: Result = field(default_factory=Result)