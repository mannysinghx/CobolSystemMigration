"""Pydantic request/response models for the CobolShift API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------

class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    source_type: Literal["vsam_flat", "db2", "ims", "mixed"] = "vsam_flat"
    target_type: Literal["postgresql", "sqlserver"] = "postgresql"
    config_json: dict[str, Any] = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=200)
    description: str | None = None
    config_json: dict[str, Any] | None = None


class ProjectResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: str | None
    source_type: str
    target_type: str
    config_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Copybook
# ---------------------------------------------------------------------------

class CopybookResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    filename: str
    file_path: str
    file_checksum: str | None
    parsed_at: datetime | None
    schema_ir: dict | None
    parse_errors: list | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Source file
# ---------------------------------------------------------------------------

class SourceFileResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    copybook_id: uuid.UUID | None
    filename: str
    file_path: str
    file_checksum: str | None
    record_format: str | None
    record_length: int | None
    encoding: str
    total_records: int | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Schema analysis
# ---------------------------------------------------------------------------

class ParseCopybookRequest(BaseModel):
    """Ask the API to parse a copybook already registered with the project."""
    copybook_id: uuid.UUID
    redefines_strategy: Literal["wide_table", "subtype_tables", "jsonb"] = "wide_table"
    occurs_strategy: Literal["child_table", "json_array", "wide_columns"] = "child_table"
    filler_strategy: Literal["skip", "include"] = "skip"


class GenerateDDLRequest(BaseModel):
    copybook_id: uuid.UUID
    dialect: Literal["postgresql", "sqlserver"] = "postgresql"
    schema_name: str = "public"
    include_comments: bool = True
    flyway_version: str = "001"
    flyway_description: str = "initial_load"


class DDLResponse(BaseModel):
    copybook_id: uuid.UUID
    dialect: str
    sql: str
    table_names: list[str]


# ---------------------------------------------------------------------------
# Migration run
# ---------------------------------------------------------------------------

class StartMigrationRequest(BaseModel):
    project_id: uuid.UUID
    run_type: Literal["full_load", "incremental", "cdc"] = "full_load"
    table_overrides: dict[str, Any] = Field(
        default_factory=dict,
        description="Per-table config overrides (mode, batch_size, etc.)",
    )

    @field_validator("table_overrides")
    @classmethod
    def validate_overrides(cls, v: dict) -> dict:
        return v


class TableStateResponse(BaseModel):
    id: uuid.UUID
    run_id: uuid.UUID
    table_name: str
    status: str
    rows_extracted: int
    rows_loaded: int
    rows_rejected: int
    source_checksum: str | None
    error_message: str | None
    started_at: datetime | None
    finished_at: datetime | None

    model_config = {"from_attributes": True}


class MigrationRunResponse(BaseModel):
    id: uuid.UUID
    project_id: uuid.UUID
    run_number: int
    run_type: str
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    rows_extracted: int
    rows_loaded: int
    rows_rejected: int
    error_message: str | None
    config_snapshot: dict[str, Any]

    model_config = {"from_attributes": True}


class RunSummaryResponse(BaseModel):
    run_id: str
    project_id: str
    run_number: int
    run_type: str
    status: str
    started_at: str | None
    finished_at: str | None
    elapsed_seconds: float | None
    tables_total: int
    tables_completed: int
    tables_failed: int
    rows_extracted: int
    rows_loaded: int
    rows_rejected: int
    error_message: str | None


# ---------------------------------------------------------------------------
# Rejection log
# ---------------------------------------------------------------------------

class RejectionLogResponse(BaseModel):
    id: uuid.UUID
    run_id: uuid.UUID
    table_state_id: uuid.UUID | None
    source_line_num: int | None
    decoded_partial: dict | None
    error_type: str | None
    error_message: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class ValidationRequest(BaseModel):
    run_id: uuid.UUID
    table_names: list[str] = Field(default_factory=list, description="Empty = all tables")
    checks: list[Literal["row_count", "aggregate", "sample", "nulls"]] = Field(
        default_factory=lambda: ["row_count", "aggregate"]
    )


class ValidationResult(BaseModel):
    table_name: str
    check: str
    passed: bool
    source_value: Any
    target_value: Any
    details: str | None = None


class ValidationResponse(BaseModel):
    run_id: uuid.UUID
    results: list[ValidationResult]
    all_passed: bool


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    version: str
    db_ok: bool
