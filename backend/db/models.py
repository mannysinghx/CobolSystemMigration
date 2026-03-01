"""SQLAlchemy ORM models for CobolShift's own state database."""

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.db.connection import Base


def new_uuid() -> uuid.UUID:
    return uuid.uuid4()


class Project(Base):
    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    source_type: Mapped[str] = mapped_column(String(30), nullable=False)  # vsam_flat|db2|ims|mixed
    target_type: Mapped[str] = mapped_column(String(20), nullable=False)  # sqlserver|postgresql
    config_json: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    copybooks: Mapped[list["Copybook"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    source_files: Mapped[list["SourceFile"]] = relationship(back_populates="project", cascade="all, delete-orphan")
    migration_runs: Mapped[list["MigrationRun"]] = relationship(back_populates="project", cascade="all, delete-orphan")


class Copybook(Base):
    __tablename__ = "copybooks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_checksum: Mapped[str | None] = mapped_column(String(64))  # SHA-256
    parsed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    schema_ir: Mapped[dict | None] = mapped_column(JSONB)           # cached Schema IR
    parse_errors: Mapped[list | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    project: Mapped["Project"] = relationship(back_populates="copybooks")
    source_files: Mapped[list["SourceFile"]] = relationship(back_populates="copybook")


class SourceFile(Base):
    __tablename__ = "source_files"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), nullable=False)
    copybook_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("copybooks.id"))
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    file_checksum: Mapped[str | None] = mapped_column(String(64))
    record_format: Mapped[str | None] = mapped_column(String(4))   # F, V, VB, D
    record_length: Mapped[int | None] = mapped_column(Integer)
    encoding: Mapped[str] = mapped_column(String(20), default="cp037")
    total_records: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    project: Mapped["Project"] = relationship(back_populates="source_files")
    copybook: Mapped["Copybook | None"] = relationship(back_populates="source_files")


class MigrationRun(Base):
    __tablename__ = "migration_runs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), nullable=False)
    run_number: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    run_type: Mapped[str] = mapped_column(String(30), nullable=False, default="full_load")  # full_load|incremental|cdc
    status: Mapped[str] = mapped_column(String(20), default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    rows_extracted: Mapped[int] = mapped_column(BigInteger, default=0)
    rows_loaded: Mapped[int] = mapped_column(BigInteger, default=0)
    rows_rejected: Mapped[int] = mapped_column(BigInteger, default=0)
    error_message: Mapped[str | None] = mapped_column(Text)
    config_snapshot: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    cdc_offset: Mapped[dict | None] = mapped_column(JSONB)

    project: Mapped["Project"] = relationship(back_populates="migration_runs")
    table_states: Mapped[list["TableMigrationState"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    rejections: Mapped[list["RejectionLog"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class TableMigrationState(Base):
    __tablename__ = "table_migration_states"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("migration_runs.id"), nullable=False)
    table_name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    rows_extracted: Mapped[int] = mapped_column(BigInteger, default=0)
    rows_loaded: Mapped[int] = mapped_column(BigInteger, default=0)
    rows_rejected: Mapped[int] = mapped_column(BigInteger, default=0)
    source_checksum: Mapped[str | None] = mapped_column(String(64))   # SHA-256 of source file
    error_message: Mapped[str | None] = mapped_column(Text)
    validation_json: Mapped[dict | None] = mapped_column(JSONB)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    run: Mapped["MigrationRun"] = relationship(back_populates="table_states")
    rejections: Mapped[list["RejectionLog"]] = relationship(
        back_populates="table_state", cascade="all, delete-orphan"
    )


class RejectionLog(Base):
    __tablename__ = "rejection_log"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=new_uuid)
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("migration_runs.id"), nullable=False)
    table_state_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("table_migration_states.id"))
    source_line_num: Mapped[int | None] = mapped_column(BigInteger)
    raw_bytes: Mapped[bytes | None] = mapped_column(LargeBinary)
    decoded_partial: Mapped[dict | None] = mapped_column(JSONB)
    error_type: Mapped[str | None] = mapped_column(String(100))
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    run: Mapped["MigrationRun"] = relationship(back_populates="rejections")
    table_state: Mapped["TableMigrationState | None"] = relationship(back_populates="rejections")
