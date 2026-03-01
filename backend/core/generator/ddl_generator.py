"""
DDL Generator.

Converts a SchemaIR into production-ready SQL DDL scripts for
PostgreSQL or SQL Server.  Output is wrapped in versioned migration
scripts compatible with Flyway (V001__name.sql convention).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Literal

from backend.core.analyzer.ir_nodes import (
    ColumnIR,
    RelationshipIR,
    SchemaIR,
    TableIR,
)


TargetDialect = Literal["postgresql", "sqlserver"]


# ─────────────────────────────────────────────────────────────────────────────
# Base generator
# ─────────────────────────────────────────────────────────────────────────────

class DDLGenerator:
    """
    Generate DDL SQL from a SchemaIR for the configured target dialect.

    Usage:
        gen = DDLGenerator("postgresql")
        ddl = gen.generate(schema_ir)
        print(ddl.full_script)
    """

    def __init__(
        self,
        dialect: TargetDialect = "postgresql",
        schema_name: str = "public",
        include_comments: bool = True,
        flyway_version: str = "001",
        flyway_description: str = "initial_schema",
    ):
        self.dialect = dialect
        self.schema_name = schema_name
        self.include_comments = include_comments
        self.flyway_version = flyway_version
        self.flyway_description = flyway_description

    # ── Public entry ──────────────────────────────────────────────────────

    def generate(self, schema: SchemaIR) -> "DDLOutput":
        parts: list[str] = []

        # Header
        parts.append(self._header(schema))

        # Enum / lookup tables first
        for enum in schema.enums:
            parts.append(self._generate_enum_table(enum))

        # Main tables (non-child first, then child tables)
        main_tables = [t for t in schema.tables if t.table_type != "occurs_child"]
        child_tables = [t for t in schema.tables if t.table_type == "occurs_child"]

        for table in main_tables:
            parts.append(self._generate_table(table))

        for table in child_tables:
            parts.append(self._generate_table(table))

        # Foreign key constraints (after all tables exist)
        if schema.relationships:
            parts.append(self._generate_foreign_keys(schema.relationships))

        script = "\n".join(p for p in parts if p.strip())
        return DDLOutput(
            full_script=script,
            dialect=self.dialect,
            flyway_filename=f"V{self.flyway_version}__{self.flyway_description}.sql",
            table_count=len(schema.tables),
        )

    # ── Table DDL ─────────────────────────────────────────────────────────

    def _generate_table(self, table: TableIR) -> str:
        lines: list[str] = []

        if self.include_comments:
            lines.append(f"-- Table: {table.name}")
            lines.append(f"-- Source: {table.source_cobol_name}")
            if table.table_type != "main":
                lines.append(f"-- Type: {table.table_type} (parent: {table.parent_table})")

        qualified = self._qualify(table.name)

        if self.dialect == "postgresql":
            lines.append(f"CREATE TABLE IF NOT EXISTS {qualified} (")
        else:
            lines.append(f"CREATE TABLE {qualified} (")

        col_lines: list[str] = []

        for col in table.columns:
            if col.is_filler:
                continue
            col_lines.append("    " + self._column_def(col))

        # PK constraint
        if table.primary_key:
            pk_cols = ", ".join(self._quote(c) for c in table.primary_key)
            col_lines.append(
                f"    CONSTRAINT {self._quote('pk_' + table.name)} "
                f"PRIMARY KEY ({pk_cols})"
            )

        # CHECK constraints (from level-88)
        for chk in table.check_constraints:
            allowed = ", ".join(f"'{v}'" for v in chk.allowed_values)
            col_def = self._quote(chk.column)
            col_lines.append(
                f"    CONSTRAINT {self._quote(chk.name)} "
                f"CHECK ({col_def} IN ({allowed}))"
            )

        lines.append(",\n".join(col_lines))
        lines.append(");")
        lines.append("")
        return "\n".join(lines)

    def _column_def(self, col: ColumnIR) -> str:
        sql_type = col.sql_type.for_dialect(self.dialect)
        null_clause = "" if col.nullable else " NOT NULL"
        default_clause = f" DEFAULT {col.default_value}" if col.default_value else ""
        name = self._quote(col.name)

        comment = ""
        if self.include_comments and col.source_pic:
            usage = f" {col.source_usage}" if col.source_usage not in ("DISPLAY", "") else ""
            comment = f"  -- {col.source_cobol_name}: PIC {col.source_pic}{usage}"

        return f"{name} {sql_type}{null_clause}{default_clause}{comment}"

    # ── Enum / lookup tables ──────────────────────────────────────────────

    def _generate_enum_table(self, enum) -> str:
        qualified = self._qualify(enum.table_name)
        code_col = self._quote(enum.code_column)
        desc_col = self._quote(enum.description_column)
        lines = [
            f"CREATE TABLE IF NOT EXISTS {qualified} (",
            f"    {code_col}  VARCHAR(20) PRIMARY KEY,",
            f"    {desc_col} VARCHAR(100) NOT NULL",
            ");",
        ]
        # INSERT values
        for code, desc in enum.values:
            lines.append(
                f"INSERT INTO {qualified} ({code_col}, {desc_col}) "
                f"VALUES ('{code}', '{desc}') "
                + ("ON CONFLICT DO NOTHING;" if self.dialect == "postgresql"
                   else "WHERE NOT EXISTS (SELECT 1 FROM {qualified} WHERE {code_col} = '{code}');")
            )
        lines.append("")
        return "\n".join(lines)

    # ── Foreign keys ──────────────────────────────────────────────────────

    def _generate_foreign_keys(self, relationships: list[RelationshipIR]) -> str:
        lines = ["-- Foreign Key Constraints", ""]
        for rel in relationships:
            from_q = self._qualify(rel.from_table)
            to_q = self._qualify(rel.to_table)
            fk_name = self._quote(rel.constraint_name)
            from_col = self._quote(rel.from_column)
            to_col = self._quote(rel.to_column)
            lines.append(
                f"ALTER TABLE {from_q} "
                f"ADD CONSTRAINT {fk_name} "
                f"FOREIGN KEY ({from_col}) REFERENCES {to_q} ({to_col});"
            )
        lines.append("")
        return "\n".join(lines)

    # ── Header ────────────────────────────────────────────────────────────

    def _header(self, schema: SchemaIR) -> str:
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        lines = [
            f"-- Generated by CobolShift v0.1.0",
            f"-- Dialect: {self.dialect}",
            f"-- Source copybook: {schema.source_copybook}",
            f"-- Generated: {ts}",
            f"-- Tables: {len(schema.tables)}",
            "",
        ]
        if self.dialect == "postgresql":
            lines += [f"SET search_path TO {self.schema_name};", ""]
        else:
            lines += [f"USE [{self.schema_name}];", "GO", ""]
        return "\n".join(lines)

    # ── Quoting / qualification ───────────────────────────────────────────

    def _quote(self, name: str) -> str:
        if self.dialect == "postgresql":
            return f'"{name}"'
        return f"[{name}]"

    def _qualify(self, table_name: str) -> str:
        if self.dialect == "postgresql":
            return f'"{self.schema_name}"."{table_name}"'
        return f"[{self.schema_name}].[{table_name}]"


# ─────────────────────────────────────────────────────────────────────────────
# Output container
# ─────────────────────────────────────────────────────────────────────────────

class DDLOutput:
    def __init__(
        self,
        full_script: str,
        dialect: TargetDialect,
        flyway_filename: str,
        table_count: int,
    ):
        self.full_script = full_script
        self.dialect = dialect
        self.flyway_filename = flyway_filename
        self.table_count = table_count

    def __str__(self) -> str:
        return self.full_script
