"""
Schema Intermediate Representation (IR).

These are the dialect-neutral descriptions of tables, columns, and relationships
that sit between the COBOL AST and the final SQL DDL output.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class SQLTypeIR:
    """The SQL type for a column, expressed for both target dialects."""
    base_type: str                     # NUMERIC|VARCHAR|CHAR|DATE|TIMESTAMP|SMALLINT|...
    precision: int | None = None       # for NUMERIC(p, s)
    scale: int | None = None
    max_length: int | None = None      # for VARCHAR(n) / CHAR(n)
    target_postgresql: str = ""        # fully formed SQL Server type string
    target_sqlserver: str = ""         # fully formed PostgreSQL type string

    def for_dialect(self, dialect: str) -> str:
        if dialect == "postgresql":
            return self.target_postgresql or self._build()
        return self.target_sqlserver or self._build()

    def _build(self) -> str:
        if self.base_type in ("NUMERIC", "DECIMAL") and self.precision is not None:
            return f"{self.base_type}({self.precision},{self.scale or 0})"
        if self.base_type in ("VARCHAR", "CHAR", "NVARCHAR", "NCHAR") and self.max_length:
            return f"{self.base_type}({self.max_length})"
        return self.base_type


@dataclass
class ColumnIR:
    """One column in the target table."""
    name: str                          # SQL column name (snake_case)
    source_cobol_name: str
    source_pic: str
    source_usage: str
    sql_type: SQLTypeIR
    nullable: bool = True
    default_value: str | None = None
    ordinal_position: int = 0
    is_filler: bool = False
    is_primary_key: bool = False
    byte_offset: int = 0
    byte_length: int = 0
    # Decoding instructions
    decode_as: Literal["display", "comp3", "comp", "comp1", "comp2", "comp5", "comp6", "index"] = "display"
    ebcdic_decode: bool = True
    date_format: str | None = None     # e.g. "YYYYMMDD", "YYDDD"
    sentinel_null_values: list[str] = field(default_factory=list)
    # Level-88 condition values → CHECK constraint
    condition_values: list[str] = field(default_factory=list)


@dataclass
class CheckConstraintIR:
    name: str
    column: str
    allowed_values: list[str]


@dataclass
class RelationshipIR:
    """FK relationship between two tables (from OCCURS normalization)."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    constraint_name: str


@dataclass
class TableIR:
    """One table in the target schema."""
    name: str                          # SQL table name (snake_case)
    source_cobol_name: str
    columns: list[ColumnIR] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    check_constraints: list[CheckConstraintIR] = field(default_factory=list)
    table_type: Literal["main", "occurs_child", "redefines_subtype"] = "main"
    parent_table: str | None = None
    parent_fk_column: str | None = None
    occurrence_index_column: str | None = None  # for occurs_child tables
    discriminator_column: str | None = None     # for redefines_subtype
    discriminator_values: list[str] = field(default_factory=list)
    source_copybook: str = ""
    source_hash: str = ""


@dataclass
class EnumIR:
    """Lookup table derived from level-88 condition names."""
    table_name: str
    code_column: str
    description_column: str
    values: list[tuple[str, str]]      # (code, description)


@dataclass
class SchemaIR:
    """The complete schema for one migration project."""
    tables: list[TableIR] = field(default_factory=list)
    enums: list[EnumIR] = field(default_factory=list)
    relationships: list[RelationshipIR] = field(default_factory=list)
    source_copybook: str = ""

    def get_table(self, name: str) -> TableIR | None:
        return next((t for t in self.tables if t.name == name), None)
