"""
COBOL Abstract Syntax Tree node definitions.

These dataclasses represent the parsed structure of a COBOL program.
Every node records the source line number for error reporting.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal


# ─────────────────────────────────────────────────────────────────────────────
# Data Description (the core schema-bearing node)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OccursClause:
    min_times: int
    max_times: int
    depending_on: str | None = None        # ODO field name (OCCURS DEPENDING ON)
    ascending_keys: list[str] = field(default_factory=list)
    descending_keys: list[str] = field(default_factory=list)
    indexed_by: list[str] = field(default_factory=list)


@dataclass
class SignClause:
    position: Literal["leading", "trailing"]
    separate: bool = False                 # SIGN IS LEADING/TRAILING SEPARATE CHARACTER


@dataclass
class DataDescription:
    """
    One entry in the DATA DIVISION (levels 01-49, 66, 77, 88).
    Elementary items have a picture; group items have children but no picture.
    """
    level: int
    name: str                              # COBOL data name, or "FILLER"
    picture: str | None = None             # Raw PIC string e.g. "S9(11)V99"
    usage: str = "DISPLAY"                 # DISPLAY|COMP|COMP-3|COMP-1|COMP-2|COMP-5|INDEX
    redefines: str | None = None           # Name of the item this redefines
    occurs: OccursClause | None = None
    depending_on: str | None = None        # top-level ODO (same as occurs.depending_on)
    value: str | None = None              # VALUE clause (literal or figurative constant)
    sign: SignClause | None = None
    synchronized: bool = False
    justified: bool = False
    blank_when_zero: bool = False
    global_: bool = False
    external_: bool = False
    children: list["DataDescription"] = field(default_factory=list)
    source_line: int = 0

    # Computed by LayoutCalculator (not from parser)
    byte_offset: int = 0
    byte_length: int = 0

    @property
    def is_filler(self) -> bool:
        return self.name.upper() == "FILLER"

    @property
    def is_group(self) -> bool:
        return len(self.children) > 0 and self.picture is None

    @property
    def is_elementary(self) -> bool:
        return self.picture is not None

    @property
    def is_condition_name(self) -> bool:
        return self.level == 88

    @property
    def is_redefines(self) -> bool:
        return self.redefines is not None

    def __repr__(self) -> str:
        pic = f" PIC {self.picture}" if self.picture else ""
        usage = f" {self.usage}" if self.usage != "DISPLAY" else ""
        return f"<DataDescription {self.level:02d} {self.name}{pic}{usage} @{self.byte_offset}+{self.byte_length}>"


# ─────────────────────────────────────────────────────────────────────────────
# File Descriptor
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FileDescriptor:
    """FD (or SD for sort files) entry."""
    fd_name: str                           # Logical file name from SELECT
    record_descriptions: list[DataDescription] = field(default_factory=list)
    recording_mode: str = "F"             # F|V|VB|U|S
    block_contains_min: int | None = None
    block_contains_max: int | None = None
    record_contains_min: int | None = None
    record_contains_max: int | None = None
    label_records: str = "STANDARD"
    data_records: list[str] = field(default_factory=list)
    source_line: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# DATA DIVISION sections
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DataDivision:
    file_section: list[FileDescriptor] = field(default_factory=list)
    working_storage: list[DataDescription] = field(default_factory=list)
    linkage_section: list[DataDescription] = field(default_factory=list)
    local_storage: list[DataDescription] = field(default_factory=list)
    screen_section: list[DataDescription] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# EXEC SQL block
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ExecSqlBlock:
    sql_text: str
    host_variables: list[str] = field(default_factory=list)
    source_line: int = 0


# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT DIVISION
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class FileControlEntry:
    select_name: str                       # COBOL file name
    assign_to: str                         # OS file name / DD name
    organization: str = "SEQUENTIAL"      # SEQUENTIAL|INDEXED|RELATIVE
    access_mode: str = "SEQUENTIAL"       # SEQUENTIAL|RANDOM|DYNAMIC
    record_key: str | None = None
    alternate_keys: list[str] = field(default_factory=list)
    file_status_var: str | None = None
    source_line: int = 0


@dataclass
class EnvironmentDivision:
    file_control: list[FileControlEntry] = field(default_factory=list)
    decimal_point_is_comma: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# PROCEDURE DIVISION (lightweight — we parse only what we need for analysis)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class EvaluateWhen:
    """A WHEN branch in an EVALUATE statement."""
    subject_values: list[str] = field(default_factory=list)  # literal values
    is_other: bool = False


@dataclass
class EvaluateStatement:
    """Parsed EVALUATE — used to detect REDEFINES discriminators."""
    subject: str                          # field name being evaluated
    when_clauses: list[EvaluateWhen] = field(default_factory=list)
    source_line: int = 0


@dataclass
class ProcedureDivision:
    exec_sql_blocks: list[ExecSqlBlock] = field(default_factory=list)
    evaluate_statements: list[EvaluateStatement] = field(default_factory=list)
    # Raw paragraph names for call-graph analysis
    paragraph_names: list[str] = field(default_factory=list)
    called_programs: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Top-level Compilation Unit
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class CompilationUnit:
    program_id: str
    source_file: str
    source_hash: str                       # SHA-256 of source content
    environment_division: EnvironmentDivision = field(default_factory=EnvironmentDivision)
    data_division: DataDivision = field(default_factory=DataDivision)
    procedure_division: ProcedureDivision = field(default_factory=ProcedureDivision)
    parse_warnings: list[str] = field(default_factory=list)
