# COBOL → SQL Migration Tool — End-to-End Application Plan

**Version**: 1.0
**Date**: 2026-03-01
**Target Databases**: SQL Server 2019+, PostgreSQL 14+
**Author**: Architecture Plan

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [COBOL Deep-Dive — What We're Parsing](#2-cobol-deep-dive--what-were-parsing)
3. [Common Migration Challenges](#3-common-migration-challenges)
4. [Application Architecture Overview](#4-application-architecture-overview)
5. [Module 1 — Project & Configuration Manager](#5-module-1--project--configuration-manager)
6. [Module 2 — COBOL Parser (AST Engine)](#6-module-2--cobol-parser-ast-engine)
7. [Module 3 — Copybook Schema Analyzer](#7-module-3--copybook-schema-analyzer)
8. [Module 4 — Schema Generator & DDL Emitter](#8-module-4--schema-generator--ddl-emitter)
9. [Module 5 — Binary & Encoding Decoder](#9-module-5--binary--encoding-decoder)
10. [Module 6 — Data Extraction Pipeline](#10-module-6--data-extraction-pipeline)
11. [Module 7 — Transformation Engine](#11-module-7--transformation-engine)
12. [Module 8 — Target Loader](#12-module-8--target-loader)
13. [Module 9 — CDC (Change Data Capture) Bridge](#13-module-9--cdc-change-data-capture-bridge)
14. [Module 10 — Validation & Reconciliation Engine](#14-module-10--validation--reconciliation-engine)
15. [Module 11 — Migration State Tracker](#15-module-11--migration-state-tracker)
16. [Module 12 — Web UI Dashboard](#16-module-12--web-ui-dashboard)
17. [Module 13 — CLI Interface](#17-module-13--cli-interface)
18. [Data Type Mapping Reference](#18-data-type-mapping-reference)
19. [Technology Stack](#19-technology-stack)
20. [Database Schema for the Tool Itself](#20-database-schema-for-the-tool-itself)
21. [File & Directory Structure](#21-file--directory-structure)
22. [Build & Deployment Plan](#22-build--deployment-plan)
23. [Testing Strategy](#23-testing-strategy)
24. [Phased Rollout Plan](#24-phased-rollout-plan)

---

## 1. Executive Summary

This document defines the architecture for **CobolShift** — a production-grade, end-to-end COBOL legacy system migration tool. It ingests COBOL source files, copybooks (`.cpy`), VSAM/flat file data, and embedded SQL (DB2/EXEC SQL), then produces:

- **Schema DDL** for SQL Server or PostgreSQL
- **Transformed data** loaded directly into the target database
- **Migration reports** with full reconciliation
- **CDC bridge** for near-zero-downtime cutovers

The tool handles **all COBOL complexity**: COMP-3 packed decimals, REDEFINES unions, OCCURS DEPENDING ON variable arrays, EBCDIC encoding, multi-record-type files, level-88 condition names, and copybook `REPLACE`/`COPY` chains.

---

## 2. COBOL Deep-Dive — What We're Parsing

### 2.1 The Four Divisions

Every COBOL program has up to four divisions that the parser must understand:

```cobol
IDENTIFICATION DIVISION.   -- Metadata: program name, author, date
ENVIRONMENT DIVISION.      -- File control: maps COBOL file names to OS files
DATA DIVISION.             -- ALL data structures (the schema)
PROCEDURE DIVISION.        -- Business logic (SQL-equivalent stored procs)
```

### 2.2 DATA DIVISION Sections We Parse

```
DATA DIVISION
├── FILE SECTION          ← FD entries defining record layouts for files
├── WORKING-STORAGE SECTION  ← Standalone variables, counters, switches
├── LINKAGE SECTION       ← Parameters passed between programs (CALL)
└── LOCAL-STORAGE SECTION ← Thread-local (per-invocation) data
```

### 2.3 Level Number Hierarchy

```
01  ROOT-RECORD.          ← Record root (always starts at 01)
  05  GROUP-ITEM.         ← Group (no PIC) = nested struct
    10  FIELD-A  PIC X(10).  ← Elementary item = column
    10  FIELD-B  PIC 9(5) COMP-3.
  05  REDEF-ITEM REDEFINES GROUP-ITEM.  ← Union / variant
    10  ALT-FIELD PIC X(15).
  05  TABLE-ITEM OCCURS 10 TIMES.  ← Array = child table
    10  ITEM-CODE PIC 9(8).
77  STANDALONE-VAR PIC 9(4).  ← Working-storage standalone
88  IS-ACTIVE VALUE 'Y'.       ← Boolean alias = CHECK constraint
```

### 2.4 PICTURE Clause Types

| Symbol | Meaning |
|--------|---------|
| `9` | Numeric digit |
| `X` | Alphanumeric character |
| `A` | Alphabetic character only |
| `V` | Implied decimal point (no stored byte) |
| `S` | Sign (leading or trailing, no extra byte in COMP-3) |
| `P` | Scaling position (implied zeros) |
| `Z` | Zero-suppressed digit (display only) |
| `$`, `,`, `.`, `-` | Editing characters (display only) |

### 2.5 USAGE Clauses (Binary Encodings)

| USAGE Clause | Encoding | Bytes for 9(n) |
|---|---|---|
| DISPLAY (default) | EBCDIC/ASCII text | n bytes |
| COMP / COMP-4 / BINARY | Two's complement big-endian | 2/4/8 bytes |
| COMP-3 / PACKED-DECIMAL | BCD, 2 digits/byte, sign nibble | ⌈(n+1)/2⌉ |
| COMP-1 | 32-bit IEEE float | 4 bytes |
| COMP-2 | 64-bit IEEE float | 8 bytes |
| COMP-5 | Two's complement native-endian | 2/4/8 bytes |
| INDEX | Binary index for OCCURS | 4 bytes |

### 2.6 REDEFINES (Union Types)

```cobol
05 TXN-DATA.
   10 AMOUNT PIC S9(11)V99 COMP-3.   ← 7 bytes
05 ALT-DATA REDEFINES TXN-DATA.
   10 CODE   PIC X(7).               ← Same 7 bytes, different view
```

The tool must detect the **discriminator field** (usually a `RECORD-TYPE` or `REC-CODE` PIC X field) by:
1. Scanning EVALUATE/IF statements in the PROCEDURE DIVISION
2. Sampling actual data values
3. User-guided annotation in the UI

### 2.7 OCCURS / OCCURS DEPENDING ON

```cobol
-- Fixed array:
05 LINE-ITEMS OCCURS 50 TIMES.
   10 PROD-ID PIC 9(8).
   10 QTY     PIC 9(4).

-- Variable array:
05 LINE-COUNT PIC 9(2).
05 LINE-ITEMS OCCURS 1 TO 50 TIMES DEPENDING ON LINE-COUNT.
   10 PROD-ID PIC 9(8).
   10 QTY     PIC 9(4).
```

Variable OCCURS requires reading `LINE-COUNT` from each record before parsing the array. These become **child tables** in the target schema.

### 2.8 Copybooks

Copybooks are external `.cpy` files included via:
```cobol
COPY CUST-REC.          ← Resolves to CUST-REC.cpy
COPY DFHAID REPLACING ==:PREFIX:== BY ==WS==.  ← With token substitution
```

The parser must recursively resolve all COPY/REPLACE chains before building the AST.

### 2.9 EXEC SQL (Embedded DB2)

```cobol
EXEC SQL
  SELECT CUST_NAME, CUST_BAL
  INTO   :WS-NAME, :WS-BAL
  FROM   CUSTOMER
  WHERE  CUST_ID = :WS-ID
END-EXEC.
```

The tool extracts these SQL blocks to:
- Identify source tables being accessed
- Map host variables (`:WS-NAME`) to COBOL data items
- Generate equivalent modern stored procedures or application queries

### 2.10 VSAM File Access Patterns

```cobol
-- Sequential (ESDS/QSAM):
READ INPUT-FILE INTO WS-RECORD AT END SET EOF TO TRUE END-READ.

-- Keyed (KSDS):
READ MASTER-FILE KEY IS WS-KEY INVALID KEY PERFORM ERROR-RTN END-READ.
REWRITE MASTER-RECORD FROM WS-UPDATE.

-- Alternate Key:
READ MASTER-FILE KEY IS ALT-KEY INVALID KEY ...
```

These become `SELECT`, `INSERT`, `UPDATE`, `DELETE` SQL statements in the migration.

---

## 3. Common Migration Challenges

Based on extensive research into real-world COBOL migrations, these are the top issues:

| # | Challenge | Impact | Our Solution |
|---|-----------|--------|--------------|
| 1 | **COMP-3 decoding errors** | Silent data corruption | Strict BCD decoder with sign nibble validation |
| 2 | **EBCDIC code page mismatch** | Character corruption | Configurable per-file code page (CP037, CP500, CP1047, etc.) |
| 3 | **REDEFINES discriminator unknown** | Wrong record interpretation | AST + data sampling + UI annotation |
| 4 | **OCCURS DEPENDING ON** | Variable-length record parsing failure | ODO counter field read before array parse |
| 5 | **Copybook REPLACE chains** | Incorrect field names | Full preprocessor before AST build |
| 6 | **Date format variety** | Invalid dates | 15+ format patterns recognized and normalized |
| 7 | **Sentinel values** (9999-12-31, spaces for NULL) | Incorrect NULLs | Configurable sentinel-to-NULL mapping |
| 8 | **COMP binary endianness** | Wrong numeric values | Big-endian (IBM mainframe) vs little-endian (PC) toggle |
| 9 | **Multi-record-type files** | Mixed schema in one file | Discriminator detection + per-type routing |
| 10 | **Level-88 conditions** | Lost business rules | Converted to CHECK constraints + lookup tables |
| 11 | **FILLER fields** | Wasted columns or hidden data | Configurable: skip or keep as `filler_n` columns |
| 12 | **Redefined group items with different lengths** | Buffer overrun parsing | Strict length validation from PIC analysis |
| 13 | **OCCURS in REDEFINES** | Nested complexity | Dedicated resolver for cross-cutting structures |
| 14 | **COMP-1/COMP-2 floats in financial data** | Precision loss | Warn + convert to NUMERIC with precision analysis |
| 15 | **Long-running migrations without CDC** | Extended downtime | Built-in CDC bridge via Debezium |

---

## 4. Application Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CobolShift Platform                          │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐  │
│  │  Web UI       │  │  CLI         │  │  REST API                │  │
│  │  (Next.js)    │  │  (Click/     │  │  (FastAPI)               │  │
│  │               │  │  Typer)      │  │                          │  │
│  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────────┘  │
│         └─────────────────┼──────────────────────┘                  │
│                           ▼                                          │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    Core Migration Engine                        │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐  │ │
│  │  │  M1: Project  │  │  M2: COBOL   │  │  M3: Copybook      │  │ │
│  │  │  Manager      │  │  Parser      │  │  Schema Analyzer   │  │ │
│  │  │               │  │  (ANTLR4)    │  │                    │  │ │
│  │  └──────────────┘  └──────┬───────┘  └────────┬───────────┘  │ │
│  │                            │                    │              │ │
│  │                            ▼                    ▼              │ │
│  │  ┌──────────────┐  ┌──────────────────────────────────────┐  │ │
│  │  │  M11: State   │  │  Schema IR (Intermediate Rep.)       │  │ │
│  │  │  Tracker      │  │  (typed Python dataclasses)          │  │ │
│  │  └──────────────┘  └──────────────┬───────────────────────┘  │ │
│  │                                    │                           │ │
│  │           ┌────────────────────────┼──────────────────────┐   │ │
│  │           ▼                        ▼                       ▼   │ │
│  │  ┌──────────────┐  ┌──────────────────────┐  ┌──────────────┐ │ │
│  │  │  M4: Schema   │  │  M5: Binary/EBCDIC    │  │  M7: Trans-  │ │ │
│  │  │  Generator    │  │  Decoder              │  │  formation   │ │ │
│  │  │  & DDL Emitter│  │                       │  │  Engine      │ │ │
│  │  └──────┬───────┘  └──────────┬────────────┘  └──────┬───────┘ │ │
│  │         │                      │                       │         │ │
│  │         ▼                      ▼                       ▼         │ │
│  │  ┌──────────────┐  ┌──────────────────────┐  ┌──────────────┐  │ │
│  │  │  M6: Extract  │  │  M8: Target Loader   │  │  M9: CDC     │  │ │
│  │  │  Pipeline     │  │  (bulk + streaming)  │  │  Bridge      │  │ │
│  │  └──────────────┘  └──────────────────────┘  └──────────────┘  │ │
│  │                                                                  │ │
│  │  ┌─────────────────────────────────────────────────────────┐    │ │
│  │  │  M10: Validation & Reconciliation Engine                │    │ │
│  │  └─────────────────────────────────────────────────────────┘    │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌─────────────────────┐  ┌──────────────────────────────────────┐  │
│  │  Tool Database       │  │  External Systems                    │  │
│  │  (PostgreSQL/SQLite) │  │  ├─ Source: VSAM/flat files          │  │
│  │  - projects          │  │  ├─ Source: DB2/EXEC SQL programs    │  │
│  │  - migrations        │  │  ├─ Target: SQL Server               │  │
│  │  - schemas           │  │  ├─ Target: PostgreSQL               │  │
│  │  - validation_runs   │  │  └─ CDC: Debezium / Kafka            │  │
│  └─────────────────────┘  └──────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 5. Module 1 — Project & Configuration Manager

### Responsibility
Manages migration projects, stores all settings, and provides the unified context object passed to all other modules.

### Data Model

```python
@dataclass
class MigrationProject:
    id: UUID
    name: str
    created_at: datetime
    source_type: Literal["vsam_flat", "db2", "ims", "mixed"]
    target_type: Literal["sqlserver", "postgresql"]
    target_connection: ConnectionConfig
    ebcdic_codepage: str          # "cp037", "cp500", "cp1047", etc.
    binary_endianness: Literal["big", "little"]  # big for mainframe
    null_sentinels: list[SentinelRule]
    date_formats: list[DateFormatRule]
    filler_strategy: Literal["skip", "keep", "warn"]
    redefines_strategy: Literal["wide_table", "subtype_tables", "jsonb"]
    occurs_strategy: Literal["child_table", "json_array", "wide_columns"]
    migration_mode: Literal["bigbang", "incremental", "cdc"]

@dataclass
class ConnectionConfig:
    host: str
    port: int
    database: str
    username: str
    password: str  # stored encrypted (Fernet)
    schema: str
    ssl_mode: str
```

### Key Behaviors
- Project settings stored in the tool's own SQLite/PostgreSQL database
- Passwords encrypted at rest with Fernet symmetric encryption
- All settings overridable per-file/per-copybook via inline annotations
- Export/import project config as JSON for team sharing

---

## 6. Module 2 — COBOL Parser (AST Engine)

### Responsibility
Parse COBOL source files (`.cob`, `.cbl`, `.cpy`) into a typed AST. This is the most critical module — correctness here guarantees correctness everywhere downstream.

### Implementation Strategy

**Grammar**: Use the ANTLR4 `Cobol85.g4` grammar (from `antlr/grammars-v4`) as the base, extended for:
- IBM Enterprise COBOL dialect (EXEC CICS, EXEC SQL blocks)
- MicroFocus COBOL extensions
- COBOL 2002 free-format

**Preprocessing Pipeline** (before ANTLR4):
```
Input file
    ↓
1. Column stripper         (remove cols 1-6 sequence, col 7 indicator)
2. COPY resolver           (recursively expand COPY statements)
3. REPLACE processor       (apply token-level substitutions)
4. Continuation handler    (join continuation lines)
5. Comment stripper        (remove * in col 7 and inline comments)
    ↓
Clean source → ANTLR4 Lexer → Token Stream → ANTLR4 Parser → CST
    ↓
CST Visitor → Typed AST
```

### AST Node Definitions

```python
@dataclass
class CompilationUnit:
    program_id: str
    identification_division: IdentificationDivision
    environment_division: EnvironmentDivision | None
    data_division: DataDivision | None
    procedure_division: ProcedureDivision | None
    source_file: Path
    source_hash: str  # SHA256 for change detection

@dataclass
class DataDivision:
    file_section: list[FileDescriptor]
    working_storage: list[DataDescription]
    linkage_section: list[DataDescription]
    local_storage: list[DataDescription]

@dataclass
class FileDescriptor:
    fd_name: str           # e.g., "CUSTOMER-FILE"
    record_descriptions: list[DataDescription]
    recording_mode: str    # F, V, VB, U
    block_contains: int | None
    record_contains: int | None

@dataclass
class DataDescription:
    level: int             # 01-49, 66, 77, 88
    name: str              # COBOL data name
    picture: str | None    # Raw PIC string e.g. "S9(11)V99"
    usage: str             # DISPLAY, COMP, COMP-3, etc.
    redefines: str | None  # Name of redefined item
    occurs: OccursClause | None
    value: str | None      # Initial value
    sign: SignClause | None
    synchronized: bool
    justified: bool
    blank_when_zero: bool
    children: list[DataDescription]
    # Computed:
    byte_offset: int       # Offset within parent record
    byte_length: int       # Physical byte length
    source_line: int

@dataclass
class OccursClause:
    min_times: int
    max_times: int
    depending_on: str | None  # ODO field name
    keys: list[str]
    indexed_by: list[str]

@dataclass
class ExecSqlBlock:
    sql_text: str
    host_variables: list[str]
    source_line: int
```

### Byte Layout Calculator

After parsing, the engine calculates exact byte offsets and lengths for every field:

```python
def calculate_layout(desc: DataDescription, base_offset: int = 0) -> None:
    """
    Recursively assign byte_offset and byte_length to each DataDescription.
    Handles REDEFINES (no new space), OCCURS (multiply child sizes),
    SYNC (alignment padding), and all USAGE types.
    """
    ...
```

This is critical for the binary decoder — it needs exact byte positions to extract each field from raw records.

---

## 7. Module 3 — Copybook Schema Analyzer

### Responsibility
Take the parsed AST's data descriptions and produce a **Schema Intermediate Representation (Schema IR)** — a database-dialect-neutral description of tables, columns, and relationships.

### Schema IR Definition

```python
@dataclass
class SchemaIR:
    tables: list[TableIR]
    enums: list[EnumIR]       # from level-88 groups
    relationships: list[RelationshipIR]

@dataclass
class TableIR:
    name: str                  # SQL table name (snake_case)
    source_cobol_name: str
    columns: list[ColumnIR]
    primary_key: list[str]
    check_constraints: list[CheckConstraintIR]
    table_type: Literal["main", "occurs_child", "redefines_subtype"]
    parent_table: str | None   # for occurs_child
    discriminator_column: str | None  # for redefines_subtype
    discriminator_values: list[str]

@dataclass
class ColumnIR:
    name: str                  # SQL column name (snake_case)
    source_cobol_name: str
    source_pic: str
    source_usage: str
    sql_type: SQLTypeIR
    nullable: bool
    default_value: str | None
    ordinal_position: int
    is_filler: bool
    byte_offset: int
    byte_length: int
    # Transformations needed:
    decode_as: Literal["display", "comp3", "comp", "comp1", "comp2", "comp5"]
    ebcdic_decode: bool
    date_format: str | None    # e.g., "YYYYMMDD", "YYDDD"
    sentinel_null_values: list[str]

@dataclass
class SQLTypeIR:
    base_type: str             # "NUMERIC", "VARCHAR", "CHAR", "DATE", etc.
    precision: int | None
    scale: int | None
    max_length: int | None
    target_sqlserver: str      # e.g., "DECIMAL(13,2)"
    target_postgresql: str     # e.g., "NUMERIC(13,2)"
```

### REDEFINES Analysis

```
Step 1: Find all REDEFINES items in the AST
Step 2: Group them by the base item they redefine
Step 3: Search PROCEDURE DIVISION for EVALUATE/IF using the discriminator
Step 4: If found → extract discriminator field name + value mapping
Step 5: If not found → flag for user annotation in UI
Step 6: Apply configured redefines_strategy (wide_table / subtype / jsonb)
```

### OCCURS Normalization

```
Step 1: Find all OCCURS (fixed and DEPENDING ON)
Step 2: Determine parent key (usually the closest 01-level primary key field)
Step 3: Create child TableIR with:
  - FK column referencing parent
  - occurrence_index column (SMALLINT, 1-based)
  - All OCCURS children as columns
Step 4: For ODO: mark the counter field so the decoder reads it first
```

---

## 8. Module 4 — Schema Generator & DDL Emitter

### Responsibility
Take the Schema IR and emit production-ready DDL SQL for either SQL Server or PostgreSQL.

### DDL Generation Rules

**Table Naming**: COBOL names → `snake_case`. `CUSTOMER-RECORD` → `customer_record`.
**Column Naming**: Same. `EMP-HIRE-DATE` → `emp_hire_date`.
**FILLER fields**: Named `filler_001`, `filler_002` (sequential) unless `skip` strategy selected.
**Level-88 Conditions**: Emit `CHECK (col IN ('A','C','S'))` constraints.

### Sample Output — PostgreSQL

```sql
-- Generated by CobolShift v1.0
-- Source: CUSTOMER.cpy  SHA256: abc123...
-- Generated: 2026-03-01T12:00:00Z

CREATE TABLE customer_record (
    -- PIC 9(8) COMP → INTEGER
    cust_id         INTEGER        NOT NULL,
    -- PIC X(30)
    cust_name       VARCHAR(30)    NOT NULL,
    -- PIC X(4)
    cust_dept       CHAR(4),
    -- PIC S9(9)V99 COMP-3 → NUMERIC(11,2)
    cust_salary     NUMERIC(11,2),
    -- PIC 9(8) → DATE (YYYYMMDD transform)
    cust_hire_date  DATE,
    -- PIC X → CHAR(1), level-88: A=Active,T=Terminated
    cust_status     CHAR(1)        CHECK (cust_status IN ('A','T')),

    CONSTRAINT pk_customer_record PRIMARY KEY (cust_id)
);

-- OCCURS child table (from: ORDER-LINES OCCURS 1 TO 50 TIMES)
CREATE TABLE order_line (
    ord_number      BIGINT         NOT NULL,
    line_seq        SMALLINT       NOT NULL,
    line_product    INTEGER,
    line_qty        SMALLINT,
    line_price      NUMERIC(9,2),

    CONSTRAINT pk_order_line PRIMARY KEY (ord_number, line_seq),
    CONSTRAINT fk_order_line_header FOREIGN KEY (ord_number)
        REFERENCES order_header(ord_number)
);

-- REDEFINES subtype table (discriminator: record_type = 'A')
CREATE TABLE txn_type_a (
    txn_id          BIGINT         NOT NULL REFERENCES transaction_base(txn_id),
    account_balance NUMERIC(13,2),
    credit_limit    NUMERIC(11,2),

    CONSTRAINT pk_txn_type_a PRIMARY KEY (txn_id)
);
```

### Sample Output — SQL Server

```sql
-- Generated by CobolShift v1.0
-- Source: CUSTOMER.cpy

CREATE TABLE [dbo].[customer_record] (
    [cust_id]        INT            NOT NULL,
    [cust_name]      NVARCHAR(30)   NOT NULL,
    [cust_dept]      NCHAR(4),
    [cust_salary]    DECIMAL(11,2),
    [cust_hire_date] DATE,
    [cust_status]    NCHAR(1)
        CONSTRAINT chk_cust_status CHECK ([cust_status] IN ('A','T')),

    CONSTRAINT [pk_customer_record] PRIMARY KEY ([cust_id])
);
```

### Migration Script Output

The DDL is wrapped in a versioned migration script compatible with **Flyway** and **Liquibase**:

```sql
-- V001__create_customer_record.sql  (Flyway compatible)
BEGIN TRANSACTION;  -- SQL Server
-- BEGIN;           -- PostgreSQL

-- DDL here...

COMMIT;
```

---

## 9. Module 5 — Binary & Encoding Decoder

### Responsibility
Decode raw bytes from COBOL data files (VSAM exports, flat files) into Python-native values using the Schema IR's column byte offsets and decode rules.

### Decoder Architecture

```python
class RecordDecoder:
    """
    Decodes a single raw bytes object into a dict of field_name → Python value
    using the TableIR column definitions.
    """
    def decode(self, raw: bytes, table: TableIR) -> dict[str, Any]:
        result = {}
        for col in table.columns:
            raw_field = raw[col.byte_offset : col.byte_offset + col.byte_length]
            result[col.name] = self._decode_field(raw_field, col)
        return result

    def _decode_field(self, raw: bytes, col: ColumnIR) -> Any:
        match col.decode_as:
            case "comp3":    return self._decode_comp3(raw, col)
            case "comp":     return self._decode_comp_binary(raw, col)
            case "comp1":    return struct.unpack(">f", raw)[0]
            case "comp2":    return struct.unpack(">d", raw)[0]
            case "comp5":    return self._decode_comp5(raw, col)
            case "display":  return self._decode_display(raw, col)
```

### COMP-3 Decoder (Packed Decimal)

```python
def _decode_comp3(self, raw: bytes, col: ColumnIR) -> Decimal | None:
    hex_str = raw.hex()
    sign_nibble = hex_str[-1].upper()
    digits = hex_str[:-1]

    # Validate sign nibble
    if sign_nibble not in ('C', 'D', 'F', 'A', 'B', 'E'):
        raise DecoderError(
            f"Invalid COMP-3 sign nibble '{sign_nibble}' at "
            f"offset {col.byte_offset} for field {col.name}"
        )

    # Validate all digit nibbles are 0-9
    if not all(c in '0123456789' for c in digits):
        raise DecoderError(
            f"Invalid COMP-3 digit nibbles in field {col.name}"
        )

    value = Decimal(digits)

    # Apply implied decimal (V clause)
    if col.sql_type.scale and col.sql_type.scale > 0:
        value = value / Decimal(10 ** col.sql_type.scale)

    # Apply sign
    if sign_nibble in ('D',):
        value = -value

    # Check sentinel NULL
    raw_str = str(value)
    if raw_str in col.sentinel_null_values:
        return None

    return value
```

### COMP Binary Decoder

```python
def _decode_comp_binary(self, raw: bytes, col: ColumnIR) -> int:
    # IBM mainframe = big-endian, PC/UNIX = little-endian
    signed = 'S' in col.source_pic
    endian = '>' if self.project.binary_endianness == 'big' else '<'
    n = len(raw)
    fmt = {1: 'b', 2: 'h', 4: 'i', 8: 'q'} if signed else \
          {1: 'B', 2: 'H', 4: 'I', 8: 'Q'}
    return struct.unpack(f"{endian}{fmt[n]}", raw)[0]
```

### EBCDIC Decoder

```python
def _decode_display(self, raw: bytes, col: ColumnIR) -> str | None:
    if col.ebcdic_decode:
        text = raw.decode(self.project.ebcdic_codepage, errors='replace')
    else:
        text = raw.decode('ascii', errors='replace')

    text = text.rstrip()  # COBOL fields are space-padded

    # Check sentinel
    if text.strip() in col.sentinel_null_values:
        return None
    if text.strip() == '' and col.nullable:
        return None

    # Date format transformation
    if col.date_format:
        return self._normalize_date(text, col.date_format)

    return text
```

### Date Normalizer

Supports all common COBOL date formats:

| COBOL Format | Example | SQL DATE |
|---|---|---|
| `YYYYMMDD` | `20260301` | `2026-03-01` |
| `YYMMDD` | `260301` | `2026-03-01` |
| `YYDDD` (Julian) | `26060` | `2026-03-01` |
| `YYYYDDD` | `2026060` | `2026-03-01` |
| `MMDDYYYY` | `03012026` | `2026-03-01` |
| `DDMMYYYY` | `01032026` | `2026-03-01` |
| `9(7)` (Lilian) | `1498120` | `2026-03-01` |

---

## 10. Module 6 — Data Extraction Pipeline

### Responsibility
Read raw records from source files (VSAM exports, flat sequential files, DB2 unloads) and stream them through the decoder.

### Supported Source Formats

| Format | Description |
|---|---|
| `RECFM=F` | Fixed-length records (most common VSAM export) |
| `RECFM=V` | Variable-length with 4-byte RDW (Record Descriptor Word) prefix |
| `RECFM=VB` | Variable blocked |
| `RECFM=D` | ASCII line-delimited (migration of PC COBOL) |
| DB2 UNLOAD | IBM DSNTIAUL or REORG unload format |
| CSV | Delimited text (for pre-converted data) |

### Streaming Architecture

```python
class ExtractionPipeline:
    """
    Streaming pipeline: yields decoded dicts, never loads entire file into memory.
    Handles files up to terabyte scale via chunked I/O.
    """
    def __init__(self, source_file: Path, table: TableIR, project: MigrationProject):
        self.reader = self._build_reader(source_file, project)
        self.decoder = RecordDecoder(project)
        self.table = table
        self.stats = ExtractionStats()

    def stream(self) -> Iterator[tuple[dict, ExtractionError | None]]:
        for raw_record, line_num in self.reader.read_records():
            try:
                # Handle multi-record-type routing
                if self.table.discriminator_column:
                    record_type = self._peek_discriminator(raw_record)
                    table = self._route_to_table(record_type)
                else:
                    table = self.table

                decoded = self.decoder.decode(raw_record, table)
                self.stats.success_count += 1
                yield decoded, None
            except DecoderError as e:
                self.stats.error_count += 1
                yield None, ExtractionError(line=line_num, raw=raw_record, error=str(e))
```

### Multi-Record-Type Router

When a file contains multiple record types (discriminated by a leading field), the router:
1. Reads the discriminator field at the known byte offset
2. Routes the record to the appropriate `TableIR`
3. Decodes using that table's column layout

---

## 11. Module 7 — Transformation Engine

### Responsibility
Apply business-level transformations to decoded records before loading: normalization, lookups, computed columns, and data cleansing rules.

### Transformation Rule Types

```python
@dataclass
class TransformationRule:
    rule_id: str
    source_column: str
    rule_type: Literal[
        "rename",           # rename column
        "type_cast",        # explicit type cast
        "lookup",           # replace code with description
        "computed",         # new column from expression
        "split",            # split one column into multiple
        "merge",            # merge multiple columns
        "regex_replace",    # regex substitution
        "sentinel_null",    # already in decoder, but secondary pass
        "constant",         # set to fixed value
        "drop",             # exclude column from output
    ]
    parameters: dict[str, Any]
```

### Rule Examples

```yaml
# rules.yaml
transformations:
  - rule_id: date_hire
    source_column: emp_hire_date
    rule_type: type_cast
    parameters:
      from_format: YYYYMMDD
      to_type: DATE

  - rule_id: status_lookup
    source_column: acct_status
    rule_type: lookup
    parameters:
      map:
        "A": "ACTIVE"
        "C": "CLOSED"
        "S": "SUSPENDED"
      create_lookup_table: true
      lookup_table_name: acct_status_codes

  - rule_id: full_name
    source_column: ["first_name", "last_name"]
    rule_type: merge
    parameters:
      output_column: full_name
      separator: " "
      order: ["first_name", "last_name"]
```

---

## 12. Module 8 — Target Loader

### Responsibility
Efficiently load transformed records into the target SQL Server or PostgreSQL database with error handling, retry logic, and progress tracking.

### Bulk Loading Strategy

**PostgreSQL**: Use `COPY FROM STDIN` (binary protocol) via `psycopg3`'s copy interface. ~10x faster than INSERT.

**SQL Server**: Use `pyodbc` with `executemany()` + `fast_executemany = True`, or BCP (Bulk Copy Program) for maximum throughput.

### Architecture

```python
class TargetLoader:
    BATCH_SIZE = 10_000  # rows per batch

    async def load_table(
        self,
        table: TableIR,
        records: AsyncIterator[dict],
        project: MigrationProject
    ) -> LoadResult:
        buffer = []
        async for record in records:
            buffer.append(record)
            if len(buffer) >= self.BATCH_SIZE:
                await self._flush_batch(table, buffer, project)
                buffer.clear()
        if buffer:
            await self._flush_batch(table, buffer, project)

    async def _flush_batch(self, table, batch, project):
        match project.target_type:
            case "postgresql":
                await self._pg_copy_batch(table, batch)
            case "sqlserver":
                await self._sql_server_bulk_batch(table, batch)
```

### Error Handling

- **Batch bisection**: On batch failure, split in half and retry each half. Isolates bad rows without losing the entire batch.
- **Rejection log**: Failed rows written to `migration_rejections` table with raw data, error message, and timestamp.
- **Constraint violations**: Captured and reported — never silently discarded.
- **Transaction management**: Each batch is a single transaction. Partial batches never partially commit.

### Loading Modes

| Mode | Description | Use Case |
|---|---|---|
| `TRUNCATE_LOAD` | Truncate target then full load | Initial migration |
| `APPEND` | INSERT only, no truncation | Incremental batches |
| `UPSERT` | INSERT ON CONFLICT UPDATE | Idempotent re-runs |
| `MERGE` | Full MERGE statement | CDC apply |

---

## 13. Module 9 — CDC (Change Data Capture) Bridge

### Responsibility
Enable near-zero-downtime migrations by maintaining a live stream of changes from the source system to the target during the transition window.

### Supported CDC Sources

| Source | Mechanism |
|---|---|
| DB2 on z/OS | IBM InfoSphere CDC or Precisely MFX |
| DB2 LUW | Debezium DB2 connector |
| SQL Server (source) | Debezium SQL Server connector (log-based) |
| MySQL (source) | Debezium MySQL connector (binlog) |
| VSAM | IBM MQ + custom VSAM exit routine |
| Files | Inotify-based file watcher for batch append detection |

### CDC Pipeline

```
Source DB/VSAM
    ↓ (log-based CDC)
Debezium Connector
    ↓
Kafka Topic (per source table)
    ↓
CobolShift CDC Consumer
    ↓ (decode + transform)
Target DB (SQL Server / PostgreSQL)
```

### Migration Phases with CDC

```
Phase 1 - Baseline:
  - Start CDC connector, capture current LSN/offset checkpoint
  - Run full bulk extraction and load (Module 6 + 8)
  - CDC events accumulate in Kafka during bulk load

Phase 2 - Catch-Up:
  - Process accumulated CDC events
  - Monitor consumer lag (events behind)
  - Apply events using MERGE/UPSERT

Phase 3 - Steady State:
  - Consumer lag approaches zero
  - Run parallel validation queries (Module 10)

Phase 4 - Cutover:
  - Schedule cutover window
  - Stop writes to source
  - Drain remaining CDC events (usually < 1 minute)
  - Final validation
  - Switch application connection strings
  - GO LIVE

Phase 5 - Rollback Capability:
  - Reverse CDC: target → source (30-day safety window)
  - After confidence period: decommission legacy
```

---

## 14. Module 10 — Validation & Reconciliation Engine

### Responsibility
Prove that the migrated data is 100% correct and complete. Generate a comprehensive audit report.

### Validation Layers

#### Layer 1: Row Count Reconciliation
```python
class RowCountValidator:
    def validate(self, source_count: int, target_count: int) -> ValidationResult:
        variance = target_count - source_count
        variance_pct = abs(variance) / source_count * 100
        return ValidationResult(
            check="row_count",
            passed=variance == 0,
            source_value=source_count,
            target_value=target_count,
            variance=variance,
            variance_pct=variance_pct
        )
```

#### Layer 2: Aggregate Financial Reconciliation
```python
# For every COMP-3 / NUMERIC column, compare SUM and COUNT(DISTINCT):
SELECT
    COUNT(*)          AS row_count,
    SUM(balance)      AS total_balance,
    SUM(credit_limit) AS total_credit,
    MAX(updated_date) AS max_date,
    MIN(updated_date) AS min_date
FROM source_table;
-- vs same query on target_table
```

#### Layer 3: Hash-Based Row Sampling
For large tables: statistical sampling with configurable confidence level (default: 99.5% confidence, ±0.1% error).

```python
class SamplingValidator:
    def validate_sample(
        self,
        table: TableIR,
        confidence: float = 0.995,
        margin_of_error: float = 0.001
    ) -> SamplingValidationResult:
        sample_size = self._calculate_sample_size(confidence, margin_of_error)
        # Fetch random sample from source + target
        # Compare row-by-row
        ...
```

#### Layer 4: Business Rule Validation
```python
business_rules = [
    "SELECT COUNT(*) FROM account WHERE balance < 0 AND status = 'ACTIVE'",  # must be 0
    "SELECT COUNT(*) FROM employee WHERE hire_date > CURRENT_DATE",           # must be 0
    "SELECT COUNT(*) FROM order_line WHERE line_seq < 1",                     # must be 0
]
```

#### Layer 5: Referential Integrity Check
After loading all tables, verify all FK relationships are satisfied. Outputs list of orphaned rows.

### Reconciliation Report (JSON + HTML)

```json
{
  "project_id": "uuid-...",
  "run_timestamp": "2026-03-01T12:00:00Z",
  "overall_status": "PASSED",
  "tables": [
    {
      "table_name": "customer_record",
      "row_count": { "source": 2847291, "target": 2847291, "passed": true },
      "financial_totals": {
        "total_balance": { "source": "984729384.92", "target": "984729384.92", "passed": true }
      },
      "sampling": { "sample_size": 6765, "mismatches": 0, "passed": true },
      "business_rules": { "all_passed": true, "failed": [] },
      "referential_integrity": { "orphaned_rows": 0, "passed": true }
    }
  ],
  "rejected_records": 0,
  "duration_seconds": 847
}
```

---

## 15. Module 11 — Migration State Tracker

### Responsibility
Track exactly what has been migrated, when, and with what result. Enable safe re-runs, partial retries, and rollback.

### State Table Schema (in tool's own DB)

```sql
CREATE TABLE migration_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id),
    run_number      INTEGER NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    status          TEXT CHECK (status IN ('running','completed','failed','rolled_back')),
    source_checksum TEXT,    -- SHA256 of all source files
    rows_extracted  BIGINT,
    rows_loaded     BIGINT,
    rows_rejected   BIGINT,
    error_message   TEXT,
    UNIQUE (project_id, run_number)
);

CREATE TABLE migration_table_states (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES migration_runs(id),
    table_name      TEXT NOT NULL,
    status          TEXT,
    rows_extracted  BIGINT,
    rows_loaded     BIGINT,
    rows_rejected   BIGINT,
    source_checksum TEXT,
    validation_passed BOOLEAN,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);

CREATE TABLE migration_rejections (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES migration_runs(id),
    table_name      TEXT NOT NULL,
    source_line_num BIGINT,
    raw_bytes       BYTEA,
    error_type      TEXT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE copybook_registry (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id),
    file_path       TEXT NOT NULL,
    file_checksum   TEXT NOT NULL,
    last_parsed_at  TIMESTAMPTZ,
    schema_ir_json  JSONB,  -- cached parsed IR
    UNIQUE (project_id, file_path)
);
```

### Re-run Safety
- Before re-running: compare source file checksums to previous run
- If unchanged: skip extraction, go straight to validation
- If changed: re-extract and re-load (configurable: `TRUNCATE_LOAD` vs `UPSERT`)

---

## 16. Module 12 — Web UI Dashboard

### Responsibility
Provide a user-friendly interface for non-technical migration managers and technical architects to configure, monitor, and validate migrations.

### Technology: Next.js 15 + Tailwind CSS + shadcn/ui

### Pages & Features

```
/                          → Dashboard (active projects, recent runs)
/projects/new              → Project wizard (step-by-step)
/projects/[id]             → Project detail
  /projects/[id]/schema    → Schema explorer (parsed tables, columns, types)
  /projects/[id]/redefines → REDEFINES annotation UI
  /projects/[id]/mappings  → Data type mapping override UI
  /projects/[id]/rules     → Transformation rules editor
  /projects/[id]/run       → Start migration (bigbang / incremental / CDC)
  /projects/[id]/runs/[rid]→ Run detail (progress, logs, errors)
  /projects/[id]/validation→ Validation & reconciliation report
/copybooks                 → Copybook library browser
/sql-preview               → Generated DDL SQL preview with syntax highlighting
```

### Key UI Features

1. **Schema Diff Viewer**: Side-by-side COBOL copybook vs generated SQL schema
2. **REDEFINES Annotator**: Visual drag-drop to assign discriminator values to record types
3. **Data Sample Preview**: Shows first 10 decoded records before full migration
4. **Live Progress Dashboard**: Real-time progress bars, row counts, throughput (rows/sec)
5. **Reconciliation Report Viewer**: Color-coded pass/fail, drilldown to mismatching rows
6. **Rejection Log Browser**: Browse rejected records, see raw bytes + decoded attempt
7. **CDC Lag Monitor**: Real-time graph of consumer lag during CDC phase

---

## 17. Module 13 — CLI Interface

### Responsibility
Full CLI for automated pipelines, CI/CD integration, and scripted migrations.

### Technology: Python Typer + Rich

### Commands

```bash
# Project management
cobolshift project create --name "Bank Core Migration" --config config.yaml
cobolshift project list
cobolshift project status PROJECT_ID

# Copybook operations
cobolshift parse COPYBOOK.cpy --output schema.json
cobolshift schema show COPYBOOK.cpy --target postgresql
cobolshift ddl generate PROJECT_ID --output migration.sql

# Migration execution
cobolshift migrate run PROJECT_ID --mode bigbang
cobolshift migrate run PROJECT_ID --mode incremental --table customer
cobolshift migrate run PROJECT_ID --mode cdc --phase baseline
cobolshift migrate run PROJECT_ID --mode cdc --phase catchup
cobolshift migrate cutover PROJECT_ID --confirm

# Validation
cobolshift validate run PROJECT_ID --table customer
cobolshift validate report PROJECT_ID --format html --output report.html

# State management
cobolshift state show PROJECT_ID
cobolshift state reset PROJECT_ID --table customer --confirm

# Utilities
cobolshift decode --file data.vsam --copybook CUSTOMER.cpy --rows 10
cobolshift ebcdic-detect --file data.vsam  # Attempt code page detection
```

---

## 18. Data Type Mapping Reference

### Complete COBOL → SQL Mapping Table

| COBOL PIC | USAGE | PostgreSQL | SQL Server |
|---|---|---|---|
| `PIC X(n)` | DISPLAY | `VARCHAR(n)` | `NVARCHAR(n)` |
| `PIC X` | DISPLAY | `CHAR(1)` | `NCHAR(1)` |
| `PIC A(n)` | DISPLAY | `CHAR(n)` | `NCHAR(n)` |
| `PIC 9(n)` n≤4 | DISPLAY/COMP | `SMALLINT` | `SMALLINT` |
| `PIC 9(n)` n≤9 | DISPLAY/COMP | `INTEGER` | `INT` |
| `PIC 9(n)` n≤18 | DISPLAY/COMP | `BIGINT` | `BIGINT` |
| `PIC 9(n)V9(m)` | DISPLAY | `NUMERIC(n+m, m)` | `DECIMAL(n+m, m)` |
| `PIC S9(n)` | COMP-3 | `NUMERIC(n, 0)` | `DECIMAL(n, 0)` |
| `PIC S9(n)V9(m)` | COMP-3 | `NUMERIC(n+m, m)` | `DECIMAL(n+m, m)` |
| `PIC 9(4)` | COMP | `SMALLINT` | `SMALLINT` |
| `PIC 9(9)` | COMP | `INTEGER` | `INT` |
| `PIC 9(18)` | COMP | `BIGINT` | `BIGINT` |
| `PIC S9(4)` | COMP-5 | `SMALLINT` | `SMALLINT` |
| `PIC S9(9)` | COMP-5 | `INTEGER` | `INT` |
| `PIC 9(n)` | COMP-1 | `REAL` ⚠️ | `REAL` ⚠️ |
| `PIC 9(n)` | COMP-2 | `DOUBLE PRECISION` ⚠️ | `FLOAT` ⚠️ |
| `PIC 9(8)` (YYYYMMDD) | DISPLAY | `DATE` | `DATE` |
| `PIC 9(6)` (HHMMSS) | DISPLAY | `TIME` | `TIME` |
| `PIC 9(14)` (datetime) | DISPLAY | `TIMESTAMP` | `DATETIME2` |

⚠️ COMP-1/COMP-2 floats in financial fields generate a warning. User must confirm or specify NUMERIC target precision.

---

## 19. Technology Stack

### Backend (Core Engine + API)

| Component | Technology | Reason |
|---|---|---|
| Language | Python 3.12+ | Rich ecosystem for data processing |
| ANTLR4 Runtime | `antlr4-python3-runtime` | COBOL grammar parsing |
| COBOL Grammar | `antlr/grammars-v4/cobol85` | Proven, maintained grammar |
| Binary decode | `struct`, `decimal` (stdlib) | Precise COMP-3 decoding |
| EBCDIC decode | Python `codecs` (`cp037`, etc.) | Built-in EBCDIC support |
| PostgreSQL client | `psycopg[binary]` (v3) | COPY protocol + async |
| SQL Server client | `pyodbc` + `sqlalchemy` | BCP bulk load support |
| Async framework | `asyncio` + `anyio` | Concurrent pipeline stages |
| API framework | `FastAPI` | Async, OpenAPI auto-docs |
| Task queue | `Celery` + `Redis` | Long-running migrations |
| CDC consumer | `confluent-kafka-python` | Debezium/Kafka integration |
| Encryption | `cryptography` (Fernet) | Connection string encryption |
| Validation | `pydantic` v2 | Schema IR validation |
| CLI | `typer` + `rich` | Beautiful terminal output |

### Frontend (Web UI)

| Component | Technology |
|---|---|
| Framework | Next.js 15 (App Router) |
| Styling | Tailwind CSS v4 |
| Components | shadcn/ui |
| Data fetching | TanStack Query v5 |
| Real-time | Server-Sent Events (SSE) for progress |
| SQL editor | Monaco Editor (VS Code engine) |
| Charts | Recharts |
| State | Zustand |

### Infrastructure

| Component | Technology |
|---|---|
| Tool database | PostgreSQL 16 (or SQLite for single-user) |
| Cache / queue broker | Redis 7 |
| CDC pipeline | Kafka 3.x + Debezium 2.x |
| Container | Docker + Docker Compose |
| Reverse proxy | Nginx |
| Auth | JWT + bcrypt |

---

## 20. Database Schema for the Tool Itself

```sql
-- The tool's own operational database

CREATE TABLE projects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    description     TEXT,
    source_type     TEXT NOT NULL,
    target_type     TEXT NOT NULL,
    config_json     JSONB NOT NULL,  -- full MigrationProject config
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE copybooks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID REFERENCES projects(id),
    filename        TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    file_checksum   TEXT NOT NULL,
    parsed_at       TIMESTAMPTZ,
    schema_ir       JSONB,
    parse_errors    JSONB,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE source_files (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID REFERENCES projects(id),
    filename        TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    file_checksum   TEXT,
    record_format   TEXT,       -- F, V, VB, D
    record_length   INTEGER,
    encoding        TEXT,       -- cp037, cp500, utf-8, etc.
    total_records   BIGINT,
    copybook_id     UUID REFERENCES copybooks(id)
);

CREATE TABLE migration_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID REFERENCES projects(id),
    run_number      SERIAL,
    mode            TEXT CHECK (mode IN ('bigbang','incremental','cdc_baseline','cdc_catchup','cdc_cutover')),
    status          TEXT CHECK (status IN ('pending','running','completed','failed','rolled_back')),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    rows_extracted  BIGINT DEFAULT 0,
    rows_loaded     BIGINT DEFAULT 0,
    rows_rejected   BIGINT DEFAULT 0,
    error_message   TEXT,
    cdc_offset      JSONB
);

CREATE TABLE table_migration_states (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID REFERENCES migration_runs(id),
    table_name      TEXT NOT NULL,
    source_file_id  UUID REFERENCES source_files(id),
    status          TEXT,
    rows_extracted  BIGINT DEFAULT 0,
    rows_loaded     BIGINT DEFAULT 0,
    rows_rejected   BIGINT DEFAULT 0,
    validation_json JSONB,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);

CREATE TABLE rejection_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID REFERENCES migration_runs(id),
    table_name      TEXT,
    source_line_num BIGINT,
    raw_bytes       BYTEA,
    decoded_partial JSONB,
    error_type      TEXT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE transformation_rules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID REFERENCES projects(id),
    table_name      TEXT NOT NULL,
    rule_order      INTEGER NOT NULL,
    rule_json       JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE validation_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    migration_run_id UUID REFERENCES migration_runs(id),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    overall_passed  BOOLEAN,
    report_json     JSONB
);
```

---

## 21. File & Directory Structure

```
cobolshift/
├── README.md
├── pyproject.toml
├── docker-compose.yml
├── .env.example
│
├── backend/
│   ├── main.py                      ← FastAPI app entry
│   ├── cli.py                       ← Typer CLI entry
│   │
│   ├── core/
│   │   ├── project.py               ← Module 1: Project Manager
│   │   ├── parser/
│   │   │   ├── preprocessor.py      ← COPY/REPLACE/column stripping
│   │   │   ├── cobol_parser.py      ← Module 2: ANTLR4 parser wrapper
│   │   │   ├── ast_nodes.py         ← AST dataclass definitions
│   │   │   ├── layout_calculator.py ← Byte offset/length computation
│   │   │   └── grammar/
│   │   │       ├── Cobol85.g4       ← ANTLR4 grammar file
│   │   │       └── CobolPreprocessor.g4
│   │   │
│   │   ├── analyzer/
│   │   │   ├── schema_analyzer.py   ← Module 3: Schema IR builder
│   │   │   ├── redefines_resolver.py← REDEFINES/discriminator detection
│   │   │   ├── occurs_normalizer.py ← OCCURS → child table builder
│   │   │   └── ir_nodes.py          ← Schema IR dataclass definitions
│   │   │
│   │   ├── generator/
│   │   │   ├── ddl_generator.py     ← Module 4: DDL emitter
│   │   │   ├── postgresql_dialect.py
│   │   │   └── sqlserver_dialect.py
│   │   │
│   │   ├── decoder/
│   │   │   ├── record_decoder.py    ← Module 5: Binary/EBCDIC decoder
│   │   │   ├── comp3_decoder.py     ← Packed decimal
│   │   │   ├── comp_decoder.py      ← Binary integer
│   │   │   ├── ebcdic_decoder.py    ← EBCDIC text
│   │   │   └── date_normalizer.py   ← Date format conversion
│   │   │
│   │   ├── pipeline/
│   │   │   ├── extraction.py        ← Module 6: Extraction pipeline
│   │   │   ├── readers/
│   │   │   │   ├── fixed_reader.py  ← RECFM=F
│   │   │   │   ├── variable_reader.py ← RECFM=V/VB
│   │   │   │   └── csv_reader.py
│   │   │   └── router.py            ← Multi-record-type router
│   │   │
│   │   ├── transform/
│   │   │   └── engine.py            ← Module 7: Transformation engine
│   │   │
│   │   ├── loader/
│   │   │   ├── loader.py            ← Module 8: Target loader
│   │   │   ├── pg_loader.py         ← PostgreSQL COPY loader
│   │   │   └── sqlserver_loader.py  ← SQL Server bulk loader
│   │   │
│   │   ├── cdc/
│   │   │   ├── bridge.py            ← Module 9: CDC bridge
│   │   │   ├── kafka_consumer.py
│   │   │   └── debezium_config.py
│   │   │
│   │   ├── validation/
│   │   │   ├── engine.py            ← Module 10: Validation engine
│   │   │   ├── row_count.py
│   │   │   ├── aggregate.py
│   │   │   ├── sampling.py
│   │   │   └── report.py
│   │   │
│   │   └── state/
│   │       └── tracker.py           ← Module 11: State tracker
│   │
│   ├── api/
│   │   ├── routes/
│   │   │   ├── projects.py
│   │   │   ├── migrations.py
│   │   │   ├── schema.py
│   │   │   ├── validation.py
│   │   │   └── sse.py               ← Server-Sent Events for live progress
│   │   └── models.py                ← Pydantic request/response models
│   │
│   ├── db/
│   │   ├── connection.py
│   │   ├── migrations/              ← Flyway scripts for tool's own DB
│   │   │   ├── V001__create_projects.sql
│   │   │   ├── V002__create_copybooks.sql
│   │   │   └── ...
│   │   └── repositories/
│   │
│   └── workers/
│       ├── celery_app.py
│       └── tasks.py                 ← Async migration task definitions
│
├── frontend/
│   ├── package.json
│   ├── app/
│   │   ├── page.tsx                 ← Dashboard
│   │   ├── projects/
│   │   └── ...
│   └── components/
│
├── tests/
│   ├── unit/
│   │   ├── test_comp3_decoder.py
│   │   ├── test_cobol_parser.py
│   │   ├── test_schema_analyzer.py
│   │   └── ...
│   ├── integration/
│   │   ├── test_full_migration.py
│   │   └── fixtures/
│   │       ├── sample.cpy
│   │       └── sample.vsam          ← Binary test fixture
│   └── e2e/
│
└── docs/
    ├── APP_PLAN.md                  ← This document
    ├── COBOL_REFERENCE.md
    └── DATA_TYPE_MAPPING.md
```

---

## 22. Build & Deployment Plan

### Docker Compose (Development)

```yaml
version: "3.9"
services:
  api:
    build: ./backend
    ports: ["8000:8000"]
    depends_on: [db, redis]
    environment:
      DATABASE_URL: postgresql://cobolshift:secret@db/cobolshift
      REDIS_URL: redis://redis:6379

  worker:
    build: ./backend
    command: celery -A workers.celery_app worker --loglevel=info
    depends_on: [db, redis]

  frontend:
    build: ./frontend
    ports: ["3000:3000"]

  db:
    image: postgres:16
    environment:
      POSTGRES_DB: cobolshift
      POSTGRES_USER: cobolshift
      POSTGRES_PASSWORD: secret
    volumes: [pgdata:/var/lib/postgresql/data]

  redis:
    image: redis:7-alpine

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    # ... (for CDC mode)

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0

volumes:
  pgdata:
```

---

## 23. Testing Strategy

### Unit Tests

| Test Suite | What it Tests |
|---|---|
| `test_comp3_decoder` | All packed decimal edge cases: -0, max precision, invalid nibbles |
| `test_comp_binary` | Big/little endian, signed/unsigned, all sizes (1/2/4/8 bytes) |
| `test_ebcdic_decoder` | All supported code pages, mixed fields, DBCS |
| `test_date_normalizer` | All 15 date formats, sentinel values, invalid dates |
| `test_cobol_parser` | All COBOL division types, REDEFINES, OCCURS, COPY, REPLACE |
| `test_schema_analyzer` | Level number hierarchy, ODO detection, redefines grouping |
| `test_ddl_generator` | PostgreSQL and SQL Server DDL output correctness |
| `test_layout_calculator` | Exact byte offsets including SYNC padding |
| `test_redefines_resolver` | Discriminator detection from EVALUATE statements |

### Integration Tests

- Full pipeline test: `.cpy` → parse → IR → DDL → extract `.vsam` → decode → load → validate
- Multi-record-type file migration
- OCCURS DEPENDING ON with real variable-length records
- CDC catch-up test with simulated source changes

### Golden File Tests

Maintain a library of real-world COBOL copybook patterns (anonymized) and their expected SQL DDL output. Any parser change that modifies the golden files requires explicit review.

---

## 24. Phased Rollout Plan

### Phase 1 — Core Parser (Weeks 1-4)
- [ ] ANTLR4 COBOL grammar integration
- [ ] Preprocessor (COPY resolver, REPLACE, column stripper)
- [ ] AST node definitions
- [ ] Byte layout calculator
- [ ] Unit tests for parser

### Phase 2 — Schema Analysis (Weeks 5-7)
- [ ] Schema IR builder from AST
- [ ] REDEFINES resolver
- [ ] OCCURS normalizer
- [ ] Data type mapping table
- [ ] DDL emitter (PostgreSQL)
- [ ] DDL emitter (SQL Server)

### Phase 3 — Data Decoder (Weeks 8-10)
- [ ] COMP-3 decoder
- [ ] COMP binary decoder
- [ ] EBCDIC decoder (CP037, CP500, CP1047)
- [ ] Date normalizer
- [ ] Sentinel value handler
- [ ] Comprehensive decoder unit tests

### Phase 4 — Extraction & Loading Pipeline (Weeks 11-14)
- [ ] RECFM=F reader
- [ ] RECFM=V/VB reader
- [ ] Multi-record-type router
- [ ] PostgreSQL COPY loader
- [ ] SQL Server bulk loader
- [ ] Batch error handling + bisection
- [ ] Rejection log

### Phase 5 — State Tracking & Validation (Weeks 15-17)
- [ ] Tool database schema + migrations
- [ ] Migration state tracker
- [ ] Row count validator
- [ ] Aggregate reconciliation validator
- [ ] Sampling validator
- [ ] Reconciliation report generator

### Phase 6 — API & CLI (Weeks 18-20)
- [ ] FastAPI routes
- [ ] SSE for live progress
- [ ] Typer CLI with all commands
- [ ] Authentication (JWT)

### Phase 7 — Web UI (Weeks 21-26)
- [ ] Project wizard
- [ ] Schema explorer
- [ ] REDEFINES annotator
- [ ] Live migration dashboard
- [ ] Reconciliation report viewer

### Phase 8 — CDC Bridge (Weeks 27-30)
- [ ] Debezium integration
- [ ] Kafka consumer
- [ ] CDC phase manager (baseline → catch-up → cutover)
- [ ] Reverse CDC for rollback

### Phase 9 — Hardening & Beta (Weeks 31-36)
- [ ] End-to-end integration tests
- [ ] Performance benchmarking (target: >500K rows/min throughput)
- [ ] Security audit
- [ ] Documentation
- [ ] Beta testing with real COBOL datasets

---

*End of CobolShift Application Plan v1.0*
