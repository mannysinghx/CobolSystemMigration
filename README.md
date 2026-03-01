# CobolShift

**End-to-end COBOL → PostgreSQL / SQL Server migration platform.**

CobolShift automates the full pipeline from legacy COBOL mainframe systems to modern relational databases — parsing copybooks, decoding binary/EBCDIC data, generating DDL, bulk-loading records, and validating results — all through a web UI, REST API, or CLI.

---

## Table of Contents

- [Why CobolShift?](#why-cobolshift)
- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [1 · Clone & configure](#1--clone--configure)
  - [2 · Start with Docker Compose](#2--start-with-docker-compose)
  - [3 · Install Python deps (local dev)](#3--install-python-deps-local-dev)
  - [4 · Install frontend deps (local dev)](#4--install-frontend-deps-local-dev)
- [CLI Usage](#cli-usage)
- [REST API](#rest-api)
- [Web UI](#web-ui)
- [Configuration](#configuration)
- [How It Works](#how-it-works)
  - [COBOL Parser](#cobol-parser)
  - [Binary Decoding](#binary-decoding)
  - [Schema IR](#schema-ir)
  - [DDL Generation](#ddl-generation)
  - [Extraction Pipeline](#extraction-pipeline)
  - [Target Loaders](#target-loaders)
  - [Migration State Tracker](#migration-state-tracker)
  - [Live Progress (SSE)](#live-progress-sse)
- [Running Tests](#running-tests)
- [COBOL Data Type Mapping](#cobol-data-type-mapping)
- [Supported Date Formats](#supported-date-formats)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Why CobolShift?

Migrating COBOL systems to modern databases is notoriously difficult:

| Challenge | CobolShift's Answer |
|---|---|
| COBOL copybooks define complex binary layouts | Hand-written recursive-descent parser handles all COBOL constructs |
| COMP-3 packed decimal, COMP binary, EBCDIC encoding | Dedicated decoders for every encoding type |
| REDEFINES creates union-type fields | Three configurable strategies: wide table, subtype tables, or JSONB |
| OCCURS arrays can be fixed or variable-length | Child-table normalization, JSON arrays, or wide-column expansion |
| Bad rows can abort entire batches | Automatic binary bisection isolates failing rows without stopping the load |
| Re-running migrations risks duplicate data | SHA-256 file checksums detect already-loaded files automatically |
| No visibility into long-running loads | Live SSE stream pushes row counts to the UI in real time |

---

## Features

- **COBOL Parser** — Pure Python recursive-descent parser. No Java, no ANTLR4 runtime. Handles all four divisions, COPY/REPLACE/continuation lines, EXEC SQL, REDEFINES, OCCURS DEPENDING ON, and level-88 condition names.
- **Binary Decoders** — COMP-3 (packed decimal BCD), COMP/COMP-4 (big-endian integer), COMP-5 (native-endian), COMP-1/COMP-2 (IEEE float), EBCDIC→UTF-8 (24 IBM code pages), overpunch numeric, 15+ date formats with Y2K windowing.
- **Schema IR** — Dialect-neutral intermediate representation (TableIR, ColumnIR, SQLTypeIR) decouples parsing from SQL generation.
- **DDL Generator** — Generates production-ready CREATE TABLE scripts for PostgreSQL and SQL Server, including Flyway-versioned migration files, PRIMARY KEY, CHECK constraints from level-88 conditions, and column comments tracing back to original COBOL names.
- **Extraction Pipeline** — Streaming readers for RECFM=F (fixed-length), RECFM=V, and RECFM=VB (blocked variable). Multi-record-type routing via discriminator fields.
- **Target Loaders** — PostgreSQL COPY FROM STDIN binary protocol (psycopg3) and SQL Server fast_executemany (pyodbc). Both support `truncate_load`, `append`, and `upsert` (INSERT ON CONFLICT / MERGE).
- **Batch Bisection** — Failing batches are split in half recursively until the single bad row is isolated and logged to the rejection table.
- **Migration State Tracker** — Every run, table, and rejected row is recorded in the tool's own PostgreSQL database. Re-run safety via SHA-256 checksums.
- **FastAPI Backend** — Async REST API with SSE live progress, file upload endpoints, and OpenAPI docs at `/docs`.
- **Celery + Redis** — Long-running migration tasks run in background workers. Progress is published to Redis pub/sub and forwarded to SSE clients.
- **Next.js 15 Frontend** — Project wizard, copybook upload, schema explorer with DDL viewer, live migration dashboard with rejection log.
- **CLI** — All operations are scriptable via `cobolshift` commands using Typer + Rich.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CobolShift Platform                          │
│                                                                     │
│  ┌──────────────┐    ┌──────────────────────────────────────────┐  │
│  │  Next.js 15  │    │              FastAPI Backend              │  │
│  │  Frontend    │◄───►  /projects  /schema  /migrations  /health│  │
│  └──────────────┘    └────────────────────┬─────────────────────┘  │
│                                           │                         │
│                              ┌────────────▼────────────┐           │
│                              │     Celery + Redis       │           │
│                              │   (background tasks)     │           │
│                              └────────────┬────────────┘           │
│                                           │                         │
│         ┌─────────────────────────────────▼──────────────────┐     │
│         │                  Core Pipeline                       │     │
│         │                                                      │     │
│         │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐  │     │
│         │  │  COBOL   │  │  Schema  │  │    Extraction    │  │     │
│         │  │  Parser  │─►│Analyzer  │  │    Pipeline      │  │     │
│         │  │          │  │  (IR)    │  │  RECFM=F/V/VB    │  │     │
│         │  └──────────┘  └────┬─────┘  └────────┬─────────┘  │     │
│         │                     │                  │             │     │
│         │              ┌──────▼──────┐    ┌──────▼──────┐    │     │
│         │              │     DDL     │    │   Decoders  │    │     │
│         │              │  Generator  │    │COMP3/COMP/  │    │     │
│         │              │ PG / MSSQL  │    │EBCDIC/Date  │    │     │
│         │              └─────────────┘    └──────┬──────┘    │     │
│         │                                        │             │     │
│         │                               ┌────────▼────────┐   │     │
│         │                               │  Target Loader  │   │     │
│         │                               │ PostgreSQL COPY  │   │     │
│         │                               │ SQL Server Bulk  │   │     │
│         │                               └─────────────────┘   │     │
│         └──────────────────────────────────────────────────────┘     │
│                                                                     │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
│  │  PostgreSQL 16 │  │    Redis 7       │  │   Migration State    │ │
│  │  (tool's DB)   │  │  (broker+cache) │  │      Tracker         │ │
│  └────────────────┘  └─────────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.12+, FastAPI, SQLAlchemy 2 (async) |
| Task Queue | Celery 5, Redis 7 |
| PostgreSQL Driver | psycopg3 (binary COPY protocol) |
| SQL Server Driver | pyodbc (fast_executemany) |
| Frontend | Next.js 15, React 19, Tailwind CSS, TanStack Query v5 |
| Tool Database | PostgreSQL 16 |
| Infra | Docker Compose |
| CLI | Typer, Rich |
| Testing | pytest, pytest-asyncio |

---

## Project Structure

```
CobolSystemMigration/
├── backend/
│   ├── api/
│   │   ├── models.py              # Pydantic request/response models
│   │   └── routes/
│   │       ├── health.py          # GET /health
│   │       ├── projects.py        # CRUD + file upload
│   │       ├── schema.py          # Parse copybook, generate DDL
│   │       ├── migrations.py      # Start/list/stream/cancel runs
│   │       └── validation.py      # Post-migration checks
│   ├── core/
│   │   ├── analyzer/
│   │   │   ├── ir_nodes.py        # Schema IR dataclasses
│   │   │   └── schema_analyzer.py # AST → Schema IR
│   │   ├── decoder/
│   │   │   ├── comp3_decoder.py   # Packed decimal BCD
│   │   │   ├── comp_decoder.py    # Binary integer (big/little endian)
│   │   │   ├── ebcdic_decoder.py  # EBCDIC → UTF-8 (24 code pages)
│   │   │   ├── date_normalizer.py # 15+ COBOL date formats + Y2K
│   │   │   └── record_decoder.py  # Orchestrates all decoders
│   │   ├── generator/
│   │   │   └── ddl_generator.py   # PostgreSQL + SQL Server DDL
│   │   ├── loader/
│   │   │   ├── pg_loader.py       # PostgreSQL COPY bulk loader
│   │   │   └── sqlserver_loader.py# SQL Server fast_executemany
│   │   ├── parser/
│   │   │   ├── ast_nodes.py       # COBOL AST dataclasses
│   │   │   ├── cobol_parser.py    # Recursive-descent parser
│   │   │   ├── layout_calculator.py# Byte offset/length computation
│   │   │   └── preprocessor.py    # COPY/REPLACE/column stripping
│   │   ├── pipeline/
│   │   │   ├── extraction.py      # Streaming pipeline + router
│   │   │   └── readers/
│   │   │       ├── fixed_reader.py  # RECFM=F
│   │   │       └── variable_reader.py # RECFM=V/VB
│   │   └── state/
│   │       └── tracker.py         # Run/table/rejection state
│   ├── db/
│   │   ├── connection.py          # Async SQLAlchemy engine
│   │   └── models.py              # ORM models
│   ├── workers/
│   │   ├── celery_app.py          # Celery factory
│   │   └── tasks.py               # run_migration task
│   ├── cli.py                     # Typer CLI entry point
│   ├── config.py                  # Pydantic Settings
│   └── main.py                    # FastAPI app factory
├── frontend/
│   └── src/
│       ├── app/                   # Next.js App Router pages
│       │   ├── page.tsx           # Home
│       │   ├── projects/          # Project list, detail, new
│       │   ├── migrations/        # Run list, live detail
│       │   └── schema/            # Schema explorer + DDL viewer
│       ├── components/
│       │   ├── layout/            # Sidebar, Providers
│       │   └── features/          # StatusBadge, etc.
│       └── lib/
│           └── api.ts             # Typed axios API client
├── tests/
│   ├── test_comp3_decoder.py
│   ├── test_date_normalizer.py
│   ├── test_layout_calculator.py
│   ├── test_cobol_parser.py
│   ├── test_schema_analyzer.py
│   └── test_ddl_generator.py
├── docs/
│   └── APP_PLAN.md                # Full 24-section architecture plan
├── docker-compose.yml
├── Dockerfile.backend
├── pyproject.toml
└── .env.example
```

---

## Getting Started

### Prerequisites

| Tool | Minimum Version |
|---|---|
| Docker | 24+ |
| Docker Compose | v2.20+ |
| Python | 3.12+ (local dev) |
| Node.js | 20+ (local dev) |
| Git | any |

### 1 · Clone & configure

```bash
git clone https://github.com/mannysinghx/CobolSystemMigration.git
cd CobolSystemMigration
cp .env.example .env
```

Edit `.env` and set at minimum:

```dotenv
DATABASE_URL=postgresql+psycopg://cobolshift:secret@localhost:5432/cobolshift
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=<generate with: python -c "import secrets; print(secrets.token_hex(32))">
FERNET_KEY=<generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
```

### 2 · Start with Docker Compose

```bash
docker compose up --build
```

This starts:

| Service | Port | Purpose |
|---|---|---|
| `api` | 8000 | FastAPI backend + OpenAPI docs |
| `worker` | — | Celery migration worker |
| `frontend` | 3000 | Next.js web UI |
| `db` | 5432 | PostgreSQL 16 (tool state) |
| `redis` | 6379 | Celery broker + SSE pub/sub |

Open the UI at **http://localhost:3000** and the API docs at **http://localhost:8000/docs**.

To also start CDC services (Debezium + Kafka):

```bash
docker compose --profile cdc up --build
```

### 3 · Install Python deps (local dev)

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Start the API server:

```bash
uvicorn backend.main:app --reload
```

Start the Celery worker:

```bash
celery -A backend.workers.celery_app worker --loglevel=info
```

### 4 · Install frontend deps (local dev)

```bash
cd frontend
npm install
npm run dev
```

---

## CLI Usage

After installing (`pip install -e .`), the `cobolshift` command is available.

### Parse a copybook

```bash
cobolshift parse customer.cpy
# Prints Schema IR as JSON and a summary table of columns found

cobolshift parse customer.cpy --out schema.json
# Writes JSON to file
```

### Generate DDL

```bash
# PostgreSQL (default)
cobolshift ddl customer.cpy --out V001__customer.sql

# SQL Server, custom schema
cobolshift ddl customer.cpy \
  --dialect sqlserver \
  --schema dbo \
  --flyway-version 001 \
  --out V001__customer.sql
```

### Load a data file directly

```bash
cobolshift load CUSTOMER.DAT \
  --copybook customer.cpy \
  --table customer \
  --target "postgresql://user:pass@localhost/mydb" \
  --format F \
  --length 250 \
  --encoding cp037 \
  --mode truncate_load
```

Options:

| Flag | Default | Description |
|---|---|---|
| `--copybook` | required | Path to the COBOL copybook |
| `--table` | required | Target table name |
| `--target` | required | Database connection string |
| `--target-type` | `postgresql` | `postgresql` or `sqlserver` |
| `--format` | `F` | Record format: `F`, `V`, or `VB` |
| `--length` | — | Record length in bytes (required for RECFM=F) |
| `--encoding` | `cp037` | EBCDIC code page |
| `--mode` | `truncate_load` | `truncate_load`, `append`, or `upsert` |
| `--schema` | `public` | Target schema name |
| `--batch-size` | `10000` | Rows per batch |

### Check run status

```bash
cobolshift status --run <run-uuid> --db postgresql://user:pass@localhost/cobolshift
```

### Start the API server

```bash
cobolshift server --port 8000 --reload
```

---

## REST API

Interactive docs are at **http://localhost:8000/docs** (Swagger UI).

### Projects

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/projects` | Create a project |
| `GET` | `/projects` | List all projects |
| `GET` | `/projects/{id}` | Get project detail |
| `PATCH` | `/projects/{id}` | Update project |
| `DELETE` | `/projects/{id}` | Delete project |
| `POST` | `/projects/{id}/copybooks` | Upload a copybook (multipart) |
| `GET` | `/projects/{id}/copybooks` | List copybooks |
| `POST` | `/projects/{id}/source-files` | Upload a source data file |
| `GET` | `/projects/{id}/source-files` | List source files |

### Schema

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/schema/parse` | Parse a copybook → Schema IR (cached in DB) |
| `POST` | `/schema/ddl` | Generate DDL from a copybook |

### Migrations

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/migrations` | Start a migration run |
| `GET` | `/migrations?project_id=…` | List runs for a project |
| `GET` | `/migrations/{id}` | Run summary |
| `GET` | `/migrations/{id}/tables` | Per-table states |
| `GET` | `/migrations/{id}/stream` | **SSE live progress stream** |
| `GET` | `/migrations/{id}/rejections` | Rejected rows |
| `DELETE` | `/migrations/{id}` | Cancel a running migration |

### Validation

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/validation` | Run post-migration checks (row counts, rejection rate) |

### Example: start a migration

```bash
curl -X POST http://localhost:8000/migrations \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "a1b2c3d4-...",
    "run_type": "full_load"
  }'
```

### Example: subscribe to SSE progress

```bash
curl -N http://localhost:8000/migrations/<run-id>/stream
# data: {"event":"table_progress","table_name":"customer","rows_loaded":10000,...}
# data: {"event":"run_complete","status":"completed","rows_loaded":125000,...}
```

---

## Web UI

| Page | Path | Description |
|---|---|---|
| Home | `/` | Feature overview + quick start links |
| Projects | `/projects` | All projects grid view |
| New Project | `/projects/new` | Wizard to create a project |
| Project Detail | `/projects/{id}` | Upload copybooks, start migration, recent runs |
| Schema Explorer | `/schema?copybook_id=…` | Schema IR tree + DDL generator with copy/download |
| Migrations | `/migrations` | All runs across all projects |
| Run Detail | `/migrations/{id}` | Live progress bar, per-table stats, rejection log |

---

## Configuration

All settings are read from environment variables (or `.env`). See `.env.example` for the full list.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | — | PostgreSQL URL for CobolShift's own state DB |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis URL (Celery broker + SSE pub/sub) |
| `SECRET_KEY` | — | Random hex string for session security |
| `FERNET_KEY` | — | Fernet key for encrypting stored connection strings |
| `API_HOST` | `0.0.0.0` | FastAPI bind host |
| `API_PORT` | `8000` | FastAPI bind port |
| `LOG_LEVEL` | `INFO` | Python log level |
| `UPLOAD_DIR` | `./uploads` | Where uploaded copybooks/data files are stored |
| `KAFKA_BOOTSTRAP_SERVERS` | — | Kafka brokers (CDC mode only) |
| `DEBEZIUM_URL` | — | Debezium REST API URL (CDC mode only) |

---

## How It Works

### COBOL Parser

The parser is a pure-Python hand-written recursive-descent parser — no Java runtime, no ANTLR4 dependency.

**Pipeline:**

```
.cpy file
    │
    ▼
CobolPreprocessor
    ├── Strip columns 1-6 (sequence area) and 73-80 (identification)
    ├── Join continuation lines (column 7 = '-')
    ├── Expand COPY statements (searches copybook library paths)
    └── Apply REPLACE directives (token-level substitution)
    │
    ▼
CobolParser (recursive descent)
    ├── Tokenizes with regex (words, strings, dots, parentheses)
    ├── Parses DATA DIVISION → flat list of DataDescription items
    ├── Parses all clauses: PIC, USAGE, OCCURS, REDEFINES, VALUE, SIGN, SYNC
    ├── Handles EXEC SQL blocks and EVALUATE statements
    └── Builds tree via level-number stack (01→05→10→…)
    │
    ▼
LayoutCalculator
    ├── Computes byte_offset and byte_length for every item
    ├── REDEFINES items share the same offset as their base
    ├── OCCURS multiplies child length by max_times
    └── Handles SYNC alignment and group-level length accumulation
```

### Binary Decoding

Each field is decoded according to its `USAGE` clause:

| USAGE | Decoder | Notes |
|---|---|---|
| `DISPLAY` | EBCDIC → UTF-8 | Uses configured code page (default cp037) |
| `COMP-3` / `PACKED-DECIMAL` | BCD nibble extraction | Sign nibble C/F=+, D=− |
| `COMP` / `COMP-4` / `BINARY` | `struct.unpack` big-endian | 2/4/8 bytes for ≤4/≤9/≤18 digits |
| `COMP-5` | `struct.unpack` little-endian | Native byte order |
| `COMP-1` | `struct.unpack` 4-byte float | IEEE 754 single |
| `COMP-2` | `struct.unpack` 8-byte float | IEEE 754 double |
| `COMP-6` | Unsigned packed decimal | No sign nibble |

**EBCDIC code pages supported:** cp037, cp273, cp277, cp278, cp280, cp284, cp285, cp290, cp297, cp420, cp424, cp437, cp500, cp720, cp737, cp775, cp850, cp852, cp855, cp857, cp860, cp861, cp862, cp863, cp864, cp865, cp866, cp869, cp1026, cp1047, cp1140, cp1141, cp1142, cp1143, cp1144, cp1145, cp1146, cp1147, cp1148, cp1149.

### Schema IR

The Schema Analyzer converts a parsed COBOL structure into a dialect-neutral Schema IR:

```python
SchemaIR
  └── tables: list[TableIR]
       ├── name: str              # snake_case SQL name
       ├── columns: list[ColumnIR]
       │    ├── name              # snake_case
       │    ├── sql_type          # SQLTypeIR(base_type, precision, scale, max_length)
       │    ├── decode_as         # display | comp3 | comp | comp1 | comp2 | comp5
       │    ├── byte_offset / byte_length
       │    └── date_format       # detected format string if it's a date field
       ├── primary_key: list[str]
       └── check_constraints      # from level-88 condition names
```

**REDEFINES strategies** (configurable):

| Strategy | Behaviour |
|---|---|
| `wide_table` | All variants become nullable columns on the same table |
| `subtype_tables` | Each variant gets its own child table with FK |
| `jsonb` | Variants are stored as a JSONB column (PostgreSQL only) |

**OCCURS strategies** (configurable):

| Strategy | Behaviour |
|---|---|
| `child_table` | Normalized: one row per array element, FK + `_seq` column |
| `json_array` | Stored as a JSON array in a single column |
| `wide_columns` | Expanded: `field_01`, `field_02`, … columns |

### DDL Generation

The DDL Generator takes a `SchemaIR` and emits a complete SQL script:

```sql
-- Generated by CobolShift v0.1.0
-- Source copybook: CUSTOMER.CPY
-- V001__initial_load.sql

CREATE TABLE "public"."customer" (
    "customer_id"   INTEGER        NOT NULL,   -- CUSTOMER-ID  PIC 9(5)
    "customer_name" VARCHAR(30),               -- CUSTOMER-NAME PIC X(30)
    "balance"       NUMERIC(9, 2),             -- BALANCE PIC S9(7)V9(2) COMP-3
    CONSTRAINT "pk_customer" PRIMARY KEY ("customer_id")
);
```

SQL Server output uses `[bracket]` quoting and `NVARCHAR` for character fields.

### Extraction Pipeline

```
Flat file (RECFM=F/V/VB)
    │
    ▼
FixedRecordReader / VariableRecordReader
    │   Yields (line_number, raw_bytes) per record
    │
    ▼
ExtractionPipeline
    ├── Reads discriminator field (if multi-record-type file)
    ├── Routes bytes to the correct TableIR
    └── Yields ExtractionResult(raw_bytes, table_name, line_number, error?)
    │
    ▼
RecordDecoder
    ├── Slices raw_bytes using byte_offset + byte_length per column
    ├── Dispatches to the correct decoder per column
    └── Returns DecodedRecord(values: dict, errors: list, ok: bool)
```

### Target Loaders

**PostgreSQL** (`pg_loader.py`):
- Uses `psycopg3` `COPY FROM STDIN` protocol — the fastest possible ingestion method
- Upsert mode uses `INSERT … ON CONFLICT DO UPDATE`
- Default batch size: 10,000 rows

**SQL Server** (`sqlserver_loader.py`):
- Uses `pyodbc` with `fast_executemany = True`
- Upsert mode generates a `MERGE` statement
- Same 10,000-row default batch size

**Batch Bisection** (both loaders):

When a batch fails, instead of rejecting all 10,000 rows, the loader splits the batch in half and retries each half independently. This continues recursively until individual rows are identified. Only the truly bad rows end up in the rejection log.

```
Batch of 10,000 rows fails
    ├── Retry first 5,000  → succeeds (5,000 loaded)
    └── Retry last 5,000   → fails
            ├── Retry first 2,500 → succeeds
            └── Retry last 2,500  → fails
                    ├── …
                    └── Single row → logged to rejection_log table
```

### Migration State Tracker

Every migration is fully auditable:

```
projects
  └── migration_runs          (one per cobolshift run)
       ├── run_number
       ├── run_type            (full_load | incremental | cdc)
       ├── status              (pending | running | completed | failed | cancelled)
       └── table_migration_states   (one per table per run)
            ├── source_checksum     (SHA-256 of source file)
            ├── rows_extracted / rows_loaded / rows_rejected
            └── rejection_log       (one row per bad record)
                 ├── source_line_num
                 ├── raw_bytes
                 ├── decoded_partial
                 └── error_message
```

**Re-run safety:** Before loading a table, the tracker checks whether a `completed` state exists for the same `(project, table_name, source_checksum)`. If found, the table is skipped automatically — no duplicate data.

### Live Progress (SSE)

```
Celery Worker                    Redis pub/sub           Browser
─────────────                    ─────────────           ───────
After each 10K batch:
  PUBLISH run:<run_id>  ──────►  Channel                SSE client
  {event:"table_progress",        (run:<id>)    ──────► /migrations/{id}/stream
   rows_loaded:10000}                                    Updates progress bar
                                                         + table stats in real time
```

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

Test modules:

| File | What it tests |
|---|---|
| `test_comp3_decoder.py` | COMP-3 encode/decode, sign nibbles, scale, edge cases |
| `test_date_normalizer.py` | 15 date formats, Y2K windowing, null detection |
| `test_layout_calculator.py` | Byte offsets, REDEFINES overlap, OCCURS multiplication |
| `test_cobol_parser.py` | PIC clauses, hierarchy, REDEFINES, OCCURS DEPENDING ON, FILLER, level-88 |
| `test_schema_analyzer.py` | Type mapping, OCCURS strategies, level-88 → check constraints |
| `test_ddl_generator.py` | PostgreSQL/SQL Server DDL output, identifier quoting, PRIMARY KEY |

---

## COBOL Data Type Mapping

| COBOL PIC + USAGE | PostgreSQL | SQL Server |
|---|---|---|
| `X(n)` DISPLAY | `VARCHAR(n)` | `NVARCHAR(n)` |
| `A(n)` DISPLAY | `CHAR(n)` | `NCHAR(n)` |
| `9(n)` DISPLAY | `NUMERIC(n,0)` | `NUMERIC(n,0)` |
| `S9(p)V9(s)` DISPLAY | `NUMERIC(p+s, s)` | `NUMERIC(p+s, s)` |
| `9(n)` COMP-3 | `NUMERIC(n,0)` | `NUMERIC(n,0)` |
| `S9(p)V9(s)` COMP-3 | `NUMERIC(p+s, s)` | `NUMERIC(p+s, s)` |
| `9(1-4)` COMP | `SMALLINT` | `SMALLINT` |
| `9(5-9)` COMP | `INTEGER` | `INT` |
| `9(10-18)` COMP | `BIGINT` | `BIGINT` |
| `S9(p)V9(s)` COMP | `NUMERIC(p+s, s)` | `NUMERIC(p+s, s)` |
| `9(7)` COMP-1 | `REAL` | `REAL` |
| `9(15)` COMP-2 | `DOUBLE PRECISION` | `FLOAT(53)` |

---

## Supported Date Formats

| Format Code | Example | Notes |
|---|---|---|
| `YYYYMMDD` | `20240315` | Standard ISO |
| `YYMMDD` | `240315` | Y2K windowing (pivot year 30) |
| `YYYYDDD` | `2024075` | Julian calendar |
| `YYDDD` | `24075` | Julian + Y2K |
| `MMDDYYYY` | `03152024` | US format |
| `DDMMYYYY` | `15032024` | European format |
| `MMDDYY` | `031524` | US short + Y2K |
| `DDMMYY` | `150324` | European short + Y2K |
| `YYYY-MM-DD` | `2024-03-15` | ISO with hyphens |
| `MM/DD/YYYY` | `03/15/2024` | US with slashes |
| `DD/MM/YYYY` | `15/03/2024` | European with slashes |
| `YYYYMM` | `202403` | Month precision (day=1) |
| `LILIAN` | `157604` | IBM Lilian day number |

Y2K windowing default pivot: year ≤ 30 → 20xx, year > 30 → 19xx. Configurable per project.

---

## Roadmap

- [ ] **Transformation Engine** — YAML-driven rules: field rename, value lookup, computed columns, type casting
- [ ] **CDC Bridge** — Debezium + Kafka consumer for near-zero-downtime migrations
- [ ] **Full Validation Engine** — Statistical sampling, aggregate checks, business rule assertions
- [ ] **DB2 / IMS Source Adapters** — Native mainframe database extraction
- [ ] **Auth** — JWT authentication for the API and web UI
- [ ] **Multi-user** — Team workspaces with role-based access
- [ ] **Audit Log** — Immutable record of all DDL changes and migration runs
- [ ] **Monaco Editor** — In-browser DDL editing before applying to target

---

## Contributing

1. Fork the repo
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Make your changes with tests
4. Run the test suite: `pytest tests/ -v`
5. Push and open a Pull Request

Please follow PEP 8 for Python and keep TypeScript strict-mode compliant.

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

> Built with Claude Code · [CobolShift Architecture Plan](docs/APP_PLAN.md)
