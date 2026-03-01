"""
Schema analysis routes.

- Parse a copybook → Schema IR
- Generate DDL from a copybook
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.models import DDLResponse, GenerateDDLRequest, ParseCopybookRequest
from backend.core.analyzer.schema_analyzer import SchemaAnalyzer, AnalyzerConfig
from backend.core.generator.ddl_generator import DDLGenerator
from backend.core.parser.cobol_parser import CobolParser
from backend.core.parser.layout_calculator import LayoutCalculator
from backend.core.parser.preprocessor import CobolPreprocessor
from backend.db.connection import get_db
from backend.db.models import Copybook

router = APIRouter(prefix="/schema", tags=["schema"])


async def _get_copybook(db: AsyncSession, copybook_id: uuid.UUID) -> Copybook:
    cb = await db.get(Copybook, copybook_id)
    if not cb:
        raise HTTPException(status_code=404, detail=f"Copybook {copybook_id} not found")
    return cb


@router.post("/parse")
async def parse_copybook(
    body: ParseCopybookRequest, db: AsyncSession = Depends(get_db)
) -> dict:
    """
    Parse a copybook and return the Schema IR as JSON.
    Also stores the result in the copybook row (cached for subsequent calls).
    """
    cb = await _get_copybook(db, body.copybook_id)
    path = Path(cb.file_path)
    if not path.exists():
        raise HTTPException(status_code=422, detail=f"File not found on disk: {path}")

    errors: list[str] = []
    try:
        preprocessor = CobolPreprocessor(copybook_lib_paths=[path.parent])
        result = preprocessor.process_file(path)
        errors.extend(result.warnings)

        parser = CobolParser()
        data_items = parser.parse_copybook(path)

        calc = LayoutCalculator()
        data_items = calc.calculate(data_items)

        config = AnalyzerConfig(
            filler_strategy=body.filler_strategy,
            redefines_strategy=body.redefines_strategy,
            occurs_strategy=body.occurs_strategy,
        )
        analyzer = SchemaAnalyzer(config)
        schema_ir = analyzer.analyze(data_items, table_name=path.stem)

        # Serialise to plain dict for JSONB storage
        import dataclasses, json
        schema_dict = _schema_to_dict(schema_ir)

        cb.schema_ir = schema_dict
        cb.parse_errors = errors or None
        cb.parsed_at = datetime.now(timezone.utc)
        await db.commit()

        return {"copybook_id": str(body.copybook_id), "schema": schema_dict, "warnings": errors}

    except Exception as exc:
        errors.append(str(exc))
        cb.parse_errors = errors
        await db.commit()
        raise HTTPException(status_code=422, detail=str(exc))


@router.post("/ddl", response_model=DDLResponse)
async def generate_ddl(
    body: GenerateDDLRequest, db: AsyncSession = Depends(get_db)
) -> DDLResponse:
    """
    Generate SQL DDL for a copybook. Returns the full SQL string.
    Triggers a parse if the copybook hasn't been parsed yet.
    """
    cb = await _get_copybook(db, body.copybook_id)
    if not cb.schema_ir:
        # Auto-parse with defaults
        await parse_copybook(
            ParseCopybookRequest(copybook_id=body.copybook_id), db=db
        )
        await db.refresh(cb)

    if not cb.schema_ir:
        raise HTTPException(status_code=422, detail="Copybook could not be parsed")

    # Reconstruct SchemaIR from cached dict
    try:
        schema_ir = _dict_to_schema(cb.schema_ir)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Schema cache corrupt: {exc}")

    gen = DDLGenerator(
        dialect=body.dialect,
        schema_name=body.schema_name,
        include_comments=body.include_comments,
        flyway_version=body.flyway_version,
        flyway_description=body.flyway_description,
    )
    output = gen.generate(schema_ir)

    return DDLResponse(
        copybook_id=body.copybook_id,
        dialect=body.dialect,
        sql=output.sql,
        table_names=output.table_names,
    )


# ---------------------------------------------------------------------------
# Schema IR serialization helpers (dataclass ↔ dict round-trip)
# ---------------------------------------------------------------------------

def _schema_to_dict(schema_ir) -> dict:
    """Recursively convert SchemaIR dataclasses to plain dicts."""
    import dataclasses

    def _convert(obj):
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {k: _convert(v) for k, v in dataclasses.asdict(obj).items()}
        if isinstance(obj, list):
            return [_convert(i) for i in obj]
        if isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        return obj

    return _convert(schema_ir)


def _dict_to_schema(d: dict):
    """
    Reconstruct SchemaIR from a plain dict (from JSONB cache).
    Returns the dict as-is — DDLGenerator accepts dicts via its
    internal _from_dict helpers when SchemaIR is not imported.
    """
    from backend.core.analyzer.ir_nodes import (
        SchemaIR,
        TableIR,
        ColumnIR,
        SQLTypeIR,
        CheckConstraintIR,
        RelationshipIR,
    )

    def _col(c: dict) -> ColumnIR:
        sql = c["sql_type"]
        return ColumnIR(
            name=c["name"],
            source_cobol_name=c["source_cobol_name"],
            source_pic=c["source_pic"],
            source_usage=c["source_usage"],
            sql_type=SQLTypeIR(
                base_type=sql["base_type"],
                precision=sql.get("precision"),
                scale=sql.get("scale"),
                max_length=sql.get("max_length"),
                pg_override=sql.get("pg_override"),
                ss_override=sql.get("ss_override"),
            ),
            nullable=c.get("nullable", True),
            byte_offset=c.get("byte_offset", 0),
            byte_length=c.get("byte_length", 0),
            decode_as=c.get("decode_as", "display"),
            ebcdic_decode=c.get("ebcdic_decode", True),
            date_format=c.get("date_format"),
            sentinel_null_values=c.get("sentinel_null_values", []),
            condition_values=c.get("condition_values", []),
        )

    def _check(c: dict) -> CheckConstraintIR:
        return CheckConstraintIR(
            name=c["name"],
            column=c["column"],
            allowed_values=c["allowed_values"],
        )

    def _table(t: dict) -> TableIR:
        return TableIR(
            name=t["name"],
            source_cobol_name=t["source_cobol_name"],
            columns=[_col(c) for c in t.get("columns", [])],
            primary_key=t.get("primary_key", []),
            check_constraints=[_check(c) for c in t.get("check_constraints", [])],
            table_type=t.get("table_type", "main"),
            parent_table=t.get("parent_table"),
            discriminator_column=t.get("discriminator_column"),
        )

    def _rel(r: dict) -> RelationshipIR:
        return RelationshipIR(
            parent_table=r["parent_table"],
            child_table=r["child_table"],
            join_columns=r["join_columns"],
        )

    return SchemaIR(
        tables=[_table(t) for t in d.get("tables", [])],
        relationships=[_rel(r) for r in d.get("relationships", [])],
        enums=[],  # enums not persisted separately
    )
