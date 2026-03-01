"""
Copybook Schema Analyzer.

Converts the COBOL AST DataDescription tree → Schema IR (TableIR, ColumnIR, etc.)

Handles:
  - Flat records → single table
  - Nested group items → either flattened columns or sub-tables (config)
  - OCCURS (fixed + DEPENDING ON) → child tables
  - REDEFINES → wide table / subtype tables / JSONB (config)
  - Level-88 conditions → CHECK constraints + optional lookup tables
  - FILLER items → skip / keep / warn (config)
  - Type mapping (PIC + USAGE → SQLTypeIR)
"""

from __future__ import annotations

import logging
import re
from typing import Literal

from backend.core.analyzer.ir_nodes import (
    CheckConstraintIR,
    ColumnIR,
    EnumIR,
    RelationshipIR,
    SchemaIR,
    SQLTypeIR,
    TableIR,
)
from backend.core.parser.ast_nodes import DataDescription
from backend.core.parser.layout_calculator import analyse_picture

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Type mapper
# ─────────────────────────────────────────────────────────────────────────────

def map_type(pic: str, usage: str) -> SQLTypeIR:
    """Map a COBOL PIC clause + USAGE to an SQLTypeIR."""
    usage = (usage or "DISPLAY").upper()
    info = analyse_picture(pic or "")

    # ── Alphanumeric / Alphabetic ──────────────────────────────────────────
    if info.category in ("ALPHANUMERIC", "ALPHABETIC", "ALPHANUMERIC_EDITED"):
        n = info.display_length or 1
        return SQLTypeIR(
            base_type="VARCHAR",
            max_length=n,
            target_postgresql=f"VARCHAR({n})",
            target_sqlserver=f"NVARCHAR({n})",
        )

    # ── Numeric edited (display only — keep as VARCHAR) ────────────────────
    if info.category == "NUMERIC_EDITED":
        n = info.display_length or 20
        return SQLTypeIR(
            base_type="VARCHAR",
            max_length=n,
            target_postgresql=f"VARCHAR({n})",
            target_sqlserver=f"NVARCHAR({n})",
        )

    # ── COMP-1 / COMP-2 (floating point) ──────────────────────────────────
    if usage == "COMP-1":
        return SQLTypeIR(
            base_type="REAL",
            target_postgresql="REAL",
            target_sqlserver="REAL",
        )
    if usage == "COMP-2":
        return SQLTypeIR(
            base_type="DOUBLE PRECISION",
            target_postgresql="DOUBLE PRECISION",
            target_sqlserver="FLOAT",
        )

    # ── NUMERIC (all integer / decimal types) ─────────────────────────────
    td = info.total_digits or 1
    dec = info.decimal_digits

    if usage in ("COMP-3", "PACKED-DECIMAL", "COMP-6"):
        p = td
        s = dec
        return SQLTypeIR(
            base_type="NUMERIC",
            precision=p,
            scale=s,
            target_postgresql=f"NUMERIC({p},{s})",
            target_sqlserver=f"DECIMAL({p},{s})",
        )

    if usage in ("COMP", "COMP-4", "BINARY", "COMP-5"):
        if td <= 4:
            return SQLTypeIR(
                base_type="SMALLINT",
                target_postgresql="SMALLINT",
                target_sqlserver="SMALLINT",
            )
        if td <= 9:
            return SQLTypeIR(
                base_type="INTEGER",
                target_postgresql="INTEGER",
                target_sqlserver="INT",
            )
        return SQLTypeIR(
            base_type="BIGINT",
            target_postgresql="BIGINT",
            target_sqlserver="BIGINT",
        )

    # DISPLAY numeric
    if dec > 0:
        p = td
        s = dec
        return SQLTypeIR(
            base_type="NUMERIC",
            precision=p,
            scale=s,
            target_postgresql=f"NUMERIC({p},{s})",
            target_sqlserver=f"DECIMAL({p},{s})",
        )
    # Integer display
    if td <= 4:
        return SQLTypeIR(
            base_type="SMALLINT",
            target_postgresql="SMALLINT",
            target_sqlserver="SMALLINT",
        )
    if td <= 9:
        return SQLTypeIR(
            base_type="INTEGER",
            target_postgresql="INTEGER",
            target_sqlserver="INT",
        )
    if td <= 18:
        return SQLTypeIR(
            base_type="BIGINT",
            target_postgresql="BIGINT",
            target_sqlserver="BIGINT",
        )
    p = td
    return SQLTypeIR(
        base_type="NUMERIC",
        precision=p,
        scale=0,
        target_postgresql=f"NUMERIC({p},0)",
        target_sqlserver=f"DECIMAL({p},0)",
    )


def decode_method(usage: str) -> str:
    u = (usage or "DISPLAY").upper()
    return {
        "COMP-3": "comp3", "PACKED-DECIMAL": "comp3", "COMP-6": "comp6",
        "COMP": "comp", "COMP-4": "comp", "BINARY": "comp",
        "COMP-5": "comp5",
        "COMP-1": "comp1",
        "COMP-2": "comp2",
        "INDEX": "index",
    }.get(u, "display")


def to_snake(name: str) -> str:
    """Convert COBOL-style UPPER-HYPHEN-NAME to sql_snake_case."""
    s = name.replace("-", "_").lower()
    s = re.sub(r"[^a-z0-9_]", "_", s)
    return s.strip("_") or "field"


# ─────────────────────────────────────────────────────────────────────────────
# Schema Analyzer
# ─────────────────────────────────────────────────────────────────────────────

class SchemaAnalyzer:
    """
    Converts a list of DataDescription roots (from a copybook or FILE SECTION)
    into a SchemaIR.

    Config options:
        filler_strategy:    "skip" | "keep" | "warn"
        redefines_strategy: "wide_table" | "subtype_tables" | "jsonb"
        occurs_strategy:    "child_table" | "json_array" | "wide_columns"
        ebcdic_decode:      True if source is EBCDIC-encoded
    """

    def __init__(
        self,
        filler_strategy: Literal["skip", "keep", "warn"] = "skip",
        redefines_strategy: Literal["wide_table", "subtype_tables", "jsonb"] = "wide_table",
        occurs_strategy: Literal["child_table", "json_array", "wide_columns"] = "child_table",
        ebcdic_decode: bool = True,
        source_copybook: str = "",
        source_hash: str = "",
    ):
        self.filler_strategy = filler_strategy
        self.redefines_strategy = redefines_strategy
        self.occurs_strategy = occurs_strategy
        self.ebcdic_decode = ebcdic_decode
        self.source_copybook = source_copybook
        self.source_hash = source_hash
        self._table_counter: dict[str, int] = {}

    def analyse(self, roots: list[DataDescription]) -> SchemaIR:
        """Convert a list of 01/77-level DataDescription nodes to a SchemaIR."""
        schema = SchemaIR(source_copybook=self.source_copybook)
        for root in roots:
            if root.level == 77:
                # Standalone item — add to a _scalars table
                self._handle_level77(root, schema)
            elif root.level == 1:
                table = self._root_to_table(root, schema)
                schema.tables.append(table)
        return schema

    # ─────────────────────────────────────────────────────────────────────
    # Root → Table
    # ─────────────────────────────────────────────────────────────────────

    def _root_to_table(self, root: DataDescription, schema: SchemaIR) -> TableIR:
        table_name = self._unique_table_name(to_snake(root.name))
        table = TableIR(
            name=table_name,
            source_cobol_name=root.name,
            source_copybook=self.source_copybook,
            source_hash=self.source_hash,
        )

        ordinal = 0
        ordinal = self._process_children(root.children, table, schema, table_name, ordinal)

        # Promote first non-nullable numeric column as PK if none assigned
        if not table.primary_key:
            for col in table.columns:
                if col.sql_type.base_type in ("INTEGER", "BIGINT", "SMALLINT", "NUMERIC") \
                        and not col.is_filler and not col.nullable:
                    table.primary_key = [col.name]
                    col.is_primary_key = True
                    break

        return table

    def _process_children(
        self,
        children: list[DataDescription],
        table: TableIR,
        schema: SchemaIR,
        parent_table_name: str,
        ordinal: int,
    ) -> int:
        """Recursively process children, emitting columns and child tables."""
        # Group condition names (level 88) by their parent field
        condition_map: dict[str, list[str]] = {}

        for item in children:
            if item.is_condition_name:
                # Level 88 — attach to parent field
                parent_col_name = to_snake(
                    next(
                        (c.name for c in reversed(children) if not c.is_condition_name and c != item),
                        "unknown",
                    )
                )
                vals = self._parse_condition_values(item.value or "")
                condition_map.setdefault(parent_col_name, []).extend(vals)
                continue

            if item.is_filler and self.filler_strategy == "skip":
                continue
            if item.is_filler and self.filler_strategy == "warn":
                logger.warning("FILLER field at offset %d in %s", item.byte_offset, table.name)
                continue

            if item.is_redefines:
                ordinal = self._handle_redefines(item, table, schema, parent_table_name, ordinal)
                continue

            if item.occurs:
                ordinal = self._handle_occurs(item, table, schema, parent_table_name, ordinal)
                continue

            if item.is_group:
                # Flatten group children into the same table
                ordinal = self._process_children(item.children, table, schema, parent_table_name, ordinal)
                continue

            # Elementary item → column
            col = self._item_to_column(item, ordinal)
            table.columns.append(col)
            ordinal += 1

        # Attach level-88 conditions as CHECK constraints
        for col_name, values in condition_map.items():
            col = next((c for c in table.columns if c.name == col_name), None)
            if col:
                col.condition_values = values
                table.check_constraints.append(
                    CheckConstraintIR(
                        name=f"chk_{table.name}_{col_name}",
                        column=col_name,
                        allowed_values=values,
                    )
                )

        return ordinal

    # ─────────────────────────────────────────────────────────────────────
    # REDEFINES handling
    # ─────────────────────────────────────────────────────────────────────

    def _handle_redefines(
        self,
        item: DataDescription,
        table: TableIR,
        schema: SchemaIR,
        parent_table_name: str,
        ordinal: int,
    ) -> int:
        if self.redefines_strategy == "wide_table":
            # Add columns from this redefines alternative (may overlap by offset)
            # Prefix column names with the redefines item name
            prefix = to_snake(item.name) + "_"
            if item.is_group:
                for child in item.children:
                    if child.is_elementary:
                        col = self._item_to_column(child, ordinal, name_prefix=prefix)
                        col.nullable = True  # redefines alts are always nullable
                        table.columns.append(col)
                        ordinal += 1
            else:
                col = self._item_to_column(item, ordinal, name_prefix=prefix)
                col.nullable = True
                table.columns.append(col)
                ordinal += 1

        elif self.redefines_strategy == "subtype_tables":
            # Create a separate subtype table
            sub_table_name = self._unique_table_name(
                f"{parent_table_name}_{to_snake(item.name)}"
            )
            sub_table = TableIR(
                name=sub_table_name,
                source_cobol_name=item.name,
                table_type="redefines_subtype",
                parent_table=parent_table_name,
                discriminator_column="record_type",  # to be annotated
            )
            sub_ordinal = 0
            if item.is_group:
                for child in item.children:
                    if child.is_elementary:
                        sub_table.columns.append(self._item_to_column(child, sub_ordinal))
                        sub_ordinal += 1
            else:
                sub_table.columns.append(self._item_to_column(item, 0))
            schema.tables.append(sub_table)

        elif self.redefines_strategy == "jsonb":
            # Add a single JSONB column for the redefines group
            jsonb_col = ColumnIR(
                name=to_snake(item.name) + "_json",
                source_cobol_name=item.name,
                source_pic="",
                source_usage="",
                sql_type=SQLTypeIR(
                    base_type="JSONB",
                    target_postgresql="JSONB",
                    target_sqlserver="NVARCHAR(MAX)",
                ),
                nullable=True,
                ordinal_position=ordinal,
                ebcdic_decode=False,
            )
            table.columns.append(jsonb_col)
            ordinal += 1

        return ordinal

    # ─────────────────────────────────────────────────────────────────────
    # OCCURS handling
    # ─────────────────────────────────────────────────────────────────────

    def _handle_occurs(
        self,
        item: DataDescription,
        parent_table: TableIR,
        schema: SchemaIR,
        parent_table_name: str,
        ordinal: int,
    ) -> int:
        if self.occurs_strategy == "child_table":
            child_table_name = self._unique_table_name(
                f"{parent_table_name}_{to_snake(item.name)}"
            )
            child_table = TableIR(
                name=child_table_name,
                source_cobol_name=item.name,
                table_type="occurs_child",
                parent_table=parent_table_name,
            )

            # FK column back to parent (will reference parent PK)
            fk_col_name = f"{parent_table_name}_fk"
            child_table.primary_key = [fk_col_name, "occurrence_index"]
            child_table.occurrence_index_column = "occurrence_index"

            # occurrence_index column (1-based)
            child_table.columns.append(ColumnIR(
                name="occurrence_index",
                source_cobol_name="__occurrence_index__",
                source_pic="9(5)",
                source_usage="COMP",
                sql_type=SQLTypeIR(
                    base_type="SMALLINT",
                    target_postgresql="SMALLINT",
                    target_sqlserver="SMALLINT",
                ),
                nullable=False,
                ordinal_position=0,
                ebcdic_decode=False,
                decode_as="comp",
            ))

            # Child columns
            child_ordinal = 1
            if item.is_group:
                for child in item.children:
                    if child.is_elementary and not child.is_filler:
                        child_table.columns.append(
                            self._item_to_column(child, child_ordinal)
                        )
                        child_ordinal += 1
            else:
                child_table.columns.append(self._item_to_column(item, 1))

            schema.tables.append(child_table)

            # Register relationship
            if parent_table.primary_key:
                schema.relationships.append(RelationshipIR(
                    from_table=child_table_name,
                    from_column=fk_col_name,
                    to_table=parent_table_name,
                    to_column=parent_table.primary_key[0],
                    constraint_name=f"fk_{child_table_name}_{parent_table_name}",
                ))

        elif self.occurs_strategy == "json_array":
            json_col = ColumnIR(
                name=to_snake(item.name) + "_array",
                source_cobol_name=item.name,
                source_pic="",
                source_usage="",
                sql_type=SQLTypeIR(
                    base_type="JSONB",
                    target_postgresql="JSONB",
                    target_sqlserver="NVARCHAR(MAX)",
                ),
                nullable=True,
                ordinal_position=ordinal,
                ebcdic_decode=False,
            )
            parent_table.columns.append(json_col)
            ordinal += 1

        elif self.occurs_strategy == "wide_columns":
            # Add columns for each occurrence index: field_name_1, field_name_2 ...
            for occ in range(1, item.occurs.max_times + 1):
                suffix = f"_{occ}"
                if item.is_group:
                    for child in item.children:
                        if child.is_elementary and not child.is_filler:
                            col = self._item_to_column(child, ordinal, name_suffix=suffix)
                            parent_table.columns.append(col)
                            ordinal += 1
                else:
                    col = self._item_to_column(item, ordinal, name_suffix=suffix)
                    parent_table.columns.append(col)
                    ordinal += 1

        return ordinal

    # ─────────────────────────────────────────────────────────────────────
    # Item → Column
    # ─────────────────────────────────────────────────────────────────────

    def _item_to_column(
        self,
        item: DataDescription,
        ordinal: int,
        name_prefix: str = "",
        name_suffix: str = "",
    ) -> ColumnIR:
        col_name = name_prefix + to_snake(item.name) + name_suffix
        sql_type = map_type(item.picture or "", item.usage or "DISPLAY")
        usage_upper = (item.usage or "DISPLAY").upper()

        return ColumnIR(
            name=col_name,
            source_cobol_name=item.name,
            source_pic=item.picture or "",
            source_usage=usage_upper,
            sql_type=sql_type,
            nullable=True,
            ordinal_position=ordinal,
            is_filler=item.is_filler,
            byte_offset=item.byte_offset,
            byte_length=item.byte_length,
            decode_as=decode_method(usage_upper),
            ebcdic_decode=self.ebcdic_decode and sql_type.base_type in ("VARCHAR", "CHAR"),
        )

    # ─────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────

    def _handle_level77(self, item: DataDescription, schema: SchemaIR) -> None:
        """Level 77 items go into a _scalars catch-all table."""
        scalars = schema.get_table("_scalars")
        if scalars is None:
            scalars = TableIR(name="_scalars", source_cobol_name="WORKING-STORAGE")
            schema.tables.append(scalars)
        col = self._item_to_column(item, len(scalars.columns))
        scalars.columns.append(col)

    def _unique_table_name(self, name: str) -> str:
        count = self._table_counter.get(name, 0)
        self._table_counter[name] = count + 1
        return name if count == 0 else f"{name}_{count}"

    def _parse_condition_values(self, value_str: str) -> list[str]:
        """Extract individual values from a level-88 VALUE clause string."""
        # Remove THRU ranges (keep start value only for simplicity)
        value_str = re.sub(r"\s+THRU\s+\S+", "", value_str, flags=re.IGNORECASE)
        # Split on spaces and commas
        raw = re.split(r"[\s,]+", value_str.strip())
        result: list[str] = []
        for v in raw:
            v = v.strip("'\"").strip()
            if v:
                result.append(v)
        return result
