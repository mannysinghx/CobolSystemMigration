"""Unit tests for SchemaAnalyzer (COBOL AST → Schema IR)."""

import pytest

from backend.core.parser.ast_nodes import DataDescription, OccursClause
from backend.core.parser.layout_calculator import LayoutCalculator
from backend.core.analyzer.schema_analyzer import SchemaAnalyzer, AnalyzerConfig
from backend.core.analyzer.ir_nodes import SchemaIR


def _build_schema(items: list[DataDescription], **config_kwargs) -> SchemaIR:
    calc = LayoutCalculator()
    items = calc.calculate(items)
    cfg = AnalyzerConfig(**config_kwargs)
    return SchemaAnalyzer(cfg).analyze(items, table_name="test_table")


class TestSimpleTable:
    def test_columns_generated(self):
        root = DataDescription(level=1, name="CUSTOMER-REC")
        f1 = DataDescription(level=5, name="CUST-ID", picture="9(5)")
        f2 = DataDescription(level=5, name="CUST-NAME", picture="X(30)")
        root.children = [f1, f2]
        items = [root, f1, f2]

        schema = _build_schema(items)
        assert len(schema.tables) >= 1
        table = schema.tables[0]
        col_names = {c.name for c in table.columns}
        assert "cust_id" in col_names
        assert "cust_name" in col_names

    def test_filler_skipped_by_default(self):
        root = DataDescription(level=1, name="REC")
        filler = DataDescription(level=5, name="FILLER", picture="X(5)")
        real = DataDescription(level=5, name="REAL-FIELD", picture="9(3)")
        root.children = [filler, real]
        items = [root, filler, real]

        schema = _build_schema(items, filler_strategy="skip")
        table = schema.tables[0]
        col_names = {c.name for c in table.columns}
        assert "filler" not in col_names
        assert "real_field" in col_names


class TestTypeMapping:
    def _single_col(self, pic: str, usage: str = "DISPLAY") -> dict:
        root = DataDescription(level=1, name="REC")
        f = DataDescription(level=5, name="FLD", picture=pic, usage=usage)
        root.children = [f]
        items = [root, f]
        schema = _build_schema(items)
        return {c.name: c for c in schema.tables[0].columns}

    def test_alphanumeric_maps_to_varchar(self):
        cols = self._single_col("X(50)")
        t = cols["fld"].sql_type
        assert "char" in t.base_type.lower() or "text" in t.base_type.lower()

    def test_comp3_maps_to_numeric(self):
        cols = self._single_col("S9(7)V9(2)", "COMP-3")
        t = cols["fld"].sql_type
        assert "numeric" in t.base_type.lower() or "decimal" in t.base_type.lower()
        # Scale should be 2
        assert cols["fld"].sql_type.scale == 2

    def test_comp_maps_to_integer(self):
        cols = self._single_col("9(9)", "COMP")
        t = cols["fld"].sql_type
        assert "int" in t.base_type.lower()


class TestOccursStrategy:
    def test_occurs_child_table(self):
        root = DataDescription(level=1, name="ORDER-REC")
        f1 = DataDescription(level=5, name="ORDER-ID", picture="9(8)")
        items_group = DataDescription(
            level=5,
            name="LINE-ITEMS",
            occurs=OccursClause(min_times=0, max_times=10, depending_on=None),
        )
        item_qty = DataDescription(level=10, name="ITEM-QTY", picture="9(3)")
        items_group.children = [item_qty]
        root.children = [f1, items_group]
        items = [root, f1, items_group, item_qty]

        schema = _build_schema(items, occurs_strategy="child_table")
        table_names = {t.name for t in schema.tables}
        # Should have a child table for LINE-ITEMS
        assert any("line_item" in n for n in table_names)

    def test_occurs_wide_columns(self):
        root = DataDescription(level=1, name="ORDER-REC")
        f1 = DataDescription(level=5, name="ORDER-ID", picture="9(8)")
        items_group = DataDescription(
            level=5,
            name="ITEM-CODE",
            picture="X(5)",
            occurs=OccursClause(min_times=0, max_times=3, depending_on=None),
        )
        root.children = [f1, items_group]
        items = [root, f1, items_group]

        schema = _build_schema(items, occurs_strategy="wide_columns")
        # Should expand to item_code_1, item_code_2, item_code_3
        table = schema.tables[0]
        col_names = {c.name for c in table.columns}
        assert "item_code_1" in col_names or "item_code_01" in col_names


class TestLevel88Conditions:
    def test_condition_names_become_check(self):
        root = DataDescription(level=1, name="REC")
        status = DataDescription(level=5, name="STATUS-CODE", picture="X")
        active = DataDescription(level=88, name="ACTIVE", value="A")
        inactive = DataDescription(level=88, name="INACTIVE", value="I")
        status.children = [active, inactive]
        root.children = [status]
        items = [root, status, active, inactive]

        schema = _build_schema(items)
        table = schema.tables[0]
        # Check constraints should include STATUS-CODE values
        checks = {c.column: c for c in table.check_constraints}
        assert "status_code" in checks or len(table.check_constraints) > 0
