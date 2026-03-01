"""Unit tests for the DDL generator."""

import pytest

from backend.core.analyzer.ir_nodes import (
    ColumnIR,
    SchemaIR,
    SQLTypeIR,
    TableIR,
)
from backend.core.generator.ddl_generator import DDLGenerator


def _simple_schema() -> SchemaIR:
    cols = [
        ColumnIR(
            name="customer_id",
            source_cobol_name="CUSTOMER-ID",
            source_pic="9(5)",
            source_usage="DISPLAY",
            sql_type=SQLTypeIR(base_type="INTEGER"),
            nullable=False,
        ),
        ColumnIR(
            name="customer_name",
            source_cobol_name="CUSTOMER-NAME",
            source_pic="X(30)",
            source_usage="DISPLAY",
            sql_type=SQLTypeIR(base_type="VARCHAR", max_length=30),
        ),
        ColumnIR(
            name="balance",
            source_cobol_name="BALANCE",
            source_pic="S9(7)V9(2)",
            source_usage="COMP-3",
            sql_type=SQLTypeIR(base_type="NUMERIC", precision=9, scale=2),
        ),
    ]
    table = TableIR(
        name="customer",
        source_cobol_name="CUSTOMER-REC",
        columns=cols,
        primary_key=["customer_id"],
        check_constraints=[],
        table_type="main",
        parent_table=None,
        discriminator_column=None,
    )
    return SchemaIR(tables=[table], relationships=[], enums=[])


class TestPostgresqlDDL:
    def setup_method(self):
        self.gen = DDLGenerator(dialect="postgresql", schema_name="public")
        self.schema = _simple_schema()
        self.output = self.gen.generate(self.schema)

    def test_creates_table(self):
        assert "CREATE TABLE" in self.output.sql

    def test_table_name_present(self):
        assert "customer" in self.output.sql

    def test_column_names_present(self):
        assert "customer_id" in self.output.sql
        assert "customer_name" in self.output.sql
        assert "balance" in self.output.sql

    def test_primary_key(self):
        assert "PRIMARY KEY" in self.output.sql.upper()

    def test_not_null_on_pk(self):
        assert "NOT NULL" in self.output.sql.upper()

    def test_varchar_type(self):
        assert "VARCHAR(30)" in self.output.sql.upper() or "CHARACTER VARYING" in self.output.sql.upper()

    def test_numeric_type_with_scale(self):
        assert "NUMERIC(9, 2)" in self.output.sql.upper() or "DECIMAL(9,2)" in self.output.sql.upper()

    def test_double_quoted_identifiers(self):
        # PostgreSQL uses double quotes
        assert '"customer"' in self.output.sql or '"public"."customer"' in self.output.sql

    def test_table_names_list(self):
        assert "customer" in self.output.table_names


class TestSqlServerDDL:
    def setup_method(self):
        self.gen = DDLGenerator(dialect="sqlserver", schema_name="dbo")
        self.schema = _simple_schema()
        self.output = self.gen.generate(self.schema)

    def test_creates_table(self):
        assert "CREATE TABLE" in self.output.sql

    def test_bracket_quoted_identifiers(self):
        assert "[customer]" in self.output.sql or "[dbo].[customer]" in self.output.sql

    def test_nvarchar_for_char_fields(self):
        # SQL Server should use NVARCHAR or VARCHAR
        assert "VARCHAR" in self.output.sql.upper()
