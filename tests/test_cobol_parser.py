"""Unit tests for the COBOL recursive-descent parser."""

import textwrap
from pathlib import Path

import pytest

from backend.core.parser.cobol_parser import CobolParser
from backend.core.parser.ast_nodes import DataDescription


def _parse_snippet(source: str) -> list[DataDescription]:
    """Helper: parse a DATA DIVISION snippet from a string."""
    parser = CobolParser()
    return parser.parse_string(source)


class TestParsePictureClauses:
    def test_simple_alpha(self):
        items = _parse_snippet(
            "       01  MY-REC.\n"
            "           05  MY-NAME    PIC X(30).\n"
        )
        by_name = {i.name: i for i in items}
        assert "MY-NAME" in by_name
        assert by_name["MY-NAME"].picture == "X(30)"

    def test_numeric_with_sign(self):
        items = _parse_snippet(
            "       01  MY-REC.\n"
            "           05  MY-NUM PIC S9(7)V9(2).\n"
        )
        by_name = {i.name: i for i in items}
        assert by_name["MY-NUM"].picture == "S9(7)V9(2)"

    def test_comp3_usage(self):
        items = _parse_snippet(
            "       01  MY-REC.\n"
            "           05  MY-AMT PIC S9(9)V9(2) COMP-3.\n"
        )
        by_name = {i.name: i for i in items}
        assert by_name["MY-AMT"].usage.upper() in ("COMP-3", "PACKED-DECIMAL")

    def test_comp_usage_binary(self):
        items = _parse_snippet(
            "       01  MY-REC.\n"
            "           05  MY-INT PIC 9(9) COMP.\n"
        )
        by_name = {i.name: i for i in items}
        assert by_name["MY-INT"].usage.upper() in ("COMP", "COMP-4", "BINARY")


class TestHierarchy:
    def test_children_attached(self):
        items = _parse_snippet(
            "       01  PARENT.\n"
            "           05  CHILD-1 PIC X(5).\n"
            "           05  CHILD-2 PIC 9(5).\n"
        )
        by_name = {i.name: i for i in items}
        parent = by_name["PARENT"]
        assert len(parent.children) == 2
        child_names = {c.name for c in parent.children}
        assert child_names == {"CHILD-1", "CHILD-2"}

    def test_nested_group(self):
        items = _parse_snippet(
            "       01  OUTER.\n"
            "           05  MIDDLE.\n"
            "               10  INNER PIC X(5).\n"
        )
        by_name = {i.name: i for i in items}
        assert "MIDDLE" in by_name
        assert len(by_name["MIDDLE"].children) == 1
        assert by_name["MIDDLE"].children[0].name == "INNER"


class TestRedefinices:
    def test_redefines_parsed(self):
        items = _parse_snippet(
            "       01  REC.\n"
            "           05  BASE-FIELD      PIC X(10).\n"
            "           05  ALT-FIELD REDEFINES BASE-FIELD PIC 9(10).\n"
        )
        by_name = {i.name: i for i in items}
        assert by_name["ALT-FIELD"].redefines == "BASE-FIELD"


class TestOccurs:
    def test_occurs_fixed(self):
        items = _parse_snippet(
            "       01  REC.\n"
            "           05  TABLE-ITEM PIC X(5) OCCURS 10 TIMES.\n"
        )
        by_name = {i.name: i for i in items}
        occ = by_name["TABLE-ITEM"].occurs
        assert occ is not None
        assert occ.max_times == 10

    def test_occurs_depending_on(self):
        items = _parse_snippet(
            "       01  REC.\n"
            "           05  COUNT    PIC 9(3).\n"
            "           05  ROWS     PIC X(5) OCCURS 1 TO 10 TIMES\n"
            "                        DEPENDING ON COUNT.\n"
        )
        by_name = {i.name: i for i in items}
        occ = by_name["ROWS"].occurs
        assert occ is not None
        assert occ.min_times == 1
        assert occ.max_times == 10
        assert occ.depending_on == "COUNT"


class TestFiller:
    def test_filler_item_parsed(self):
        items = _parse_snippet(
            "       01  REC.\n"
            "           05  FILLER     PIC X(5).\n"
            "           05  REAL-FIELD PIC 9(3).\n"
        )
        by_name = {i.name: i for i in items}
        assert "FILLER" in by_name

    def test_filler_is_filler_flag(self):
        items = _parse_snippet(
            "       01  REC.\n"
            "           05  FILLER PIC X(5).\n"
        )
        filler_items = [i for i in items if i.is_filler]
        assert len(filler_items) >= 1


class TestLevel88:
    def test_condition_name(self):
        items = _parse_snippet(
            "       01  REC.\n"
            "           05  STATUS-CODE  PIC X.\n"
            "               88  ACTIVE   VALUE 'A'.\n"
            "               88  INACTIVE VALUE 'I'.\n"
        )
        by_name = {i.name: i for i in items}
        assert "ACTIVE" in by_name or any(
            i.level == 88 for i in items
        )
