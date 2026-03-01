"""Unit tests for the COBOL layout calculator."""

import pytest

from backend.core.parser.ast_nodes import DataDescription, OccursClause
from backend.core.parser.layout_calculator import (
    LayoutCalculator,
    analyse_picture,
    compute_byte_length,
    expand_picture,
)


class TestExpandPicture:
    def test_simple_x(self):
        assert expand_picture("X") == "X"

    def test_x_repeated(self):
        assert expand_picture("X(5)") == "XXXXX"

    def test_nine_repeated(self):
        assert expand_picture("9(3)") == "999"

    def test_mixed_picture(self):
        result = expand_picture("S9(5)V9(2)")
        assert result == "S99999V99"

    def test_no_parens(self):
        assert expand_picture("999") == "999"


class TestAnalysePicture:
    def test_display_numeric(self):
        info = analyse_picture("9(5)", "DISPLAY")
        assert info.integer_digits == 5
        assert info.decimal_digits == 0
        assert info.category == "numeric"

    def test_display_with_decimal(self):
        info = analyse_picture("S9(7)V9(2)", "DISPLAY")
        assert info.integer_digits == 7
        assert info.decimal_digits == 2
        assert info.signed is True

    def test_alphanumeric(self):
        info = analyse_picture("X(30)", "DISPLAY")
        assert info.category == "alphanumeric"
        assert info.display_length == 30

    def test_comp3_digit_count(self):
        info = analyse_picture("9(7)V9(2)", "COMP-3")
        # 7+2 = 9 digits → 5 bytes
        assert info.integer_digits == 7
        assert info.decimal_digits == 2


class TestComputeByteLength:
    def test_display_alphanumeric(self):
        assert compute_byte_length("X(10)", "DISPLAY") == 10

    def test_display_numeric(self):
        assert compute_byte_length("9(5)", "DISPLAY") == 5

    def test_display_with_sign(self):
        assert compute_byte_length("S9(5)", "DISPLAY") == 5

    def test_comp3_odd_digits(self):
        # 5 digits → ceil((5+1)/2) = 3 bytes
        assert compute_byte_length("9(5)", "COMP-3") == 3

    def test_comp3_even_digits(self):
        # 4 digits → ceil((4+1)/2) = 3 bytes  (sign nibble counts)
        assert compute_byte_length("9(4)", "COMP-3") == 3

    def test_comp3_nine_digits(self):
        # 9 digits → ceil((9+1)/2) = 5 bytes
        assert compute_byte_length("9(9)", "COMP-3") == 5

    def test_comp_small(self):
        # ≤4 digits → 2 bytes
        assert compute_byte_length("9(4)", "COMP") == 2

    def test_comp_medium(self):
        # 5-9 digits → 4 bytes
        assert compute_byte_length("9(9)", "COMP") == 4

    def test_comp_large(self):
        # 10-18 digits → 8 bytes
        assert compute_byte_length("9(18)", "COMP") == 8

    def test_comp1(self):
        assert compute_byte_length("9(7)", "COMP-1") == 4

    def test_comp2(self):
        assert compute_byte_length("9(15)", "COMP-2") == 8


class TestLayoutCalculator:
    def _make_flat_record(self) -> list[DataDescription]:
        """01 CUSTOMER-REC with three fields."""
        root = DataDescription(level=1, name="CUSTOMER-REC")
        f1 = DataDescription(level=5, name="CUST-ID", picture="9(5)")
        f2 = DataDescription(level=5, name="CUST-NAME", picture="X(20)")
        f3 = DataDescription(level=5, name="CUST-BAL", picture="S9(7)V9(2)", usage="COMP-3")
        root.children = [f1, f2, f3]
        return [root, f1, f2, f3]

    def test_offsets(self):
        items = self._make_flat_record()
        calc = LayoutCalculator()
        calc.calculate(items)

        # Find by name
        by_name = {i.name: i for i in items}
        assert by_name["CUST-ID"].byte_offset == 0
        assert by_name["CUST-ID"].byte_length == 5
        assert by_name["CUST-NAME"].byte_offset == 5
        assert by_name["CUST-NAME"].byte_length == 20
        # COMP-3: S9(7)V9(2) = 9 digits → ceil((9+1)/2) = 5 bytes
        assert by_name["CUST-BAL"].byte_offset == 25
        assert by_name["CUST-BAL"].byte_length == 5

    def test_group_length_equals_sum_of_children(self):
        items = self._make_flat_record()
        calc = LayoutCalculator()
        calc.calculate(items)
        by_name = {i.name: i for i in items}
        root = by_name["CUSTOMER-REC"]
        assert root.byte_length == 5 + 20 + 5

    def test_redefines_shares_offset(self):
        root = DataDescription(level=1, name="REC")
        base = DataDescription(level=5, name="BASE-FIELD", picture="X(10)")
        alt = DataDescription(level=5, name="ALT-FIELD", picture="9(10)", redefines="BASE-FIELD")
        root.children = [base, alt]
        items = [root, base, alt]
        calc = LayoutCalculator()
        calc.calculate(items)
        by_name = {i.name: i for i in items}
        assert by_name["BASE-FIELD"].byte_offset == by_name["ALT-FIELD"].byte_offset

    def test_occurs_multiplies_length(self):
        root = DataDescription(level=1, name="REC")
        arr = DataDescription(
            level=5,
            name="ITEMS",
            picture="X(5)",
            occurs=OccursClause(min_times=0, max_times=10, depending_on=None),
        )
        root.children = [arr]
        items = [root, arr]
        calc = LayoutCalculator()
        calc.calculate(items)
        by_name = {i.name: i for i in items}
        assert by_name["ITEMS"].byte_length == 5 * 10
        assert by_name["REC"].byte_length == 5 * 10
