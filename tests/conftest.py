"""Shared pytest fixtures."""

import pytest


@pytest.fixture
def sample_comp3_values():
    """Collection of (raw_bytes, expected_decimal) pairs for COMP-3 testing."""
    from decimal import Decimal
    return [
        (bytes.fromhex("12345C"), Decimal("12345")),
        (bytes.fromhex("09999D"), Decimal("-9999")),
        (bytes.fromhex("0C"), Decimal("0")),
        (bytes.fromhex("00100F"), Decimal("100")),
    ]
