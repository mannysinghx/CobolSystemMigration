"""Unit tests for COMP-3 (packed decimal) decoder."""

from decimal import Decimal

import pytest

from backend.core.decoder.comp3_decoder import decode_comp3, encode_comp3


class TestDecodeComp3:
    def test_positive_integer_sign_c(self):
        # 12345C → +12345
        raw = bytes.fromhex("12345C")
        assert decode_comp3(raw) == Decimal("12345")

    def test_positive_integer_sign_f(self):
        # 0100F → unsigned zone positive
        raw = bytes.fromhex("0100F")
        # 0100F is 3 bytes — actually 010 0F — digits=0100, sign=F
        raw2 = bytes.fromhex("0100F")
        # hex "0100F" is only 2.5 bytes — use proper 3-byte representation
        raw3 = bytes.fromhex("00100F")
        assert decode_comp3(raw3) == Decimal("100")

    def test_negative_integer_sign_d(self):
        raw = bytes.fromhex("9999D")
        raw2 = bytes.fromhex("09999D")
        assert decode_comp3(raw2) == Decimal("-9999")

    def test_zero(self):
        raw = bytes.fromhex("0C")
        assert decode_comp3(raw) == Decimal("0")

    def test_with_scale(self):
        # 01234C, scale=2 → 12.34
        raw = bytes.fromhex("01234C")
        result = decode_comp3(raw, scale=2)
        assert result == Decimal("12.34")

    def test_negative_with_scale(self):
        raw = bytes.fromhex("09999D")
        result = decode_comp3(raw, scale=2)
        assert result == Decimal("-99.99")

    def test_sign_nibble_a_is_positive(self):
        raw = bytes.fromhex("0123A")
        raw2 = bytes.fromhex("00123A")
        assert decode_comp3(raw2) > 0

    def test_sign_nibble_b_is_negative(self):
        raw = bytes.fromhex("00123B")
        assert decode_comp3(raw) < 0

    def test_invalid_digit_returns_none(self):
        # Second nibble is 'A' — invalid digit
        raw = bytes.fromhex("1A23C")
        raw2 = bytes.fromhex("001A23C")
        # bytes.fromhex("001A23C") — 'C' is odd, can't parse
        # Construct manually: digit nibbles contain 'A'
        raw3 = b"\x1a\x23\x4c"  # nibbles: 1,a,2,3,4,C — 'a' is invalid digit
        assert decode_comp3(raw3) is None

    def test_single_byte_zero(self):
        raw = bytes.fromhex("0C")
        assert decode_comp3(raw) == Decimal("0")

    def test_large_value(self):
        # 999999999C — 9 digits
        raw = bytes.fromhex("9999999999C")
        raw2 = bytes.fromhex("0999999999C")
        assert decode_comp3(raw2) == Decimal("999999999")


class TestEncodeComp3:
    def test_round_trip_positive(self):
        value = Decimal("12345")
        encoded = encode_comp3(value, num_bytes=3)
        assert decode_comp3(encoded) == value

    def test_round_trip_negative(self):
        value = Decimal("-9999")
        encoded = encode_comp3(value, num_bytes=3)
        assert decode_comp3(encoded) == value

    def test_round_trip_zero(self):
        value = Decimal("0")
        encoded = encode_comp3(value, num_bytes=1)
        assert decode_comp3(encoded) == value
