"""Unit tests for date normalization."""

from datetime import date

import pytest

from backend.core.decoder.date_normalizer import normalize_date, detect_date_format


class TestNormalizeDate:
    def test_yyyymmdd(self):
        assert normalize_date("20240315", "YYYYMMDD") == date(2024, 3, 15)

    def test_yymmdd_y2k_before_pivot(self):
        # 28 < 30 → 2028
        assert normalize_date("280315", "YYMMDD") == date(2028, 3, 15)

    def test_yymmdd_y2k_after_pivot(self):
        # 45 > 30 → 1945
        assert normalize_date("450315", "YYMMDD") == date(1945, 3, 15)

    def test_yyyyddd_julian(self):
        assert normalize_date("2024075", "YYYYDDD") == date(2024, 3, 15)

    def test_yyddd_julian_y2k(self):
        # Day 075 of 2024 (pivot: 28 < 30 → 2028)
        result = normalize_date("28075", "YYDDD")
        assert result == date(2028, 3, 15)

    def test_mmddyyyy(self):
        assert normalize_date("03152024", "MMDDYYYY") == date(2024, 3, 15)

    def test_ddmmyyyy(self):
        assert normalize_date("15032024", "DDMMYYYY") == date(2024, 3, 15)

    def test_iso_hyphen(self):
        assert normalize_date("2024-03-15", "YYYY-MM-DD") == date(2024, 3, 15)

    def test_slash_mmddyyyy(self):
        assert normalize_date("03/15/2024", "MM/DD/YYYY") == date(2024, 3, 15)

    def test_yyyymm_day_1(self):
        result = normalize_date("202403", "YYYYMM")
        assert result == date(2024, 3, 1)

    def test_empty_string_returns_none(self):
        assert normalize_date("", "YYYYMMDD") is None

    def test_zeros_date_returns_none(self):
        assert normalize_date("00000000", "YYYYMMDD") is None

    def test_spaces_date_returns_none(self):
        assert normalize_date("        ", "YYYYMMDD") is None

    def test_invalid_date_returns_none(self):
        assert normalize_date("99999999", "YYYYMMDD") is None

    def test_custom_y2k_pivot(self):
        # With pivot=50: year 49 → 2049, year 51 → 1951
        assert normalize_date("490315", "YYMMDD", y2k_pivot=50) == date(2049, 3, 15)
        assert normalize_date("510315", "YYMMDD", y2k_pivot=50) == date(1951, 3, 15)


class TestDetectDateFormat:
    def test_pic_9_8_detects_yyyymmdd(self):
        # PIC 9(8) → YYYYMMDD
        result = detect_date_format("9(8)")
        assert result in ("YYYYMMDD", "MMDDYYYY", "DDMMYYYY")

    def test_pic_9_6_detects_yymmdd(self):
        result = detect_date_format("9(6)")
        assert result in ("YYMMDD", "MMDDYY", "DDMMYY")

    def test_non_date_pic_returns_none(self):
        assert detect_date_format("9(5)") is None
        assert detect_date_format("X(30)") is None
