"""
COBOL Date Format Normalizer.

COBOL has no native date type. Dates are stored as numeric/character strings
in many formats. This module detects and converts all common patterns to
Python date/datetime objects (which become SQL DATE / TIMESTAMP columns).
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

# Y2K windowing defaults: 00-30 → 2000-2030, 31-99 → 1931-1999
Y2K_PIVOT = 30


def _window_year(yy: int, pivot: int = Y2K_PIVOT) -> int:
    """Convert 2-digit year to 4-digit using Y2K windowing."""
    return 2000 + yy if yy <= pivot else 1900 + yy


def normalize_date(value: str, fmt: str, y2k_pivot: int = Y2K_PIVOT) -> date | None:
    """
    Convert a COBOL date string to a Python date object.

    Args:
        value:      Raw string from data file (e.g., "20260301").
        fmt:        Format code (see table below).
        y2k_pivot:  Y2K windowing pivot year for 2-digit year formats.

    Supported formats:
        YYYYMMDD    → "20260301"
        YYMMDD      → "260301" (Y2K windowed)
        YYYYDDD     → "2026060" (Julian)
        YYDDD       → "26060" (Julian, Y2K windowed)
        MMDDYYYY    → "03012026"
        DDMMYYYY    → "01032026"
        MMDDYY      → "030126"
        DDMMYY      → "010326"
        YYYY-MM-DD  → "2026-03-01" (ISO)
        MM/DD/YYYY  → "03/01/2026"
        DD/MM/YYYY  → "01/03/2026"
        YYYYMM      → "202603" (month precision — day defaults to 01)
        LILIAN      → integer days since Oct 14 1582 (base-10 string)

    Returns:
        Python date, or None if the value is blank / known sentinel.
    """
    if not value:
        return None
    v = value.strip()
    if not v or v in ("00000000", "99999999", "0000000", "9999999",
                      "000000", "999999", "00/00/0000", "99/99/9999"):
        return None

    try:
        fmt = fmt.upper()
        if fmt == "YYYYMMDD":
            return date(int(v[0:4]), int(v[4:6]), int(v[6:8]))
        if fmt == "YYMMDD":
            yy = int(v[0:2])
            return date(_window_year(yy, y2k_pivot), int(v[2:4]), int(v[4:6]))
        if fmt == "YYYYDDD":
            return date(int(v[0:4]), 1, 1) + timedelta(days=int(v[4:7]) - 1)
        if fmt == "YYDDD":
            yy = int(v[0:2])
            return date(_window_year(yy, y2k_pivot), 1, 1) + timedelta(days=int(v[2:5]) - 1)
        if fmt == "MMDDYYYY":
            return date(int(v[4:8]), int(v[0:2]), int(v[2:4]))
        if fmt == "DDMMYYYY":
            return date(int(v[4:8]), int(v[2:4]), int(v[0:2]))
        if fmt == "MMDDYY":
            yy = int(v[4:6])
            return date(_window_year(yy, y2k_pivot), int(v[0:2]), int(v[2:4]))
        if fmt == "DDMMYY":
            yy = int(v[4:6])
            return date(_window_year(yy, y2k_pivot), int(v[2:4]), int(v[0:2]))
        if fmt in ("YYYY-MM-DD", "ISO"):
            return date.fromisoformat(v[:10])
        if fmt == "MM/DD/YYYY":
            return date(int(v[6:10]), int(v[0:2]), int(v[3:5]))
        if fmt == "DD/MM/YYYY":
            return date(int(v[6:10]), int(v[3:5]), int(v[0:2]))
        if fmt == "YYYYMM":
            return date(int(v[0:4]), int(v[4:6]), 1)
        if fmt == "LILIAN":
            lilian_base = date(1582, 10, 15)
            return lilian_base + timedelta(days=int(v) - 1)
    except (ValueError, IndexError):
        logger.debug("Could not parse date '%s' with format '%s'", value, fmt)
        return None
    return None


def detect_date_format(pic: str) -> str | None:
    """
    Heuristically detect the date format from a COBOL PIC clause.

    Returns a format string or None if no date pattern is detected.
    """
    pic = pic.upper()
    # Strip S prefix and COMP usage hints
    pic = re.sub(r"^S", "", pic)
    # Expand parenthesized repeats for length check
    expanded = re.sub(r"9\((\d+)\)", lambda m: "9" * int(m.group(1)), pic)
    n = len(expanded.replace("V", ""))

    # Pure numeric patterns by length
    if expanded == "9" * 8:
        return "YYYYMMDD"  # most common
    if expanded == "9" * 7:
        return "YYYYDDD"   # Julian
    if expanded == "9" * 6:
        return "YYMMDD"    # 2-digit year
    if expanded == "9" * 5:
        return "YYDDD"     # 2-digit Julian
    return None
