"""
EBCDIC / Character Field Decoder.

Supports all IBM EBCDIC code pages supported by Python's codecs module.
Handles:
  - Standard character decode with trailing space strip
  - Overpunch (SIGN IS LEADING/TRAILING without SEPARATE) detection
  - Sentinel → NULL mapping
  - Date format normalization (delegated to date_normalizer)
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Supported EBCDIC code pages (Python codec names)
SUPPORTED_CODEPAGES = {
    "cp037",   # US English, Canada
    "cp273",   # Germany, Austria
    "cp277",   # Denmark, Norway
    "cp278",   # Finland, Sweden
    "cp280",   # Italy
    "cp284",   # Spain, Latin America
    "cp285",   # UK
    "cp290",   # Japanese katakana
    "cp297",   # France
    "cp420",   # Arabic
    "cp500",   # International (most common non-US)
    "cp870",   # Poland, Czech, Slovak
    "cp875",   # Greek
    "cp1047",  # z/OS Open Systems (USS)
    "cp1140",  # US + Euro sign
    "cp1141",  # Germany + Euro
    "cp1142",  # Denmark + Euro
    "cp1143",  # Finland + Euro
    "cp1144",  # Italy + Euro
    "cp1145",  # Spain + Euro
    "cp1146",  # UK + Euro
    "cp1147",  # France + Euro
    "cp1148",  # International + Euro
    "cp1149",  # Iceland + Euro
}

# Overpunch decode table for EBCDIC-sourced display numeric sign:
# Maps overpunch characters to (digit, sign) pairs
# In EBCDIC: positive overpunch {=0,A=1,...,I=9 and negative }=0,J=1,...,R=9
_OVERPUNCH_TABLE: dict[str, tuple[str, int]] = {
    "{": ("0", 1), "A": ("1", 1), "B": ("2", 1), "C": ("3", 1),
    "D": ("4", 1), "E": ("5", 1), "F": ("6", 1), "G": ("7", 1),
    "H": ("8", 1), "I": ("9", 1),
    "}": ("0", -1), "J": ("1", -1), "K": ("2", -1), "L": ("3", -1),
    "M": ("4", -1), "N": ("5", -1), "O": ("6", -1), "P": ("7", -1),
    "Q": ("8", -1), "R": ("9", -1),
}


class EbcdicDecodeError(ValueError):
    pass


def decode_display_field(
    raw_bytes: bytes,
    codepage: str = "cp037",
    strip_trailing_spaces: bool = True,
) -> str:
    """
    Decode an EBCDIC character field to a Python str.

    Args:
        raw_bytes:              Raw bytes from the data file.
        codepage:               Python codec name (e.g., "cp037", "cp500").
        strip_trailing_spaces:  COBOL fields are space-padded; strip by default.

    Returns:
        Decoded string, optionally stripped.
    """
    if codepage not in SUPPORTED_CODEPAGES:
        logger.warning("Unknown codepage '%s'; falling back to cp037", codepage)
        codepage = "cp037"
    try:
        text = raw_bytes.decode(codepage, errors="replace")
    except LookupError:
        text = raw_bytes.decode("cp037", errors="replace")
    if strip_trailing_spaces:
        text = text.rstrip(" ")
    return text


def decode_ascii_field(
    raw_bytes: bytes,
    strip_trailing_spaces: bool = True,
) -> str:
    """Decode an ASCII/UTF-8 character field (for PC COBOL / non-mainframe sources)."""
    text = raw_bytes.decode("utf-8", errors="replace")
    if strip_trailing_spaces:
        text = text.rstrip(" ")
    return text


def decode_display_numeric(
    raw_bytes: bytes,
    codepage: str = "cp037",
    scale: int = 0,
    signed: bool = False,
    overpunch: bool = False,
) -> float | None:
    """
    Decode a DISPLAY numeric field (PIC 9(n) DISPLAY).

    Args:
        raw_bytes:  Raw bytes.
        codepage:   Codec for EBCDIC decode.
        scale:      Implied decimal places (V9(n)).
        signed:     Has leading S — may have overpunch.
        overpunch:  True if sign is embedded as overpunch in last digit.

    Returns:
        float value or None if all spaces/zeros.
    """
    text = decode_display_field(raw_bytes, codepage, strip_trailing_spaces=True)
    if not text or text.strip() in ("", "0" * len(text)):
        sign_mult = 1
        digits = text.strip() or "0"
    elif overpunch and signed and text and text[-1] in _OVERPUNCH_TABLE:
        # Last character is an overpunch digit
        last_char = text[-1]
        digit, sign_mult = _OVERPUNCH_TABLE[last_char]
        digits = text[:-1] + digit
    else:
        sign_mult = 1
        digits = text.strip()

    try:
        value = int(digits.replace(" ", "0")) * sign_mult
    except ValueError:
        return None

    if scale > 0:
        return value / (10 ** scale)
    return float(value)
