"""
COMP / COMP-4 / COMP-5 Binary Integer Decoder.

COMP / COMP-4 (IBM mainframe): big-endian two's complement.
COMP-5 (native binary): machine-native endian (little-endian on x86).
"""

from __future__ import annotations

import struct


class CompDecodeError(ValueError):
    pass


# Format codes: (signed_fmt, unsigned_fmt)
_FORMATS: dict[int, tuple[str, str]] = {
    1: ("b", "B"),
    2: ("h", "H"),
    4: ("i", "I"),
    8: ("q", "Q"),
}


def decode_comp(
    raw_bytes: bytes,
    signed: bool = True,
    big_endian: bool = True,
) -> int:
    """
    Decode a COMP / COMP-4 binary integer field.

    Args:
        raw_bytes:  Raw bytes (must be 1, 2, 4, or 8 bytes long).
        signed:     True if the PIC clause has a leading S (signed).
        big_endian: True for IBM mainframe (big-endian), False for COMP-5 on x86.

    Returns:
        Python int.

    Raises:
        CompDecodeError: If byte length is not 1/2/4/8 or bytes are invalid.
    """
    n = len(raw_bytes)
    if n not in _FORMATS:
        raise CompDecodeError(f"COMP field must be 1, 2, 4, or 8 bytes; got {n}")

    signed_fmt, unsigned_fmt = _FORMATS[n]
    endian = ">" if big_endian else "<"
    fmt = endian + (signed_fmt if signed else unsigned_fmt)
    try:
        (value,) = struct.unpack(fmt, raw_bytes)
    except struct.error as e:
        raise CompDecodeError(f"Failed to unpack COMP bytes: {e}") from e
    return value


def decode_comp5(raw_bytes: bytes, signed: bool = True) -> int:
    """COMP-5 is always native-endian (little-endian on x86/x64)."""
    return decode_comp(raw_bytes, signed=signed, big_endian=False)
