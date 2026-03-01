"""
COMP-3 (Packed Decimal / BCD) Decoder.

IBM packed decimal format:
  - Two decimal digits per byte (high nibble = first digit, low nibble = second)
  - Last nibble is the sign: C or F = positive, D = negative
  - A and E are also treated as positive (non-standard systems)
  - B is treated as negative (non-standard)

Example: PIC S9(7)V99 COMP-3 stores -1234567.89 as:
  Bytes: 01 23 45 67 8D  (9 nibbles of digits, last nibble D = negative)
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation


class Comp3DecodeError(ValueError):
    pass


# Valid sign nibbles
_POSITIVE_SIGNS = frozenset(("C", "F", "A", "E"))
_NEGATIVE_SIGNS = frozenset(("D", "B"))
_ALL_SIGNS = _POSITIVE_SIGNS | _NEGATIVE_SIGNS


def decode_comp3(raw_bytes: bytes, scale: int = 0) -> Decimal | None:
    """
    Decode a COMP-3 / PACKED-DECIMAL field to a Python Decimal.

    Args:
        raw_bytes: Raw bytes from the data file.
        scale:     Number of implied decimal places (the 'n' in V9(n)).
                   e.g., PIC S9(7)V99 → scale=2

    Returns:
        Decimal value, or None if the field is all zeros or empty.

    Raises:
        Comp3DecodeError: If the byte sequence is invalid packed decimal.
    """
    if not raw_bytes:
        return Decimal(0)

    hex_str = raw_bytes.hex().upper()

    # Minimum valid length is 1 byte (1 digit + sign)
    if len(hex_str) < 2:
        raise Comp3DecodeError(f"COMP-3 field too short: {raw_bytes!r}")

    sign_nibble = hex_str[-1]
    digit_nibbles = hex_str[:-1]

    # Validate sign nibble
    if sign_nibble not in _ALL_SIGNS:
        raise Comp3DecodeError(
            f"Invalid COMP-3 sign nibble '{sign_nibble}' in {raw_bytes.hex()!r}"
        )

    # Validate digit nibbles (all must be 0-9)
    for i, c in enumerate(digit_nibbles):
        if c not in "0123456789":
            raise Comp3DecodeError(
                f"Invalid COMP-3 digit nibble '{c}' at position {i} in {raw_bytes.hex()!r}"
            )

    # Build the decimal value
    try:
        integer_value = Decimal(digit_nibbles if digit_nibbles else "0")
    except InvalidOperation as e:
        raise Comp3DecodeError(f"Cannot build Decimal from '{digit_nibbles}'") from e

    # Apply scale (implied decimal point)
    if scale > 0:
        divisor = Decimal(10) ** scale
        integer_value = integer_value / divisor

    # Apply sign
    if sign_nibble in _NEGATIVE_SIGNS:
        integer_value = -integer_value

    return integer_value


def encode_comp3(value: Decimal, num_bytes: int) -> bytes:
    """
    Encode a Decimal value back to COMP-3 bytes.
    Used for writing / round-trip testing.

    Args:
        value:     The decimal value to encode.
        num_bytes: Target byte length (from PIC analysis).
    """
    negative = value < 0
    abs_val = abs(value)

    # Convert to integer (shift by scale) — we work with the raw integer digits
    # The caller is responsible for providing the right scale
    digits = str(abs_val).replace(".", "").replace("-", "").lstrip("0") or "0"

    # Pad to fill the nibble count (num_bytes * 2 - 1 digit nibbles + 1 sign nibble)
    total_nibbles = num_bytes * 2
    digit_nibble_count = total_nibbles - 1
    digits = digits.zfill(digit_nibble_count)

    if len(digits) > digit_nibble_count:
        raise ValueError(
            f"Value {value} has too many digits for {num_bytes}-byte COMP-3 field"
        )

    sign_nibble = "D" if negative else "C"
    hex_str = digits + sign_nibble
    return bytes.fromhex(hex_str)
