"""
COBOL Byte Layout Calculator.

After parsing, every DataDescription node needs its exact byte_offset and
byte_length filled in.  This module walks the hierarchy and computes them.

Rules:
  - Group items span the combined bytes of their children.
  - REDEFINES items overlay the same bytes as the base item (length = base length).
  - OCCURS items multiply child bytes by occurrence count.
  - COMP fields are sized by digit count (2/4/8 bytes).
  - COMP-3 fields: ⌈(digits+1)/2⌉ bytes.
  - SYNCHRONIZED may add alignment padding (configurable).
  - FILLER fields consume space but are anonymous.
"""

from __future__ import annotations

import logging
import re
import struct
from dataclasses import dataclass

from backend.core.parser.ast_nodes import DataDescription, OccursClause

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# PIC Analyser — extracts digit counts and category from a PIC string
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PicInfo:
    category: str          # NUMERIC | ALPHANUMERIC | ALPHABETIC | NUMERIC_EDITED | ALPHANUMERIC_EDITED
    total_digits: int      # integer + decimal digits (no V)
    integer_digits: int    # digits before V
    decimal_digits: int    # digits after V (0 if no V)
    signed: bool           # has S
    sign_separate: bool    # sign occupies its own byte (SIGN IS SEPARATE)
    display_length: int    # byte length for DISPLAY usage
    has_p: bool            # PIC 9(3)P(2) — scaling


_EXPAND_RE = re.compile(r"([9XASZVPB0$+\-.,/*])\((\d+)\)")


def expand_picture(pic: str) -> str:
    """Expand parenthesized repeat counts: 9(5) → 99999, X(3) → XXX."""
    while True:
        m = _EXPAND_RE.search(pic)
        if not m:
            break
        pic = pic[:m.start()] + m.group(1) * int(m.group(2)) + pic[m.end():]
    return pic


def analyse_picture(pic: str, sign_separate: bool = False) -> PicInfo:
    """Parse a COBOL PICTURE string and return sizing information."""
    if not pic:
        return PicInfo("ALPHANUMERIC", 0, 0, 0, False, False, 0, False)

    expanded = expand_picture(pic.upper().replace("S", "", 1))  # strip leading S for counting
    signed = pic.upper().startswith("S")

    # Count digits before and after V
    before_v, _, after_v = expanded.partition("V")
    int_digits = sum(1 for c in before_v if c == "9")
    dec_digits = sum(1 for c in after_v if c == "9")
    total_digits = int_digits + dec_digits

    has_p = "P" in expanded
    alpha_chars = sum(1 for c in expanded if c == "X")
    alpha_only = sum(1 for c in expanded if c == "A")

    # Category
    if alpha_chars > 0 or alpha_only > 0:
        if any(c in expanded for c in ("9", "Z", "*", "+", "-", "$", "B", "0", ".", ",")):
            category = "ALPHANUMERIC_EDITED"
        elif alpha_only and not alpha_chars:
            category = "ALPHABETIC"
        else:
            category = "ALPHANUMERIC"
    elif any(c in expanded for c in ("Z", "*", "+", "-", "$", "B", "0", ".", ",")):
        category = "NUMERIC_EDITED"
    else:
        category = "NUMERIC"

    # Display length (byte count for DISPLAY usage)
    if category in ("ALPHANUMERIC", "ALPHABETIC"):
        display_length = len(expanded.replace("V", ""))
    elif category == "NUMERIC":
        display_length = total_digits
        if signed and not sign_separate:
            pass  # sign embedded in last digit (overpunch), no extra byte
        elif signed and sign_separate:
            display_length += 1  # separate sign character
    elif category == "NUMERIC_EDITED":
        display_length = len(expanded.replace("V", ""))
    else:
        display_length = len(expanded.replace("V", ""))

    return PicInfo(
        category=category,
        total_digits=total_digits,
        integer_digits=int_digits,
        decimal_digits=dec_digits,
        signed=signed,
        sign_separate=sign_separate,
        display_length=display_length,
        has_p=has_p,
    )


def compute_byte_length(dd: DataDescription) -> int:
    """
    Compute the physical byte length for one DataDescription entry
    (does not recurse into children — call calculate_layout for that).
    """
    if dd.is_group:
        # Will be computed from children sum
        return 0

    usage = dd.usage.upper() if dd.usage else "DISPLAY"
    pic = dd.picture or ""
    sign_sep = dd.sign is not None and dd.sign.separate if dd.sign else False
    info = analyse_picture(pic, sign_separate=sign_sep)

    if usage in ("DISPLAY", "DISPLAY-1", "NATIONAL"):
        return info.display_length

    if usage in ("COMP-3", "PACKED-DECIMAL", "COMP-6"):
        if info.total_digits == 0:
            return 1
        # COMP-3: ⌈(n+1)/2⌉ where n = total digits
        n = info.total_digits
        # COMP-6 has no sign nibble
        if usage == "COMP-6":
            return (n + 1) // 2
        return (n + 1 + 1) // 2  # +1 for sign nibble

    if usage in ("COMP", "COMP-4", "BINARY"):
        # IBM mainframe: size by digit count
        d = info.total_digits
        if d <= 4:
            return 2
        if d <= 9:
            return 4
        return 8

    if usage == "COMP-5":
        # Native binary: same sizes as COMP
        d = info.total_digits
        if d <= 4:
            return 2
        if d <= 9:
            return 4
        return 8

    if usage == "COMP-1":
        return 4  # IEEE 754 single precision

    if usage == "COMP-2":
        return 8  # IEEE 754 double precision

    if usage == "INDEX":
        return 4

    if usage in ("POINTER", "PROCEDURE-POINTER", "FUNCTION-POINTER"):
        return 8

    # Fallback
    return info.display_length


# ─────────────────────────────────────────────────────────────────────────────
# Layout Calculator
# ─────────────────────────────────────────────────────────────────────────────

class LayoutCalculator:
    """
    Walks a list of DataDescription trees and assigns byte_offset / byte_length
    to every node.

    Usage:
        calc = LayoutCalculator(synchronized=True)
        calc.calculate(root_descs)
    """

    def __init__(self, synchronized: bool = True):
        """
        Args:
            synchronized: Whether to apply SYNC clause alignment padding.
                          Set False for PC COBOL; True for mainframe COBOL.
        """
        self.synchronized = synchronized

    def calculate(self, items: list[DataDescription], base_offset: int = 0) -> int:
        """
        Assign offsets and lengths to all items in the list.
        Returns the total byte length consumed.
        """
        # First pass: identify REDEFINES base items and their sizes
        base_sizes: dict[str, int] = {}
        for item in items:
            if not item.is_redefines:
                size = self._size_item(item, base_offset)
                base_sizes[item.name.upper()] = size

        # Second pass: assign offsets
        offset = base_offset
        for item in items:
            if item.is_redefines:
                # Overlay: same offset as base item
                base_name = item.redefines.upper()
                # Find base offset by scanning siblings
                base_item = next(
                    (i for i in items if i.name.upper() == base_name), None
                )
                if base_item is not None:
                    item.byte_offset = base_item.byte_offset
                else:
                    item.byte_offset = offset  # fallback
                item.byte_length = self._size_item(item, item.byte_offset)
            else:
                item.byte_offset = offset
                item.byte_length = self._size_item(item, offset)
                offset += item.byte_length

        return offset - base_offset

    def _size_item(self, item: DataDescription, base: int) -> int:
        """Compute and assign byte_length for a single item recursively."""
        if item.is_group:
            # Recurse into children
            total = self.calculate(item.children, base_offset=base if not item.occurs else base)
            if item.occurs:
                occ_total = item.occurs.max_times * total
                item.byte_length = occ_total
                return occ_total
            item.byte_length = total
            return total

        # Elementary item
        length = compute_byte_length(item)

        # Apply SYNCHRONIZED padding
        if self.synchronized and item.synchronized:
            align = self._alignment(item)
            if align > 0:
                remainder = base % align
                if remainder:
                    pad = align - remainder
                    length += pad  # add alignment padding

        # Apply OCCURS multiplication
        if item.occurs:
            length = length * item.occurs.max_times

        item.byte_length = length
        return length

    def _alignment(self, item: DataDescription) -> int:
        """Return the alignment boundary for SYNCHRONIZED items."""
        usage = (item.usage or "DISPLAY").upper()
        if usage in ("COMP", "COMP-4", "BINARY", "COMP-5"):
            return compute_byte_length(item)  # natural alignment
        if usage == "COMP-2":
            return 8
        if usage == "COMP-1":
            return 4
        return 0  # no alignment needed
