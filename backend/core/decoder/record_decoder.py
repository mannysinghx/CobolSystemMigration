"""
Record Decoder.

Given a raw bytes object and a TableIR, decodes every field using the
appropriate decoder (COMP-3, COMP binary, EBCDIC display, dates, etc.)
and returns a dict mapping column names to Python-native values.
"""

from __future__ import annotations

import logging
import struct
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from backend.core.analyzer.ir_nodes import ColumnIR, TableIR
from backend.core.decoder.comp3_decoder import Comp3DecodeError, decode_comp3
from backend.core.decoder.comp_decoder import CompDecodeError, decode_comp, decode_comp5
from backend.core.decoder.date_normalizer import normalize_date
from backend.core.decoder.ebcdic_decoder import decode_display_field

logger = logging.getLogger(__name__)


@dataclass
class DecoderError:
    column: str
    byte_offset: int
    byte_length: int
    raw_hex: str
    message: str


@dataclass
class DecodedRecord:
    values: dict[str, Any]
    errors: list[DecoderError]

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0


class RecordDecoder:
    """
    Decodes a raw bytes record into a Python dict.

    Usage:
        decoder = RecordDecoder(ebcdic_codepage="cp037", big_endian=True)
        result = decoder.decode(raw_bytes, table_ir)
        if result.ok:
            load(result.values)
        else:
            log_rejection(result.errors)
    """

    def __init__(
        self,
        ebcdic_codepage: str = "cp037",
        big_endian: bool = True,          # True for IBM mainframe, False for x86
        sentinel_null_map: dict[str, list[str]] | None = None,
    ):
        self.codepage = ebcdic_codepage
        self.big_endian = big_endian
        self.sentinel_null_map = sentinel_null_map or {}

    def decode(self, raw: bytes, table: TableIR) -> DecodedRecord:
        """Decode all columns of a table from raw record bytes."""
        values: dict[str, Any] = {}
        errors: list[DecoderError] = []

        for col in table.columns:
            if col.is_filler:
                continue
            if col.byte_offset + col.byte_length > len(raw):
                errors.append(DecoderError(
                    column=col.name,
                    byte_offset=col.byte_offset,
                    byte_length=col.byte_length,
                    raw_hex="",
                    message=f"Record too short: need {col.byte_offset + col.byte_length} bytes, "
                            f"got {len(raw)}",
                ))
                values[col.name] = None
                continue

            raw_field = raw[col.byte_offset: col.byte_offset + col.byte_length]

            try:
                value = self._decode_column(raw_field, col)
                values[col.name] = value
            except (Comp3DecodeError, CompDecodeError, Exception) as e:
                errors.append(DecoderError(
                    column=col.name,
                    byte_offset=col.byte_offset,
                    byte_length=col.byte_length,
                    raw_hex=raw_field.hex(),
                    message=str(e),
                ))
                values[col.name] = None

        return DecodedRecord(values=values, errors=errors)

    # ─────────────────────────────────────────────────────────────────────
    # Per-column dispatch
    # ─────────────────────────────────────────────────────────────────────

    def _decode_column(self, raw: bytes, col: ColumnIR) -> Any:
        method = col.decode_as

        if method == "comp3":
            value = decode_comp3(raw, scale=col.sql_type.scale or 0)
        elif method == "comp6":
            # COMP-6: unsigned packed decimal (no sign nibble)
            value = self._decode_comp6(raw, col)
        elif method == "comp":
            signed = col.source_pic.upper().startswith("S") if col.source_pic else True
            value = decode_comp(raw, signed=signed, big_endian=self.big_endian)
        elif method == "comp5":
            signed = col.source_pic.upper().startswith("S") if col.source_pic else True
            value = decode_comp5(raw, signed=signed)
        elif method == "comp1":
            (value,) = struct.unpack(">f" if self.big_endian else "<f", raw)
        elif method == "comp2":
            (value,) = struct.unpack(">d" if self.big_endian else "<d", raw)
        elif method == "index":
            value = int.from_bytes(raw, byteorder="big" if self.big_endian else "little")
        else:
            # DISPLAY
            value = self._decode_display(raw, col)

        # Apply sentinel → NULL mapping
        if value is not None:
            sentinel_key = col.source_cobol_name.upper()
            sentinels = (
                col.sentinel_null_values
                or self.sentinel_null_map.get(sentinel_key, [])
            )
            if str(value) in sentinels or (isinstance(value, str) and value.strip() in sentinels):
                return None

        return value

    def _decode_display(self, raw: bytes, col: ColumnIR) -> Any:
        """Decode a DISPLAY (character) field."""
        if col.ebcdic_decode:
            text = decode_display_field(raw, self.codepage)
        else:
            text = raw.decode("utf-8", errors="replace").rstrip()

        # Check for blank → NULL
        if not text.strip() and col.nullable:
            return None

        # Date normalization
        if col.date_format:
            parsed: date | None = normalize_date(text, col.date_format)
            return parsed

        return text

    def _decode_comp6(self, raw: bytes, col: ColumnIR) -> Decimal:
        """COMP-6: unsigned packed decimal — no sign nibble, all nibbles are digits."""
        hex_str = raw.hex().upper()
        for c in hex_str:
            if c not in "0123456789":
                from backend.core.decoder.comp3_decoder import Comp3DecodeError
                raise Comp3DecodeError(f"Invalid COMP-6 nibble '{c}' in {raw.hex()!r}")
        value = Decimal(hex_str or "0")
        scale = col.sql_type.scale or 0
        if scale > 0:
            value = value / Decimal(10 ** scale)
        return value
