"""
Data Extraction Pipeline.

Streams records from a COBOL source file, decodes each one using the
RecordDecoder, routes multi-record-type files to the correct TableIR,
and yields decoded dicts (or errors) for the loader.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from backend.core.analyzer.ir_nodes import TableIR
from backend.core.decoder.record_decoder import DecoderError, DecodedRecord, RecordDecoder
from backend.core.pipeline.readers.fixed_reader import FixedRecordReader
from backend.core.pipeline.readers.variable_reader import VariableRecordReader

logger = logging.getLogger(__name__)


@dataclass
class ExtractionStats:
    success_count: int = 0
    error_count: int = 0
    skip_count: int = 0
    bytes_read: int = 0


@dataclass
class ExtractionResult:
    line_num: int
    table_name: str
    values: dict | None
    errors: list[DecoderError] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.values is not None and not self.errors


class ExtractionPipeline:
    """
    Streaming extraction pipeline.

    Usage:
        pipeline = ExtractionPipeline(
            source_file=Path("data.vsam"),
            record_format="F",
            record_length=200,
            tables=[customer_table_ir],
            decoder=RecordDecoder(ebcdic_codepage="cp037"),
        )
        for result in pipeline.stream():
            if result.ok:
                loader.buffer(result.table_name, result.values)
            else:
                tracker.log_rejection(result)

        print(pipeline.stats)
    """

    def __init__(
        self,
        source_file: Path,
        record_format: str,          # "F", "V", "VB", "D" (delimiter-separated)
        tables: list[TableIR],
        decoder: RecordDecoder,
        record_length: int = 0,      # required for RECFM=F
        discriminator_offset: int | None = None,   # byte offset of record-type field
        discriminator_length: int | None = None,   # byte length of record-type field
        discriminator_map: dict[str, str] | None = None,  # value → table_name
        skip_header_records: int = 0,
    ):
        self.source_file = source_file
        self.record_format = record_format.upper()
        self.tables = {t.name: t for t in tables}
        # If only one table, all records go there
        self.default_table = tables[0] if tables else None
        self.decoder = decoder
        self.record_length = record_length
        self.discriminator_offset = discriminator_offset
        self.discriminator_length = discriminator_length
        self.discriminator_map = discriminator_map or {}
        self.skip_header_records = skip_header_records
        self.stats = ExtractionStats()

    def stream(self) -> Iterator[ExtractionResult]:
        """Yield ExtractionResult for every record."""
        reader = self._build_reader()

        for line_num, raw in reader.read_records():
            self.stats.bytes_read += len(raw)

            # Route to the correct table
            table = self._route(raw)
            if table is None:
                self.stats.skip_count += 1
                logger.debug("No table for record at line %d — skipping", line_num)
                continue

            # Decode
            decoded: DecodedRecord = self.decoder.decode(raw, table)

            if decoded.ok:
                self.stats.success_count += 1
            else:
                self.stats.error_count += 1

            yield ExtractionResult(
                line_num=line_num,
                table_name=table.name,
                values=decoded.values if decoded.ok else None,
                errors=decoded.errors,
            )

    def _route(self, raw: bytes) -> TableIR | None:
        """Determine which TableIR this record belongs to."""
        if not self.discriminator_map:
            return self.default_table

        if (
            self.discriminator_offset is not None
            and self.discriminator_length is not None
        ):
            field_bytes = raw[
                self.discriminator_offset:
                self.discriminator_offset + self.discriminator_length
            ]
            # Decode as raw ASCII/EBCDIC 1-byte key
            disc_value = field_bytes.decode(self.decoder.codepage, errors="replace").strip()
            table_name = self.discriminator_map.get(disc_value)
            if table_name:
                return self.tables.get(table_name)

        return self.default_table

    def _build_reader(self):
        fmt = self.record_format
        if fmt == "F":
            if not self.record_length:
                raise ValueError("RECFM=F requires record_length to be set")
            return FixedRecordReader(
                self.source_file,
                self.record_length,
                skip_header_records=self.skip_header_records,
            )
        if fmt in ("V", "VB"):
            return VariableRecordReader(self.source_file, blocked=(fmt == "VB"))
        raise ValueError(f"Unsupported record format: {fmt}")
