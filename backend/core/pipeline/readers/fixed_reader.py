"""
RECFM=F (Fixed-length Record) Reader.

Reads a flat binary file where every record is exactly `record_length` bytes.
Streams records one at a time — never loads the whole file into memory.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)


class FixedRecordReader:
    """
    Reads RECFM=F (fixed-length) data files.

    Usage:
        reader = FixedRecordReader(path, record_length=150)
        for line_num, raw in reader.read_records():
            process(raw)
    """

    def __init__(self, path: Path, record_length: int, skip_header_records: int = 0):
        self.path = path
        self.record_length = record_length
        self.skip_header_records = skip_header_records

    def read_records(self) -> Iterator[tuple[int, bytes]]:
        """Yield (1-based line number, raw bytes) for every record."""
        with open(self.path, "rb") as f:
            line_num = 0
            while True:
                chunk = f.read(self.record_length)
                if not chunk:
                    break
                if len(chunk) < self.record_length:
                    logger.warning(
                        "Truncated record at line %d: expected %d bytes, got %d",
                        line_num + 1,
                        self.record_length,
                        len(chunk),
                    )
                    break
                line_num += 1
                if line_num <= self.skip_header_records:
                    continue
                yield line_num, chunk

    def count_records(self) -> int:
        """Return the total number of records in the file."""
        size = self.path.stat().st_size
        return size // self.record_length
