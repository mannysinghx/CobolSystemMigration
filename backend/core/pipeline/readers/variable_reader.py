"""
RECFM=V / RECFM=VB (Variable-Length Record) Reader.

IBM variable-length records are prefixed with a 4-byte Record Descriptor Word (RDW):
  Bytes 0-1: Total record length including the 4-byte RDW (big-endian uint16)
  Bytes 2-3: Reserved (zeros)
  Bytes 4+:  Actual record data

RECFM=VB (variable blocked) adds a Block Descriptor Word (BDW) before each block:
  Bytes 0-1: Total block length including the 4-byte BDW (big-endian uint16)
  Bytes 2-3: Reserved (zeros)
  Then a series of RDW+data records.
"""

from __future__ import annotations

import logging
import struct
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

_RDW_SIZE = 4
_BDW_SIZE = 4


class VariableRecordReader:
    """
    Reads RECFM=V variable-length records from an IBM-format binary file.

    Usage:
        reader = VariableRecordReader(path, blocked=False)
        for line_num, raw in reader.read_records():
            process(raw)
    """

    def __init__(self, path: Path, blocked: bool = False):
        self.path = path
        self.blocked = blocked   # True for RECFM=VB

    def read_records(self) -> Iterator[tuple[int, bytes]]:
        """Yield (1-based record number, raw data bytes) — RDW stripped."""
        if self.blocked:
            yield from self._read_blocked()
        else:
            yield from self._read_unblocked()

    def _read_unblocked(self) -> Iterator[tuple[int, bytes]]:
        """RECFM=V: each record is RDW(4) + data."""
        line_num = 0
        with open(self.path, "rb") as f:
            while True:
                rdw = f.read(_RDW_SIZE)
                if not rdw:
                    break
                if len(rdw) < _RDW_SIZE:
                    logger.warning("Truncated RDW at record %d", line_num + 1)
                    break
                total_len = struct.unpack(">H", rdw[:2])[0]
                data_len = total_len - _RDW_SIZE
                if data_len <= 0:
                    logger.warning("Invalid RDW length %d at record %d", total_len, line_num + 1)
                    continue
                data = f.read(data_len)
                if len(data) < data_len:
                    logger.warning("Truncated record data at record %d", line_num + 1)
                    break
                line_num += 1
                yield line_num, data

    def _read_blocked(self) -> Iterator[tuple[int, bytes]]:
        """RECFM=VB: BDW(4) + [RDW(4) + data ...] repeated."""
        line_num = 0
        with open(self.path, "rb") as f:
            while True:
                bdw = f.read(_BDW_SIZE)
                if not bdw:
                    break
                if len(bdw) < _BDW_SIZE:
                    break
                block_len = struct.unpack(">H", bdw[:2])[0]
                block_data_len = block_len - _BDW_SIZE
                if block_data_len <= 0:
                    continue
                block = f.read(block_data_len)
                if len(block) < block_data_len:
                    logger.warning("Truncated block")
                    break
                # Parse records within the block
                pos = 0
                while pos + _RDW_SIZE <= len(block):
                    total_len = struct.unpack(">H", block[pos: pos + 2])[0]
                    data_len = total_len - _RDW_SIZE
                    if data_len <= 0:
                        pos += _RDW_SIZE
                        continue
                    data = block[pos + _RDW_SIZE: pos + _RDW_SIZE + data_len]
                    line_num += 1
                    yield line_num, data
                    pos += total_len
