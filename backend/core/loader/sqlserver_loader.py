"""
SQL Server Bulk Loader.

Uses pyodbc fast_executemany for batched inserts.
Falls back to single-row INSERT for debugging/small loads.

Features:
  - Batched executemany (10 000 rows/batch by default)
  - Automatic batch bisection on failure to isolate bad rows
  - Rejection log for failed rows
  - Idempotent UPSERT mode via MERGE statement
  - Progress callback for live UI updates
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, AsyncIterator, Callable

logger = logging.getLogger(__name__)

BATCH_SIZE = 10_000


@dataclass
class LoadStats:
    rows_loaded: int = 0
    rows_rejected: int = 0
    batches_ok: int = 0
    batches_failed: int = 0
    rejected_rows: list[dict] = field(default_factory=list)


@dataclass
class LoadConfig:
    table_name: str
    schema_name: str = "dbo"
    column_names: list[str] = field(default_factory=list)
    mode: str = "truncate_load"  # truncate_load | append | upsert
    pk_columns: list[str] = field(default_factory=list)  # for upsert/MERGE
    batch_size: int = BATCH_SIZE


class SqlServerLoader:
    """
    Bulk loader for SQL Server using pyodbc.

    Usage:
        loader = SqlServerLoader(conn_string)
        stats = await loader.load_table(config, record_stream)

    conn_string examples:
        "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=mydb;Uid=sa;Pwd=pass;"
        "Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=mydb;Trusted_Connection=yes;"
    """

    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self._conn = None

    def _get_conn(self):
        """Return (or create) a synchronous pyodbc connection."""
        if self._conn is None:
            import pyodbc  # type: ignore[import-untyped]
            self._conn = pyodbc.connect(self.conn_string, autocommit=False)
            self._conn.fast_executemany = True
        return self._conn

    def _qualified(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"

    async def prepare_table(self, config: LoadConfig) -> None:
        """Run pre-load operations (TRUNCATE for truncate_load mode)."""
        conn = self._get_conn()
        qualified = self._qualified(config.schema_name, config.table_name)
        if config.mode == "truncate_load":
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {qualified}")
            conn.commit()
            cur.close()
            logger.info("Truncated %s", qualified)

    async def load_table(
        self,
        config: LoadConfig,
        records: AsyncIterator[dict[str, Any]],
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> LoadStats:
        """
        Load all records into the target table.
        progress_callback(rows_loaded, rows_rejected) is called after each batch.
        """
        stats = LoadStats()
        buffer: list[dict] = []

        async for record in records:
            buffer.append(record)
            if len(buffer) >= config.batch_size:
                self._flush_batch(config, buffer, stats)
                buffer.clear()
                if progress_callback:
                    progress_callback(stats.rows_loaded, stats.rows_rejected)

        if buffer:
            self._flush_batch(config, buffer, stats)
            if progress_callback:
                progress_callback(stats.rows_loaded, stats.rows_rejected)

        return stats

    def _flush_batch(
        self, config: LoadConfig, batch: list[dict], stats: LoadStats
    ) -> None:
        conn = self._get_conn()
        qualified = self._qualified(config.schema_name, config.table_name)
        cols = config.column_names

        try:
            if config.mode == "upsert" and config.pk_columns:
                self._merge_batch(conn, qualified, cols, config.pk_columns, batch)
            else:
                self._insert_batch(conn, qualified, cols, batch)
            conn.commit()
            stats.rows_loaded += len(batch)
            stats.batches_ok += 1
        except Exception as e:
            conn.rollback()
            logger.warning("Batch of %d rows failed: %s — bisecting", len(batch), e)
            stats.batches_failed += 1
            self._bisect_batch(config, batch, stats)

    def _insert_batch(
        self, conn, qualified: str, cols: list[str], batch: list[dict]
    ) -> None:
        """Fast executemany INSERT for maximum throughput."""
        col_list = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders})"
        rows = [tuple(self._coerce(row.get(c)) for c in cols) for row in batch]
        cur = conn.cursor()
        cur.executemany(sql, rows)
        cur.close()

    def _merge_batch(
        self,
        conn,
        qualified: str,
        cols: list[str],
        pk_cols: list[str],
        batch: list[dict],
    ) -> None:
        """
        MERGE (upsert) for idempotent loads.

        Generates T-SQL MERGE statement:
            MERGE target AS tgt
            USING (VALUES (...)) AS src (...)
            ON (tgt.pk = src.pk AND ...)
            WHEN MATCHED THEN UPDATE SET ...
            WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
        """
        col_list = ", ".join(f"[{c}]" for c in cols)
        src_col_list = ", ".join(f"src.[{c}]" for c in cols)
        placeholders = ", ".join("?" for _ in cols)

        on_clause = " AND ".join(f"tgt.[{c}] = src.[{c}]" for c in pk_cols)
        non_pk_cols = [c for c in cols if c not in pk_cols]
        update_set = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in non_pk_cols)

        sql = (
            f"MERGE {qualified} AS tgt "
            f"USING (VALUES ({placeholders})) AS src ({col_list}) "
            f"ON ({on_clause}) "
            f"WHEN MATCHED THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({src_col_list});"
        )

        rows = [tuple(self._coerce(row.get(c)) for c in cols) for row in batch]
        cur = conn.cursor()
        cur.executemany(sql, rows)
        cur.close()

    def _bisect_batch(
        self, config: LoadConfig, batch: list[dict], stats: LoadStats
    ) -> None:
        """Split a failing batch in half and retry. Isolates bad rows."""
        if len(batch) == 1:
            stats.rows_rejected += 1
            stats.rejected_rows.append(batch[0])
            logger.error("Rejecting row: %s", batch[0])
            return
        mid = len(batch) // 2
        self._flush_batch(config, batch[:mid], stats)
        self._flush_batch(config, batch[mid:], stats)

    def _coerce(self, value: Any) -> Any:
        """Coerce Python types to pyodbc-compatible types."""
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (date, datetime)):
            return value
        return value

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
