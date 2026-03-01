"""
PostgreSQL Bulk Loader.

Uses psycopg3's COPY FROM STDIN binary protocol for maximum throughput.
Falls back to executemany INSERT for debugging/small loads.

Features:
  - Batched COPY (10 000 rows/batch by default)
  - Automatic batch bisection on failure to isolate bad rows
  - Rejection log for failed rows
  - Idempotent UPSERT mode via INSERT ON CONFLICT
  - Progress callback for live UI updates
"""

from __future__ import annotations

import asyncio
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
    schema_name: str = "public"
    column_names: list[str] = field(default_factory=list)
    mode: str = "truncate_load"  # truncate_load | append | upsert
    pk_columns: list[str] = field(default_factory=list)  # for upsert
    batch_size: int = BATCH_SIZE


class PostgresLoader:
    """
    Async bulk loader for PostgreSQL.

    Usage:
        loader = PostgresLoader(conn_string)
        stats = await loader.load_table(config, record_stream)
    """

    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self._conn = None

    async def _get_conn(self):
        if self._conn is None or self._conn.closed:
            import psycopg
            self._conn = await psycopg.AsyncConnection.connect(self.conn_string)
        return self._conn

    async def prepare_table(self, config: LoadConfig) -> None:
        """Run pre-load operations (TRUNCATE for truncate_load mode)."""
        conn = await self._get_conn()
        qualified = f'"{config.schema_name}"."{config.table_name}"'
        if config.mode == "truncate_load":
            async with conn.cursor() as cur:
                await cur.execute(f"TRUNCATE TABLE {qualified} RESTART IDENTITY CASCADE")
            await conn.commit()
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
                await self._flush_batch(config, buffer, stats)
                buffer.clear()
                if progress_callback:
                    progress_callback(stats.rows_loaded, stats.rows_rejected)

        if buffer:
            await self._flush_batch(config, buffer, stats)
            if progress_callback:
                progress_callback(stats.rows_loaded, stats.rows_rejected)

        return stats

    async def _flush_batch(
        self, config: LoadConfig, batch: list[dict], stats: LoadStats
    ) -> None:
        conn = await self._get_conn()
        qualified = f'"{config.schema_name}"."{config.table_name}"'
        cols = config.column_names

        try:
            if config.mode == "upsert" and config.pk_columns:
                await self._upsert_batch(conn, qualified, cols, config.pk_columns, batch)
            else:
                await self._copy_batch(conn, qualified, cols, batch)
            await conn.commit()
            stats.rows_loaded += len(batch)
            stats.batches_ok += 1
        except Exception as e:
            await conn.rollback()
            logger.warning("Batch of %d rows failed: %s — bisecting", len(batch), e)
            stats.batches_failed += 1
            await self._bisect_batch(config, batch, stats)

    async def _copy_batch(self, conn, qualified: str, cols: list[str], batch: list[dict]) -> None:
        """Use psycopg3 COPY for maximum throughput."""
        import psycopg
        col_list = ", ".join(f'"{c}"' for c in cols)
        async with conn.cursor() as cur:
            async with cur.copy(
                f'COPY {qualified} ({col_list}) FROM STDIN'
            ) as copy:
                for row in batch:
                    values = tuple(self._coerce(row.get(c)) for c in cols)
                    await copy.write_row(values)

    async def _upsert_batch(
        self, conn, qualified: str, cols: list[str], pk_cols: list[str], batch: list[dict]
    ) -> None:
        """INSERT ON CONFLICT DO UPDATE for idempotent loads."""
        col_list = ", ".join(f'"{c}"' for c in cols)
        placeholders = ", ".join(f"%s" for _ in cols)
        conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
        update_set = ", ".join(
            f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in pk_cols
        )
        sql = (
            f"INSERT INTO {qualified} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
        )
        async with conn.cursor() as cur:
            await cur.executemany(sql, [
                tuple(self._coerce(row.get(c)) for c in cols) for row in batch
            ])

    async def _bisect_batch(
        self, config: LoadConfig, batch: list[dict], stats: LoadStats
    ) -> None:
        """Split a failing batch in half and retry. Isolates bad rows."""
        if len(batch) == 1:
            # Single row failed — log as rejection
            stats.rows_rejected += 1
            stats.rejected_rows.append(batch[0])
            logger.error("Rejecting row: %s", batch[0])
            return
        mid = len(batch) // 2
        await self._flush_batch(config, batch[:mid], stats)
        await self._flush_batch(config, batch[mid:], stats)

    def _coerce(self, value: Any) -> Any:
        """Coerce Python types to psycopg3-compatible types."""
        if isinstance(value, Decimal):
            return float(value)
        if isinstance(value, (date, datetime)):
            return value
        return value

    async def close(self) -> None:
        if self._conn and not self._conn.closed:
            await self._conn.close()
