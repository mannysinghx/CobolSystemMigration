"""
Migration State Tracker.

Tracks the state of each migration run, table-level progress, checksums,
and rejection logs. Provides re-run safety (idempotent restart) via
file/table-level checksums stored in the tool's own PostgreSQL database.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db.models import (
    MigrationRun,
    RejectionLog,
    TableMigrationState,
)

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def file_checksum(path: Path, chunk_size: int = 1 << 20) -> str:
    """Return SHA-256 hex digest of a file (1 MB chunks)."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


class MigrationTracker:
    """
    Persists migration run state to the tool's internal database.

    Usage (inside FastAPI / Celery task):
        tracker = MigrationTracker(db_session)
        run = await tracker.start_run(project_id, "full_load")
        tbl = await tracker.start_table(run.id, "customer", source_path)
        await tracker.finish_table(tbl.id, rows_loaded=50000, rows_rejected=3)
        await tracker.finish_run(run.id)
    """

    def __init__(self, session: AsyncSession):
        self._session = session

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    async def start_run(
        self,
        project_id: uuid.UUID,
        run_type: str = "full_load",
        run_number: int = 1,
        config_snapshot: dict | None = None,
    ) -> MigrationRun:
        """Create a new MigrationRun row and return it."""
        run = MigrationRun(
            project_id=project_id,
            run_type=run_type,
            run_number=run_number,
            status="running",
            started_at=_utcnow(),
            config_snapshot=config_snapshot or {},
        )
        self._session.add(run)
        await self._session.commit()
        await self._session.refresh(run)
        logger.info("Started migration run %s (project=%s)", run.id, project_id)
        return run

    async def finish_run(
        self,
        run_id: uuid.UUID,
        status: str = "completed",
        error_message: str | None = None,
    ) -> None:
        """Mark a run as completed / failed / cancelled."""
        await self._session.execute(
            update(MigrationRun)
            .where(MigrationRun.id == run_id)
            .values(
                status=status,
                finished_at=_utcnow(),
                error_message=error_message,
            )
        )
        await self._session.commit()
        logger.info("Run %s → %s", run_id, status)

    async def get_run(self, run_id: uuid.UUID) -> MigrationRun | None:
        result = await self._session.execute(
            select(MigrationRun).where(MigrationRun.id == run_id)
        )
        return result.scalar_one_or_none()

    async def list_runs(self, project_id: uuid.UUID) -> list[MigrationRun]:
        result = await self._session.execute(
            select(MigrationRun)
            .where(MigrationRun.project_id == project_id)
            .order_by(MigrationRun.started_at.desc())
        )
        return list(result.scalars().all())

    async def next_run_number(self, project_id: uuid.UUID) -> int:
        """Return the next sequential run number for a project."""
        result = await self._session.execute(
            select(MigrationRun.run_number)
            .where(MigrationRun.project_id == project_id)
            .order_by(MigrationRun.run_number.desc())
            .limit(1)
        )
        last = result.scalar_one_or_none()
        return (last or 0) + 1

    # ------------------------------------------------------------------
    # Table-level state
    # ------------------------------------------------------------------

    async def start_table(
        self,
        run_id: uuid.UUID,
        table_name: str,
        source_path: Path | None = None,
    ) -> TableMigrationState:
        """Create a TableMigrationState row for one table in a run."""
        checksum = file_checksum(source_path) if source_path else None
        state = TableMigrationState(
            run_id=run_id,
            table_name=table_name,
            status="running",
            started_at=_utcnow(),
            source_checksum=checksum,
        )
        self._session.add(state)
        await self._session.commit()
        await self._session.refresh(state)
        return state

    async def finish_table(
        self,
        table_state_id: uuid.UUID,
        rows_loaded: int = 0,
        rows_rejected: int = 0,
        rows_extracted: int = 0,
        status: str = "completed",
        error_message: str | None = None,
    ) -> None:
        """Update a TableMigrationState row on completion."""
        await self._session.execute(
            update(TableMigrationState)
            .where(TableMigrationState.id == table_state_id)
            .values(
                status=status,
                finished_at=_utcnow(),
                rows_loaded=rows_loaded,
                rows_rejected=rows_rejected,
                rows_extracted=rows_extracted,
                error_message=error_message,
            )
        )
        await self._session.commit()

    async def get_table_state(
        self, run_id: uuid.UUID, table_name: str
    ) -> TableMigrationState | None:
        result = await self._session.execute(
            select(TableMigrationState).where(
                TableMigrationState.run_id == run_id,
                TableMigrationState.table_name == table_name,
            )
        )
        return result.scalar_one_or_none()

    async def list_table_states(self, run_id: uuid.UUID) -> list[TableMigrationState]:
        result = await self._session.execute(
            select(TableMigrationState)
            .where(TableMigrationState.run_id == run_id)
            .order_by(TableMigrationState.started_at)
        )
        return list(result.scalars().all())

    # ------------------------------------------------------------------
    # Checksum-based re-run safety
    # ------------------------------------------------------------------

    async def already_loaded(
        self, project_id: uuid.UUID, table_name: str, source_path: Path
    ) -> bool:
        """
        Return True if the source file was already successfully loaded for
        this project (same checksum, status=completed).

        Call this before starting a table load to skip unchanged files on re-runs.
        """
        checksum = file_checksum(source_path)
        result = await self._session.execute(
            select(TableMigrationState)
            .join(MigrationRun, MigrationRun.id == TableMigrationState.run_id)
            .where(
                MigrationRun.project_id == project_id,
                TableMigrationState.table_name == table_name,
                TableMigrationState.source_checksum == checksum,
                TableMigrationState.status == "completed",
            )
            .limit(1)
        )
        found = result.scalar_one_or_none()
        if found:
            logger.info(
                "Skipping %s — already loaded (checksum=%s)", table_name, checksum[:12]
            )
        return found is not None

    # ------------------------------------------------------------------
    # Rejection log
    # ------------------------------------------------------------------

    async def log_rejection(
        self,
        run_id: uuid.UUID,
        table_state_id: uuid.UUID | None,
        source_line_num: int | None,
        raw_bytes: bytes | None,
        decoded_partial: dict | None,
        error_type: str | None,
        error_message: str,
    ) -> None:
        """Write one rejected row to the rejection log."""
        entry = RejectionLog(
            run_id=run_id,
            table_state_id=table_state_id,
            source_line_num=source_line_num,
            raw_bytes=raw_bytes,
            decoded_partial=decoded_partial or {},
            error_type=error_type,
            error_message=error_message,
            created_at=_utcnow(),
        )
        self._session.add(entry)
        await self._session.commit()

    async def get_rejections(
        self,
        run_id: uuid.UUID,
        table_state_id: uuid.UUID | None = None,
        limit: int = 200,
    ) -> list[RejectionLog]:
        """Retrieve rejection log entries for a run."""
        query = select(RejectionLog).where(RejectionLog.run_id == run_id)
        if table_state_id:
            query = query.where(RejectionLog.table_state_id == table_state_id)
        result = await self._session.execute(
            query.order_by(RejectionLog.created_at).limit(limit)
        )
        return list(result.scalars().all())

    # ------------------------------------------------------------------
    # Aggregated run summary (for API / SSE events)
    # ------------------------------------------------------------------

    async def run_summary(self, run_id: uuid.UUID) -> dict[str, Any]:
        """Return a summary dict for a run."""
        run = await self.get_run(run_id)
        if not run:
            return {}

        tables = await self.list_table_states(run_id)
        total_loaded = sum(t.rows_loaded or 0 for t in tables)
        total_rejected = sum(t.rows_rejected or 0 for t in tables)
        total_extracted = sum(t.rows_extracted or 0 for t in tables)

        elapsed: float | None = None
        if run.started_at and run.finished_at:
            elapsed = (run.finished_at - run.started_at).total_seconds()
        elif run.started_at:
            elapsed = (_utcnow() - run.started_at).total_seconds()

        return {
            "run_id": str(run.id),
            "project_id": str(run.project_id),
            "run_number": run.run_number,
            "run_type": run.run_type,
            "status": run.status,
            "started_at": run.started_at.isoformat() if run.started_at else None,
            "finished_at": run.finished_at.isoformat() if run.finished_at else None,
            "elapsed_seconds": elapsed,
            "tables_total": len(tables),
            "tables_completed": sum(1 for t in tables if t.status == "completed"),
            "tables_failed": sum(1 for t in tables if t.status == "failed"),
            "rows_extracted": total_extracted,
            "rows_loaded": total_loaded,
            "rows_rejected": total_rejected,
            "error_message": run.error_message,
        }
