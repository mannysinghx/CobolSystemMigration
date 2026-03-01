"""
Celery tasks for long-running migration operations.

Each task:
  1. Receives a run_id (UUID string)
  2. Publishes progress updates via Redis pub/sub (picked up by SSE endpoint)
  3. Updates MigrationRun / TableMigrationState in the DB
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path
from typing import Any

from celery import Task

from backend.workers.celery_app import celery_app

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_async(coro):
    """Run a coroutine from a synchronous Celery task."""
    return asyncio.get_event_loop().run_until_complete(coro)


def _publish_progress(run_id: str, payload: dict) -> None:
    """Push a progress event to Redis for SSE consumers."""
    import json
    from redis import Redis
    from backend.config import get_settings

    settings = get_settings()
    r = Redis.from_url(settings.redis_url)
    r.publish(f"run:{run_id}", json.dumps(payload))


# ---------------------------------------------------------------------------
# Full-load migration task
# ---------------------------------------------------------------------------

@celery_app.task(bind=True, name="cobolshift.run_migration", max_retries=0)
def run_migration(self: Task, run_id: str, project_id: str, config: dict) -> dict:
    """
    Execute a full migration run.

    config keys:
      source_files: list of {source_file_id, table_name, copybook_id}
      target: {type, conn_string, schema_name}
      loader_mode: truncate_load | append | upsert
      batch_size: int (default 10000)
    """
    from backend.db.connection import AsyncSessionLocal
    from backend.core.state.tracker import MigrationTracker

    logger.info("Starting migration task run_id=%s", run_id)
    run_uuid = uuid.UUID(run_id)

    async def _execute():
        async with AsyncSessionLocal() as session:
            tracker = MigrationTracker(session)
            run = await tracker.get_run(run_uuid)
            if not run:
                raise ValueError(f"Run {run_id} not found")

            try:
                await _process_tables(
                    run_uuid=run_uuid,
                    project_id=uuid.UUID(project_id),
                    config=config,
                    tracker=tracker,
                    task=self,
                )
                await tracker.finish_run(run_uuid, status="completed")
            except Exception as exc:
                logger.exception("Migration run %s failed", run_id)
                await tracker.finish_run(run_uuid, status="failed", error_message=str(exc))
                raise

            summary = await tracker.run_summary(run_uuid)
            _publish_progress(run_id, {"event": "run_complete", **summary})
            return summary

    return _run_async(_execute())


async def _process_tables(
    run_uuid: uuid.UUID,
    project_id: uuid.UUID,
    config: dict,
    tracker: "MigrationTracker",
    task: Task,
) -> None:
    """Process each source file → decode → load pipeline."""
    from backend.db.connection import AsyncSessionLocal
    from backend.db.models import SourceFile, Copybook
    from backend.core.analyzer.schema_analyzer import SchemaAnalyzer, AnalyzerConfig
    from backend.core.decoder.record_decoder import RecordDecoder
    from backend.core.parser.cobol_parser import CobolParser
    from backend.core.parser.layout_calculator import LayoutCalculator
    from backend.core.pipeline.extraction import ExtractionConfig, ExtractionPipeline
    from backend.api.routes.schema import _dict_to_schema

    target_cfg = config.get("target", {})
    target_type = target_cfg.get("type", "postgresql")
    conn_string = target_cfg.get("conn_string", "")
    schema_name = target_cfg.get("schema_name", "public")
    loader_mode = config.get("loader_mode", "truncate_load")
    batch_size = config.get("batch_size", 10_000)

    # Build loader
    if target_type == "postgresql":
        from backend.core.loader.pg_loader import PostgresLoader, LoadConfig

        loader = PostgresLoader(conn_string)
    else:
        from backend.core.loader.sqlserver_loader import SqlServerLoader, LoadConfig

        loader = SqlServerLoader(conn_string)

    source_file_entries: list[dict] = config.get("source_files", [])

    async with AsyncSessionLocal() as session:
        for entry in source_file_entries:
            sf_id = uuid.UUID(entry["source_file_id"])
            table_name = entry.get("table_name", "")
            copybook_id_str = entry.get("copybook_id")

            sf = await session.get(SourceFile, sf_id)
            if not sf:
                logger.warning("SourceFile %s not found — skipping", sf_id)
                continue

            # Re-run safety: skip if already loaded with same checksum
            source_path = Path(sf.file_path)
            if await tracker.already_loaded(project_id, table_name, source_path):
                continue

            tbl_state = await tracker.start_table(run_uuid, table_name, source_path)

            try:
                # Get schema IR (from cached copybook or re-parse)
                schema_dict: dict | None = None
                if copybook_id_str:
                    cb = await session.get(Copybook, uuid.UUID(copybook_id_str))
                    if cb and cb.schema_ir:
                        schema_dict = cb.schema_ir

                if not schema_dict:
                    raise ValueError(f"No schema IR for source file {sf_id}")

                schema_ir = _dict_to_schema(schema_dict)
                # Find the matching table in the schema
                table_ir = next(
                    (t for t in schema_ir.tables if t.name == table_name),
                    schema_ir.tables[0] if schema_ir.tables else None,
                )
                if not table_ir:
                    raise ValueError(f"Table {table_name!r} not in schema IR")

                # Stream extraction
                ext_config = ExtractionConfig(
                    source_path=source_path,
                    record_format=sf.record_format or "F",
                    record_length=sf.record_length or 0,
                    encoding=sf.encoding or "cp037",
                    table_name=table_name,
                )
                pipeline = ExtractionPipeline(ext_config)
                record_decoder = RecordDecoder(encoding=sf.encoding or "cp037")

                # Async generator of decoded dicts
                async def decoded_records():
                    for result in pipeline.stream():
                        if result.error:
                            await tracker.log_rejection(
                                run_id=run_uuid,
                                table_state_id=tbl_state.id,
                                source_line_num=result.line_number,
                                raw_bytes=result.raw_bytes,
                                decoded_partial=None,
                                error_type="extraction_error",
                                error_message=result.error,
                            )
                            continue
                        decoded = record_decoder.decode(result.raw_bytes, table_ir)
                        if not decoded.ok:
                            for err in decoded.errors:
                                await tracker.log_rejection(
                                    run_id=run_uuid,
                                    table_state_id=tbl_state.id,
                                    source_line_num=result.line_number,
                                    raw_bytes=result.raw_bytes,
                                    decoded_partial=decoded.values,
                                    error_type="decode_error",
                                    error_message=err.message,
                                )
                        yield decoded.values

                # Prepare target table
                load_cfg = LoadConfig(
                    table_name=table_name,
                    schema_name=schema_name,
                    column_names=[c.name for c in table_ir.columns],
                    mode=loader_mode,
                    batch_size=batch_size,
                )
                await loader.prepare_table(load_cfg)

                def on_progress(loaded: int, rejected: int) -> None:
                    _publish_progress(
                        str(run_uuid),
                        {
                            "event": "table_progress",
                            "table_name": table_name,
                            "rows_loaded": loaded,
                            "rows_rejected": rejected,
                        },
                    )

                stats = await loader.load_table(load_cfg, decoded_records(), on_progress)

                await tracker.finish_table(
                    tbl_state.id,
                    rows_loaded=stats.rows_loaded,
                    rows_rejected=stats.rows_rejected,
                    status="completed",
                )

            except Exception as exc:
                logger.exception("Table %s failed", table_name)
                await tracker.finish_table(
                    tbl_state.id,
                    status="failed",
                    error_message=str(exc),
                )

    if hasattr(loader, "close"):
        result = loader.close()
        if asyncio.iscoroutine(result):
            await result
