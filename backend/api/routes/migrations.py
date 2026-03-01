"""
Migration run routes.

- POST /migrations          → start a run (dispatches Celery task)
- GET  /migrations          → list runs for a project
- GET  /migrations/{run_id} → run detail + table states
- GET  /migrations/{run_id}/stream → SSE live progress
- GET  /migrations/{run_id}/rejections → rejected rows
- DELETE /migrations/{run_id} → cancel a running task
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import AsyncGenerator

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.models import (
    MigrationRunResponse,
    RejectionLogResponse,
    RunSummaryResponse,
    StartMigrationRequest,
    TableStateResponse,
)
from backend.core.state.tracker import MigrationTracker
from backend.db.connection import get_db
from backend.db.models import MigrationRun, Project

router = APIRouter(prefix="/migrations", tags=["migrations"])


async def _get_tracker(db: AsyncSession = Depends(get_db)) -> MigrationTracker:
    return MigrationTracker(db)


# ---------------------------------------------------------------------------
# Start migration
# ---------------------------------------------------------------------------

@router.post("", response_model=MigrationRunResponse, status_code=201)
async def start_migration(
    body: StartMigrationRequest,
    db: AsyncSession = Depends(get_db),
) -> MigrationRunResponse:
    project = await db.get(Project, body.project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project {body.project_id} not found")

    tracker = MigrationTracker(db)
    run_number = await tracker.next_run_number(body.project_id)
    run = await tracker.start_run(
        project_id=body.project_id,
        run_type=body.run_type,
        run_number=run_number,
        config_snapshot=body.table_overrides,
    )

    # Dispatch Celery task
    from backend.workers.tasks import run_migration

    run_migration.delay(
        run_id=str(run.id),
        project_id=str(body.project_id),
        config={
            "source_files": [],  # caller populates via PUT /migrations/{id}/config
            "target": project.config_json.get("target", {}),
            "loader_mode": project.config_json.get("loader_mode", "truncate_load"),
            "batch_size": project.config_json.get("batch_size", 10_000),
            **body.table_overrides,
        },
    )

    return MigrationRunResponse.model_validate(run)


# ---------------------------------------------------------------------------
# List / get runs
# ---------------------------------------------------------------------------

@router.get("")
async def list_migrations(
    project_id: uuid.UUID = Query(...),
    db: AsyncSession = Depends(get_db),
) -> list[MigrationRunResponse]:
    tracker = MigrationTracker(db)
    runs = await tracker.list_runs(project_id)
    return [MigrationRunResponse.model_validate(r) for r in runs]


@router.get("/{run_id}", response_model=RunSummaryResponse)
async def get_migration(
    run_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> RunSummaryResponse:
    tracker = MigrationTracker(db)
    summary = await tracker.run_summary(run_id)
    if not summary:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    return RunSummaryResponse(**summary)


@router.get("/{run_id}/tables", response_model=list[TableStateResponse])
async def get_table_states(
    run_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> list[TableStateResponse]:
    tracker = MigrationTracker(db)
    run = await tracker.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    states = await tracker.list_table_states(run_id)
    return [TableStateResponse.model_validate(s) for s in states]


# ---------------------------------------------------------------------------
# SSE progress stream
# ---------------------------------------------------------------------------

@router.get("/{run_id}/stream")
async def stream_progress(run_id: uuid.UUID) -> StreamingResponse:
    """
    Server-Sent Events endpoint.
    Subscribes to Redis pub/sub channel `run:<run_id>` and forwards
    messages to the client as SSE events.
    """
    from backend.config import get_settings
    settings = get_settings()

    async def event_generator() -> AsyncGenerator[str, None]:
        import redis.asyncio as aioredis

        r = aioredis.from_url(settings.redis_url)
        pubsub = r.pubsub()
        await pubsub.subscribe(f"run:{run_id}")

        try:
            async for message in pubsub.listen():
                if message["type"] == "message":
                    data = message["data"]
                    if isinstance(data, bytes):
                        data = data.decode()
                    yield f"data: {data}\n\n"
                    # Stop when the run completes
                    try:
                        payload = json.loads(data)
                        if payload.get("event") == "run_complete":
                            break
                    except json.JSONDecodeError:
                        pass
                await asyncio.sleep(0)
        finally:
            await pubsub.unsubscribe(f"run:{run_id}")
            await r.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------

@router.get("/{run_id}/rejections", response_model=list[RejectionLogResponse])
async def get_rejections(
    run_id: uuid.UUID,
    table_state_id: uuid.UUID | None = Query(None),
    limit: int = Query(200, le=1000),
    db: AsyncSession = Depends(get_db),
) -> list[RejectionLogResponse]:
    tracker = MigrationTracker(db)
    entries = await tracker.get_rejections(run_id, table_state_id=table_state_id, limit=limit)
    return [RejectionLogResponse.model_validate(e) for e in entries]


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------

@router.delete("/{run_id}", status_code=204)
async def cancel_migration(
    run_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> None:
    tracker = MigrationTracker(db)
    run = await tracker.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")
    if run.status not in ("running", "pending"):
        raise HTTPException(
            status_code=409, detail=f"Run is already {run.status}"
        )
    # Revoke the Celery task (best-effort; task checks its status itself)
    from backend.workers.celery_app import celery_app
    celery_app.control.revoke(str(run_id), terminate=True)
    await tracker.finish_run(run_id, status="cancelled")
