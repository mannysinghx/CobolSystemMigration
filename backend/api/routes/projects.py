"""
Project management routes.

Handles CRUD for projects, copybook registration/upload,
source-file registration, and upload endpoints.
"""

from __future__ import annotations

import hashlib
import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.models import (
    CopybookResponse,
    ProjectCreate,
    ProjectResponse,
    ProjectUpdate,
    SourceFileResponse,
)
from backend.config import get_settings
from backend.db.connection import get_db
from backend.db.models import Copybook, Project, SourceFile

router = APIRouter(prefix="/projects", tags=["projects"])


def _not_found(resource: str, id: uuid.UUID) -> HTTPException:
    return HTTPException(status_code=404, detail=f"{resource} {id} not found")


# ---------------------------------------------------------------------------
# Project CRUD
# ---------------------------------------------------------------------------

@router.post("", response_model=ProjectResponse, status_code=status.HTTP_201_CREATED)
async def create_project(
    body: ProjectCreate, db: AsyncSession = Depends(get_db)
) -> ProjectResponse:
    project = Project(**body.model_dump())
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return ProjectResponse.model_validate(project)


@router.get("", response_model=list[ProjectResponse])
async def list_projects(db: AsyncSession = Depends(get_db)) -> list[ProjectResponse]:
    result = await db.execute(select(Project).order_by(Project.created_at.desc()))
    return [ProjectResponse.model_validate(p) for p in result.scalars().all()]


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> ProjectResponse:
    project = await db.get(Project, project_id)
    if not project:
        raise _not_found("Project", project_id)
    return ProjectResponse.model_validate(project)


@router.patch("/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: uuid.UUID, body: ProjectUpdate, db: AsyncSession = Depends(get_db)
) -> ProjectResponse:
    project = await db.get(Project, project_id)
    if not project:
        raise _not_found("Project", project_id)
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(project, field, value)
    await db.commit()
    await db.refresh(project)
    return ProjectResponse.model_validate(project)


@router.delete("/{project_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_project(
    project_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> None:
    project = await db.get(Project, project_id)
    if not project:
        raise _not_found("Project", project_id)
    await db.delete(project)
    await db.commit()


# ---------------------------------------------------------------------------
# Copybook upload + registration
# ---------------------------------------------------------------------------

@router.post(
    "/{project_id}/copybooks",
    response_model=CopybookResponse,
    status_code=status.HTTP_201_CREATED,
)
async def upload_copybook(
    project_id: uuid.UUID,
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
) -> CopybookResponse:
    project = await db.get(Project, project_id)
    if not project:
        raise _not_found("Project", project_id)

    settings = get_settings()
    dest_dir = Path(settings.upload_dir) / str(project_id) / "copybooks"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / (file.filename or "copybook.cpy")

    # Stream to disk and compute checksum simultaneously
    h = hashlib.sha256()
    with open(dest_path, "wb") as f:
        while chunk := await file.read(1 << 16):
            f.write(chunk)
            h.update(chunk)

    cb = Copybook(
        project_id=project_id,
        filename=file.filename or dest_path.name,
        file_path=str(dest_path),
        file_checksum=h.hexdigest(),
    )
    db.add(cb)
    await db.commit()
    await db.refresh(cb)
    return CopybookResponse.model_validate(cb)


@router.get("/{project_id}/copybooks", response_model=list[CopybookResponse])
async def list_copybooks(
    project_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> list[CopybookResponse]:
    result = await db.execute(
        select(Copybook)
        .where(Copybook.project_id == project_id)
        .order_by(Copybook.created_at)
    )
    return [CopybookResponse.model_validate(c) for c in result.scalars().all()]


@router.delete(
    "/{project_id}/copybooks/{copybook_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_copybook(
    project_id: uuid.UUID,
    copybook_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
) -> None:
    cb = await db.get(Copybook, copybook_id)
    if not cb or cb.project_id != project_id:
        raise _not_found("Copybook", copybook_id)
    # Remove file from disk (best effort)
    try:
        Path(cb.file_path).unlink(missing_ok=True)
    except Exception:
        pass
    await db.delete(cb)
    await db.commit()


# ---------------------------------------------------------------------------
# Source file upload + registration
# ---------------------------------------------------------------------------

@router.post(
    "/{project_id}/source-files",
    response_model=SourceFileResponse,
    status_code=status.HTTP_201_CREATED,
)
async def upload_source_file(
    project_id: uuid.UUID,
    copybook_id: uuid.UUID | None = None,
    record_format: str = "F",
    record_length: int | None = None,
    encoding: str = "cp037",
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
) -> SourceFileResponse:
    project = await db.get(Project, project_id)
    if not project:
        raise _not_found("Project", project_id)

    settings = get_settings()
    dest_dir = Path(settings.upload_dir) / str(project_id) / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / (file.filename or "data.dat")

    h = hashlib.sha256()
    with open(dest_path, "wb") as f:
        while chunk := await file.read(1 << 16):
            f.write(chunk)
            h.update(chunk)

    sf = SourceFile(
        project_id=project_id,
        copybook_id=copybook_id,
        filename=file.filename or dest_path.name,
        file_path=str(dest_path),
        file_checksum=h.hexdigest(),
        record_format=record_format.upper(),
        record_length=record_length,
        encoding=encoding,
    )
    db.add(sf)
    await db.commit()
    await db.refresh(sf)
    return SourceFileResponse.model_validate(sf)


@router.get("/{project_id}/source-files", response_model=list[SourceFileResponse])
async def list_source_files(
    project_id: uuid.UUID, db: AsyncSession = Depends(get_db)
) -> list[SourceFileResponse]:
    result = await db.execute(
        select(SourceFile)
        .where(SourceFile.project_id == project_id)
        .order_by(SourceFile.created_at)
    )
    return [SourceFileResponse.model_validate(sf) for sf in result.scalars().all()]
