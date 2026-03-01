"""Health check endpoint."""

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.api.models import HealthResponse
from backend.db.connection import get_db

router = APIRouter(prefix="/health", tags=["health"])

VERSION = "0.1.0"


@router.get("", response_model=HealthResponse)
async def health_check(db: AsyncSession = Depends(get_db)) -> HealthResponse:
    db_ok = False
    try:
        await db.execute(text("SELECT 1"))
        db_ok = True
    except Exception:
        pass

    return HealthResponse(
        status="ok" if db_ok else "degraded",
        version=VERSION,
        db_ok=db_ok,
    )
