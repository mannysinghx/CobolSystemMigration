"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.config import get_settings
from backend.db.connection import init_db
from backend.api.routes import health, migrations, projects, schema, validation

settings = get_settings()

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("CobolShift starting up…")
    await init_db()
    yield
    logger.info("CobolShift shutting down…")


app = FastAPI(
    title="CobolShift",
    description="End-to-end COBOL → SQL Server / PostgreSQL migration platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers — each module owns its own prefix (e.g. /health, /projects …)
app.include_router(health.router)
app.include_router(projects.router)
app.include_router(migrations.router)
app.include_router(schema.router)
app.include_router(validation.router)
