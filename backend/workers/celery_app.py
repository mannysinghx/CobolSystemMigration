"""Celery application factory for CobolShift background tasks."""

from celery import Celery

from backend.config import get_settings


def create_celery() -> Celery:
    settings = get_settings()
    app = Celery(
        "cobolshift",
        broker=settings.redis_url,
        backend=settings.redis_url,
        include=["backend.workers.tasks"],
    )
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,  # one task at a time per worker
        result_expires=3600 * 24,      # keep results for 24 h
    )
    return app


celery_app = create_celery()
