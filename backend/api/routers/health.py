from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter

from backend.api.config import get_settings
from backend.api.dependencies import get_frame_store
from backend.api.schemas.radar import HealthResponse
from backend.shared.time import utc_now


router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
@router.get("/api/v1/health", response_model=HealthResponse)
async def get_health() -> HealthResponse:
    settings = get_settings()
    store = get_frame_store()
    db_ok = True
    latest_run = None
    try:
        await store.initialize()
        latest_run = await store.latest_run()
    except Exception:
        db_ok = False

    processor_last_run = None
    processor_status = "never_run"
    if latest_run:
        processor_last_run = latest_run.finished_at or latest_run.started_at
        if latest_run.status == "error":
            processor_status = "error"
        else:
            threshold = timedelta(seconds=settings.update_interval_sec * 2)
            last_activity = latest_run.finished_at or latest_run.started_at
            processor_status = "stale" if (utc_now() - last_activity) > threshold else "ok"

    status = "ok" if db_ok else "degraded"

    return HealthResponse(
        status=status,
        version=settings.app_version,
        processor_last_run=processor_last_run,
        processor_status=processor_status,
        db_ok=db_ok,
    )
