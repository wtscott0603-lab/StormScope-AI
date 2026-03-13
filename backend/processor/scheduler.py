"""Scheduler — v15

Builds and manages two independent APScheduler jobs:

  ingest_job     — real-time ingest + processing cycle (fast, every update_interval_sec)
  history_job    — history aggregation pass (slower, every history_interval_sec)

Key features:
  - max_instances=1 + coalesce=True on both jobs prevents overlap
  - misfire_grace_time allows catchup if a cycle takes slightly longer than expected
  - job-level error isolation: history job failure never disrupts ingest job

Both jobs log their own start/finish/error with structured fields.
"""

from __future__ import annotations

import logging
from typing import Callable, Awaitable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

LOGGER = logging.getLogger(__name__)


def build_scheduler(
    ingest_job: Callable,
    *,
    history_job: Callable | None = None,
    interval_seconds: int,
    history_interval_seconds: int = 120,
) -> AsyncIOScheduler:
    """Build an AsyncIOScheduler with ingest and (optionally) history aggregation jobs."""
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        _safe_wrap(ingest_job, "ingest"),
        trigger=IntervalTrigger(seconds=interval_seconds),
        id="ingest_cycle",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=max(interval_seconds * 2, 120),
        replace_existing=True,
    )

    if history_job is not None:
        scheduler.add_job(
            _safe_wrap(history_job, "history_aggregation"),
            trigger=IntervalTrigger(seconds=history_interval_seconds),
            id="history_aggregation",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=max(history_interval_seconds * 2, 300),
            replace_existing=True,
        )

    return scheduler


def _safe_wrap(job: Callable, name: str) -> Callable:
    async def _wrapped() -> None:
        try:
            await job()
        except Exception:
            LOGGER.exception("Scheduled job %r raised an unhandled exception", name)
    _wrapped.__name__ = f"safe_{name}"
    return _wrapped
