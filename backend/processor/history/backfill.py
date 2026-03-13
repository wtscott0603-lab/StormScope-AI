"""Backfill — v15

Provides two modes:

  startup_catchup(service, max_hours)
    Called automatically on processor startup.  Looks at the most recent
    successfully-processed frame and re-triggers processing for any raw frames
    that are still marked ``pending`` within the last ``max_hours`` window.
    Safe on fresh installs (does nothing if no frames exist).

  run_backfill_for_site(site, start, end, service)
    Manual backfill: processes all raw frames for a site that fall in a
    [start, end] time window and whose status is not already ``processed``.
    Can be run from the CLI or called programmatically.

Both modes use the same processing pipeline as the live cycle and are idempotent
— re-running after a crash will resume from the last unprocessed frame.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from backend.shared.time import utc_now, isoformat_utc

if TYPE_CHECKING:
    from backend.processor.main import RadarProcessorService

LOGGER = logging.getLogger(__name__)


async def startup_catchup(service: "RadarProcessorService", max_hours: int = 2) -> int:
    """On restart, re-process any pending frames from the last ``max_hours``.

    This recovers gracefully from:
    - processor crash mid-cycle (frames stuck in 'processing' status)
    - missed ingest windows (frames ingested but never processed)

    Returns total frames reprocessed.
    """
    LOGGER.info("Startup catchup: scanning last %d hours for pending frames", max_hours)

    cutoff = utc_now() - timedelta(hours=max_hours)
    cutoff_iso = isoformat_utc(cutoff)

    # Reset any frames stuck in 'processing' back to 'pending'
    # (These were being processed when the service crashed.)
    try:
        async with service.frame_store._connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE radar_frames
                SET status = 'pending'
                WHERE status = 'processing'
                  AND created_at >= ?
                """,
                (cutoff_iso,),
            )
            stuck = cursor.rowcount or 0
            await connection.commit()
        if stuck:
            LOGGER.info("Startup catchup: reset %d stuck 'processing' frames to pending", stuck)
    except Exception:
        LOGGER.exception("Startup catchup: failed to reset stuck frames")

    # Now run the normal processing pass — it picks up all pending frames
    try:
        processed = await service.process_pending_frames()
        LOGGER.info("Startup catchup complete: %d frames processed", processed)
        return processed
    except Exception:
        LOGGER.exception("Startup catchup: process_pending_frames failed")
        return 0


async def run_backfill_for_site(
    service: "RadarProcessorService",
    site: str,
    start: datetime,
    end: datetime,
) -> dict[str, int]:
    """Manually backfill all unprocessed frames for a site in [start, end].

    Safe to re-run.  Already-processed frames are skipped.
    Returns a summary dict.
    """
    LOGGER.info(
        "Backfill started: site=%s window=%s to %s",
        site,
        isoformat_utc(start),
        isoformat_utc(end),
    )

    # Find frames in window that are not yet processed
    async with service.frame_store._connection() as connection:
        import aiosqlite
        connection.row_factory = aiosqlite.Row
        cursor = await connection.execute(
            """
            SELECT frame_id FROM radar_frames
            WHERE site = ?
              AND scan_time >= ?
              AND scan_time <= ?
              AND status != 'processed'
            ORDER BY scan_time ASC
            """,
            (site.upper(), isoformat_utc(start), isoformat_utc(end)),
        )
        rows = await cursor.fetchall()

    frame_ids = [row["frame_id"] for row in rows]
    total = len(frame_ids)
    LOGGER.info("Backfill: %d unprocessed frames found for site=%s", total, site)

    if not total:
        return {"site": site, "frames_found": 0, "frames_processed": 0, "errors": 0}

    # Reset them all to pending so process_pending_frames picks them up
    async with service.frame_store._connection() as connection:
        placeholders = ",".join("?" * len(frame_ids))
        await connection.execute(
            f"UPDATE radar_frames SET status = 'pending' WHERE frame_id IN ({placeholders})",
            frame_ids,
        )
        await connection.commit()

    # Process in normal pipeline
    processed = await service.process_pending_frames()
    LOGGER.info("Backfill complete: site=%s processed=%d/%d", site, processed, total)
    return {"site": site, "frames_found": total, "frames_processed": processed, "errors": total - processed}


async def rebuild_event_history_from_snapshots(
    service: "RadarProcessorService",
    site: str,
) -> int:
    """Re-derive storm_event_history rows from existing storm_snapshots.

    Useful when the event history table is empty due to an upgrade from pre-v15.
    This is a one-time recovery operation, safe to run multiple times (idempotent).
    """
    from backend.processor.history.aggregator import HistoryAggregator
    LOGGER.info("Rebuilding event history from snapshots: site=%s", site)
    aggregator = HistoryAggregator(service.frame_store)
    written = await aggregator._persist_event_history(site)
    LOGGER.info("Event history rebuild complete: site=%s rows_written=%d", site, written)
    return written
