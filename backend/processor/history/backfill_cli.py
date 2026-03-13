"""Backfill CLI — v15

Usage examples:

  # Process last 4 hours of missed frames for KGRR
  python -m backend.processor.history.backfill_cli --site KGRR --hours 4

  # Process a specific window
  python -m backend.processor.history.backfill_cli \
      --site KDTX \
      --start 2025-06-01T18:00:00 \
      --end   2025-06-01T22:00:00

  # Rebuild event history table from existing snapshots (post-upgrade recovery)
  python -m backend.processor.history.backfill_cli --site KGRR --rebuild-history

  # Run history aggregation pass only (no frame reprocessing)
  python -m backend.processor.history.backfill_cli --site KGRR --aggregate-only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

from backend.shared.logging import configure_logging


def _parse_dt(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Cannot parse datetime: {s!r}")


async def _run(args: argparse.Namespace) -> int:
    from backend.processor.main import RadarProcessorService
    from backend.processor.history.backfill import (
        run_backfill_for_site,
        rebuild_event_history_from_snapshots,
        startup_catchup,
    )
    from backend.processor.history.aggregator import HistoryAggregator
    from backend.shared.time import utc_now
    from datetime import timedelta

    configure_logging(args.log_level)
    LOGGER = logging.getLogger(__name__)

    service = RadarProcessorService()
    await service.setup()
    site = args.site.upper()

    # --- Rebuild event history from snapshots (upgrade recovery) ---
    if args.rebuild_history:
        written = await rebuild_event_history_from_snapshots(service, site)
        print(f"[backfill] Rebuilt {written} event history rows from snapshots for site={site}")
        return 0

    # --- Aggregate-only pass ---
    if args.aggregate_only:
        aggregator = HistoryAggregator(service.frame_store)
        result = await aggregator.run_for_site(site)
        print(f"[backfill] Aggregation complete: {result}")
        return 0

    # --- Frame backfill ---
    if args.hours:
        end_dt = utc_now()
        start_dt = end_dt - timedelta(hours=args.hours)
    elif args.start and args.end:
        start_dt = args.start
        end_dt = args.end
    else:
        # Default: re-process last 2 hours via startup catchup
        print("[backfill] No window specified — running startup catchup (last 2 hours)")
        processed = await startup_catchup(service, max_hours=2)
        print(f"[backfill] Startup catchup complete: {processed} frames processed")
        return 0

    result = await run_backfill_for_site(service, site, start_dt, end_dt)
    print(f"[backfill] {result}")

    # Always run an aggregation pass after backfill
    aggregator = HistoryAggregator(service.frame_store)
    agg_result = await aggregator.run_for_site(site)
    print(f"[backfill] Post-backfill aggregation: {agg_result}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Radar11 backfill tool — process missed history without the frontend"
    )
    parser.add_argument("--site", required=True, help="4-letter NEXRAD site (e.g. KGRR)")
    parser.add_argument("--hours", type=float, help="Process last N hours of frames")
    parser.add_argument("--start", type=_parse_dt, help="Start datetime (ISO 8601)")
    parser.add_argument("--end", type=_parse_dt, help="End datetime (ISO 8601)")
    parser.add_argument("--rebuild-history", action="store_true",
                        help="Rebuild storm_event_history from existing snapshots")
    parser.add_argument("--aggregate-only", action="store_true",
                        help="Run history aggregation pass only (no frame reprocessing)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    exit_code = asyncio.run(_run(args))
    sys.exit(exit_code or 0)


if __name__ == "__main__":
    main()
