"""History Aggregator — v15

Runs as a separate scheduled pass (independent of the real-time ingest cycle)
to:
  1. Persist per-scan storm event history rows from already-processed snapshots.
  2. Build / refresh precomputed storm summaries used by the frontend.
  3. Update per-site processor_history_status rows that expose freshness to
     the status API.

This module never touches the ingest pipeline or raw frames.  It only reads
existing snapshot/storm-object rows and writes to the three v15 history tables.
It is safe to run concurrently with the main processing cycle, and safe to re-run
after a restart (all writes are idempotent / upsert-based).
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from datetime import timedelta
from typing import Any

from backend.processor.cache.frame_store import FrameStore
from backend.shared.models import PrecomputedStormSummary
from backend.shared.time import isoformat_utc, utc_now

LOGGER = logging.getLogger(__name__)


class HistoryAggregator:
    """Stateless history aggregation worker.

    Call ``run_for_site(site)`` from the scheduler.  All DB operations are
    idempotent so a crash mid-pass causes no data loss — the next run will
    finish the work.
    """

    def __init__(self, store: FrameStore) -> None:
        self._store = store

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run_for_site(self, site: str) -> dict[str, int]:
        """Run a full aggregation pass for one site.

        Returns a summary dict suitable for logging.
        """
        t0 = utc_now()
        event_rows_written = 0
        summaries_refreshed = 0

        try:
            event_rows_written = await self._persist_event_history(site)
        except Exception:
            LOGGER.exception("Event history persistence failed for site=%s", site)

        try:
            summaries_refreshed = await self._refresh_precomputed_summaries(site)
        except Exception:
            LOGGER.exception("Precomputed summary refresh failed for site=%s", site)

        try:
            await self._update_history_status(site)
        except Exception:
            LOGGER.exception("History status update failed for site=%s", site)

        elapsed = round((utc_now() - t0).total_seconds(), 2)
        summary = {
            "site": site,
            "event_rows_written": event_rows_written,
            "summaries_refreshed": summaries_refreshed,
            "elapsed_sec": elapsed,
        }
        LOGGER.info("History aggregation complete: %s", summary)
        return summary

    # ------------------------------------------------------------------
    # 1. Event history rows — one row per storm per scan
    # ------------------------------------------------------------------

    async def _persist_event_history(self, site: str) -> int:
        """Walk recent storm_snapshots and ensure storm_event_history rows exist.

        Uses INSERT OR IGNORE so re-runs add nothing for already-persisted scans.
        Processes up to the last 4 hours of snapshots to keep the pass fast.
        """
        horizon = isoformat_utc(utc_now() - timedelta(hours=4))
        async with self._store._connection() as connection:
            import aiosqlite
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                """
                SELECT ss.storm_id, ss.site, ss.scan_time,
                       so.event_flags_json, so.lifecycle_state,
                       so.priority_score, so.priority_label,
                       ss.severity_level, ss.primary_threat,
                       ss.threat_scores_json, so.storm_mode,
                       ss.motion_heading_deg, ss.motion_speed_kmh, ss.confidence
                FROM storm_snapshots ss
                LEFT JOIN storm_objects so ON ss.storm_id = so.storm_id
                WHERE ss.site = ? AND ss.scan_time >= ?
                ORDER BY ss.scan_time ASC
                """,
                (site, horizon),
            )
            rows = await cursor.fetchall()

        if not rows:
            return 0

        now = isoformat_utc()
        written = 0
        async with self._store._connection() as connection:
            for row in rows:
                result = await connection.execute(
                    """
                    INSERT OR IGNORE INTO storm_event_history
                        (storm_id, site, scan_time, event_flags_json,
                         lifecycle_state, priority_score, priority_label,
                         severity_level, primary_threat, threat_scores_json,
                         storm_mode, motion_heading_deg, motion_speed_kmh,
                         confidence, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        row["storm_id"],
                        row["site"],
                        row["scan_time"],
                        row["event_flags_json"] or "[]",
                        row["lifecycle_state"],
                        row["priority_score"],
                        row["priority_label"],
                        row["severity_level"],
                        row["primary_threat"],
                        row["threat_scores_json"] or "{}",
                        row["storm_mode"],
                        row["motion_heading_deg"],
                        row["motion_speed_kmh"],
                        row["confidence"],
                        now,
                    ),
                )
                if result.rowcount:
                    written += 1
            await connection.commit()
        return written

    # ------------------------------------------------------------------
    # 2. Precomputed summaries
    # ------------------------------------------------------------------

    async def _refresh_precomputed_summaries(self, site: str, stale_minutes: int = 10) -> int:
        """Rebuild precomputed summaries for storms whose summary is absent or stale."""
        storm_ids = await self._store.list_storm_ids_needing_summary(site, stale_minutes)
        if not storm_ids:
            return 0

        refreshed = 0
        for storm_id in storm_ids:
            try:
                summary = await self._build_summary(storm_id, site)
                if summary is not None:
                    await self._store.upsert_precomputed_summary(summary)
                    refreshed += 1
            except Exception:
                LOGGER.warning("Failed to build summary for storm_id=%s", storm_id, exc_info=True)
        return refreshed

    async def _build_summary(self, storm_id: str, site: str) -> PrecomputedStormSummary | None:
        import json
        snapshots = await self._store.list_storm_snapshots(storm_id, limit=120)
        if not snapshots:
            return None

        scan_count = len(snapshots)

        scan_times = [s.scan_time for s in snapshots if s.scan_time]
        first_seen = min(scan_times) if scan_times else None
        last_seen = max(scan_times) if scan_times else None

        # Peak values
        severity_rank = {"TORNADO_EMERGENCY": 5, "TORNADO": 4, "SEVERE": 3, "MARGINAL": 2, "NONE": 1}
        peak_severity = max(
            (s.severity_level for s in snapshots if s.severity_level),
            key=lambda sv: severity_rank.get(sv, 0),
            default=None,
        )
        peak_ref = max((s.max_reflectivity for s in snapshots if s.max_reflectivity is not None), default=None)
        max_area = max((s.area_km2 for s in snapshots if s.area_km2 is not None), default=None)
        max_speed = max((s.motion_speed_kmh for s in snapshots if s.motion_speed_kmh is not None), default=None)

        # Peak threat scores — max per threat type across all scans
        peak_threats: dict[str, float] = {}
        for snap in snapshots:
            for k, v in (snap.threat_scores or {}).items():
                peak_threats[k] = max(peak_threats.get(k, 0.0), float(v))

        # Dominant storm mode (most common non-None, non-unknown value)
        mode_counts: Counter = Counter()
        for snap in snapshots:
            mode = getattr(snap, "storm_mode", None)
            if mode and mode not in ("unknown", "none"):
                mode_counts[mode] += 1
        dominant_mode = mode_counts.most_common(1)[0][0] if mode_counts else None

        # Event flag summary — occurrence counts across history rows
        flag_counts: Counter = Counter()
        history_rows = await self._store.list_storm_event_history(storm_id, limit=120)
        for hist in history_rows:
            for flag in hist.event_flags:
                if isinstance(flag, dict) and flag.get("flag"):
                    flag_counts[flag["flag"]] += 1
        flag_summary = [
            {"flag": flag, "label": flag.replace("_", " ").title(), "occurrence_count": count}
            for flag, count in flag_counts.most_common(10)
        ]

        # Threat trend — last 24 scans, max_score per scan
        threat_trend = []
        for snap in sorted(snapshots, key=lambda s: s.scan_time or "")[-24:]:
            scores = snap.threat_scores or {}
            max_score = max(scores.values()) if scores else 0.0
            threat_trend.append({
                "scan_time": isoformat_utc(snap.scan_time) if hasattr(snap.scan_time, "isoformat") else str(snap.scan_time),
                "max_score": round(max_score, 3),
                "primary_threat": snap.primary_threat,
            })

        # Motion trend — last 24 scans
        motion_trend = []
        for snap in sorted(snapshots, key=lambda s: s.scan_time or "")[-24:]:
            motion_trend.append({
                "scan_time": isoformat_utc(snap.scan_time) if hasattr(snap.scan_time, "isoformat") else str(snap.scan_time),
                "speed_kmh": snap.motion_speed_kmh,
                "heading_deg": snap.motion_heading_deg,
            })

        # Max priority score
        max_priority = max(
            (hist.priority_score for hist in history_rows if hist.priority_score is not None),
            default=None,
        )

        # Impact location IDs (distinct)
        impacts = await self._store.batch_storm_impacts([storm_id])
        storm_impacts = impacts.get(storm_id, [])
        impact_location_ids = list({i.location_id for i in storm_impacts if i.location_id})

        # Narrative
        storm_obj = await self._store.get_storm_object(storm_id)
        narrative = None
        if storm_obj:
            narrative = (
                f"{scan_count} scans tracked. "
                f"Peak severity: {peak_severity or 'NONE'}. "
                f"Mode: {dominant_mode or 'unknown'}. "
                + (f"Priority peak: {round(max_priority, 2)}. " if max_priority is not None else "")
                + (f"Top flags: {', '.join(f['flag'] for f in flag_summary[:3])}." if flag_summary else "")
            )

        return PrecomputedStormSummary(
            storm_id=storm_id,
            site=site,
            computed_at=utc_now(),
            scan_count=scan_count,
            first_seen=first_seen,
            last_seen=last_seen,
            peak_severity=peak_severity,
            peak_threat_scores=peak_threats,
            peak_reflectivity=peak_ref,
            max_area_km2=max_area,
            max_speed_kmh=max_speed,
            max_priority_score=max_priority,
            dominant_mode=dominant_mode,
            flag_summary=flag_summary,
            threat_trend=threat_trend,
            motion_trend=motion_trend,
            impact_location_ids=impact_location_ids,
            summary_narrative=narrative,
        )

    # ------------------------------------------------------------------
    # 3. Processor history status update
    # ------------------------------------------------------------------

    async def _update_history_status(self, site: str) -> None:
        from backend.shared.models import ProcessorHistoryStatus

        now = utc_now()
        stale_threshold = timedelta(minutes=30)

        last_ingest = await self._store.get_latest_ingest_time(site)
        last_snapshot = await self._store.get_latest_snapshot_time(site)
        snapshot_count_val = await self._store.count_storm_event_history(site)
        precomputed_count_val = await self._store.count_precomputed_summaries(site)
        backlog = await self._store.count_backlog_frames(site)

        is_caught_up = backlog == 0
        history_stale = (
            last_snapshot is None
            or (now - last_snapshot) > stale_threshold
        )

        status = ProcessorHistoryStatus(
            id=None,
            site=site,
            last_ingest_time=last_ingest,
            last_processing_cycle_time=last_snapshot,
            last_history_aggregation_time=now,
            last_retention_time=None,
            snapshot_count=snapshot_count_val,
            event_history_count=snapshot_count_val,
            precomputed_summary_count=precomputed_count_val,
            backlog_frame_count=backlog,
            is_caught_up=is_caught_up,
            history_stale=history_stale,
            updated_at=now,
        )
        await self._store.upsert_processor_history_status(status)
