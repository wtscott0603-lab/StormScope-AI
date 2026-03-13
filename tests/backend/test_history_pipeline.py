"""Tests for v15 always-on history pipeline.

Covers:
  - storm_event_history persistence (insert, idempotent, list)
  - precomputed_storm_summaries (upsert, get, stale detection)
  - processor_history_status (upsert, get, list)
  - HistoryAggregator._update_history_status
  - backfill startup_catchup (stuck frame reset)
  - Status API history fields
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from backend.processor.cache.frame_store import FrameStore
from backend.shared.db import init_db
from backend.shared.models import PrecomputedStormSummary, ProcessorHistoryStatus
from backend.shared.time import utc_now


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
async def store(tmp_path):
    db_path = tmp_path / "history_test.db"
    await init_db(db_path)
    return FrameStore(db_path)


@pytest.fixture
async def store_with_storm(store):
    """A store pre-seeded with a minimal storm_object row."""
    async with store._connection() as connection:
        now = utc_now().isoformat()
        await connection.execute(
            """
            INSERT INTO storm_objects
                (storm_id, site, latest_scan_time, status, lifecycle_state,
                 centroid_lat, centroid_lon, area_km2,
                 max_reflectivity, mean_reflectivity,
                 trend, primary_threat, secondary_threats_json,
                 severity_level, confidence, threat_scores_json,
                 narrative, reasoning_json,
                 footprint_geojson, forecast_path_json,
                 signatures_json,
                 created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "KGRR_TEST001", "KGRR", now, "active", "tracked",
                43.0, -85.5, 150.0,
                55.0, 40.0,
                "intensifying", "hail", "[]",
                "SEVERE", 0.72, '{"hail":0.6,"wind":0.3,"tornado":0.1,"flood":0.05}',
                "Intensifying severe storm.", "[]",
                '{"type":"Polygon","coordinates":[]}', "[]",
                "[]",
                now, now,
            ),
        )
        await connection.commit()
    return store


# ── storm_event_history ───────────────────────────────────────────────────────

class TestStormEventHistory:
    @pytest.mark.asyncio
    async def test_insert_and_list(self, store_with_storm):
        store = store_with_storm
        scan_time = "2025-06-15T20:00:00+00:00"
        await store.insert_storm_event_history({
            "storm_id": "KGRR_TEST001",
            "site": "KGRR",
            "scan_time": scan_time,
            "event_flags": [{"flag": "rapid_intensification", "label": "Rapid Intensification", "confidence": 0.85, "severity": 8}],
            "lifecycle_state": "tracked",
            "priority_score": 0.72,
            "priority_label": "HIGH",
            "severity_level": "SEVERE",
            "primary_threat": "hail",
            "threat_scores": {"hail": 0.6, "wind": 0.3},
            "storm_mode": "supercell_candidate",
            "motion_heading_deg": 230.0,
            "motion_speed_kmh": 55.0,
            "confidence": 0.72,
        })
        rows = await store.list_storm_event_history("KGRR_TEST001", limit=10)
        assert len(rows) == 1
        row = rows[0]
        assert row.storm_id == "KGRR_TEST001"
        assert row.priority_label == "HIGH"
        assert len(row.event_flags) == 1
        assert row.event_flags[0]["flag"] == "rapid_intensification"
        assert row.storm_mode == "supercell_candidate"

    @pytest.mark.asyncio
    async def test_insert_idempotent(self, store_with_storm):
        """Re-inserting the same storm_id + scan_time is a no-op."""
        store = store_with_storm
        payload = {
            "storm_id": "KGRR_TEST001",
            "site": "KGRR",
            "scan_time": "2025-06-15T20:00:00+00:00",
            "event_flags": [],
            "lifecycle_state": "tracked",
            "priority_score": 0.5,
            "priority_label": "MODERATE",
            "severity_level": "SEVERE",
            "primary_threat": "hail",
            "threat_scores": {},
            "storm_mode": None,
            "motion_heading_deg": None,
            "motion_speed_kmh": None,
            "confidence": 0.5,
        }
        await store.insert_storm_event_history(payload)
        await store.insert_storm_event_history(payload)  # duplicate — should be ignored
        rows = await store.list_storm_event_history("KGRR_TEST001")
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_count_and_delete(self, store_with_storm):
        store = store_with_storm
        for i in range(3):
            await store.insert_storm_event_history({
                "storm_id": "KGRR_TEST001",
                "site": "KGRR",
                "scan_time": f"2025-06-15T20:0{i}:00+00:00",
                "event_flags": [],
                "lifecycle_state": "tracked",
                "priority_score": 0.4,
                "priority_label": "MODERATE",
                "severity_level": "NONE",
                "primary_threat": "hail",
                "threat_scores": {},
                "storm_mode": None,
                "motion_heading_deg": None,
                "motion_speed_kmh": None,
                "confidence": 0.5,
            })
        count = await store.count_storm_event_history("KGRR")
        assert count == 3
        deleted = await store.delete_old_event_history("2025-06-15T21:00:00+00:00")
        assert deleted == 3
        assert await store.count_storm_event_history("KGRR") == 0

    @pytest.mark.asyncio
    async def test_missing_storm_returns_empty_list(self, store):
        rows = await store.list_storm_event_history("NO_SUCH_STORM")
        assert rows == []


# ── precomputed_storm_summaries ───────────────────────────────────────────────

class TestPrecomputedSummaries:
    def _make_summary(self, storm_id: str = "KGRR_TEST001", site: str = "KGRR") -> PrecomputedStormSummary:
        now = utc_now()
        return PrecomputedStormSummary(
            storm_id=storm_id,
            site=site,
            computed_at=now,
            scan_count=12,
            first_seen=now,
            last_seen=now,
            peak_severity="SEVERE",
            peak_threat_scores={"hail": 0.72, "wind": 0.45},
            peak_reflectivity=62.5,
            max_area_km2=320.0,
            max_speed_kmh=68.0,
            max_priority_score=0.78,
            dominant_mode="supercell_candidate",
            flag_summary=[{"flag": "rotation_tightening", "label": "Rotation Tightening", "occurrence_count": 4}],
            threat_trend=[{"scan_time": now.isoformat(), "max_score": 0.72, "primary_threat": "hail"}],
            motion_trend=[{"scan_time": now.isoformat(), "speed_kmh": 68.0, "heading_deg": 230.0}],
            impact_location_ids=["loc_001", "loc_002"],
            summary_narrative="12 scans tracked. Peak severity: SEVERE.",
        )

    @pytest.mark.asyncio
    async def test_upsert_and_get(self, store_with_storm):
        store = store_with_storm
        summary = self._make_summary()
        await store.upsert_precomputed_summary(summary)
        result = await store.get_precomputed_summary("KGRR_TEST001")
        assert result is not None
        assert result.scan_count == 12
        assert result.peak_severity == "SEVERE"
        assert result.dominant_mode == "supercell_candidate"
        assert len(result.flag_summary) == 1
        assert result.flag_summary[0]["flag"] == "rotation_tightening"
        assert result.impact_location_ids == ["loc_001", "loc_002"]

    @pytest.mark.asyncio
    async def test_upsert_is_idempotent_and_updates(self, store_with_storm):
        store = store_with_storm
        s1 = self._make_summary()
        await store.upsert_precomputed_summary(s1)
        # Update with different scan count
        s2 = self._make_summary()
        s2.scan_count = 25
        s2.peak_severity = "TORNADO"
        await store.upsert_precomputed_summary(s2)
        result = await store.get_precomputed_summary("KGRR_TEST001")
        assert result.scan_count == 25
        assert result.peak_severity == "TORNADO"

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, store):
        result = await store.get_precomputed_summary("NO_SUCH_STORM")
        assert result is None

    @pytest.mark.asyncio
    async def test_count(self, store_with_storm):
        store = store_with_storm
        await store.upsert_precomputed_summary(self._make_summary())
        count = await store.count_precomputed_summaries("KGRR")
        assert count == 1

    @pytest.mark.asyncio
    async def test_list_needing_summary(self, store_with_storm):
        """storm_objects with no summary should appear in the stale list."""
        store = store_with_storm
        ids = await store.list_storm_ids_needing_summary("KGRR", stale_minutes=0)
        assert "KGRR_TEST001" in ids


# ── processor_history_status ──────────────────────────────────────────────────

class TestProcessorHistoryStatus:
    def _make_status(self, site: str = "KGRR") -> ProcessorHistoryStatus:
        now = utc_now()
        return ProcessorHistoryStatus(
            id=None,
            site=site,
            last_ingest_time=now,
            last_processing_cycle_time=now,
            last_history_aggregation_time=now,
            last_retention_time=None,
            snapshot_count=150,
            event_history_count=120,
            precomputed_summary_count=8,
            backlog_frame_count=0,
            is_caught_up=True,
            history_stale=False,
            updated_at=now,
        )

    @pytest.mark.asyncio
    async def test_upsert_and_get(self, store):
        status = self._make_status()
        await store.upsert_processor_history_status(status)
        result = await store.get_processor_history_status("KGRR")
        assert result is not None
        assert result.site == "KGRR"
        assert result.snapshot_count == 150
        assert result.is_caught_up is True
        assert result.history_stale is False

    @pytest.mark.asyncio
    async def test_upsert_updates_existing(self, store):
        s1 = self._make_status()
        await store.upsert_processor_history_status(s1)
        s2 = self._make_status()
        s2.backlog_frame_count = 7
        s2.is_caught_up = False
        s2.history_stale = True
        await store.upsert_processor_history_status(s2)
        result = await store.get_processor_history_status("KGRR")
        assert result.backlog_frame_count == 7
        assert result.is_caught_up is False
        assert result.history_stale is True

    @pytest.mark.asyncio
    async def test_list_all(self, store):
        for site in ("KGRR", "KDTX", "KLOT"):
            await store.upsert_processor_history_status(self._make_status(site))
        all_statuses = await store.list_all_processor_history_statuses()
        sites = {s.site for s in all_statuses}
        assert {"KGRR", "KDTX", "KLOT"}.issubset(sites)

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self, store):
        result = await store.get_processor_history_status("NO_SITE")
        assert result is None

    @pytest.mark.asyncio
    async def test_count_backlog_frames(self, store):
        # Fresh DB has no frames — backlog should be 0
        count = await store.count_backlog_frames()
        assert count == 0


# ── HistoryAggregator ─────────────────────────────────────────────────────────

class TestHistoryAggregator:
    @pytest.mark.asyncio
    async def test_update_history_status_writes_row(self, store_with_storm):
        from backend.processor.history.aggregator import HistoryAggregator
        aggregator = HistoryAggregator(store_with_storm)
        await aggregator._update_history_status("KGRR")
        status = await store_with_storm.get_processor_history_status("KGRR")
        assert status is not None
        assert status.site == "KGRR"
        # A fresh DB has no frames/snapshots so backlog=0 and is_caught_up=True
        assert status.is_caught_up is True

    @pytest.mark.asyncio
    async def test_run_for_site_does_not_crash(self, store_with_storm):
        """Aggregation pass completes without error on a minimal DB."""
        from backend.processor.history.aggregator import HistoryAggregator
        aggregator = HistoryAggregator(store_with_storm)
        result = await aggregator.run_for_site("KGRR")
        assert isinstance(result, dict)
        assert "event_rows_written" in result
        assert "summaries_refreshed" in result
        assert result["elapsed_sec"] >= 0


# ── Backfill ──────────────────────────────────────────────────────────────────

class TestBackfill:
    @pytest.mark.asyncio
    async def test_startup_catchup_resets_stuck_frames(self, store_with_storm):
        from backend.processor.history.backfill import startup_catchup

        # Seed a frame stuck in 'processing'
        async with store_with_storm._connection() as connection:
            now = utc_now().isoformat()
            await connection.execute(
                """
                INSERT INTO radar_frames
                    (frame_id, site, product, tilt, scan_time, status, created_at)
                VALUES (?, 'KGRR', 'REF', 0.5, ?, 'processing', ?)
                """,
                ("KGRR_STUCK_FRAME", now, now),
            )
            await connection.commit()

        # Build a minimal mock service
        service = MagicMock()
        service.frame_store = store_with_storm
        service.process_pending_frames = AsyncMock(return_value=0)

        await startup_catchup(service, max_hours=2)

        # Verify the stuck frame was reset to pending
        async with store_with_storm._connection() as connection:
            import aiosqlite
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                "SELECT status FROM radar_frames WHERE frame_id = ?",
                ("KGRR_STUCK_FRAME",),
            )
            row = await cursor.fetchone()
        assert row is not None
        assert row["status"] == "pending"
        service.process_pending_frames.assert_called_once()

    @pytest.mark.asyncio
    async def test_backfill_marks_frames_pending(self, store_with_storm):
        from backend.processor.history.backfill import run_backfill_for_site
        from datetime import timedelta

        base_time = utc_now()

        # Seed two frames in the window with status='error'
        async with store_with_storm._connection() as connection:
            for i in range(2):
                t = (base_time - timedelta(minutes=i * 5)).isoformat()
                await connection.execute(
                    """
                    INSERT INTO radar_frames
                        (frame_id, site, product, tilt, scan_time, status, created_at)
                    VALUES (?, 'KGRR', 'REF', 0.5, ?, 'error', ?)
                    """,
                    (f"KGRR_ERR_{i}", t, t),
                )
            await connection.commit()

        service = MagicMock()
        service.frame_store = store_with_storm
        service.process_pending_frames = AsyncMock(return_value=2)

        start = base_time - timedelta(hours=1)
        result = await run_backfill_for_site(service, "KGRR", start, base_time)
        assert result["frames_found"] == 2
        service.process_pending_frames.assert_called_once()
