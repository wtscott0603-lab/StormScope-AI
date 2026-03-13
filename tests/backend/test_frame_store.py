import pytest

from backend.processor.cache.frame_store import FrameStore
from backend.shared.db import init_db
from backend.shared.time import utc_now


@pytest.mark.asyncio
async def test_frame_store_insert_and_read(tmp_path):
    db_path = tmp_path / "frame-store.db"
    await init_db(db_path)
    store = FrameStore(db_path)
    scan_time = utc_now()

    inserted = await store.insert_raw_frame(
        frame_id="TEST_FRAME",
        site="KLOT",
        product="REF",
        tilt=0.5,
        scan_time=scan_time,
        raw_path="/tmp/frame.ar2v",
    )

    assert inserted is True
    frame = await store.get_frame("TEST_FRAME")
    assert frame is not None
    assert frame.site == "KLOT"
    assert frame.product == "REF"
    assert frame.raw_path == "/tmp/frame.ar2v"


@pytest.mark.asyncio
async def test_delete_frame_clears_storm_references_and_snapshots(tmp_path):
    db_path = tmp_path / "retention.db"
    await init_db(db_path)
    store = FrameStore(db_path)
    await store.initialize()
    scan_time = utc_now()

    inserted = await store.insert_raw_frame(
        frame_id="FRAME_KEEP",
        site="KLOT",
        product="REF",
        tilt=0.5,
        scan_time=scan_time,
        raw_path="/tmp/frame.ar2v",
    )
    assert inserted is True
    await store.update_frame_status(
        "FRAME_KEEP",
        status="processed",
        image_path="/tmp/frame.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
    )

    await store.upsert_storm_object(
        {
            "storm_id": "KLOT-STORM-1",
            "site": "KLOT",
            "latest_frame_id": "FRAME_KEEP",
            "latest_scan_time": scan_time.isoformat().replace("+00:00", "Z"),
            "status": "active",
            "lifecycle_state": "tracked",
            "centroid_lat": 41.5,
            "centroid_lon": -88.0,
            "area_km2": 45.0,
            "max_reflectivity": 58.0,
            "mean_reflectivity": 46.0,
            "motion_heading_deg": 240.0,
            "motion_speed_kmh": 40.0,
            "trend": "steady",
            "primary_threat": "hail",
            "secondary_threats": ["wind"],
            "severity_level": "SEVERE",
            "confidence": 0.7,
            "threat_scores": {"hail": 0.7},
            "narrative": "test storm",
            "reasoning_factors": ["test"],
            "footprint_geojson": {"type": "Polygon", "coordinates": []},
            "forecast_path": [],
            "associated_signatures": [],
            "environment_summary": None,
            "prediction_summary": None,
            "created_at": scan_time.isoformat().replace("+00:00", "Z"),
            "updated_at": scan_time.isoformat().replace("+00:00", "Z"),
        }
    )
    await store.insert_storm_snapshot(
        {
            "storm_id": "KLOT-STORM-1",
            "frame_id": "FRAME_KEEP",
            "site": "KLOT",
            "scan_time": scan_time.isoformat().replace("+00:00", "Z"),
            "centroid_lat": 41.5,
            "centroid_lon": -88.0,
            "area_km2": 45.0,
            "max_reflectivity": 58.0,
            "mean_reflectivity": 46.0,
            "motion_heading_deg": 240.0,
            "motion_speed_kmh": 40.0,
            "trend": "steady",
            "primary_threat": "hail",
            "secondary_threats": ["wind"],
            "severity_level": "SEVERE",
            "confidence": 0.7,
            "threat_scores": {"hail": 0.7},
            "footprint_geojson": {"type": "Polygon", "coordinates": []},
            "forecast_path": [],
            "associated_signatures": [],
            "reasoning_factors": ["test"],
            "near_term_expectation": "steady",
            "prediction_summary": None,
            "created_at": scan_time.isoformat().replace("+00:00", "Z"),
        }
    )

    await store.delete_frame("FRAME_KEEP")

    storm = await store.get_storm_object("KLOT-STORM-1")
    assert storm is not None
    assert storm.latest_frame_id is None
    assert await store.get_frame("FRAME_KEEP") is None
    assert await store.list_storm_snapshots("KLOT-STORM-1") == []
