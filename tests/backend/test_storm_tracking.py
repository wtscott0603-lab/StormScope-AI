from __future__ import annotations

from datetime import timedelta
import math

import numpy as np
import pytest

from backend.processor.analysis.base import SweepArrays
from backend.processor.storms.segmentation import detect_storm_cells
from backend.processor.storms.threats import build_forecast_path, compute_location_impacts, compute_threats
from backend.processor.storms.tracking import match_storms
from backend.shared.models import SavedLocationRecord, StormObjectRecord
from backend.shared.time import isoformat_utc, utc_now


def make_rect_sweep(*, shift_lon_deg: float = 0.0) -> SweepArrays:
    n_rays = 14
    n_gates = 18
    latitudes = np.zeros((n_rays, n_gates), dtype=np.float32)
    longitudes = np.zeros((n_rays, n_gates), dtype=np.float32)
    values = np.full((n_rays, n_gates), np.nan, dtype=np.float32)

    for ray_index in range(n_rays):
        for gate_index in range(n_gates):
            latitudes[ray_index, gate_index] = 41.2 + ray_index * 0.014
            longitudes[ray_index, gate_index] = -88.5 + shift_lon_deg + gate_index * 0.02

    values[4:10, 4:11] = 52.0
    values[6:8, 6:9] = 60.0

    return SweepArrays(
        values=values,
        latitudes=latitudes,
        longitudes=longitudes,
        azimuths=np.linspace(0.0, 360.0, n_rays, endpoint=False, dtype=np.float32),
        ranges_km=np.linspace(10.0, 180.0, n_gates, dtype=np.float32),
        site_lat=41.6,
        site_lon=-88.0,
        nyquist_velocity=29.0,
    )


def test_detect_storm_cells_finds_reflectivity_core() -> None:
    sweep = make_rect_sweep()

    detections = detect_storm_cells(sweep, threshold_dbz=40.0, min_area_km2=10.0)

    assert len(detections) == 1
    detection = detections[0]
    assert detection.max_reflectivity >= 60.0
    assert detection.area_km2 > 10.0
    assert detection.core_gate_count > 0
    assert detection.core_fraction > 0.0
    assert detection.footprint_geojson["type"] == "Polygon"


def test_match_storms_preserves_id_and_motion() -> None:
    scan_time = utc_now()
    first_detection = detect_storm_cells(make_rect_sweep(), threshold_dbz=40.0, min_area_km2=10.0)[0]
    previous = StormObjectRecord(
        storm_id="KLOT-TESTSTORM",
        site="KLOT",
        latest_frame_id="FRAME1",
        latest_scan_time=scan_time,
        status="active",
        lifecycle_state="tracked",
        centroid_lat=first_detection.centroid_lat,
        centroid_lon=first_detection.centroid_lon,
        area_km2=first_detection.area_km2,
        max_reflectivity=first_detection.max_reflectivity,
        mean_reflectivity=first_detection.mean_reflectivity,
        motion_heading_deg=None,
        motion_speed_kmh=None,
        trend="steady",
        primary_threat="hail",
        secondary_threats=[],
        severity_level="SEVERE",
        confidence=0.6,
        threat_scores={"hail": 0.7},
        narrative="test",
        reasoning_factors=[],
        footprint_geojson=first_detection.footprint_geojson,
        forecast_path=[],
        associated_signatures=[],
        environment_summary=None,
        prediction_summary=None,
        created_at=scan_time,
        updated_at=scan_time,
    )

    second_detection = detect_storm_cells(make_rect_sweep(shift_lon_deg=0.03), threshold_dbz=40.0, min_area_km2=10.0)[0]
    assignments = match_storms("KLOT", scan_time + timedelta(minutes=10), [second_detection], [previous])

    active_assignment = next(item for item in assignments if item["detection"] is not None)
    assert active_assignment["storm_id"] == "KLOT-TESTSTORM"
    assert active_assignment["lifecycle_state"] in {"tracked", "split", "merged"}
    assert active_assignment["motion_heading_deg"] is not None
    assert active_assignment["motion_speed_kmh"] is not None
    assert active_assignment["motion_speed_kmh"] > 0
    assert active_assignment["match_score"] > 0.0
    assert active_assignment["motion_confidence"] > 0.3


def test_compute_location_impacts_returns_eta() -> None:
    forecast_path = build_forecast_path(
        centroid_lat=41.5,
        centroid_lon=-88.0,
        motion_heading_deg=90.0,
        motion_speed_kmh=60.0,
        horizon_minutes=60,
        step_minutes=10,
        destination_point_func=lambda lat, lon, heading, distance_km: (lat, lon + distance_km / 85.0),
    )
    location = SavedLocationRecord(
        location_id="loc-1",
        name="Home",
        lat=41.5,
        lon=-87.78,
        kind="custom",
        created_at=utc_now(),
        updated_at=utc_now(),
    )

    impacts = compute_location_impacts(
        centroid_lat=41.5,
        centroid_lon=-88.0,
        radius_km=8.0,
        forecast_path=forecast_path,
        motion_heading_deg=90.0,
        motion_speed_kmh=60.0,
        locations=[location],
        primary_threat="hail",
        trend="strengthening",
        confidence=0.7,
    )

    assert impacts
    impact = impacts[0]
    assert impact.location_name == "Home"
    assert impact.eta_minutes_low is not None
    assert impact.eta_minutes_high is not None
    assert impact.distance_km is not None


def test_compute_location_impacts_expands_eta_when_motion_confidence_is_low() -> None:
    forecast_path = build_forecast_path(
        centroid_lat=41.5,
        centroid_lon=-88.0,
        motion_heading_deg=90.0,
        motion_speed_kmh=60.0,
        horizon_minutes=60,
        step_minutes=10,
        destination_point_func=lambda lat, lon, heading, distance_km: (lat, lon + distance_km / 85.0),
    )
    location = SavedLocationRecord(
        location_id="loc-2",
        name="School",
        lat=41.5,
        lon=-87.78,
        kind="custom",
        created_at=utc_now(),
        updated_at=utc_now(),
    )

    impacts = compute_location_impacts(
        centroid_lat=41.5,
        centroid_lon=-88.0,
        radius_km=8.0,
        forecast_path=forecast_path,
        motion_heading_deg=90.0,
        motion_speed_kmh=60.0,
        locations=[location],
        primary_threat="wind",
        trend="steady",
        confidence=0.6,
        prediction_summary={
            "projected_confidence": 0.62,
            "motion_confidence": 0.28,
            "persistence_score": 0.40,
            "forecast_stability_score": 0.32,
            "projected_primary_threat": "wind",
            "projected_secondary_threats": ["hail"],
            "projected_trend": "uncertain",
            "projected_threat_scores": {"hail": 0.21, "wind": 0.66, "tornado": 0.13, "flood": 0.19},
            "uncertainty_factors": ["Storm-motion confidence is limited."],
        },
        environment_summary={"ahead_trend": "some environmental change ahead", "environment_confidence": 0.42},
        operational_context={
            "spc": {"category": "Slight Risk"},
            "watch": {"watch_type": "Severe Thunderstorm Watch", "pds": False},
            "lsr": {"nearby_reports": 2},
            "md": {"active_discussions": 1},
        },
    )

    assert impacts
    impact = impacts[0]
    assert impact.details is not None
    assert int(impact.details["eta_uncertainty_minutes"]) >= 8
    assert float(impact.details["path_confidence"]) < 0.4
    assert impact.details["projected_secondary_threats"] == ["hail"]
    assert impact.details["arrival_operational_summary"]["watch_type"] == "Severe Thunderstorm Watch"
    assert impact.details["arrival_operational_summary"]["spc_category"] == "Slight Risk"


def test_compute_threats_adds_uncertainty_for_stale_environment() -> None:
    detection = detect_storm_cells(make_rect_sweep(), threshold_dbz=40.0, min_area_km2=10.0)[0]

    payload = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "hail_favorability": 0.62,
            "wind_favorability": 0.45,
            "tornado_favorability": 0.21,
            "heavy_rain_favorability": 0.34,
            "convective_signal": 0.55,
            "intensification_signal": 0.58,
            "weakening_signal": 0.22,
            "cape_jkg": 1600.0,
            "bulk_shear_06km_kt": 38.0,
            "bulk_shear_01km_kt": 18.0,
            "srh_surface_925hpa_m2s2": 120.0,
            "freezing_level_m": 3600.0,
            "lapse_rate_midlevel_cpkm": 7.1,
            "environment_confidence": 0.38,
            "environment_freshness_minutes": 120,
            "environment_ahead_delta": {"cape_jkg": 250.0, "bulk_shear_06km_kt": 4.0, "srh_surface_925hpa_m2s2": 40.0},
            "ahead_trend": "greater instability ahead",
            "limitation": "Model confidence is limited.",
        },
        srv_metrics={"available": True, "delta_v_ms": 28.0},
        motion_speed_kmh=42.0,
        match_score=0.24,
        motion_confidence=0.32,
        operational_context={"spc": {"category": "Slight"}},
    )

    prediction = payload["prediction_summary"]
    assert prediction["projected_confidence"] < 0.65
    assert prediction["motion_confidence"] == 0.32
    assert prediction["uncertainty_factors"]


@pytest.mark.asyncio
async def test_v1_storm_routes_return_objects_and_locations(client, frame_store):
    now = utc_now()
    ref_frames = await frame_store.list_frames(site="KLOT", product="REF", limit=1)
    assert ref_frames
    frame_id = ref_frames[-1].frame_id
    await frame_store.upsert_saved_location(
        location_id="loc-test",
        name="Office",
        lat=41.6,
        lon=-88.1,
        kind="custom",
    )
    await frame_store.upsert_storm_object(
        {
            "storm_id": "KLOT-STORM-1",
            "site": "KLOT",
            "latest_frame_id": frame_id,
            "latest_scan_time": isoformat_utc(now),
            "status": "active",
            "lifecycle_state": "tracked",
            "centroid_lat": 41.55,
            "centroid_lon": -88.2,
            "area_km2": 84.0,
            "max_reflectivity": 61.0,
            "mean_reflectivity": 50.0,
            "motion_heading_deg": 90.0,
            "motion_speed_kmh": 48.0,
            "trend": "strengthening",
            "primary_threat": "hail",
            "secondary_threats": ["wind"],
            "severity_level": "SEVERE",
            "confidence": 0.74,
            "threat_scores": {"hail": 0.81, "wind": 0.42, "tornado": 0.11, "flood": 0.18},
            "narrative": "Synthetic tracked storm for API testing.",
            "reasoning_factors": ["Max reflectivity 61 dBZ", "Surface proxy slightly more favorable ahead"],
            "footprint_geojson": {
                "type": "Polygon",
                "coordinates": [[[-88.24, 41.52], [-88.16, 41.52], [-88.16, 41.58], [-88.24, 41.58], [-88.24, 41.52]]],
            },
            "forecast_path": [{"lat": 41.55, "lon": -88.05, "eta_minutes": 10, "label": "+10m"}],
            "associated_signatures": [{"signature_type": "HAIL_LARGE", "severity": "SEVERE", "lat": 41.55, "lon": -88.2}],
            "environment_summary": {
                "source": "surface_proxy",
                "current_station_id": "KLOT",
                "hail_favorability": 0.7,
                "wind_favorability": 0.4,
                "tornado_favorability": 0.2,
                "ahead_trend": "slightly richer low-level moisture ahead",
                "limitation": "Surface METAR proxy only.",
            },
            "prediction_summary": {
                "projected_trend": "may strengthen",
                "projected_primary_threat": "hail",
                "projected_secondary_threats": ["wind"],
                "projected_confidence": 0.68,
                "projected_threat_scores": {"hail": 0.84, "wind": 0.51, "tornado": 0.18, "flood": 0.22},
                "forecast_reasoning_factors": ["Synthetic projected reasoning"],
            },
            "created_at": isoformat_utc(now),
            "updated_at": isoformat_utc(now),
        }
    )
    await frame_store.insert_storm_snapshot(
        {
            "storm_id": "KLOT-STORM-1",
            "frame_id": frame_id,
            "site": "KLOT",
            "scan_time": isoformat_utc(now),
            "centroid_lat": 41.55,
            "centroid_lon": -88.2,
            "area_km2": 84.0,
            "max_reflectivity": 61.0,
            "mean_reflectivity": 50.0,
            "motion_heading_deg": 90.0,
            "motion_speed_kmh": 48.0,
            "trend": "strengthening",
            "primary_threat": "hail",
            "secondary_threats": ["wind"],
            "severity_level": "SEVERE",
            "confidence": 0.74,
            "threat_scores": {"hail": 0.81},
            "footprint_geojson": {
                "type": "Polygon",
                "coordinates": [[[-88.24, 41.52], [-88.16, 41.52], [-88.16, 41.58], [-88.24, 41.58], [-88.24, 41.52]]],
            },
            "forecast_path": [{"lat": 41.55, "lon": -88.05, "eta_minutes": 10, "label": "+10m"}],
            "associated_signatures": [],
            "reasoning_factors": ["Max reflectivity 61 dBZ"],
            "near_term_expectation": "Recent radar intensity is increasing.",
            "prediction_summary": {
                "projected_trend": "may strengthen",
                "projected_primary_threat": "hail",
                "projected_secondary_threats": ["wind"],
                "projected_confidence": 0.68,
                "projected_threat_scores": {"hail": 0.84, "wind": 0.51, "tornado": 0.18, "flood": 0.22},
                "forecast_reasoning_factors": ["Synthetic projected reasoning"],
            },
            "created_at": isoformat_utc(now),
        }
    )
    await frame_store.replace_storm_impacts(
        "KLOT-STORM-1",
        [
            {
                "location_id": "loc-test",
                "computed_at": isoformat_utc(now),
                "eta_minutes_low": 12,
                "eta_minutes_high": 18,
                "distance_km": 8.4,
                "threat_at_arrival": "hail",
                "trend_at_arrival": "strengthening",
                "confidence": 0.66,
                "summary": "Closest pass to Office is estimated in about 15 minutes.",
                "impact_rank": 0.66,
                "details": {
                    "projected_primary_threat": "hail",
                    "projected_trend": "may strengthen",
                },
            }
        ],
    )

    storms_response = await client.get("/api/v1/storms", params={"site": "KLOT"})
    locations_response = await client.get("/api/v1/locations")
    track_response = await client.get("/api/v1/storms/KLOT-STORM-1/track")
    impacts_response = await client.get("/api/v1/storms/KLOT-STORM-1/impacts")
    environment_response = await client.get("/api/v1/storms/KLOT-STORM-1/environment")

    assert storms_response.status_code == 200
    storms_payload = storms_response.json()
    assert storms_payload
    assert storms_payload[0]["storm_id"] == "KLOT-STORM-1"
    assert storms_payload[0]["primary_threat"] == "hail"

    assert locations_response.status_code == 200
    assert locations_response.json()[0]["name"] == "Office"

    assert track_response.status_code == 200
    assert track_response.json()[0]["trend"] == "strengthening"

    assert impacts_response.status_code == 200
    assert impacts_response.json()[0]["location_name"] == "Office"

    assert environment_response.status_code == 200
    assert environment_response.json()["current_station_id"] == "KLOT"


# ---------------------------------------------------------------------------
# v12 — multi-frame motion estimation
# ---------------------------------------------------------------------------

def test_estimate_motion_from_history_basic() -> None:
    from backend.processor.storms.tracking import estimate_motion_from_history
    from backend.shared.models import StormSnapshotRecord

    now = utc_now()

    def make_snap(lat, lon, secs_ago):
        snap_time = now - timedelta(seconds=secs_ago)
        return StormSnapshotRecord(
            id=1,
            storm_id="S1",
            frame_id="F1",
            site="KLOT",
            scan_time=snap_time,
            centroid_lat=lat,
            centroid_lon=lon,
            area_km2=100.0,
            max_reflectivity=52.0,
            mean_reflectivity=45.0,
            motion_heading_deg=None,
            motion_speed_kmh=None,
            trend="steady",
            primary_threat="hail",
            secondary_threats=[],
            severity_level="SEVERE",
            confidence=0.6,
            threat_scores={},
            footprint_geojson={"type": "Polygon", "coordinates": []},
            forecast_path=[],
            associated_signatures=[],
            reasoning_factors=[],
            near_term_expectation="",
            prediction_summary=None,
            created_at=snap_time,
        )

    # Storm moving roughly north at ~60 km/h for 3 scans
    snaps = [
        make_snap(41.00, -88.5, 600),
        make_snap(41.045, -88.5, 300),
    ]
    current_lat, current_lon = 41.09, -88.5
    heading, speed, uncertainty = estimate_motion_from_history(
        current_lat, current_lon, now, snaps
    )
    assert heading is not None, "Expected a heading estimate"
    assert speed is not None and speed > 0
    assert 0.0 <= heading <= 360.0
    # Northward motion should produce heading near 0 (± 30 deg)
    assert heading < 30.0 or heading > 330.0, f"Expected northward heading, got {heading:.1f}"
    # Speed should be roughly 60-70 km/h
    assert 40.0 < speed < 100.0, f"Unexpected speed: {speed:.1f}"
    # Uncertainty should be non-negative
    assert uncertainty >= 0.0


def test_estimate_motion_from_history_empty() -> None:
    from backend.processor.storms.tracking import estimate_motion_from_history

    heading, speed, uncertainty = estimate_motion_from_history(41.0, -88.0, utc_now(), [])
    assert heading is None
    assert speed is None
    assert uncertainty == 0.0


def test_uncertainty_cone_shape() -> None:
    from backend.processor.storms.tracking import compute_uncertainty_cone
    from backend.processor.storms.geometry import destination_point

    cone = compute_uncertainty_cone(
        centroid_lat=41.0,
        centroid_lon=-88.0,
        heading_deg=45.0,
        speed_kmh=60.0,
        track_uncertainty_km=8.0,
        motion_confidence=0.6,
        horizon_minutes=60,
        step_minutes=15,
        destination_point_func=destination_point,
    )
    # Should have step at 0, 15, 30, 45, 60
    assert len(cone) == 5
    first = cone[0]
    assert first["eta_minutes"] == 0
    assert first["half_width_km"] == 0.0

    last = cone[-1]
    assert last["eta_minutes"] == 60
    assert last["half_width_km"] > 0.0
    # Width should grow monotonically
    widths = [step["half_width_km"] for step in cone]
    assert widths == sorted(widths)

    # Each step should have center/left/right keys
    for step in cone:
        assert "center" in step and "left" in step and "right" in step


def test_uncertainty_cone_empty_when_no_motion() -> None:
    from backend.processor.storms.tracking import compute_uncertainty_cone
    from backend.processor.storms.geometry import destination_point

    cone = compute_uncertainty_cone(
        centroid_lat=41.0,
        centroid_lon=-88.0,
        heading_deg=None,
        speed_kmh=None,
        destination_point_func=destination_point,
    )
    assert cone == []
