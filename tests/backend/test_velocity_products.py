from __future__ import annotations

from datetime import timedelta

import numpy as np
import pytest

from backend.processor.processing.velocity import derive_storm_relative_velocity, quality_control_velocity
from backend.processor.storms.environment import build_environment_snapshot
from backend.shared.time import utc_now


def test_derive_storm_relative_velocity_reduces_forward_motion_component() -> None:
    raw_velocity = np.array([[20.0, 20.0], [20.0, 20.0]], dtype=np.float32)
    latitudes = np.array([[41.0, 41.05], [41.0, 41.05]], dtype=np.float32)
    longitudes = np.array([[-88.0, -88.0], [-87.95, -87.95]], dtype=np.float32)

    srv = derive_storm_relative_velocity(
        raw_velocity,
        latitudes,
        longitudes,
        site_lat=41.0,
        site_lon=-88.0,
        motion_heading_deg=0.0,
        motion_speed_kmh=36.0,
        nyquist_velocity=29.0,
    )

    assert srv.shape == raw_velocity.shape
    assert np.nanmean(srv) < np.nanmean(raw_velocity)


def test_quality_control_velocity_replaces_isolated_spike() -> None:
    raw_velocity = np.array(
        [
            [12.0, 11.0, 13.0],
            [11.0, 78.0, 10.0],
            [12.0, 11.0, 12.0],
        ],
        dtype=np.float32,
    )

    qc = quality_control_velocity(raw_velocity, nyquist_velocity=29.0)

    assert qc[1, 1] < 20.0
    assert np.isfinite(qc[1, 1])


@pytest.mark.asyncio
async def test_build_environment_snapshot_uses_grid_context(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_grid(lat: float, lon: float, *, valid_at, ttl_minutes: int, cache_dir=None):
        base = {
            "gridpoint_id": "LOT/61,59",
            "temperature_c": 24.0,
            "dewpoint_c": 16.0,
            "wind_speed_kmh": 35.0,
            "wind_dir_deg": 210.0,
            "weather_summary": "chance thunderstorms",
            "hazards": [],
        }
        if valid_at > utc_now() + timedelta(minutes=30):
            return {
                **base,
                "probability_of_thunder_pct": 65.0,
                "quantitative_precip_mm": 8.0,
            }
        return {
            **base,
            "probability_of_thunder_pct": 25.0,
            "quantitative_precip_mm": 2.0,
        }

    monkeypatch.setattr("backend.processor.storms.environment._sample_nws_gridpoint", fake_grid)
    async def fake_model(lat: float, lon: float, *, valid_at, ttl_minutes: int, cache_dir=None):
        base = {
            "valid_at": utc_now().isoformat(),
            "field_count": 12,
            "temperature_2m": 24.0,
            "dew_point_2m": 16.0,
            "wind_speed_10m": 36.0,
            "wind_direction_10m": 210.0,
            "wind_gusts_10m": 48.0,
            "precipitation": 1.2,
            "cape": 1400.0,
            "convective_inhibition": -35.0,
            "freezing_level_height": 3400.0,
            "wind_speed_925hPa": 42.0,
            "wind_direction_925hPa": 220.0,
            "wind_speed_500hPa": 82.0,
            "wind_direction_500hPa": 245.0,
            "temperature_700hPa": -6.0,
            "temperature_500hPa": -18.0,
        }
        if valid_at > utc_now() + timedelta(minutes=30):
            return {**base, "cape": 1900.0}
        return base

    monkeypatch.setattr("backend.processor.storms.environment._sample_open_meteo_environment", fake_model)

    payload = await build_environment_snapshot(
        site="KLOT",
        storm_id="KLOT-STORM-1",
        centroid_lat=41.6,
        centroid_lon=-88.1,
        motion_heading_deg=90.0,
        motion_speed_kmh=50.0,
        observations=[
            {
                "station_id": "KDPA",
                "observation_time": utc_now().isoformat(),
                "lat": 41.91,
                "lon": -88.25,
                "temp_c": 23.0,
                "dewpoint_c": 15.0,
                "wind_dir_deg": 190.0,
                "wind_speed_kt": 18.0,
                "pressure_hpa": 1008.0,
                "visibility_mi": 10.0,
            }
        ],
        grid_cache_ttl_minutes=30,
    )

    assert payload is not None
    assert payload["source"] == "open_meteo_model+aviationweather_metar+nws_gridpoint_forecast"
    assert payload["summary"]["gridpoint_id"] == "LOT/61,59"
    assert payload["summary"]["ahead_probability_of_thunder"] == 65.0
    assert payload["summary"]["cape_jkg"] == 1400.0
    assert payload["summary"]["environment_confidence"] > 0.0
    assert payload["summary"]["intensification_signal"] >= 0.0
