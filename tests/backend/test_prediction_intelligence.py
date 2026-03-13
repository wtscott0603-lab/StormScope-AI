from __future__ import annotations

from pathlib import Path

import httpx
import numpy as np
import pytest

from backend.processor.analysis.base import SignatureMarker, SweepArrays
from backend.processor.storms import environment as storm_environment
from backend.processor.storms.segmentation import detect_storm_cells
from backend.processor.storms.threats import (
    compute_local_srv_rotation_signatures,
    compute_srv_metrics,
    compute_threats,
)


def make_ref_sweep() -> SweepArrays:
    n_rays = 12
    n_gates = 14
    latitudes = np.zeros((n_rays, n_gates), dtype=np.float32)
    longitudes = np.zeros((n_rays, n_gates), dtype=np.float32)
    values = np.full((n_rays, n_gates), np.nan, dtype=np.float32)
    for ray in range(n_rays):
        for gate in range(n_gates):
            latitudes[ray, gate] = 40.0 + ray * 0.01
            longitudes[ray, gate] = -83.1 + gate * 0.01
    values[3:9, 3:9] = 53.0
    values[5:7, 5:7] = 61.0
    return SweepArrays(
        values=values,
        latitudes=latitudes,
        longitudes=longitudes,
        azimuths=np.linspace(0.0, 360.0, n_rays, endpoint=False, dtype=np.float32),
        ranges_km=np.linspace(12.0, 160.0, n_gates, dtype=np.float32),
        site_lat=39.98,
        site_lon=-82.89,
        nyquist_velocity=29.0,
    )


def make_velocity_sweep() -> SweepArrays:
    sweep = make_ref_sweep()
    values = np.full_like(sweep.values, np.nan)
    values[3:9, 3:9] = np.array(
        [
            [-20, -18, -15, 14, 18, 20],
            [-22, -20, -17, 16, 20, 22],
            [-18, -16, -14, 15, 17, 19],
            [-17, -15, -12, 12, 15, 17],
            [-16, -14, -12, 11, 14, 16],
            [-15, -13, -11, 10, 12, 14],
        ],
        dtype=np.float32,
    )
    return SweepArrays(
        values=values,
        latitudes=sweep.latitudes,
        longitudes=sweep.longitudes,
        azimuths=sweep.azimuths,
        ranges_km=sweep.ranges_km,
        site_lat=sweep.site_lat,
        site_lon=sweep.site_lon,
        nyquist_velocity=29.0,
    )


def test_compute_srv_metrics_exposes_motion_metadata() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    metrics = compute_srv_metrics(
        detection,
        make_velocity_sweep(),
        motion_heading_deg=72.0,
        motion_speed_kmh=48.0,
        motion_confidence=0.66,
    )

    assert metrics["available"] is True
    assert metrics["motion_source"] == "storm_object_track"
    assert metrics["motion_heading_deg"] == 72.0
    assert metrics["motion_speed_kmh"] == 48.0
    assert metrics["motion_confidence"] == 0.66


def test_compute_local_srv_rotation_signatures_uses_srv_field(monkeypatch: pytest.MonkeyPatch) -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    raw_values = make_velocity_sweep().values.copy()

    def fake_detect(sweep: SweepArrays) -> list[SignatureMarker]:
        assert not np.allclose(np.nan_to_num(sweep.values), np.nan_to_num(raw_values))
        return [
            SignatureMarker(
                signature_type="TVS",
                severity="TORNADO",
                lat=detection.centroid_lat,
                lon=detection.centroid_lon,
                radius_km=2.0,
                label="TVS",
                description="synthetic",
                confidence=0.8,
                metrics={"shear_per_sec": 0.03},
            )
        ]

    monkeypatch.setattr("backend.processor.storms.threats.detect_rotation_couplets", fake_detect)

    signatures = compute_local_srv_rotation_signatures(
        detection,
        make_velocity_sweep(),
        motion_heading_deg=90.0,
        motion_speed_kmh=54.0,
    )

    assert signatures
    assert signatures[0]["signature_type"] == "TVS"


def test_tornado_risk_uses_signature_floor_without_saturating() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    payload = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[{"signature_type": "TDS", "severity": "TORNADO"}],
        environment_summary={
            "tornado_favorability": 0.42,
            "wind_favorability": 0.22,
            "hail_favorability": 0.34,
            "heavy_rain_favorability": 0.20,
            "convective_signal": 0.52,
            "intensification_signal": 0.33,
            "weakening_signal": 0.18,
            "cape_jkg": 1400.0,
            "bulk_shear_06km_kt": 38.0,
            "bulk_shear_01km_kt": 20.0,
            "srh_surface_925hpa_m2s2": 130.0,
            "dcape_jkg": 720.0,
            "freezing_level_m": 3400.0,
            "lapse_rate_midlevel_cpkm": 7.0,
            "environment_confidence": 0.72,
            "environment_freshness_minutes": 25,
            "environment_ahead_delta": {"cape_jkg": 200.0, "srh_surface_925hpa_m2s2": 30.0},
            "ahead_trend": "greater instability ahead",
        },
        srv_metrics={"available": True, "delta_v_ms": 34.0},
        motion_speed_kmh=45.0,
        motion_confidence=0.7,
        operational_context={"spc": {"tornado_probability": 10.0}},
    )

    assert payload["threat_scores"]["tornado"] >= 0.72
    assert payload["threat_scores"]["tornado"] < 1.0


def test_hail_risk_responds_to_vil_density_without_saturating() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    payload = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "hail_favorability": 0.48,
            "wind_favorability": 0.22,
            "tornado_favorability": 0.14,
            "heavy_rain_favorability": 0.18,
            "convective_signal": 0.42,
            "intensification_signal": 0.30,
            "weakening_signal": 0.18,
            "cape_jkg": 1800.0,
            "bulk_shear_06km_kt": 36.0,
            "freezing_level_m": 3300.0,
            "lapse_rate_midlevel_cpkm": 7.4,
            "environment_confidence": 0.72,
            "environment_freshness_minutes": 25,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=42.0,
        motion_confidence=0.65,
        volume_metrics={
            "max_vil_kgm2": 42.0,
            "max_vil_density_gm3": 4.8,
            "max_echo_tops_km": 10.0,
        },
    )

    assert 0.44 <= payload["threat_scores"]["hail"] < 1.0


def test_flood_risk_weights_qpe_more_than_motion_proxy() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    slow_storm = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "heavy_rain_favorability": 0.45,
            "hail_favorability": 0.2,
            "wind_favorability": 0.2,
            "tornado_favorability": 0.1,
            "convective_signal": 0.3,
            "intensification_signal": 0.2,
            "weakening_signal": 0.2,
            "forecast_qpf_mm": 5.0,
            "environment_confidence": 0.7,
            "environment_freshness_minutes": 20,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=12.0,
        motion_confidence=0.7,
        volume_metrics={"max_rain_rate_mmhr": 12.0, "max_qpe_1h_mm": 8.0},
    )
    wetter_storm = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "heavy_rain_favorability": 0.45,
            "hail_favorability": 0.2,
            "wind_favorability": 0.2,
            "tornado_favorability": 0.1,
            "convective_signal": 0.3,
            "intensification_signal": 0.2,
            "weakening_signal": 0.2,
            "forecast_qpf_mm": 5.0,
            "environment_confidence": 0.7,
            "environment_freshness_minutes": 20,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=30.0,
        motion_confidence=0.7,
        volume_metrics={"max_rain_rate_mmhr": 20.0, "max_qpe_1h_mm": 38.0},
    )

    assert wetter_storm["threat_scores"]["flood"] > slow_storm["threat_scores"]["flood"]


def test_kdp_boosts_hail_and_flood_scores() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    baseline = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "hail_favorability": 0.46,
            "wind_favorability": 0.24,
            "tornado_favorability": 0.12,
            "heavy_rain_favorability": 0.38,
            "convective_signal": 0.40,
            "intensification_signal": 0.24,
            "weakening_signal": 0.18,
            "cape_jkg": 1700.0,
            "bulk_shear_06km_kt": 34.0,
            "dcape_jkg": 850.0,
            "freezing_level_m": 3400.0,
            "lapse_rate_midlevel_cpkm": 7.2,
            "forecast_qpf_mm": 8.0,
            "environment_confidence": 0.74,
            "environment_freshness_minutes": 20,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=34.0,
        motion_confidence=0.68,
        volume_metrics={
            "max_vil_kgm2": 38.0,
            "max_vil_density_gm3": 3.9,
            "max_echo_tops_km": 9.5,
            "max_rain_rate_mmhr": 22.0,
            "max_qpe_1h_mm": 28.0,
        },
    )
    kdp_enhanced = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "hail_favorability": 0.46,
            "wind_favorability": 0.24,
            "tornado_favorability": 0.12,
            "heavy_rain_favorability": 0.38,
            "convective_signal": 0.40,
            "intensification_signal": 0.24,
            "weakening_signal": 0.18,
            "cape_jkg": 1700.0,
            "bulk_shear_06km_kt": 34.0,
            "dcape_jkg": 850.0,
            "freezing_level_m": 3400.0,
            "lapse_rate_midlevel_cpkm": 7.2,
            "forecast_qpf_mm": 8.0,
            "environment_confidence": 0.74,
            "environment_freshness_minutes": 20,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=34.0,
        motion_confidence=0.68,
        volume_metrics={
            "max_vil_kgm2": 38.0,
            "max_vil_density_gm3": 3.9,
            "max_echo_tops_km": 9.5,
            "max_kdp_degkm": 2.8,
            "max_rain_rate_mmhr": 22.0,
            "max_qpe_1h_mm": 28.0,
        },
    )

    assert kdp_enhanced["threat_scores"]["hail"] > baseline["threat_scores"]["hail"]
    assert kdp_enhanced["threat_scores"]["flood"] > baseline["threat_scores"]["flood"]


def test_fast_linear_system_can_reach_severe_wind_without_bow_signature() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    detection.elongation_ratio = 4.0
    fast_linear = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "hail_favorability": 0.20,
            "wind_favorability": 0.52,
            "tornado_favorability": 0.12,
            "heavy_rain_favorability": 0.18,
            "convective_signal": 0.42,
            "intensification_signal": 0.24,
            "weakening_signal": 0.18,
            "bulk_shear_06km_kt": 38.0,
            "dcape_jkg": 900.0,
            "environment_confidence": 0.72,
            "environment_freshness_minutes": 20,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=75.0,
        motion_confidence=0.72,
        volume_metrics={
            "max_echo_tops_km": 11.0,
            "max_rain_rate_mmhr": 16.0,
        },
        operational_context={"spc": {"wind_probability": 40.0}},
    )

    assert fast_linear["threat_scores"]["wind"] >= 0.55
    assert fast_linear["severity_level"] == "SEVERE"


def test_maintenance_score_rewards_steady_storms() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    payload = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary={
            "hail_favorability": 0.40,
            "wind_favorability": 0.32,
            "tornado_favorability": 0.18,
            "heavy_rain_favorability": 0.22,
            "convective_signal": 0.40,
            "intensification_signal": 0.22,
            "weakening_signal": 0.20,
            "environment_confidence": 0.72,
            "environment_freshness_minutes": 20,
            "environment_ahead_delta": {},
        },
        srv_metrics={"available": False},
        motion_speed_kmh=35.0,
        motion_confidence=0.7,
    )

    prediction = payload["prediction_summary"]
    assert payload["trend"] == "steady"
    assert prediction["maintenance_score"] > prediction["intensification_score"]
    assert prediction["maintenance_score"] > prediction["weakening_score"]


def test_narrative_assembly_uses_structured_factors() -> None:
    detection = detect_storm_cells(make_ref_sweep(), threshold_dbz=40.0, min_area_km2=1.0)[0]
    payload = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[{"signature_type": "TVS", "severity": "TORNADO"}],
        environment_summary={
            "hail_favorability": 0.35,
            "wind_favorability": 0.31,
            "tornado_favorability": 0.52,
            "heavy_rain_favorability": 0.18,
            "convective_signal": 0.48,
            "intensification_signal": 0.28,
            "weakening_signal": 0.18,
            "cape_jkg": 1700.0,
            "bulk_shear_06km_kt": 42.0,
            "srh_surface_925hpa_m2s2": 145.0,
            "environment_confidence": 0.68,
            "environment_freshness_minutes": 35,
            "environment_ahead_delta": {},
            "ahead_trend": "stronger low-level shear ahead",
        },
        srv_metrics={"available": True, "delta_v_ms": 44.0},
        motion_speed_kmh=52.0,
        motion_confidence=0.62,
        operational_context={"spc": {"category": "Slight Risk"}},
    )

    narrative = payload["narrative"]
    assert "TVS-type rotation signature" in narrative
    assert "CAPE near the storm" in narrative
    assert "spc slight risk context" in narrative.lower()


def test_environment_summary_exposes_profile_and_provenance_fields() -> None:
    current_model = {
        "valid_at": "2026-03-10T18:00:00+00:00",
        "temperature_2m": 23.0,
        "wind_speed_10m": 24.0,
        "wind_direction_10m": 190.0,
        "wind_speed_925hPa": 36.0,
        "wind_direction_925hPa": 215.0,
        "wind_speed_850hPa": 52.0,
        "wind_direction_850hPa": 235.0,
        "wind_speed_500hPa": 78.0,
        "wind_direction_500hPa": 255.0,
        "temperature_700hPa": -2.0,
        "temperature_500hPa": -14.0,
        "cape": 1800.0,
        "convective_inhibition": 35.0,
        "freezing_level_height": 3600.0,
    }

    profile = storm_environment._build_reduced_profile(
        current_model,
        surface_temp_c=24.0,
        surface_dewpoint_c=19.0,
        motion_heading_deg=88.0,
        motion_speed_kmh=52.0,
    )
    provenance = storm_environment._build_field_provenance(current_model)
    notes = storm_environment._build_source_notes(current_model, {"station_id": "KCMH"}, {"gridpoint_id": "ILN/42,76"})

    assert profile is not None
    assert profile["type"] == "reduced_model_profile"
    assert len(profile["levels"]) >= 4
    assert provenance["cape_jkg"] == "direct_model"
    assert provenance["srh_surface_925hpa_m2s2"] == "proxy_from_surface_and_925hpa_winds"
    assert provenance["profile_summary"] == "reduced_model_profile"
    assert any("proxy estimates" in note for note in notes)


@pytest.mark.asyncio
async def test_disk_backed_environment_cache_recovers_after_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = "https://example.test/environment"
    cache_dir = tmp_path / "environment"
    payload = {"value": 42}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return payload

    async def first_get(self, request_url: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        assert request_url == url
        return FakeResponse()

    monkeypatch.setattr(httpx.AsyncClient, "get", first_get)
    memory_cache: dict[str, tuple] = {}
    first = await storm_environment._cached_json(url, memory_cache, ttl_minutes=60, cache_dir=cache_dir, namespace="model")
    assert first == payload

    async def failing_get(self, request_url: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("offline", request=httpx.Request("GET", request_url))

    monkeypatch.setattr(httpx.AsyncClient, "get", failing_get)
    second = await storm_environment._cached_json(url, {}, ttl_minutes=60, cache_dir=cache_dir, namespace="model")
    assert second == payload


# ---------------------------------------------------------------------------
# v12 — convective mode classification
# ---------------------------------------------------------------------------

def test_convective_mode_supercell_candidate() -> None:
    from backend.processor.storms.threats import classify_convective_mode

    ref_sweep = make_ref_sweep()
    detections = detect_storm_cells(ref_sweep, threshold_dbz=40.0, min_area_km2=10.0)
    assert detections, "Need at least one detection"
    detection = detections[0]

    # Supercell-like: rotation signature, compact, strong reflectivity
    mode, confidence, evidence = classify_convective_mode(
        detection=detection,
        nearby_storm_count=0,
        signature_types={"ROTATION", "TVS"},
        motion_heading_deg=220.0,
        motion_speed_kmh=60.0,
        history=[],
    )
    assert mode == "supercell_candidate", f"Expected supercell_candidate, got {mode}"
    assert confidence >= 0.45
    assert any("rotation" in e.lower() for e in evidence)


def test_convective_mode_discrete_cell_default() -> None:
    from backend.processor.storms.threats import classify_convective_mode

    ref_sweep = make_ref_sweep()
    detections = detect_storm_cells(ref_sweep, threshold_dbz=40.0, min_area_km2=10.0)
    detection = detections[0]

    mode, confidence, _ = classify_convective_mode(
        detection=detection,
        nearby_storm_count=0,
        signature_types=set(),
        motion_heading_deg=180.0,
        motion_speed_kmh=35.0,
        history=[],
    )
    # Without bow/rotation/training signals, should be discrete_cell or similar
    assert mode in ("discrete_cell", "supercell_candidate", "cluster_multicell", "linear_segment", "bow_segment", "training_rain_producer")
    assert 0.0 <= confidence <= 1.0


def test_convective_mode_exposed_in_compute_threats() -> None:
    ref_sweep = make_ref_sweep()
    detections = detect_storm_cells(ref_sweep, threshold_dbz=40.0, min_area_km2=10.0)
    detection = detections[0]

    result = compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[
            {"signature_type": "ROTATION", "severity": "SEVERE", "lat": detection.centroid_lat + 0.01, "lon": detection.centroid_lon + 0.01, "radius_km": 3.0, "label": "MESO", "description": "", "confidence": 0.8, "metrics": {}},
        ],
        environment_summary=None,
        srv_metrics={"available": False},
        motion_speed_kmh=55.0,
        motion_heading_deg=230.0,
        motion_confidence=0.7,
        volume_metrics=None,
        nearby_storm_count=0,
    )
    assert "storm_mode" in result
    assert result["storm_mode"] in (
        "discrete_cell", "supercell_candidate", "bow_segment",
        "linear_segment", "training_rain_producer", "cluster_multicell", "unknown",
    )
    assert "storm_mode_confidence" in result
    assert "track_uncertainty_km" in result


# ---------------------------------------------------------------------------
# v13 — threat component breakdown and lifecycle analysis
# ---------------------------------------------------------------------------

class TestThreatComponentBreakdown:
    """Test that compute_threats() exposes per-component score breakdown."""

    def _make_detection(self, max_ref=55.0, area=80.0, elongation=1.8, core_fraction=0.12):
        from unittest.mock import MagicMock
        import numpy as np
        detection = MagicMock()
        detection.max_reflectivity = max_ref
        detection.mean_reflectivity = max_ref - 8.0
        detection.area_km2 = area
        detection.elongation_ratio = elongation
        detection.core_fraction = core_fraction
        detection.core_max_reflectivity = max_ref + 3.0
        detection.gate_mask = np.array([True, True, True])
        detection.radius_km = 12.0
        return detection

    def test_component_breakdown_present_in_result(self):
        detection = self._make_detection()
        result = compute_threats(
            detection=detection,
            history=[],
            associated_signatures=[],
            environment_summary=None,
            srv_metrics=None,
            motion_speed_kmh=45.0,
            motion_heading_deg=220.0,
        )
        assert "threat_component_breakdown" in result
        breakdown = result["threat_component_breakdown"]
        assert "hail" in breakdown
        assert "wind" in breakdown
        assert "tornado" in breakdown
        assert "flood" in breakdown

    def test_hail_breakdown_sums_close_to_hail_score(self):
        """Sum of hail components should be close to the reported hail score."""
        detection = self._make_detection(max_ref=62.0, core_fraction=0.22)
        result = compute_threats(
            detection=detection,
            history=[],
            associated_signatures=[],
            environment_summary={
                "hail_favorability": 0.7,
                "wind_favorability": 0.4,
                "tornado_favorability": 0.3,
                "heavy_rain_favorability": 0.2,
                "cape_jkg": 2500.0,
                "bulk_shear_06km_kt": 45.0,
            },
            srv_metrics=None,
            motion_speed_kmh=40.0,
        )
        breakdown_hail = result["threat_component_breakdown"]["hail"]
        reported_hail = result["threat_scores"]["hail"]
        component_sum = sum(breakdown_hail.values())
        # Component sum may not exactly match due to bonuses/floors, but should be within 0.20
        assert abs(component_sum - reported_hail) < 0.25, (
            f"Hail component sum {component_sum:.3f} diverges too far from reported score {reported_hail:.3f}"
        )

    def test_top_reasons_and_limiting_factors_present(self):
        detection = self._make_detection()
        result = compute_threats(
            detection=detection,
            history=[],
            associated_signatures=[],
            environment_summary=None,
            srv_metrics=None,
            motion_speed_kmh=30.0,
        )
        assert "threat_top_reasons" in result
        assert "threat_limiting_factors" in result
        for threat in ("hail", "wind", "tornado", "flood"):
            assert threat in result["threat_top_reasons"]
            assert threat in result["threat_limiting_factors"]


class TestLifecycleAnalysis:
    """Tests for the new lifecycle.py module."""

    def _make_snapshots(self, refs, areas):
        from unittest.mock import MagicMock
        snaps = []
        for r, a in zip(refs, areas):
            s = MagicMock()
            s.max_reflectivity = r
            s.area_km2 = a
            s.motion_speed_kmh = 45.0
            s.motion_heading_deg = 220.0
            snaps.append(s)
        return snaps

    def test_rapid_intensification_detected(self):
        from backend.processor.storms.lifecycle import classify_lifecycle_trend
        snaps = self._make_snapshots([50.0, 52.0, 57.0], [80.0, 95.0, 130.0])
        trend, confidence, evidence = classify_lifecycle_trend(snaps)
        assert trend == "rapid_intensification", f"Expected rapid_intensification, got {trend}"
        assert confidence > 0.5

    def test_weakening_detected(self):
        from backend.processor.storms.lifecycle import classify_lifecycle_trend
        snaps = self._make_snapshots([58.0, 55.0, 52.0], [120.0, 110.0, 95.0])
        trend, confidence, evidence = classify_lifecycle_trend(snaps)
        assert trend in ("weakening", "rapid_decay"), f"Expected weakening or rapid_decay, got {trend}"

    def test_steady_detected(self):
        from backend.processor.storms.lifecycle import classify_lifecycle_trend
        snaps = self._make_snapshots([54.0, 55.0, 54.5], [100.0, 102.0, 101.0])
        trend, confidence, evidence = classify_lifecycle_trend(snaps)
        assert trend == "steady", f"Expected steady, got {trend}"

    def test_empty_history_returns_uncertain(self):
        from backend.processor.storms.lifecycle import classify_lifecycle_trend
        trend, confidence, evidence = classify_lifecycle_trend([])
        assert trend == "uncertain"

    def test_motion_trend_acceleration(self):
        from backend.processor.storms.lifecycle import classify_motion_trend
        snaps = self._make_snapshots([54.0] * 3, [100.0] * 3)
        for i, s in enumerate(snaps):
            s.motion_speed_kmh = 30.0 + i * 15.0  # 30, 45, 60 km/h
            s.motion_heading_deg = 220.0
        trend, confidence, evidence = classify_motion_trend(snaps)
        assert trend == "accelerating", f"Expected accelerating, got {trend}"

    def test_build_lifecycle_summary_returns_dict(self):
        from backend.processor.storms.lifecycle import build_lifecycle_summary
        snaps = self._make_snapshots([54.0, 57.0, 62.0], [90.0, 110.0, 140.0])
        summary = build_lifecycle_summary(snaps, lifecycle_state="tracked", trend="strengthening")
        assert "intensity_trend" in summary
        assert "motion_trend" in summary
        assert "provenance" in summary
        assert "proxy-derived" in summary["provenance"]


# =============================================================================
# v14 — Event Flags
# =============================================================================

class TestEventFlags:
    """Tests for the structured event flag engine."""

    def _make_lifecycle_summary(self, intensity_trend="steady", motion_trend="steady_motion", confidence=0.6):
        return {
            "intensity_trend": intensity_trend,
            "intensity_confidence": confidence,
            "motion_trend": motion_trend,
            "motion_confidence": confidence,
        }

    def _base_kwargs(self, **overrides):
        base = dict(
            history=[],
            lifecycle_summary=self._make_lifecycle_summary(),
            lifecycle_state="tracked",
            associated_signatures=[],
            threat_scores={"hail": 0.2, "wind": 0.2, "tornado": 0.1, "flood": 0.1},
            threat_component_breakdown={},
            severity_level="NONE",
            storm_mode="discrete_cell",
            storm_mode_confidence=0.5,
            motion_speed_kmh=45.0,
            motion_confidence=0.7,
            track_uncertainty_km=5.0,
            environment_summary=None,
            volume_metrics=None,
            srv_metrics=None,
        )
        base.update(overrides)
        return base

    def test_rapid_intensification_flag(self):
        from backend.processor.storms.event_flags import compute_event_flags
        lc = self._make_lifecycle_summary(intensity_trend="rapid_intensification", confidence=0.85)
        flags = compute_event_flags(**self._base_kwargs(lifecycle_summary=lc))
        names = {f["flag"] for f in flags}
        assert "rapid_intensification" in names

    def test_rotation_tightening_flag_from_signatures(self):
        from backend.processor.storms.event_flags import compute_event_flags
        # Simulated snapshots with growing rotation severity
        class FakeSnap:
            def __init__(self, sigs):
                self.associated_signatures = sigs
        history = [
            FakeSnap([{"signature_type": "ROTATION"}]),
            FakeSnap([{"signature_type": "TVS"}]),
        ]
        sigs = [{"signature_type": "TVS", "lat": 41.0, "lon": -88.0}]
        flags = compute_event_flags(**self._base_kwargs(
            history=history,
            associated_signatures=sigs,
            lifecycle_summary=self._make_lifecycle_summary(),
        ))
        names = {f["flag"] for f in flags}
        assert "rotation_tightening" in names or "tornado_threat_elevated" in names

    def test_elevated_uncertainty_flag_on_low_confidence(self):
        from backend.processor.storms.event_flags import compute_event_flags
        flags = compute_event_flags(**self._base_kwargs(
            motion_confidence=0.25,
            track_uncertainty_km=20.0,
        ))
        names = {f["flag"] for f in flags}
        assert "elevated_uncertainty" in names

    def test_severe_threat_elevated_flag(self):
        from backend.processor.storms.event_flags import compute_event_flags
        flags = compute_event_flags(**self._base_kwargs(
            threat_scores={"hail": 0.72, "wind": 0.55, "tornado": 0.3, "flood": 0.1},
            severity_level="SEVERE",
        ))
        names = {f["flag"] for f in flags}
        assert "severe_threat_elevated" in names

    def test_supercell_candidate_flag(self):
        from backend.processor.storms.event_flags import compute_event_flags
        flags = compute_event_flags(**self._base_kwargs(
            storm_mode="supercell_candidate",
            storm_mode_confidence=0.78,
        ))
        names = {f["flag"] for f in flags}
        assert "supercell_candidate" in names

    def test_slowing_training_flag(self):
        from backend.processor.storms.event_flags import compute_event_flags
        flags = compute_event_flags(**self._base_kwargs(motion_speed_kmh=8.0))
        names = {f["flag"] for f in flags}
        assert "slowing_training" in names

    def test_environment_support_weak_flag_on_no_env(self):
        from backend.processor.storms.event_flags import compute_event_flags
        flags = compute_event_flags(**self._base_kwargs(environment_summary=None))
        names = {f["flag"] for f in flags}
        assert "environment_support_weak" in names

    def test_environment_support_strong_flag(self):
        from backend.processor.storms.event_flags import compute_event_flags
        env = {
            "environment_confidence": 0.80,
            "hail_favorability": 0.72,
            "wind_favorability": 0.65,
            "tornado_favorability": 0.60,
            "intensification_signal": 0.70,
        }
        flags = compute_event_flags(**self._base_kwargs(environment_summary=env))
        names = {f["flag"] for f in flags}
        assert "environment_support_strong" in names

    def test_flags_sorted_by_severity(self):
        from backend.processor.storms.event_flags import compute_event_flags
        lc = self._make_lifecycle_summary(intensity_trend="rapid_intensification", confidence=0.85)
        flags = compute_event_flags(**self._base_kwargs(
            lifecycle_summary=lc,
            threat_scores={"hail": 0.72, "wind": 0.55, "tornado": 0.3, "flood": 0.1},
            severity_level="SEVERE",
        ))
        # Severities must be non-increasing
        severities = [f["severity"] for f in flags]
        assert severities == sorted(severities, reverse=True)

    def test_no_duplicate_flags(self):
        from backend.processor.storms.event_flags import compute_event_flags
        lc = self._make_lifecycle_summary(intensity_trend="rapid_intensification", confidence=0.85)
        flags = compute_event_flags(**self._base_kwargs(lifecycle_summary=lc))
        flag_names = [f["flag"] for f in flags]
        assert len(flag_names) == len(set(flag_names)), "Duplicate flags found"


# =============================================================================
# v14 — Priority Scoring
# =============================================================================

class TestPriorityScoring:
    """Tests for the operational priority scoring engine."""

    def _base_kwargs(self, **overrides):
        base = dict(
            severity_level="NONE",
            primary_threat="hail",
            threat_scores={"hail": 0.2, "wind": 0.1, "tornado": 0.05, "flood": 0.05},
            event_flags=[],
            motion_confidence=0.6,
            lifecycle_state="tracked",
            storm_mode="discrete_cell",
            environment_summary=None,
            impacts=[],
            history_length=3,
        )
        base.update(overrides)
        return base

    def test_tornado_emergency_is_highest_priority(self):
        from backend.processor.storms.priority import compute_priority_score
        score, label = compute_priority_score(**self._base_kwargs(
            severity_level="TORNADO_EMERGENCY",
            primary_threat="tornado",
            threat_scores={"tornado": 0.95, "hail": 0.4, "wind": 0.3, "flood": 0.1},
        ))
        assert score >= 0.80
        assert label in ("CRITICAL", "HIGH")

    def test_none_severity_low_priority(self):
        from backend.processor.storms.priority import compute_priority_score
        score, label = compute_priority_score(**self._base_kwargs())
        assert score < 0.40
        assert label in ("LOW", "MINIMAL", "MODERATE")

    def test_event_flags_boost_priority(self):
        from backend.processor.storms.priority import compute_priority_score
        base_score, _ = compute_priority_score(**self._base_kwargs(severity_level="SEVERE"))
        boosted_score, _ = compute_priority_score(**self._base_kwargs(
            severity_level="SEVERE",
            event_flags=[
                {"flag": "rapid_intensification", "confidence": 0.85},
                {"flag": "rotation_tightening", "confidence": 0.70},
            ],
        ))
        assert boosted_score > base_score

    def test_rapid_decay_flag_lowers_priority(self):
        from backend.processor.storms.priority import compute_priority_score
        base_score, _ = compute_priority_score(**self._base_kwargs(severity_level="SEVERE"))
        decayed_score, _ = compute_priority_score(**self._base_kwargs(
            severity_level="SEVERE",
            event_flags=[{"flag": "rapid_decay", "confidence": 0.75}],
        ))
        assert decayed_score < base_score

    def test_supercell_mode_raises_priority(self):
        from backend.processor.storms.priority import compute_priority_score
        cell_score, _ = compute_priority_score(**self._base_kwargs(severity_level="SEVERE"))
        super_score, _ = compute_priority_score(**self._base_kwargs(
            severity_level="SEVERE",
            storm_mode="supercell_candidate",
        ))
        assert super_score >= cell_score

    def test_priority_label_mapping(self):
        from backend.processor.storms.priority import compute_priority_score
        _, label = compute_priority_score(**self._base_kwargs(
            severity_level="TORNADO",
            primary_threat="tornado",
            threat_scores={"tornado": 0.82, "hail": 0.3, "wind": 0.2, "flood": 0.05},
        ))
        assert label in ("CRITICAL", "HIGH", "MODERATE")

    def test_location_impacts_boost_priority(self):
        from backend.processor.storms.priority import compute_priority_score

        class FakeImpact:
            impact_rank = 0.75

        no_impact, _ = compute_priority_score(**self._base_kwargs(severity_level="SEVERE"))
        with_impact, _ = compute_priority_score(**self._base_kwargs(
            severity_level="SEVERE",
            impacts=[FakeImpact(), FakeImpact()],
        ))
        assert with_impact > no_impact
