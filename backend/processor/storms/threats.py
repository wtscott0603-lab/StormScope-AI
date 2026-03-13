from __future__ import annotations

import numpy as np

from backend.processor.analysis.base import SweepArrays
from backend.processor.analysis.rotation import detect_rotation_couplets
from backend.processor.processing.velocity import derive_storm_relative_velocity
from backend.processor.storms.types import StormDetection, StormImpact
from backend.processor.analysis.utils import haversine_km


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _score(value: float | None, low: float, high: float) -> float:
    if value is None:
        return 0.0
    if value <= low:
        return 0.0
    if value >= high:
        return 1.0
    return (value - low) / (high - low)


# ---------------------------------------------------------------------------
# Convective mode classification
# ---------------------------------------------------------------------------
# Labels are deliberately humble — "candidate" / "proxy" language used where
# the evidence is indirect.  No official WDSS-II algorithm is implied.

CONVECTIVE_MODES = (
    "discrete_cell",
    "supercell_candidate",
    "bow_segment",
    "linear_segment",
    "training_rain_producer",
    "cluster_multicell",
    "unknown",
)


def classify_convective_mode(
    *,
    detection: StormDetection,
    nearby_storm_count: int = 0,
    signature_types: set[str],
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
    history: list,
) -> tuple[str, float, list[str]]:
    """Classify the convective mode of a storm cell.

    Returns (mode_label, mode_confidence, supporting_evidence_list).
    All evidence is proxy-derived from radar geometry and storm object fields.
    No official mesocyclone detection algorithm is used.
    """
    evidence: list[str] = []
    elongation = detection.elongation_ratio
    area = detection.area_km2
    core_frac = detection.core_fraction
    max_ref = detection.max_reflectivity

    has_rotation = "ROTATION" in signature_types or "TVS" in signature_types or "TDS" in signature_types
    has_bow = "BOW_ECHO" in signature_types or "BWER" in signature_types
    has_hail_core = "HAIL_LARGE" in signature_types or "HAIL_CORE" in signature_types

    # --- Supercell candidate ---
    # Criteria: rotation signature present, compact/discrete, strong core, sufficient shear proxy from history
    supercell_score = 0.0
    if has_rotation:
        supercell_score += 0.45
        evidence.append("rotation signature detected")
    if core_frac >= 0.18:
        supercell_score += 0.15
        evidence.append(f"compact reflectivity core fraction {core_frac:.0%}")
    if max_ref >= 58.0:
        supercell_score += 0.15
        evidence.append(f"high core reflectivity {max_ref:.0f} dBZ")
    if has_hail_core:
        supercell_score += 0.10
        evidence.append("hail-core signature")
    if elongation < 2.5 and area < 600.0:
        supercell_score += 0.10
        evidence.append("compact discrete footprint")
    if nearby_storm_count <= 1:
        supercell_score += 0.05
    if supercell_score >= 0.60:
        return "supercell_candidate", round(min(1.0, supercell_score), 2), evidence

    # --- Bow segment ---
    bow_score = 0.0
    if has_bow:
        bow_score += 0.55
        evidence.append("bow echo or BWER signature")
    if elongation >= 3.0:
        bow_score += 0.20
        evidence.append(f"elongated shape ({elongation:.1f}:1)")
    if motion_speed_kmh is not None and motion_speed_kmh >= 55.0:
        bow_score += 0.20
        evidence.append(f"fast motion {motion_speed_kmh:.0f} km/h")
    if bow_score >= 0.55:
        return "bow_segment", round(min(1.0, bow_score), 2), evidence

    # --- Linear segment ---
    linear_score = 0.0
    if elongation >= 3.5:
        linear_score += 0.45
        evidence.append(f"highly elongated shape ({elongation:.1f}:1)")
    if area >= 400.0:
        linear_score += 0.20
        evidence.append(f"large footprint {area:.0f} km²")
    if nearby_storm_count >= 2:
        linear_score += 0.15
        evidence.append(f"{nearby_storm_count} nearby storms suggest organized line")
    if motion_speed_kmh is not None and motion_speed_kmh >= 40.0:
        linear_score += 0.10
    if linear_score >= 0.55:
        return "linear_segment", round(min(1.0, linear_score), 2), evidence

    # --- Training rain producer ---
    # Moves slowly, relatively large area, high rain-rate-indicative mean ref
    training_score = 0.0
    if motion_speed_kmh is not None and motion_speed_kmh < 15.0:
        training_score += 0.35
        evidence.append(f"slow motion {motion_speed_kmh:.0f} km/h favors cell training")
    if detection.mean_reflectivity >= 42.0:
        training_score += 0.20
        evidence.append(f"high mean reflectivity {detection.mean_reflectivity:.0f} dBZ")
    if area >= 150.0:
        training_score += 0.15
    if len(history) >= 3:
        # Check if the centroid has moved very little over history — proxy for training
        lats = [getattr(s, "centroid_lat", None) for s in history[-3:]]
        lons = [getattr(s, "centroid_lon", None) for s in history[-3:]]
        if all(v is not None for v in lats + lons):
            from backend.processor.analysis.utils import haversine_km
            total_disp = haversine_km(float(lats[0]), float(lons[0]), float(lats[-1]), float(lons[-1]))
            if total_disp < 15.0:
                training_score += 0.25
                evidence.append("centroid has moved < 15 km over last 3 scans (quasi-stationary)")
    if training_score >= 0.55:
        return "training_rain_producer", round(min(1.0, training_score), 2), evidence

    # --- Cluster multicell ---
    if nearby_storm_count >= 3:
        cluster_score = 0.40 + min(0.30, nearby_storm_count * 0.05)
        evidence.append(f"{nearby_storm_count} nearby storms indicate cluster organisation")
        if elongation >= 2.0:
            cluster_score += 0.10
        return "cluster_multicell", round(min(1.0, cluster_score), 2), evidence

    # --- Discrete cell (default fallback) ---
    discrete_score = 0.40
    if core_frac >= 0.10:
        discrete_score += 0.15
    if max_ref >= 50.0:
        discrete_score += 0.10
    if elongation < 2.5:
        discrete_score += 0.10
    if nearby_storm_count == 0:
        discrete_score += 0.10
        evidence.append("isolated storm with no nearby companions")
    return "discrete_cell", round(min(1.0, discrete_score), 2), evidence
    if value >= high:
        return 1.0
    return (value - low) / (high - low)


def _mean_history_delta(history: list, attribute: str, limit: int = 3) -> float:
    if not history:
        return 0.0
    values = [getattr(snapshot, attribute, None) for snapshot in history[-limit:]]
    values = [float(value) for value in values if isinstance(value, (int, float))]
    if len(values) < 2:
        return 0.0
    return values[-1] - values[0]


def _history_persistence_score(history: list, limit: int = 4) -> float:
    if not history:
        return 0.0
    recent = history[-limit:]
    if len(recent) == 1:
        return 0.25
    ref_values = [float(snapshot.max_reflectivity) for snapshot in recent if snapshot.max_reflectivity is not None]
    area_values = [float(snapshot.area_km2) for snapshot in recent if snapshot.area_km2 is not None]
    threat_values = [str(snapshot.primary_threat) for snapshot in recent if getattr(snapshot, "primary_threat", None)]
    ref_stability = 1.0 - min(1.0, (float(np.std(ref_values)) / 8.0 if len(ref_values) > 1 else 0.35))
    area_stability = 1.0 - min(1.0, (float(np.std(area_values)) / max(np.mean(area_values), 35.0) if len(area_values) > 1 else 0.30))
    dominant_threat_fraction = 0.0
    if threat_values:
        dominant_threat_fraction = max(threat_values.count(value) for value in set(threat_values)) / len(threat_values)
    return _clamp(
        (min(1.0, len(recent) / limit) * 0.35)
        + (ref_stability * 0.25)
        + (area_stability * 0.20)
        + (dominant_threat_fraction * 0.20)
    )


def _sorted_threats(scores: dict[str, float]) -> list[tuple[str, float]]:
    return sorted(scores.items(), key=lambda item: item[1], reverse=True)


def compute_srv_metrics(
    detection: StormDetection,
    vel_sweep,
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
    *,
    motion_confidence: float | None = None,
) -> dict:
    if vel_sweep is None or motion_heading_deg is None or motion_speed_kmh is None or motion_speed_kmh <= 0:
        return {"available": False}

    mask = detection.gate_mask & np.isfinite(vel_sweep.values)
    if int(mask.sum()) < 4:
        return {"available": False}

    srv_values = derive_storm_relative_velocity(
        vel_sweep.values,
        vel_sweep.latitudes,
        vel_sweep.longitudes,
        site_lat=vel_sweep.site_lat,
        site_lon=vel_sweep.site_lon,
        motion_heading_deg=motion_heading_deg,
        motion_speed_kmh=motion_speed_kmh,
        nyquist_velocity=vel_sweep.nyquist_velocity,
    )[mask]
    finite_srv = srv_values[np.isfinite(srv_values)]
    if finite_srv.size == 0:
        return {"available": False}

    return {
        "available": True,
        "max_outbound_ms": round(float(np.nanmax(finite_srv)), 1),
        "max_inbound_ms": round(float(np.nanmin(finite_srv)), 1),
        "delta_v_ms": round(float(np.nanmax(finite_srv) - np.nanmin(finite_srv)), 1),
        "motion_heading_deg": round(float(motion_heading_deg), 1),
        "motion_speed_kmh": round(float(motion_speed_kmh), 1),
        "motion_confidence": round(float(motion_confidence), 2) if motion_confidence is not None else None,
        "motion_source": "storm_object_track",
        "limitations": [
            "Per-storm SRV is derived from the tracked storm-motion vector and remains sensitive to motion uncertainty."
        ],
    }


def compute_local_srv_rotation_signatures(
    detection: StormDetection,
    vel_sweep: SweepArrays | None,
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
) -> list[dict]:
    if vel_sweep is None or motion_heading_deg is None or motion_speed_kmh is None or motion_speed_kmh <= 0:
        return []
    try:
        srv_values = derive_storm_relative_velocity(
            vel_sweep.values,
            vel_sweep.latitudes,
            vel_sweep.longitudes,
            site_lat=vel_sweep.site_lat,
            site_lon=vel_sweep.site_lon,
            motion_heading_deg=motion_heading_deg,
            motion_speed_kmh=motion_speed_kmh,
            nyquist_velocity=vel_sweep.nyquist_velocity,
        )
        srv_sweep = SweepArrays(
            values=srv_values,
            latitudes=vel_sweep.latitudes,
            longitudes=vel_sweep.longitudes,
            azimuths=vel_sweep.azimuths,
            ranges_km=vel_sweep.ranges_km,
            site_lat=vel_sweep.site_lat,
            site_lon=vel_sweep.site_lon,
            nyquist_velocity=vel_sweep.nyquist_velocity,
        )
        return collect_associated_signatures(detection, [_marker_to_signature_dict(marker) for marker in detect_rotation_couplets(srv_sweep)])
    except Exception:
        return []


def _marker_to_signature_dict(marker) -> dict:
    return {
        "signature_type": marker.signature_type,
        "severity": marker.severity,
        "lat": marker.lat,
        "lon": marker.lon,
        "radius_km": marker.radius_km,
        "label": marker.label,
        "description": marker.description,
        "confidence": marker.confidence,
        "metrics": marker.metrics,
    }


def collect_associated_signatures(detection: StormDetection, signatures: list[dict]) -> list[dict]:
    associated: list[dict] = []
    for signature in signatures:
        lat = signature.get("lat")
        lon = signature.get("lon")
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue
        distance = haversine_km(detection.centroid_lat, detection.centroid_lon, float(lat), float(lon))
        if distance <= max(18.0, detection.radius_km * 2.2):
            associated.append(signature)
    return associated


def compute_threats(
    *,
    detection: StormDetection,
    history: list,
    associated_signatures: list[dict],
    environment_summary: dict | None,
    srv_metrics: dict | None,
    motion_speed_kmh: float | None,
    motion_heading_deg: float | None = None,
    match_score: float | None = None,
    motion_confidence: float | None = None,
    operational_context: dict | None = None,
    volume_metrics: dict | None = None,
    nearby_storm_count: int = 0,
    track_uncertainty_km: float = 5.0,
) -> dict:
    signature_types = {signature.get("signature_type") for signature in associated_signatures}
    max_signature_severity = "NONE"
    severity_rank = {"TORNADO_EMERGENCY": 5, "TORNADO": 4, "SEVERE": 3, "MARGINAL": 2, "NONE": 1}
    for signature in associated_signatures:
        severity = signature.get("severity", "NONE")
        if severity_rank.get(severity, 0) > severity_rank.get(max_signature_severity, 0):
            max_signature_severity = severity

    # Convective mode classification (proxy-derived from radar geometry and signatures)
    storm_mode, storm_mode_confidence, storm_mode_evidence = classify_convective_mode(
        detection=detection,
        nearby_storm_count=nearby_storm_count,
        signature_types=signature_types,
        motion_heading_deg=motion_heading_deg,
        motion_speed_kmh=motion_speed_kmh,
        history=history,
    )

    history_ref_delta = _mean_history_delta(history, "max_reflectivity")
    history_area_delta = _mean_history_delta(history, "area_km2")
    history_confidence_delta = _mean_history_delta(history, "confidence")

    hail_support = environment_summary.get("hail_favorability", 0.0) if environment_summary else 0.0
    wind_support = environment_summary.get("wind_favorability", 0.0) if environment_summary else 0.0
    tornado_support = environment_summary.get("tornado_favorability", 0.0) if environment_summary else 0.0
    heavy_rain_support = environment_summary.get("heavy_rain_favorability", 0.0) if environment_summary else 0.0
    convective_signal = environment_summary.get("convective_signal", 0.0) if environment_summary else 0.0
    intensification_signal = environment_summary.get("intensification_signal", 0.0) if environment_summary else 0.0
    weakening_signal = environment_summary.get("weakening_signal", 0.0) if environment_summary else 0.0
    forecast_qpf_mm = environment_summary.get("forecast_qpf_mm") if environment_summary else None
    cape = environment_summary.get("cape_jkg") if environment_summary else None
    shear_06 = environment_summary.get("bulk_shear_06km_kt") if environment_summary else None
    shear_01 = environment_summary.get("bulk_shear_01km_kt") if environment_summary else None
    srh_surface_925 = environment_summary.get("srh_surface_925hpa_m2s2") if environment_summary else None
    dcape = environment_summary.get("dcape_jkg") if environment_summary else None
    freezing_level = environment_summary.get("freezing_level_m") if environment_summary else None
    lapse_rate = environment_summary.get("lapse_rate_midlevel_cpkm") if environment_summary else None
    lcl_m = environment_summary.get("lcl_m") if environment_summary else None
    env_confidence = environment_summary.get("environment_confidence", 0.0) if environment_summary else 0.0
    env_freshness_minutes = environment_summary.get("environment_freshness_minutes") if environment_summary else None
    ahead_delta = environment_summary.get("environment_ahead_delta", {}) if environment_summary else {}
    echo_tops_km = volume_metrics.get("max_echo_tops_km") if volume_metrics else None
    vil_kgm2 = volume_metrics.get("max_vil_kgm2") if volume_metrics else None
    vil_density_gm3 = volume_metrics.get("max_vil_density_gm3") if volume_metrics else None
    kdp_degkm = volume_metrics.get("max_kdp_degkm") if volume_metrics else None
    rain_rate_mmhr = volume_metrics.get("max_rain_rate_mmhr") if volume_metrics else None
    qpe_1h_mm = volume_metrics.get("max_qpe_1h_mm") if volume_metrics else None
    dominant_hydrometeor = volume_metrics.get("dominant_hydrometeor") if volume_metrics else None
    spc_context = (operational_context or {}).get("spc", {})
    lsr_context = (operational_context or {}).get("lsr", {})
    md_context = (operational_context or {}).get("md", {})
    watch_context = (operational_context or {}).get("watch", {})
    persistence_score = _history_persistence_score(history)
    motion_confidence_value = motion_confidence if motion_confidence is not None else (0.28 if motion_speed_kmh else 0.0)
    core_fraction = max(0.0, min(1.0, getattr(detection, "core_fraction", 0.0)))
    core_max_reflectivity = float(getattr(detection, "core_max_reflectivity", detection.max_reflectivity))
    env_staleness_penalty = _score(env_freshness_minutes, 45.0, 180.0)

    # Low cloud-base / LCL influence on tornado potential.
    # Low LCL (< 800 m) is a proxy for low cloud base, which is associated with
    # higher tornado probability in supercell environments.  High LCL (> 2000 m)
    # suppresses tornado potential.  Field is proxy-derived from surface T/Td.
    lcl_tornado_bonus = 0.0
    if lcl_m is not None:
        if lcl_m < 800.0:
            lcl_tornado_bonus = 0.08  # low cloud base — favourable
        elif lcl_m < 1400.0:
            lcl_tornado_bonus = 0.04  # marginal
        # lcl_m >= 2000 m contributes 0 or could penalise, but we keep it at 0
        # to avoid over-penalising marginal supercell environments

    # Supercell-mode boost to tornado and hail scores
    supercell_mode_boost = 0.06 if storm_mode == "supercell_candidate" else 0.0
    linear_mode_wind_boost = 0.05 if storm_mode in ("bow_segment", "linear_segment") else 0.0
    training_mode_flood_boost = 0.04 if storm_mode == "training_rain_producer" else 0.0

    hail_base_score = (
        (_score(detection.max_reflectivity, 46.0, 65.0) * 0.10)
        + (_score(core_max_reflectivity, 52.0, 68.0) * 0.05)
        + (core_fraction * 0.03)
        + (_score(cape, 800.0, 3000.0) * 0.06)
        + (_score(shear_06, 25.0, 60.0) * 0.05)
        + (_score(lapse_rate, 6.3, 8.5) * 0.05)
        + ((1.0 - _score(freezing_level, 3200.0, 5200.0)) * 0.04)
        + (_score(vil_kgm2, 20.0, 60.0) * 0.01)
        + (_score(vil_density_gm3, 2.0, 5.0) * 0.18)
        + (_score(kdp_degkm, 1.0, 4.0) * 0.06)
        + (_score(echo_tops_km, 7.0, 15.0) * 0.02)
        + supercell_mode_boost  # supercell mode raises hail potential slightly
    )
    hail_bonus_score = (
        (0.05 if dominant_hydrometeor == "hail" else 0.0)
        + (0.17 if "HAIL_LARGE" in signature_types or "HAIL_CORE" in signature_types else 0.0)
        + (hail_support * 0.09)
        + (_score(spc_context.get("hail_probability"), 15.0, 45.0) * 0.03)
        + (persistence_score * 0.01)
    )
    hail_risk = _clamp(hail_base_score + hail_bonus_score)
    wind_base_score = (
        (_score(detection.elongation_ratio, 1.8, 4.8) * 0.12)
        + (_score(motion_speed_kmh, 20.0, 85.0) * 0.08)
        + (_score(shear_06, 20.0, 55.0) * 0.09)
        + (_score(dcape, 400.0, 1400.0) * 0.14)
        + (_score(echo_tops_km, 8.0, 16.0) * 0.04)
        + (_score(rain_rate_mmhr, 10.0, 50.0) * 0.04)
        + (wind_support * 0.09)
        + linear_mode_wind_boost  # bow/linear mode raises damaging wind concern
    )
    linear_severe_bonus = 0.0
    if (
        detection.elongation_ratio >= 3.5
        and motion_speed_kmh is not None
        and motion_speed_kmh >= 65.0
    ):
        linear_severe_bonus = 0.08
        if (shear_06 is not None and shear_06 >= 35.0) or (dcape is not None and dcape >= 800.0):
            linear_severe_bonus += 0.07
    wind_bonus_score = (
        (0.14 if "BOW_ECHO" in signature_types or "BWER" in signature_types else 0.0)
        + (_score(spc_context.get("wind_probability"), 15.0, 45.0) * 0.05)
        + (_score(watch_context.get("wind_watch_rank"), 1.0, 3.0) * 0.05)
        + (_score(lsr_context.get("nearby_reports"), 1.0, 5.0) * 0.03)
        + (motion_confidence_value * 0.03)
        + (persistence_score * 0.02)
        + linear_severe_bonus
    )
    wind_risk = _clamp(wind_base_score + wind_bonus_score)
    tornado_signature_floor = 0.0
    if "ROTATION" in signature_types:
        tornado_signature_floor = max(tornado_signature_floor, 0.30)
    if "TVS" in signature_types:
        tornado_signature_floor = max(tornado_signature_floor, 0.52)
    if "TDS" in signature_types:
        tornado_signature_floor = max(tornado_signature_floor, 0.72)
    tornado_base_score = (
        (_score((srv_metrics or {}).get("delta_v_ms"), 20.0, 65.0) * 0.24)
        + (_score(shear_01, 10.0, 30.0) * 0.12)
        + (_score(srh_surface_925, 75.0, 250.0) * 0.14)
        + (_score(cape, 500.0, 2500.0) * 0.08)
        + (_score(core_max_reflectivity, 48.0, 66.0) * 0.05)
        + (_score(echo_tops_km, 8.0, 15.0) * 0.03)
        + (tornado_support * 0.11)
        + (_score(spc_context.get("tornado_probability"), 5.0, 25.0) * 0.08)
        + (_score(md_context.get("active_discussions"), 1.0, 3.0) * 0.03)
        + (_score(watch_context.get("tornado_watch_rank"), 1.0, 3.0) * 0.06)
        + (motion_confidence_value * 0.04)
        + (persistence_score * 0.05)
        + lcl_tornado_bonus   # low cloud-base proxy bonus (labelled as proxy in reasoning)
        + supercell_mode_boost  # supercell candidate mode raises tornado potential
    )
    tornado_risk = _clamp(max(tornado_base_score, tornado_signature_floor))
    flood_base_score = (
        (_score(detection.area_km2, 35.0, 250.0) * 0.10)
        + (_score(detection.mean_reflectivity, 35.0, 58.0) * 0.09)
        + (_score(forecast_qpf_mm, 2.0, 18.0) * 0.09)
        + (_score(rain_rate_mmhr, 8.0, 45.0) * 0.06)
        + (_score(qpe_1h_mm, 15.0, 60.0) * 0.22)
        + (_score(kdp_degkm, 0.5, 3.0) * 0.08)
        + (heavy_rain_support * 0.08)
        + training_mode_flood_boost  # training rain producer mode raises flood concern
    )
    flood_bonus_score = (
        ((_score(20.0 - (motion_speed_kmh or 20.0), 0.0, 15.0)) * 0.10)
        + (_score(lsr_context.get("nearby_reports"), 1.0, 6.0) * 0.05)
        + (persistence_score * 0.04)
    )
    flood_risk = _clamp(flood_base_score + flood_bonus_score)

    risks = {
        "hail": round(hail_risk, 2),
        "wind": round(wind_risk, 2),
        "tornado": round(tornado_risk, 2),
        "flood": round(flood_risk, 2),
    }
    primary_threat = max(risks, key=risks.get)
    secondary_threats = [hazard for hazard, score in _sorted_threats(risks)[1:] if score >= 0.35]

    if max_signature_severity == "TORNADO_EMERGENCY" or tornado_risk >= 0.92:
        severity_level = "TORNADO_EMERGENCY"
    elif max_signature_severity == "TORNADO" or tornado_risk >= 0.72:
        severity_level = "TORNADO"
    elif max(risks.values()) >= 0.55:
        severity_level = "SEVERE"
    elif max(risks.values()) >= 0.30:
        severity_level = "MARGINAL"
    else:
        severity_level = "NONE"

    if history_ref_delta >= 3.0 or history_area_delta >= 15.0:
        trend = "strengthening"
    elif history_ref_delta <= -3.0 or history_area_delta <= -15.0:
        trend = "weakening"
    else:
        trend = "steady"

    radar_growth_signal = _clamp(
        (_score(history_ref_delta, 1.5, 7.0) * 0.45)
        + (_score(history_area_delta, 5.0, 45.0) * 0.35)
        + (_score(history_confidence_delta, 0.05, 0.20) * 0.20)
    )
    radar_decay_signal = _clamp(
        (_score(-1.0 * history_ref_delta, 1.5, 7.0) * 0.50)
        + (_score(-1.0 * history_area_delta, 5.0, 45.0) * 0.35)
        + (_score(-1.0 * history_confidence_delta, 0.05, 0.20) * 0.15)
    )
    intensification_score = _clamp(
        (radar_growth_signal * 0.42)
        + (intensification_signal * 0.28)
        + (convective_signal * 0.15)
        + (persistence_score * 0.08)
        + (motion_confidence_value * 0.07)
    )
    weakening_score = _clamp(
        (radar_decay_signal * 0.42)
        + (weakening_signal * 0.30)
        + ((1.0 - convective_signal) * 0.12)
        + ((1.0 - persistence_score) * 0.08)
        + ((1.0 - motion_confidence_value) * 0.08)
    )
    disagreement_score = 0.0
    if radar_growth_signal >= 0.55 and weakening_signal >= 0.55:
        disagreement_score += 0.18
    if radar_decay_signal >= 0.55 and intensification_signal >= 0.55:
        disagreement_score += 0.18
    disagreement_score += env_staleness_penalty * 0.18
    maintenance_score = round(
        _clamp(
            ((1.0 - abs(intensification_score - weakening_score)) * 0.45)
            + (persistence_score * 0.30)
            + ((1.0 - disagreement_score) * 0.15)
            + (0.10 if trend == "steady" else 0.0)
        ),
        2,
    )

    projected_trend = trend
    if intensification_score >= 0.60 and weakening_score <= 0.42:
        projected_trend = "may strengthen"
    elif weakening_score >= 0.58 and intensification_score <= 0.42:
        projected_trend = "could weaken"
    elif abs(intensification_score - weakening_score) <= 0.12:
        projected_trend = "uncertain"

    ahead_weight = _clamp((env_confidence * 0.65) + ((1.0 - env_staleness_penalty) * 0.35))
    future_scores = dict(risks)
    future_scores["hail"] = round(
        _clamp(risks["hail"] + (intensification_score * 0.10) + (_score(ahead_delta.get("cape_jkg"), 50.0, 500.0) * 0.10 * ahead_weight)),
        2,
    )
    future_scores["wind"] = round(
        _clamp(risks["wind"] + (intensification_score * 0.07) + (_score(ahead_delta.get("bulk_shear_06km_kt"), 2.0, 10.0) * 0.10 * ahead_weight)),
        2,
    )
    future_scores["tornado"] = round(
        _clamp(
            risks["tornado"]
            + (intensification_score * 0.09)
            + (_score(ahead_delta.get("srh_surface_925hpa_m2s2"), 10.0, 120.0) * 0.12 * ahead_weight)
        ),
        2,
    )
    future_scores["flood"] = round(
        _clamp(risks["flood"] + (_score(ahead_delta.get("precipitation_mm"), 0.5, 4.0) * 0.10 * ahead_weight) + (_score(ahead_delta.get("thunder_probability_pct"), 5.0, 25.0) * 0.08 * ahead_weight)),
        2,
    )
    if projected_trend == "could weaken":
        for key in future_scores:
            future_scores[key] = round(_clamp(future_scores[key] - 0.10), 2)
    if projected_trend == "uncertain":
        for key, baseline in risks.items():
            future_scores[key] = round((future_scores[key] * 0.6) + (baseline * 0.4), 2)

    projected_primary_threat = max(future_scores, key=future_scores.get)
    projected_secondary_threats = [hazard for hazard, score in _sorted_threats(future_scores)[1:] if score >= 0.35]

    confidence = 0.30
    confidence += min(0.22, len(history) * 0.06)
    confidence += min(0.18, len(associated_signatures) * 0.04)
    if environment_summary:
        confidence += 0.08 + min(0.12, env_confidence * 0.12)
    if (srv_metrics or {}).get("available"):
        confidence += 0.08
    confidence += motion_confidence_value * 0.08
    confidence += persistence_score * 0.08
    confidence -= env_staleness_penalty * 0.12
    confidence -= disagreement_score * 0.18
    confidence = round(_clamp(confidence), 2)

    uncertainty_factors: list[str] = []
    if len(history) < 2:
        uncertainty_factors.append("Limited storm history reduces persistence confidence.")
    if motion_confidence_value < 0.45:
        uncertainty_factors.append("Storm-motion confidence is limited, so ETA and SRV guidance are less stable.")
    if environment_summary and env_confidence < 0.45:
        uncertainty_factors.append("Environmental confidence is limited, so near-path trend guidance is lower confidence.")
    if env_freshness_minutes is not None and env_freshness_minutes >= 90:
        uncertainty_factors.append("Environment guidance is becoming stale relative to the latest radar scan.")
    if disagreement_score >= 0.18:
        uncertainty_factors.append("Radar trends and downstream environment signals do not agree strongly on the short-term trend.")
    if match_score is not None and match_score < 0.30:
        uncertainty_factors.append("Storm association quality is modest, which lowers motion and trend confidence.")

    data_quality_score = round(
        _clamp(
            (motion_confidence_value * 0.30)
            + (persistence_score * 0.25)
            + (env_confidence * 0.25)
            + ((1.0 - env_staleness_penalty) * 0.20)
        ),
        2,
    )
    forecast_stability_score = round(
        _clamp(
            (persistence_score * 0.35)
            + (motion_confidence_value * 0.25)
            + ((1.0 - disagreement_score) * 0.20)
            + (ahead_weight * 0.20)
        ),
        2,
    )
    projected_confidence = round(
        _clamp(
            (confidence * 0.40)
            + (env_confidence * 0.20)
            + (motion_confidence_value * 0.20)
            + (persistence_score * 0.20)
            - (env_staleness_penalty * 0.10)
            - (disagreement_score * 0.12)
        ),
        2,
    )

    reasoning_factors = [
        f"Max reflectivity {detection.max_reflectivity:.0f} dBZ with {detection.area_km2:.0f} km2 footprint",
        f"Storm shape ratio {detection.elongation_ratio:.1f}:1",
    ]
    forecast_reasoning_factors: list[str] = []
    if motion_speed_kmh is not None:
        reasoning_factors.append(f"Motion estimate {motion_speed_kmh:.0f} km/h")
    if motion_confidence is not None:
        reasoning_factors.append(f"Motion confidence {motion_confidence_value:.2f}")
    if abs(history_ref_delta) >= 1.0:
        reasoning_factors.append(f"Recent max reflectivity trend {history_ref_delta:+.1f} dBZ over the last scans")
    if abs(history_area_delta) >= 5.0:
        reasoning_factors.append(f"Footprint trend {history_area_delta:+.0f} km2 over the last scans")
    if core_fraction >= 0.08:
        reasoning_factors.append(f"Reflectivity core fraction {core_fraction * 100:.0f}% above severe-core threshold")
    if associated_signatures:
        reasoning_factors.append("Linked signatures: " + ", ".join(sorted({str(signature.get('signature_type')) for signature in associated_signatures})))
    if echo_tops_km is not None:
        reasoning_factors.append(f"Echo tops peak near {echo_tops_km:.1f} km")
    if vil_kgm2 is not None:
        reasoning_factors.append(f"VIL peaks near {vil_kgm2:.0f} kg/m²")
    if vil_density_gm3 is not None:
        reasoning_factors.append(f"VIL density peaks near {vil_density_gm3:.1f} g/m³")
    if kdp_degkm is not None:
        reasoning_factors.append(f"KDP peaks near {kdp_degkm:.1f} deg/km")
    if rain_rate_mmhr is not None:
        reasoning_factors.append(f"Rain-rate estimate peaks near {rain_rate_mmhr:.0f} mm/h")
    if qpe_1h_mm is not None:
        reasoning_factors.append(f"Rolling 1h radar accumulation peaks near {qpe_1h_mm:.0f} mm")
    if dominant_hydrometeor:
        reasoning_factors.append(f"Rules-based hydrometeor class favors {dominant_hydrometeor.replace('_', ' ')}")
    if linear_severe_bonus >= 0.15:
        reasoning_factors.append(
            "Fast linear storm mode plus supportive shear or downdraft potential raises damaging wind concern even before a bow signature fully develops"
        )
    elif linear_severe_bonus > 0.0:
        reasoning_factors.append(
            "Fast linear storm mode supports damaging wind concern even before a bow signature fully develops"
        )
    if environment_summary:
        if cape is not None:
            reasoning_factors.append(f"Model CAPE near storm {cape:.0f} J/kg")
        if shear_06 is not None:
            reasoning_factors.append(f"0-6 km bulk shear near storm {shear_06:.0f} kt")
        if srh_surface_925 is not None:
            reasoning_factors.append(f"Surface-to-925 hPa helicity proxy near storm {srh_surface_925:.0f} m2/s2")
        if dcape is not None:
            reasoning_factors.append(f"Estimated DCAPE proxy near storm {dcape:.0f} J/kg")
        reasoning_factors.append(environment_summary.get("ahead_trend", "environment changes are modest ahead"))
        if environment_summary.get("forecast_probability_of_thunder") is not None:
            reasoning_factors.append(
                f"Thunder probability now/ahead {environment_summary.get('forecast_probability_of_thunder', 0):.0f}%/{environment_summary.get('ahead_probability_of_thunder', 0):.0f}%"
            )
        if env_freshness_minutes is not None:
            reasoning_factors.append(f"Environment data age {env_freshness_minutes} minutes")
        if environment_summary.get("limitation"):
            reasoning_factors.append(f"Environment limits: {environment_summary['limitation']}")
        forecast_reasoning_factors.append(environment_summary.get("ahead_trend", "environment support is roughly steady"))
    if spc_context.get("category"):
        reasoning_factors.append(f"SPC context: {spc_context['category']}")
        forecast_reasoning_factors.append(f"Storm remains within SPC {spc_context['category'].lower()} severe context")
    if watch_context.get("watch_type"):
        watch_label = str(watch_context["watch_type"])
        if watch_context.get("pds"):
            watch_label = f"PDS {watch_label}"
        reasoning_factors.append(f"Watch context: {watch_label}")
        forecast_reasoning_factors.append(f"Official watch context remains in place: {watch_label.lower()}")
    if md_context.get("active_discussions"):
        reasoning_factors.append(f"Nearby mesoscale discussions: {md_context['active_discussions']}")
        forecast_reasoning_factors.append("Mesoscale discussion coverage supports elevated situational concern")
    if lsr_context.get("nearby_reports"):
        reasoning_factors.append(f"Nearby local storm reports: {lsr_context['nearby_reports']}")
        forecast_reasoning_factors.append("Recent nearby storm reports increase confidence that radar signatures are operationally meaningful")
    if (srv_metrics or {}).get("available"):
        reasoning_factors.append(f"SRV delta-V {srv_metrics['delta_v_ms']:.0f} m/s")
    if persistence_score >= 0.40:
        forecast_reasoning_factors.append(f"Recent storm persistence score {persistence_score:.2f} supports continuity in the near-term forecast")
    if uncertainty_factors:
        forecast_reasoning_factors.extend(uncertainty_factors[:2])

    if projected_trend == "may strengthen":
        near_term_expectation = (
            "Recent radar trends and the environment ahead suggest some additional strengthening is possible if the storm holds its current organization."
        )
        forecast_reasoning_factors.append("Radar growth and downstream environment support are aligned enough to support modest strengthening potential")
    elif projected_trend == "could weaken":
        near_term_expectation = (
            "Radar and environment signals both suggest weakening is possible if the storm continues into less supportive conditions."
        )
        forecast_reasoning_factors.append("The downstream environment appears less favorable than the current storm position")
    elif projected_trend == "uncertain":
        near_term_expectation = "Competing radar and environment signals keep the short-term trend uncertain."
        forecast_reasoning_factors.append("Radar persistence and environmental support do not agree strongly enough for a higher-confidence trend")
    else:
        near_term_expectation = "The storm is most likely to maintain something close to its current character in the near term."
        forecast_reasoning_factors.append("Recent radar history and near-path environment are broadly consistent with maintenance")

    if uncertainty_factors:
        near_term_expectation += f" Confidence is tempered because {uncertainty_factors[0].lower()}"
    narrative_parts = [
        f"{severity_level.lower().replace('_', ' ')} concern with {primary_threat} as the primary threat."
    ]
    if "TDS" in signature_types:
        narrative_parts.append("A debris signature is collocated with the storm, which supports strong tornado concern but does not remove forecast uncertainty.")
    elif "TVS" in signature_types:
        narrative_parts.append("A TVS-type rotation signature is present, keeping tornado concern elevated.")
    elif "ROTATION" in signature_types:
        narrative_parts.append("Persistent rotation is present, but the tornado signal still depends on whether organization is maintained.")
    if cape is not None or shear_06 is not None or srh_surface_925 is not None:
        env_bits: list[str] = []
        if cape is not None:
            env_bits.append(f"CAPE near the storm is about {cape:.0f} J/kg")
        if shear_06 is not None:
            env_bits.append(f"0-6 km shear is near {shear_06:.0f} kt")
        if srh_surface_925 is not None:
            env_bits.append(f"surface-to-925 hPa helicity proxy is near {srh_surface_925:.0f} m2/s2")
        narrative_parts.append(". ".join(env_bits) + ".")
    if spc_context.get("category"):
        narrative_parts.append(f"The storm remains within SPC {str(spc_context['category']).lower()} context.")
    if watch_context.get("watch_type"):
        watch_text = str(watch_context["watch_type"]).lower()
        if watch_context.get("pds"):
            watch_text = f"PDS {watch_text}"
        narrative_parts.append(f"Official watch context also supports concern: {watch_text}.")
    narrative_parts.append(
        f"Recent radar trends classify the storm as {trend}, and the short-term projection is {projected_trend}."
    )
    confidence_phrase = "low"
    if projected_confidence >= 0.7:
        confidence_phrase = "high"
    elif projected_confidence >= 0.45:
        confidence_phrase = "moderate"
    narrative_parts.append(
        f"Projected confidence is {confidence_phrase}; this remains probabilistic and depends on the storm maintaining its current structure."
    )
    if uncertainty_factors:
        narrative_parts.append(f"Key uncertainty: {uncertainty_factors[0]}")
    # Add mode and LCL to reasoning factors
    if storm_mode != "unknown":
        reasoning_factors.append(
            f"Convective mode classified as {storm_mode.replace('_', ' ')} "
            f"(proxy, confidence {storm_mode_confidence:.2f}); "
            + (", ".join(storm_mode_evidence[:2]) if storm_mode_evidence else "geometry-based")
        )
    if lcl_tornado_bonus > 0.0 and lcl_m is not None:
        reasoning_factors.append(
            f"LCL proxy estimate {lcl_m:.0f} m AGL — low cloud base raises tornado potential (proxy-derived from surface T/Td)"
        )
    elif lcl_m is not None and lcl_m >= 2000.0:
        reasoning_factors.append(
            f"LCL proxy estimate {lcl_m:.0f} m AGL — high cloud base is less favourable for tornadoes (proxy-derived)"
        )

    narrative = " ".join(part.strip() for part in narrative_parts if part).strip()

    # Per-threat component score breakdown — exposes the contributing factors as
    # labelled sub-scores so the UI can show "why is hail risk 0.72?" without
    # the user reading raw reasoning_factors strings.
    threat_component_breakdown: dict[str, dict[str, float]] = {
        "hail": {
            "max_reflectivity": round(_score(detection.max_reflectivity, 46.0, 65.0) * 0.10, 3),
            "core_reflectivity": round(_score(core_max_reflectivity, 52.0, 68.0) * 0.05, 3),
            "core_fraction": round(core_fraction * 0.03, 3),
            "cape": round(_score(cape, 800.0, 3000.0) * 0.06, 3),
            "shear_06km": round(_score(shear_06, 25.0, 60.0) * 0.05, 3),
            "lapse_rate": round(_score(lapse_rate, 6.3, 8.5) * 0.05, 3),
            "freezing_level": round((1.0 - _score(freezing_level, 3200.0, 5200.0)) * 0.04, 3),
            "vil_density": round(_score(vil_density_gm3, 2.0, 5.0) * 0.18, 3),
            "kdp": round(_score(kdp_degkm, 1.0, 4.0) * 0.06, 3),
            "echo_tops": round(_score(echo_tops_km, 7.0, 15.0) * 0.02, 3),
            "hail_signatures": round(0.17 if "HAIL_LARGE" in signature_types or "HAIL_CORE" in signature_types else 0.0, 3),
            "environment_support": round(hail_support * 0.09, 3),
            "supercell_mode_boost": round(supercell_mode_boost, 3),
        },
        "wind": {
            "elongation": round(_score(detection.elongation_ratio, 1.8, 4.8) * 0.12, 3),
            "motion_speed": round(_score(motion_speed_kmh, 20.0, 85.0) * 0.08, 3),
            "shear_06km": round(_score(shear_06, 20.0, 55.0) * 0.09, 3),
            "dcape": round(_score(dcape, 400.0, 1400.0) * 0.14, 3),
            "echo_tops": round(_score(echo_tops_km, 8.0, 16.0) * 0.04, 3),
            "rain_rate": round(_score(rain_rate_mmhr, 10.0, 50.0) * 0.04, 3),
            "environment_support": round(wind_support * 0.09, 3),
            "bow_bwer_signature": round(0.14 if "BOW_ECHO" in signature_types or "BWER" in signature_types else 0.0, 3),
            "linear_severe_bonus": round(linear_severe_bonus, 3),
            "linear_mode_boost": round(linear_mode_wind_boost, 3),
        },
        "tornado": {
            "srv_delta_v": round(_score((srv_metrics or {}).get("delta_v_ms"), 20.0, 65.0) * 0.24, 3),
            "shear_01km": round(_score(shear_01, 10.0, 30.0) * 0.12, 3),
            "srh_proxy": round(_score(srh_surface_925, 75.0, 250.0) * 0.14, 3),
            "cape": round(_score(cape, 500.0, 2500.0) * 0.08, 3),
            "core_reflectivity": round(_score(core_max_reflectivity, 48.0, 66.0) * 0.05, 3),
            "tornado_support": round(tornado_support * 0.11, 3),
            "spc_context": round(_score(spc_context.get("tornado_probability"), 5.0, 25.0) * 0.08, 3),
            "watch_context": round(_score(watch_context.get("tornado_watch_rank"), 1.0, 3.0) * 0.06, 3),
            "persistence": round(persistence_score * 0.05, 3),
            "lcl_bonus": round(lcl_tornado_bonus, 3),
            "supercell_mode_boost": round(supercell_mode_boost, 3),
            "signature_floor": round(tornado_signature_floor, 3),
        },
        "flood": {
            "area": round(_score(detection.area_km2, 35.0, 250.0) * 0.10, 3),
            "mean_reflectivity": round(_score(detection.mean_reflectivity, 35.0, 58.0) * 0.09, 3),
            "forecast_qpf": round(_score(forecast_qpf_mm, 2.0, 18.0) * 0.09, 3),
            "rain_rate": round(_score(rain_rate_mmhr, 8.0, 45.0) * 0.06, 3),
            "qpe_1h": round(_score(qpe_1h_mm, 15.0, 60.0) * 0.22, 3),
            "kdp": round(_score(kdp_degkm, 0.5, 3.0) * 0.08, 3),
            "environment_support": round(heavy_rain_support * 0.08, 3),
            "slow_motion_bonus": round(_score(20.0 - (motion_speed_kmh or 20.0), 0.0, 15.0) * 0.10, 3),
            "training_mode_boost": round(training_mode_flood_boost, 3),
        },
    }

    # Top supporting and limiting factors per threat (for UI display)
    def _top_components(components: dict[str, float], n: int = 3) -> list[str]:
        return [k for k, v in sorted(components.items(), key=lambda x: x[1], reverse=True) if v > 0.0][:n]

    def _missing_components(components: dict[str, float], n: int = 2) -> list[str]:
        """Fields that scored 0 but could have contributed."""
        return [k for k, v in sorted(components.items(), key=lambda x: x[1]) if v == 0.0][:n]

    threat_top_reasons = {
        threat: _top_components(threat_component_breakdown[threat]) for threat in threat_component_breakdown
    }
    threat_limiting_factors = {
        threat: _missing_components(threat_component_breakdown[threat]) for threat in threat_component_breakdown
    }

    return {
        "trend": trend,
        "confidence": confidence,
        "primary_threat": primary_threat,
        "secondary_threats": secondary_threats,
        "severity_level": severity_level,
        "threat_scores": risks,
        "threat_component_breakdown": threat_component_breakdown,
        "threat_top_reasons": threat_top_reasons,
        "threat_limiting_factors": threat_limiting_factors,
        "reasoning_factors": reasoning_factors,
        "projected_trend": projected_trend,
        "near_term_expectation": near_term_expectation,
        "narrative": narrative,
        "storm_mode": storm_mode,
        "storm_mode_confidence": storm_mode_confidence,
        "storm_mode_evidence": storm_mode_evidence,
        "track_uncertainty_km": round(track_uncertainty_km, 1),
        "prediction_summary": {
            "intensification_score": round(intensification_score, 2),
            "weakening_score": round(weakening_score, 2),
            "maintenance_score": maintenance_score,
            "projected_trend": projected_trend,
            "projected_primary_threat": projected_primary_threat,
            "projected_secondary_threats": projected_secondary_threats,
            "projected_confidence": projected_confidence,
            "projected_threat_scores": future_scores,
            "forecast_reasoning_factors": forecast_reasoning_factors,
            "environment_confidence": round(env_confidence, 2) if environment_summary else None,
            "motion_confidence": round(motion_confidence_value, 2),
            "persistence_score": round(persistence_score, 2),
            "forecast_stability_score": forecast_stability_score,
            "data_quality_score": data_quality_score,
            "uncertainty_factors": uncertainty_factors,
        },
    }


def build_forecast_path(
    *,
    centroid_lat: float,
    centroid_lon: float,
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
    horizon_minutes: int,
    step_minutes: int,
    destination_point_func,
) -> list[dict]:
    if motion_heading_deg is None or motion_speed_kmh is None or motion_speed_kmh < 8.0:
        return []
    points: list[dict] = []
    for eta_minutes in range(step_minutes, horizon_minutes + step_minutes, step_minutes):
        distance_km = motion_speed_kmh * (eta_minutes / 60.0)
        lat, lon = destination_point_func(centroid_lat, centroid_lon, motion_heading_deg, distance_km)
        points.append({"lat": round(lat, 4), "lon": round(lon, 4), "eta_minutes": eta_minutes, "label": f"+{eta_minutes}m"})
    return points


def compute_location_impacts(
    *,
    centroid_lat: float,
    centroid_lon: float,
    radius_km: float,
    forecast_path: list[dict],
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
    locations: list,
    primary_threat: str,
    trend: str,
    confidence: float,
    threat_scores: dict[str, float] | None = None,
    prediction_summary: dict | None = None,
    environment_summary: dict | None = None,
    operational_context: dict | None = None,
) -> list[StormImpact]:
    if motion_heading_deg is None or motion_speed_kmh is None or motion_speed_kmh < 8.0:
        return []

    current_scores = threat_scores or {"hail": 0.0, "wind": 0.0, "tornado": 0.0, "flood": 0.0}
    projected_scores = dict((prediction_summary or {}).get("projected_threat_scores", current_scores))
    projected_primary = (prediction_summary or {}).get("projected_primary_threat", primary_threat)
    projected_secondary = list((prediction_summary or {}).get("projected_secondary_threats", []))
    projected_trend = (prediction_summary or {}).get("projected_trend", trend)
    projected_confidence = float((prediction_summary or {}).get("projected_confidence", confidence))
    motion_confidence = float((prediction_summary or {}).get("motion_confidence", confidence))
    persistence_score = float((prediction_summary or {}).get("persistence_score", confidence))
    forecast_stability_score = float((prediction_summary or {}).get("forecast_stability_score", projected_confidence))
    spc_context = dict((operational_context or {}).get("spc", {}))
    watch_context = dict((operational_context or {}).get("watch", {}))
    lsr_context = dict((operational_context or {}).get("lsr", {}))
    md_context = dict((operational_context or {}).get("md", {}))
    arrival_operational_summary = {
        "spc_category": spc_context.get("category"),
        "tornado_probability": spc_context.get("tornado_probability"),
        "wind_probability": spc_context.get("wind_probability"),
        "hail_probability": spc_context.get("hail_probability"),
        "watch_type": watch_context.get("watch_type"),
        "watch_pds": bool(watch_context.get("pds")),
        "nearby_reports": lsr_context.get("nearby_reports"),
        "active_discussions": md_context.get("active_discussions"),
    }
    impacts: list[StormImpact] = []

    for location in locations:
        distance = haversine_km(centroid_lat, centroid_lon, location.lat, location.lon)
        if distance <= radius_km:
            eta_uncertainty = max(5, int(5 + ((1.0 - motion_confidence) * 8.0)))
            details = {
                "projected_primary_threat": projected_primary,
                "projected_secondary_threats": projected_secondary,
                "projected_hail_risk": round(projected_scores.get("hail", current_scores.get("hail", 0.0)), 2),
                "projected_wind_risk": round(projected_scores.get("wind", current_scores.get("wind", 0.0)), 2),
                "projected_tornado_risk": round(projected_scores.get("tornado", current_scores.get("tornado", 0.0)), 2),
                "projected_heavy_rain_risk": round(projected_scores.get("flood", current_scores.get("flood", 0.0)), 2),
                "projected_trend": projected_trend,
                "arrival_environment_summary": (environment_summary or {}).get("ahead_trend"),
                "arrival_operational_summary": arrival_operational_summary,
                "impact_confidence": round(projected_confidence, 2),
                "eta_uncertainty_minutes": eta_uncertainty,
                "path_confidence": round(motion_confidence, 2),
                "forecast_stability_score": round(forecast_stability_score, 2),
                "environment_confidence": (environment_summary or {}).get("environment_confidence"),
                "reasoning_factors": ["Location is already within or near the present storm footprint."],
            }
            if arrival_operational_summary["watch_type"]:
                details["reasoning_factors"].append(
                    f"Official {arrival_operational_summary['watch_type']} context is already in effect for the storm area."
                )
            if arrival_operational_summary["spc_category"]:
                details["reasoning_factors"].append(
                    f"SPC {str(arrival_operational_summary['spc_category']).lower()} context supports continued local concern."
                )
            impacts.append(
                StormImpact(
                    location_id=location.location_id,
                    location_name=location.name,
                    eta_minutes_low=0,
                    eta_minutes_high=eta_uncertainty,
                    distance_km=round(distance, 1),
                    threat_at_arrival=projected_primary,
                    trend_at_arrival=projected_trend,
                    confidence=projected_confidence,
                    summary=f"{location.name} is already within or near the current storm footprint.",
                    impact_rank=round(projected_confidence, 2),
                    details=details,
                )
            )
            continue

        best_eta = None
        best_distance = None
        for point in forecast_path:
            point_distance = haversine_km(point["lat"], point["lon"], location.lat, location.lon)
            if best_distance is None or point_distance < best_distance:
                best_distance = point_distance
                best_eta = point["eta_minutes"]

        if best_distance is None or best_eta is None or best_distance > max(35.0, radius_km * 1.8):
            continue

        base_eta_spread = max(4, int(max(6.0, best_eta * 0.18)))
        uncertainty_multiplier = 1.0 + max(0.0, 0.55 - motion_confidence) + max(0.0, 0.50 - forecast_stability_score)
        eta_spread = max(base_eta_spread, int(round(base_eta_spread * uncertainty_multiplier)))
        distance_factor = max(0.45, 1.0 - (best_distance / 45.0))
        impact_confidence = round(projected_confidence * distance_factor * max(0.55, motion_confidence), 2)
        details = {
                "projected_primary_threat": projected_primary,
                "projected_secondary_threats": projected_secondary,
                "projected_hail_risk": round(projected_scores.get("hail", current_scores.get("hail", 0.0)), 2),
            "projected_wind_risk": round(projected_scores.get("wind", current_scores.get("wind", 0.0)), 2),
            "projected_tornado_risk": round(projected_scores.get("tornado", current_scores.get("tornado", 0.0)), 2),
            "projected_heavy_rain_risk": round(projected_scores.get("flood", current_scores.get("flood", 0.0)), 2),
            "projected_trend": projected_trend,
            "arrival_environment_summary": (environment_summary or {}).get("ahead_trend"),
            "arrival_operational_summary": arrival_operational_summary,
            "impact_confidence": impact_confidence,
            "eta_uncertainty_minutes": eta_spread,
            "path_confidence": round(motion_confidence, 2),
            "persistence_score": round(persistence_score, 2),
            "forecast_stability_score": round(forecast_stability_score, 2),
            "environment_confidence": (environment_summary or {}).get("environment_confidence"),
            "reasoning_factors": list((prediction_summary or {}).get("forecast_reasoning_factors", [])),
            "uncertainty_factors": list((prediction_summary or {}).get("uncertainty_factors", [])),
        }
        if arrival_operational_summary["watch_type"]:
            details["reasoning_factors"].append(
                f"Arrival window remains inside {arrival_operational_summary['watch_type']} context."
            )
        if arrival_operational_summary["spc_category"]:
            details["reasoning_factors"].append(
                f"Projected path remains within SPC {str(arrival_operational_summary['spc_category']).lower()} context."
            )
        if arrival_operational_summary["nearby_reports"]:
            details["reasoning_factors"].append(
                f"{int(arrival_operational_summary['nearby_reports'])} recent nearby storm reports support operational concern."
            )
        impacts.append(
            StormImpact(
                location_id=location.location_id,
                location_name=location.name,
                eta_minutes_low=max(0, best_eta - eta_spread),
                eta_minutes_high=best_eta + eta_spread,
                distance_km=round(best_distance, 1),
                threat_at_arrival=projected_primary,
                trend_at_arrival=projected_trend,
                confidence=impact_confidence,
                summary=(
                    f"Closest pass to {location.name} is estimated in about {best_eta} minutes. "
                    f"The leading threat at arrival remains {projected_primary} if the present motion and evolution hold."
                ),
                impact_rank=round(max(0.05, impact_confidence * (1.0 - min(best_distance, 50.0) / 50.0)), 2),
                details=details,
            )
        )

    impacts.sort(key=lambda impact: impact.impact_rank, reverse=True)
    return impacts
