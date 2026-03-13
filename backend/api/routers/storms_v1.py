from __future__ import annotations

import asyncio
import math
from fastapi import APIRouter, HTTPException, Query

from backend.api.dependencies import get_frame_store
from backend.api.schemas.storms import (
    EventFlagResponse,
    StormCompareField,
    StormCompareResponse,
    StormEnvironmentResponse,
    StormEventHistoryPoint,
    StormEventHistoryResponse,
    StormHotspotResponse,
    StormImpactResponse,
    StormNarrativeResponse,
    StormPredictionResponse,
    StormPrecomputedSummaryResponse,
    StormSummaryResponse,
    StormTrackPointResponse,
    StormTimeSeriesResponse,
    StormTimeSeriesPoint,
    ThreatComponentBreakdownResponse,
    LocationRiskEntry,
)


router = APIRouter(tags=["storms-v1"])


def _impacts_to_response(impacts, locations: dict) -> list[StormImpactResponse]:
    return [
        StormImpactResponse(
            location_id=impact.location_id,
            location_name=locations.get(impact.location_id, impact.location_id),
            eta_minutes_low=impact.eta_minutes_low,
            eta_minutes_high=impact.eta_minutes_high,
            distance_km=impact.distance_km,
            threat_at_arrival=impact.threat_at_arrival,
            trend_at_arrival=impact.trend_at_arrival,
            confidence=impact.confidence,
            summary=impact.summary,
            impact_rank=impact.impact_rank,
            details=impact.details or {},
        )
        for impact in impacts
    ]


def _build_response(storm, near_term: str, impacts, locations: dict) -> StormSummaryResponse:
    return StormSummaryResponse(
        storm_id=storm.storm_id,
        site=storm.site,
        latest_frame_id=storm.latest_frame_id,
        latest_scan_time=storm.latest_scan_time,
        created_at=storm.created_at,
        updated_at=storm.updated_at,
        status=storm.status,
        lifecycle_state=storm.lifecycle_state,
        centroid_lat=storm.centroid_lat,
        centroid_lon=storm.centroid_lon,
        area_km2=storm.area_km2,
        max_reflectivity=storm.max_reflectivity,
        mean_reflectivity=storm.mean_reflectivity,
        motion_heading_deg=storm.motion_heading_deg,
        motion_speed_kmh=storm.motion_speed_kmh,
        trend=storm.trend,
        primary_threat=storm.primary_threat,
        secondary_threats=storm.secondary_threats,
        severity_level=storm.severity_level,
        confidence=storm.confidence,
        threat_scores=storm.threat_scores,
        narrative=storm.narrative,
        reasoning_factors=storm.reasoning_factors,
        footprint_geojson=storm.footprint_geojson,
        forecast_path=storm.forecast_path,
        uncertainty_cone=getattr(storm, "uncertainty_cone", []) or [],
        storm_mode=getattr(storm, "storm_mode", "unknown") or "unknown",
        storm_mode_confidence=getattr(storm, "storm_mode_confidence", 0.0) or 0.0,
        storm_mode_evidence=getattr(storm, "storm_mode_evidence", []) or [],
        track_uncertainty_km=getattr(storm, "track_uncertainty_km", 5.0) or 5.0,
        associated_signatures=storm.associated_signatures,
        environment_summary=StormEnvironmentResponse(**storm.environment_summary) if storm.environment_summary else None,
        prediction_summary=StormPredictionResponse(**storm.prediction_summary) if storm.prediction_summary else None,
        near_term_expectation=near_term,
        impacts=_impacts_to_response(impacts, locations),
        threat_component_breakdown=getattr(storm, "threat_component_breakdown", {}) or {},
        threat_top_reasons=getattr(storm, "threat_top_reasons", {}) or {},
        threat_limiting_factors=getattr(storm, "threat_limiting_factors", {}) or {},
        lifecycle_summary=getattr(storm, "lifecycle_summary", {}) or {},
        event_flags=getattr(storm, "event_flags", []) or [],
        priority_score=getattr(storm, "priority_score", 0.0) or 0.0,
        priority_label=getattr(storm, "priority_label", "MINIMAL") or "MINIMAL",
    )


@router.get("/api/v1/storms", response_model=list[StormSummaryResponse])
async def list_storms(site: str = Query(..., min_length=4, max_length=4)) -> list[StormSummaryResponse]:
    """List active storms — 3 DB queries total regardless of storm count."""
    store = get_frame_store()
    storms = await store.list_storm_objects(site=site.upper(), include_inactive=False, limit=50)
    if not storms:
        return []
    storm_ids = [s.storm_id for s in storms]
    near_terms, impacts_by_storm, all_locations = await asyncio.gather(
        store.batch_latest_snapshots(storm_ids),
        store.batch_storm_impacts(storm_ids),
        store.list_saved_locations(),
    )
    location_names = {loc.location_id: loc.name for loc in all_locations}
    return [
        _build_response(
            storm,
            near_terms.get(storm.storm_id, ""),
            impacts_by_storm.get(storm.storm_id, []),
            location_names,
        )
        for storm in storms
    ]


@router.get("/api/v1/storms/{storm_id}", response_model=StormSummaryResponse)
async def get_storm(storm_id: str) -> StormSummaryResponse:
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    near_terms, impacts_batch, all_locations = await asyncio.gather(
        store.batch_latest_snapshots([storm_id]),
        store.batch_storm_impacts([storm_id]),
        store.list_saved_locations(),
    )
    location_names = {loc.location_id: loc.name for loc in all_locations}
    return _build_response(
        storm,
        near_terms.get(storm_id, ""),
        impacts_batch.get(storm_id, []),
        location_names,
    )


@router.get("/api/v1/storms/{storm_id}/track", response_model=list[StormTrackPointResponse])
async def get_storm_track(storm_id: str) -> list[StormTrackPointResponse]:
    store = get_frame_store()
    snapshots = await store.list_storm_snapshots(storm_id, limit=24)
    if not snapshots:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    return [
        StormTrackPointResponse(
            scan_time=snapshot.scan_time,
            centroid_lat=snapshot.centroid_lat,
            centroid_lon=snapshot.centroid_lon,
            max_reflectivity=snapshot.max_reflectivity,
            mean_reflectivity=snapshot.mean_reflectivity,
            motion_heading_deg=snapshot.motion_heading_deg,
            motion_speed_kmh=snapshot.motion_speed_kmh,
            trend=snapshot.trend,
        )
        for snapshot in snapshots
    ]


@router.get("/api/v1/storms/{storm_id}/environment", response_model=StormEnvironmentResponse)
async def get_storm_environment(storm_id: str) -> StormEnvironmentResponse:
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    environment = storm.environment_summary
    if not environment:
        latest = await store.get_latest_environment_snapshot(storm_id)
        if latest is None:
            return StormEnvironmentResponse(limitation="No environment snapshot is available for this storm yet.")
        environment = {
            "source": latest.source,
            "current_station_id": latest.station_id,
            "surface_temp_c": latest.surface_temp_c,
            "dewpoint_c": latest.dewpoint_c,
            "wind_speed_kt": latest.wind_speed_kt,
            "cape_jkg": latest.cape_jkg,
            "cin_jkg": latest.cin_jkg,
            "bulk_shear_06km_kt": latest.bulk_shear_06km_kt,
            "bulk_shear_01km_kt": latest.bulk_shear_01km_kt,
            "srh_surface_925hpa_m2s2": latest.helicity_01km,
            "dcape_jkg": latest.dcape_jkg,
            "dcape_is_proxy": latest.dcape_jkg is not None,
            "freezing_level_m": latest.freezing_level_m,
            "pwat_mm": latest.pwat_mm,
            "lapse_rate_midlevel_cpkm": latest.lapse_rate_midlevel_cpkm,
            "lcl_m": latest.lcl_m,
            "lfc_m": latest.lfc_m,
            "environment_confidence": latest.environment_confidence,
            "environment_freshness_minutes": latest.environment_freshness_minutes,
            "hail_favorability": latest.hail_favorability,
            "wind_favorability": latest.wind_favorability,
            "tornado_favorability": latest.tornado_favorability,
            "ahead_trend": latest.narrative,
            "profile_summary": dict((latest.raw_payload or {}).get("profile_summary", {})),
            "field_provenance": dict((latest.raw_payload or {}).get("field_provenance", {})),
            "source_notes": list((latest.raw_payload or {}).get("source_notes", [])),
            "limitation": "Stored environment snapshot available but richer summary was not persisted.",
        }
    return StormEnvironmentResponse(**environment)


@router.get("/api/v1/storms/{storm_id}/impacts", response_model=list[StormImpactResponse])
async def get_storm_impacts(storm_id: str) -> list[StormImpactResponse]:
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    impacts_batch, all_locations = await asyncio.gather(
        store.batch_storm_impacts([storm_id]),
        store.list_saved_locations(),
    )
    location_names = {loc.location_id: loc.name for loc in all_locations}
    return _impacts_to_response(impacts_batch.get(storm_id, []), location_names)


@router.get("/api/v1/storms/{storm_id}/narrative", response_model=StormNarrativeResponse)
async def get_storm_narrative(storm_id: str) -> StormNarrativeResponse:
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    prediction_summary = storm.prediction_summary or {}
    near_terms = await store.batch_latest_snapshots([storm_id])
    near_term = near_terms.get(storm_id) or str(prediction_summary.get("projected_trend") or "trend steady")
    return StormNarrativeResponse(
        storm_id=storm.storm_id,
        narrative=storm.narrative,
        near_term_expectation=near_term,
        confidence=storm.confidence,
        projected_confidence=prediction_summary.get("projected_confidence"),
        reasoning_factors=storm.reasoning_factors,
        forecast_reasoning_factors=list(prediction_summary.get("forecast_reasoning_factors", [])),
    )


@router.get("/api/v1/storms/{storm_id}/timeseries", response_model=StormTimeSeriesResponse)
async def get_storm_timeseries(
    storm_id: str,
    limit: int = Query(default=20, ge=1, le=60),
) -> StormTimeSeriesResponse:
    """Return the per-scan time series for a tracked storm.

    Each point covers one radar scan and includes reflectivity, area, motion,
    severity, and threat scores — useful for rendering history sparklines and
    trend charts in the frontend.
    """
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    snapshots = await store.list_storm_snapshots(storm_id, limit=limit)
    points = [
        StormTimeSeriesPoint(
            scan_time=snap.scan_time.isoformat() if hasattr(snap.scan_time, "isoformat") else str(snap.scan_time),
            centroid_lat=snap.centroid_lat,
            centroid_lon=snap.centroid_lon,
            area_km2=snap.area_km2,
            max_reflectivity=snap.max_reflectivity,
            mean_reflectivity=snap.mean_reflectivity,
            motion_speed_kmh=snap.motion_speed_kmh,
            motion_heading_deg=snap.motion_heading_deg,
            trend=snap.trend,
            severity_level=snap.severity_level,
            confidence=snap.confidence,
            threat_scores=snap.threat_scores or {},
        )
        for snap in snapshots
    ]
    return StormTimeSeriesResponse(
        storm_id=storm_id,
        site=storm.site,
        point_count=len(points),
        points=points,
    )


@router.get("/api/v1/storms/{storm_id}/breakdown", response_model=ThreatComponentBreakdownResponse)
async def get_storm_threat_breakdown(storm_id: str) -> ThreatComponentBreakdownResponse:
    """Return the per-component threat score breakdown for a storm.

    Exposes the sub-components contributing to each threat score (hail, wind,
    tornado, flood), the top supporting factors, and the top limiting factors
    for each threat type.  All values are proxy-derived.
    """
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    return ThreatComponentBreakdownResponse(
        storm_id=storm_id,
        threat_scores=storm.threat_scores or {},
        component_breakdown=getattr(storm, "threat_component_breakdown", {}),
        top_reasons=getattr(storm, "threat_top_reasons", {}),
        limiting_factors=getattr(storm, "threat_limiting_factors", {}),
        lifecycle_summary=getattr(storm, "lifecycle_summary", {}),
    )


@router.get("/api/v1/storms/hotspots", response_model=list[StormHotspotResponse])
async def list_storm_hotspots(
    site: str = Query(..., min_length=4, max_length=4),
    limit: int = Query(default=10, ge=1, le=30),
) -> list[StormHotspotResponse]:
    """Return active storms ranked by operational priority score (highest first).

    Priority is a heuristic combining severity, threat scores, event flags,
    motion confidence, location impacts, and storm mode.  It is NOT an official
    NWS operational product.
    """
    store = get_frame_store()
    storms = await store.list_storm_objects(site=site.upper(), include_inactive=False, limit=50)
    if not storms:
        return []
    storm_ids = [s.storm_id for s in storms]
    impacts_by_storm = await store.batch_storm_impacts(storm_ids)

    results: list[StormHotspotResponse] = []
    for storm in storms:
        flags = getattr(storm, "event_flags", []) or []
        top_flag = flags[0]["label"] if flags else None
        impacts = impacts_by_storm.get(storm.storm_id, [])
        results.append(StormHotspotResponse(
            storm_id=storm.storm_id,
            site=storm.site,
            priority_score=getattr(storm, "priority_score", 0.0),
            priority_label=getattr(storm, "priority_label", "MINIMAL"),
            severity_level=storm.severity_level,
            primary_threat=storm.primary_threat,
            threat_scores=storm.threat_scores or {},
            storm_mode=getattr(storm, "storm_mode", "unknown"),
            centroid_lat=storm.centroid_lat,
            centroid_lon=storm.centroid_lon,
            motion_heading_deg=storm.motion_heading_deg,
            motion_speed_kmh=storm.motion_speed_kmh,
            confidence=storm.confidence,
            trend=storm.trend,
            event_flags=[EventFlagResponse(**f) for f in flags if isinstance(f, dict)],
            top_flag=top_flag,
            impact_count=len(impacts),
            latest_scan_time=storm.latest_scan_time,
        ))

    results.sort(key=lambda r: r.priority_score, reverse=True)
    return results[:limit]


@router.get("/api/v1/storms/compare", response_model=StormCompareResponse)
async def compare_storms(
    storm_a: str = Query(...),
    storm_b: str = Query(...),
) -> StormCompareResponse:
    """Compare two storms side-by-side on motion, trends, threats, structure,
    environment, and confidence.

    All metrics are proxy-derived from radar and model data.
    """
    store = get_frame_store()
    a, b = await asyncio.gather(
        store.get_storm_object(storm_a),
        store.get_storm_object(storm_b),
    )
    if a is None:
        raise HTTPException(status_code=404, detail=f"Storm A not found: {storm_a}")
    if b is None:
        raise HTTPException(status_code=404, detail=f"Storm B not found: {storm_b}")

    def _ts(s, k, default=None):
        v = getattr(s, k, default)
        return v if v is not None else default

    def _td(va, vb):
        """Numeric delta or None."""
        if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
            return round(vb - va, 3)
        return None

    env_a = _ts(a, "environment_summary") or {}
    env_b = _ts(b, "environment_summary") or {}

    fields: list[StormCompareField] = [
        StormCompareField(label="Severity", storm_a=a.severity_level, storm_b=b.severity_level),
        StormCompareField(label="Primary Threat", storm_a=a.primary_threat, storm_b=b.primary_threat),
        StormCompareField(label="Tornado Score", storm_a=a.threat_scores.get("tornado"), storm_b=b.threat_scores.get("tornado"), delta=_td(a.threat_scores.get("tornado", 0), b.threat_scores.get("tornado", 0))),
        StormCompareField(label="Hail Score", storm_a=a.threat_scores.get("hail"), storm_b=b.threat_scores.get("hail"), delta=_td(a.threat_scores.get("hail", 0), b.threat_scores.get("hail", 0))),
        StormCompareField(label="Wind Score", storm_a=a.threat_scores.get("wind"), storm_b=b.threat_scores.get("wind"), delta=_td(a.threat_scores.get("wind", 0), b.threat_scores.get("wind", 0))),
        StormCompareField(label="Flood Score", storm_a=a.threat_scores.get("flood"), storm_b=b.threat_scores.get("flood"), delta=_td(a.threat_scores.get("flood", 0), b.threat_scores.get("flood", 0))),
        StormCompareField(label="Confidence", storm_a=round(a.confidence, 2), storm_b=round(b.confidence, 2), delta=_td(a.confidence, b.confidence)),
        StormCompareField(label="Max Reflectivity (dBZ)", storm_a=round(a.max_reflectivity, 1), storm_b=round(b.max_reflectivity, 1), delta=_td(a.max_reflectivity, b.max_reflectivity)),
        StormCompareField(label="Area (km²)", storm_a=round(a.area_km2, 0), storm_b=round(b.area_km2, 0), delta=_td(a.area_km2, b.area_km2)),
        StormCompareField(label="Motion Speed (km/h)", storm_a=a.motion_speed_kmh, storm_b=b.motion_speed_kmh, delta=_td(a.motion_speed_kmh or 0, b.motion_speed_kmh or 0)),
        StormCompareField(label="Motion Heading (°)", storm_a=a.motion_heading_deg, storm_b=b.motion_heading_deg),
        StormCompareField(label="Trend", storm_a=a.trend, storm_b=b.trend),
        StormCompareField(label="Convective Mode", storm_a=getattr(a, "storm_mode", "unknown"), storm_b=getattr(b, "storm_mode", "unknown")),
        StormCompareField(label="Priority", storm_a=getattr(a, "priority_label", "MINIMAL"), storm_b=getattr(b, "priority_label", "MINIMAL"), delta=_td(getattr(a, "priority_score", 0), getattr(b, "priority_score", 0))),
        StormCompareField(label="Track Uncertainty (km)", storm_a=getattr(a, "track_uncertainty_km", None), storm_b=getattr(b, "track_uncertainty_km", None)),
        StormCompareField(label="Lifecycle State", storm_a=a.lifecycle_state, storm_b=b.lifecycle_state),
        StormCompareField(label="Environment CAPE (J/kg)", storm_a=env_a.get("cape_jkg"), storm_b=env_b.get("cape_jkg"), delta=_td(env_a.get("cape_jkg") or 0, env_b.get("cape_jkg") or 0)),
        StormCompareField(label="0–6 km Shear (kt)", storm_a=env_a.get("bulk_shear_06km_kt"), storm_b=env_b.get("bulk_shear_06km_kt")),
        StormCompareField(label="SRH Proxy (m²/s²)", storm_a=env_a.get("srh_surface_925hpa_m2s2"), storm_b=env_b.get("srh_surface_925hpa_m2s2")),
        StormCompareField(label="Environment Confidence", storm_a=env_a.get("environment_confidence"), storm_b=env_b.get("environment_confidence")),
        StormCompareField(label="Top Event Flag", storm_a=(getattr(a, "event_flags", []) or [{}])[0].get("label") if getattr(a, "event_flags", []) else None, storm_b=(getattr(b, "event_flags", []) or [{}])[0].get("label") if getattr(b, "event_flags", []) else None),
    ]

    return StormCompareResponse(storm_a_id=storm_a, storm_b_id=storm_b, fields=fields)


@router.get("/api/v1/locations/risk", response_model=list[LocationRiskEntry])
async def list_location_risk(
    site: str = Query(..., min_length=4, max_length=4),
) -> list[LocationRiskEntry]:
    """Return all saved locations sorted by current storm threat risk.

    Aggregates all active storm impacts to produce a per-location risk summary
    showing: risk level, threatening storm count, soonest ETA, primary threat,
    and top event flag labels.  Risk levels are heuristic and proxy-derived.
    """
    store = get_frame_store()
    storms, locations = await asyncio.gather(
        store.list_storm_objects(site=site.upper(), include_inactive=False, limit=50),
        store.list_saved_locations(),
    )
    if not locations:
        return []

    storm_ids = [s.storm_id for s in storms]
    impacts_by_storm: dict = {}
    if storm_ids:
        impacts_by_storm = await store.batch_storm_impacts(storm_ids)

    # Build a map of storm flags for quick lookup
    storm_flag_labels: dict[str, list[str]] = {}
    storm_by_id: dict = {s.storm_id: s for s in storms}
    for s in storms:
        flags = getattr(s, "event_flags", []) or []
        storm_flag_labels[s.storm_id] = [f.get("label", "") for f in flags[:3] if isinstance(f, dict)]

    # Aggregate impacts per location
    loc_impacts: dict[str, list] = {}
    for storm_id, impacts in impacts_by_storm.items():
        for impact in impacts:
            lid = impact.location_id
            if lid not in loc_impacts:
                loc_impacts[lid] = []
            loc_impacts[lid].append((storm_id, impact))

    entries: list[LocationRiskEntry] = []
    for loc in locations:
        impacts_here = loc_impacts.get(loc.location_id, [])
        if not impacts_here:
            entries.append(LocationRiskEntry(
                location_id=loc.location_id,
                location_name=loc.name,
                lat=loc.lat,
                lon=loc.lon,
                risk_level="NONE",
                risk_score=0.0,
                threatening_storm_count=0,
            ))
            continue

        # Sort by impact_rank desc
        impacts_here.sort(key=lambda x: getattr(x[1], "impact_rank", 0.0), reverse=True)
        top_storm_id, top_impact = impacts_here[0]
        top_storm = storm_by_id.get(top_storm_id)

        # Derive earliest ETA across all impacts
        all_eta_low = [getattr(i, "eta_minutes_low", None) for _, i in impacts_here if getattr(i, "eta_minutes_low", None) is not None]
        all_eta_high = [getattr(i, "eta_minutes_high", None) for _, i in impacts_here if getattr(i, "eta_minutes_high", None) is not None]
        nearest_eta_low = min(all_eta_low) if all_eta_low else None
        nearest_eta_high = min(all_eta_high) if all_eta_high else None

        # Aggregate threat scores across all threatening storms
        agg_scores: dict[str, float] = {"tornado": 0.0, "hail": 0.0, "wind": 0.0, "flood": 0.0}
        for sid, _ in impacts_here:
            st = storm_by_id.get(sid)
            if st:
                for k in agg_scores:
                    agg_scores[k] = max(agg_scores[k], (st.threat_scores or {}).get(k, 0.0))

        risk_score = max(agg_scores.values()) if agg_scores else 0.0
        if top_storm:
            severity = top_storm.severity_level
        else:
            severity = "NONE"

        if severity in ("TORNADO", "TORNADO_EMERGENCY") or risk_score >= 0.65:
            risk_level = "HIGH"
        elif severity == "SEVERE" or risk_score >= 0.40:
            risk_level = "MODERATE"
        elif risk_score >= 0.20:
            risk_level = "LOW"
        else:
            risk_level = "NONE"

        flag_labels: list[str] = []
        for sid, _ in impacts_here:
            flag_labels.extend(storm_flag_labels.get(sid, []))
        # dedupe while preserving order
        seen_flags: set[str] = set()
        deduped_flags: list[str] = []
        for fl in flag_labels:
            if fl and fl not in seen_flags:
                seen_flags.add(fl)
                deduped_flags.append(fl)

        entries.append(LocationRiskEntry(
            location_id=loc.location_id,
            location_name=loc.name,
            lat=loc.lat,
            lon=loc.lon,
            risk_level=risk_level,
            risk_score=round(risk_score, 3),
            threatening_storm_count=len(impacts_here),
            nearest_eta_low=nearest_eta_low,
            nearest_eta_high=nearest_eta_high,
            primary_threat=top_storm.primary_threat if top_storm else None,
            threat_scores={k: round(v, 3) for k, v in agg_scores.items()},
            top_storm_id=top_storm_id,
            top_storm_severity=severity,
            top_impact_summary=getattr(top_impact, "summary", None),
            trend=top_storm.trend if top_storm else None,
            confidence=round(getattr(top_impact, "confidence", 0.0), 2),
            event_flag_labels=deduped_flags[:4],
        ))

    entries.sort(key=lambda e: e.risk_score, reverse=True)
    return entries


@router.get("/api/v1/storms/{storm_id}/event-history", response_model=StormEventHistoryResponse)
async def get_storm_event_history(
    storm_id: str,
    limit: int = Query(default=60, ge=1, le=240),
) -> StormEventHistoryResponse:
    """Return the persisted per-scan event flag history for a storm.

    Each point represents one radar scan and carries the operational flags,
    priority, threat scores, and convective mode that were computed at that
    time.  Points are sorted newest-first.

    This history is built server-side by the history aggregator and continues
    accumulating whether or not the frontend is open.  All flags are
    proxy-derived heuristics — not official NWS operational products.
    """
    store = get_frame_store()
    storm = await store.get_storm_object(storm_id)
    if storm is None:
        raise HTTPException(status_code=404, detail=f"Storm not found: {storm_id}")
    history = await store.list_storm_event_history(storm_id, limit=limit)
    points = [
        StormEventHistoryPoint(
            scan_time=h.scan_time.isoformat() if hasattr(h.scan_time, "isoformat") else str(h.scan_time),
            event_flags=h.event_flags or [],
            lifecycle_state=h.lifecycle_state,
            priority_score=h.priority_score,
            priority_label=h.priority_label,
            severity_level=h.severity_level,
            primary_threat=h.primary_threat,
            threat_scores=h.threat_scores or {},
            storm_mode=h.storm_mode,
            motion_heading_deg=h.motion_heading_deg,
            motion_speed_kmh=h.motion_speed_kmh,
            confidence=h.confidence,
        )
        for h in history
    ]
    return StormEventHistoryResponse(
        storm_id=storm_id,
        site=storm.site,
        point_count=len(points),
        points=points,
    )


@router.get("/api/v1/storms/{storm_id}/summary", response_model=StormPrecomputedSummaryResponse)
async def get_storm_precomputed_summary(storm_id: str) -> StormPrecomputedSummaryResponse:
    """Return the precomputed aggregated summary for a storm.

    The summary is built by the history aggregator running server-side on a
    scheduled pass (every ~2 minutes by default).  It includes peak severity,
    peak threat scores, dominant convective mode, flag occurrence counts, threat
    trend (last 24 scans), motion trend (last 24 scans), and impact location IDs.

    If no precomputed summary exists yet for this storm, the endpoint returns
    a 404; retry after the next aggregation pass (~2 minutes after the storm
    first appears).
    """
    store = get_frame_store()
    summary = await store.get_precomputed_summary(storm_id)
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail=f"No precomputed summary for storm {storm_id} — try again after the next aggregation pass."
        )
    return StormPrecomputedSummaryResponse(
        storm_id=summary.storm_id,
        site=summary.site,
        computed_at=summary.computed_at.isoformat() if hasattr(summary.computed_at, "isoformat") else str(summary.computed_at),
        scan_count=summary.scan_count,
        first_seen=summary.first_seen.isoformat() if summary.first_seen and hasattr(summary.first_seen, "isoformat") else (str(summary.first_seen) if summary.first_seen else None),
        last_seen=summary.last_seen.isoformat() if summary.last_seen and hasattr(summary.last_seen, "isoformat") else (str(summary.last_seen) if summary.last_seen else None),
        peak_severity=summary.peak_severity,
        peak_threat_scores=summary.peak_threat_scores,
        peak_reflectivity=summary.peak_reflectivity,
        max_area_km2=summary.max_area_km2,
        max_speed_kmh=summary.max_speed_kmh,
        max_priority_score=summary.max_priority_score,
        dominant_mode=summary.dominant_mode,
        flag_summary=summary.flag_summary,
        threat_trend=summary.threat_trend,
        motion_trend=summary.motion_trend,
        impact_location_ids=summary.impact_location_ids,
        summary_narrative=summary.summary_narrative,
    )
