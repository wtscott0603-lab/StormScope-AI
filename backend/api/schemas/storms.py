from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class StormForecastPointResponse(BaseModel):
    lat: float
    lon: float
    eta_minutes: int
    label: str


class StormImpactResponse(BaseModel):
    location_id: str
    location_name: str
    eta_minutes_low: int | None
    eta_minutes_high: int | None
    distance_km: float | None
    threat_at_arrival: str
    trend_at_arrival: str
    confidence: float
    summary: str
    impact_rank: float
    details: dict[str, Any] = Field(default_factory=dict)


class StormEnvironmentResponse(BaseModel):
    source: str | None = None
    current_station_id: str | None = None
    future_station_id: str | None = None
    gridpoint_id: str | None = None
    surface_temp_c: float | None = None
    dewpoint_c: float | None = None
    wind_speed_kt: float | None = None
    forecast_probability_of_thunder: float | None = None
    ahead_probability_of_thunder: float | None = None
    forecast_qpf_mm: float | None = None
    forecast_wind_speed_kmh: float | None = None
    hail_favorability: float | None = None
    wind_favorability: float | None = None
    tornado_favorability: float | None = None
    heavy_rain_favorability: float | None = None
    convective_signal: float | None = None
    intensification_signal: float | None = None
    weakening_signal: float | None = None
    projected_trend: str | None = None
    projection_confidence: float | None = None
    environment_confidence: float | None = None
    environment_freshness_minutes: int | None = None
    environment_ahead_delta: dict[str, float | None] = Field(default_factory=dict)
    cape_jkg: float | None = None
    cin_jkg: float | None = None
    bulk_shear_06km_kt: float | None = None
    bulk_shear_01km_kt: float | None = None
    srh_surface_925hpa_m2s2: float | None = None
    dcape_jkg: float | None = None
    dcape_is_proxy: bool | None = None
    freezing_level_m: float | None = None
    pwat_mm: float | None = None
    lapse_rate_midlevel_cpkm: float | None = None
    lcl_m: float | None = None
    lfc_m: float | None = None
    model_valid_at: str | None = None
    ahead_model_valid_at: str | None = None
    weather_summary: str | None = None
    hazards: list[str] = Field(default_factory=list)
    ahead_trend: str | None = None
    limitation: str | None = None
    profile_summary: dict[str, Any] = Field(default_factory=dict)
    field_provenance: dict[str, str] = Field(default_factory=dict)
    source_notes: list[str] = Field(default_factory=list)
    hodograph: dict[str, Any] = Field(default_factory=dict)
    srv_metrics: dict[str, Any] = Field(default_factory=dict)
    operational_context: dict[str, Any] = Field(default_factory=dict)
    volume_metrics: dict[str, Any] = Field(default_factory=dict)


class StormPredictionResponse(BaseModel):
    intensification_score: float | None = None
    weakening_score: float | None = None
    maintenance_score: float | None = None
    projected_trend: str | None = None
    projected_primary_threat: str | None = None
    projected_secondary_threats: list[str] = Field(default_factory=list)
    projected_confidence: float | None = None
    projected_threat_scores: dict[str, float] = Field(default_factory=dict)
    forecast_reasoning_factors: list[str] = Field(default_factory=list)
    environment_confidence: float | None = None
    motion_confidence: float | None = None
    persistence_score: float | None = None
    forecast_stability_score: float | None = None
    data_quality_score: float | None = None
    uncertainty_factors: list[str] = Field(default_factory=list)


class StormSummaryResponse(BaseModel):
    storm_id: str
    site: str
    latest_frame_id: str | None
    latest_scan_time: datetime
    created_at: datetime | None = None
    updated_at: datetime | None = None
    status: str
    lifecycle_state: str
    centroid_lat: float
    centroid_lon: float
    area_km2: float
    max_reflectivity: float
    mean_reflectivity: float
    motion_heading_deg: float | None
    motion_speed_kmh: float | None
    trend: str
    primary_threat: str
    secondary_threats: list[str] = Field(default_factory=list)
    severity_level: str
    confidence: float
    threat_scores: dict[str, float] = Field(default_factory=dict)
    narrative: str
    reasoning_factors: list[str] = Field(default_factory=list)
    footprint_geojson: dict[str, Any]
    forecast_path: list[StormForecastPointResponse] = Field(default_factory=list)
    # v12 — uncertainty cone and convective mode
    uncertainty_cone: list[dict[str, Any]] = Field(default_factory=list)
    storm_mode: str = "unknown"
    storm_mode_confidence: float = 0.0
    storm_mode_evidence: list[str] = Field(default_factory=list)
    track_uncertainty_km: float = 5.0
    associated_signatures: list[dict[str, Any]] = Field(default_factory=list)
    environment_summary: StormEnvironmentResponse | None = None
    prediction_summary: StormPredictionResponse | None = None
    near_term_expectation: str = ""
    impacts: list[StormImpactResponse] = Field(default_factory=list)
    # v13 — threat component breakdown and lifecycle summary
    threat_component_breakdown: dict[str, dict[str, float]] = Field(default_factory=dict)
    threat_top_reasons: dict[str, list[str]] = Field(default_factory=dict)
    threat_limiting_factors: dict[str, list[str]] = Field(default_factory=dict)
    lifecycle_summary: dict[str, Any] = Field(default_factory=dict)
    # v14 — event flags and operational priority
    event_flags: list[dict[str, Any]] = Field(default_factory=list)
    priority_score: float = 0.0
    priority_label: str = "MINIMAL"


class StormTrackPointResponse(BaseModel):
    scan_time: datetime
    centroid_lat: float
    centroid_lon: float
    max_reflectivity: float
    mean_reflectivity: float
    motion_heading_deg: float | None
    motion_speed_kmh: float | None
    trend: str


class SavedLocationCreate(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    kind: str = Field(default="custom", min_length=1, max_length=32)


class SavedLocationResponse(BaseModel):
    location_id: str
    name: str
    lat: float
    lon: float
    kind: str
    created_at: datetime
    updated_at: datetime


class MetarObservationResponse(BaseModel):
    station_id: str
    observation_time: str | None
    lat: float
    lon: float
    temp_c: float | None
    dewpoint_c: float | None
    wind_dir_deg: float | None
    wind_speed_kt: float | None
    wind_gust_kt: float | None
    visibility_mi: float | None
    pressure_hpa: float | None
    flight_category: str | None
    raw_text: str | None
    distance_km: float | None = None


class StormNarrativeResponse(BaseModel):
    storm_id: str
    narrative: str
    near_term_expectation: str
    confidence: float
    projected_confidence: float | None = None
    reasoning_factors: list[str] = Field(default_factory=list)
    forecast_reasoning_factors: list[str] = Field(default_factory=list)


class StormTimeSeriesPoint(BaseModel):
    """Single per-scan data point in a storm's time series."""
    scan_time: str
    centroid_lat: float
    centroid_lon: float
    area_km2: float
    max_reflectivity: float
    mean_reflectivity: float
    motion_speed_kmh: float | None = None
    motion_heading_deg: float | None = None
    trend: str
    severity_level: str
    confidence: float
    threat_scores: dict[str, float] = Field(default_factory=dict)


class StormTimeSeriesResponse(BaseModel):
    """Per-storm historical time series covering all tracked scans."""
    storm_id: str
    site: str
    point_count: int
    points: list[StormTimeSeriesPoint] = Field(default_factory=list)
    provenance: str = "Derived from stored storm-object snapshots. Each point represents one radar scan."


class ThreatComponentBreakdownResponse(BaseModel):
    """Per-component score breakdown for each threat type."""
    storm_id: str
    threat_scores: dict[str, float] = Field(default_factory=dict)
    component_breakdown: dict[str, dict[str, float]] = Field(default_factory=dict)
    top_reasons: dict[str, list[str]] = Field(default_factory=dict)
    limiting_factors: dict[str, list[str]] = Field(default_factory=dict)
    lifecycle_summary: dict = Field(default_factory=dict)
    provenance: str = (
        "Component scores are proxy-derived from radar-object fields and model environment data. "
        "They are not official operational scores."
    )


class EventFlagResponse(BaseModel):
    """A single operational event flag for a storm."""
    flag: str
    label: str
    confidence: float
    rationale: str
    severity: int
    provenance: str


class StormHotspotResponse(BaseModel):
    """A storm entry in the operational hotspots / priority ranking."""
    storm_id: str
    site: str
    priority_score: float
    priority_label: str
    severity_level: str
    primary_threat: str
    threat_scores: dict[str, float] = Field(default_factory=dict)
    storm_mode: str = "unknown"
    centroid_lat: float
    centroid_lon: float
    motion_heading_deg: float | None = None
    motion_speed_kmh: float | None = None
    confidence: float
    trend: str
    event_flags: list[EventFlagResponse] = Field(default_factory=list)
    top_flag: str | None = None
    impact_count: int = 0
    latest_scan_time: datetime | None = None


class LocationRiskEntry(BaseModel):
    """A saved location with its current storm threat intelligence."""
    location_id: str
    location_name: str
    lat: float
    lon: float
    risk_level: str          # HIGH / MODERATE / LOW / NONE
    risk_score: float        # 0–1
    threatening_storm_count: int
    nearest_eta_low: int | None = None
    nearest_eta_high: int | None = None
    primary_threat: str | None = None
    threat_scores: dict[str, float] = Field(default_factory=dict)
    top_storm_id: str | None = None
    top_storm_severity: str | None = None
    top_impact_summary: str | None = None
    trend: str | None = None
    confidence: float | None = None
    event_flag_labels: list[str] = Field(default_factory=list)


class StormCompareField(BaseModel):
    label: str
    storm_a: Any
    storm_b: Any
    delta: Any | None = None
    note: str | None = None


class StormCompareResponse(BaseModel):
    """Side-by-side storm comparison."""
    storm_a_id: str
    storm_b_id: str
    fields: list[StormCompareField] = Field(default_factory=list)
    provenance: str = "Comparison derived from current storm-object state. All metrics are proxy-derived."


class StormEventHistoryPoint(BaseModel):
    """Single scan-time snapshot of a storm's event flags and operational state."""
    scan_time: str
    event_flags: list[dict[str, Any]] = Field(default_factory=list)
    lifecycle_state: str | None = None
    priority_score: float | None = None
    priority_label: str | None = None
    severity_level: str | None = None
    primary_threat: str | None = None
    threat_scores: dict[str, float] = Field(default_factory=dict)
    storm_mode: str | None = None
    motion_heading_deg: float | None = None
    motion_speed_kmh: float | None = None
    confidence: float | None = None


class StormEventHistoryResponse(BaseModel):
    storm_id: str
    site: str
    point_count: int
    points: list[StormEventHistoryPoint] = Field(default_factory=list)
    provenance: str = "Event history persisted server-side per scan. All flags are proxy-derived heuristics."


class StormPrecomputedSummaryResponse(BaseModel):
    """Precomputed aggregated summary for a storm, built by the history aggregator."""
    storm_id: str
    site: str
    computed_at: str
    scan_count: int
    first_seen: str | None = None
    last_seen: str | None = None
    peak_severity: str | None = None
    peak_threat_scores: dict[str, float] = Field(default_factory=dict)
    peak_reflectivity: float | None = None
    max_area_km2: float | None = None
    max_speed_kmh: float | None = None
    max_priority_score: float | None = None
    dominant_mode: str | None = None
    flag_summary: list[dict[str, Any]] = Field(default_factory=list)
    threat_trend: list[dict[str, Any]] = Field(default_factory=list)
    motion_trend: list[dict[str, Any]] = Field(default_factory=list)
    impact_location_ids: list[str] = Field(default_factory=list)
    summary_narrative: str | None = None
    provenance: str = "Precomputed server-side by history aggregator. All metrics are proxy-derived."
