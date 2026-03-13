from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class RadarFrameRecord:
    frame_id: str
    site: str
    product: str
    tilt: float
    tilts_available: list[float]
    scan_time: datetime
    raw_path: str | None
    image_path: str | None
    min_lat: float | None
    max_lat: float | None
    min_lon: float | None
    max_lon: float | None
    status: str
    error_msg: str | None
    created_at: datetime


@dataclass
class ProcessorRunRecord:
    id: int
    started_at: datetime
    finished_at: datetime | None
    status: str
    frames_added: int
    error_msg: str | None


@dataclass
class SiteInfo:
    id: str
    name: str
    lat: float
    lon: float
    state: str
    elevation_m: float


@dataclass
class StormObjectRecord:
    storm_id: str
    site: str
    latest_frame_id: str | None
    latest_scan_time: datetime
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
    secondary_threats: list[str]
    severity_level: str
    confidence: float
    threat_scores: dict[str, float]
    narrative: str
    reasoning_factors: list[str]
    footprint_geojson: dict[str, Any]
    forecast_path: list[dict[str, Any]]
    associated_signatures: list[dict[str, Any]]
    environment_summary: dict[str, Any] | None
    prediction_summary: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime
    # v12 fields
    storm_mode: str = "unknown"
    storm_mode_confidence: float = 0.0
    storm_mode_evidence: list = field(default_factory=list)
    uncertainty_cone: list = field(default_factory=list)
    track_uncertainty_km: float = 5.0
    # v13 fields
    threat_component_breakdown: dict = field(default_factory=dict)
    threat_top_reasons: dict = field(default_factory=dict)
    threat_limiting_factors: dict = field(default_factory=dict)
    lifecycle_summary: dict = field(default_factory=dict)
    # v14 fields
    event_flags: list = field(default_factory=list)
    priority_score: float = 0.0
    priority_label: str = "MINIMAL"


@dataclass
class StormSnapshotRecord:
    id: int
    storm_id: str
    frame_id: str | None
    site: str
    scan_time: datetime
    centroid_lat: float
    centroid_lon: float
    area_km2: float
    max_reflectivity: float
    mean_reflectivity: float
    motion_heading_deg: float | None
    motion_speed_kmh: float | None
    trend: str
    primary_threat: str
    secondary_threats: list[str]
    severity_level: str
    confidence: float
    threat_scores: dict[str, float]
    footprint_geojson: dict[str, Any]
    forecast_path: list[dict[str, Any]]
    associated_signatures: list[dict[str, Any]]
    reasoning_factors: list[str]
    near_term_expectation: str
    prediction_summary: dict[str, Any] | None
    created_at: datetime


@dataclass
class SavedLocationRecord:
    location_id: str
    name: str
    lat: float
    lon: float
    kind: str
    created_at: datetime
    updated_at: datetime


@dataclass
class StormLocationImpactRecord:
    id: int
    storm_id: str
    location_id: str
    computed_at: datetime
    eta_minutes_low: int | None
    eta_minutes_high: int | None
    distance_km: float | None
    threat_at_arrival: str
    trend_at_arrival: str
    confidence: float
    summary: str
    impact_rank: float
    details: dict[str, Any] | None


@dataclass
class EnvironmentSnapshotRecord:
    id: int
    site: str
    storm_id: str | None
    snapshot_time: datetime
    source: str
    lat: float
    lon: float
    station_id: str | None
    station_name: str | None
    observed_at: datetime | None
    surface_temp_c: float | None
    dewpoint_c: float | None
    wind_dir_deg: float | None
    wind_speed_kt: float | None
    pressure_hpa: float | None
    visibility_mi: float | None
    cape_jkg: float | None
    cin_jkg: float | None
    bulk_shear_06km_kt: float | None
    bulk_shear_01km_kt: float | None
    helicity_01km: float | None
    dcape_jkg: float | None
    freezing_level_m: float | None
    pwat_mm: float | None
    lapse_rate_midlevel_cpkm: float | None
    lcl_m: float | None
    lfc_m: float | None
    environment_confidence: float | None
    environment_freshness_minutes: int | None
    hail_favorability: float
    wind_favorability: float
    tornado_favorability: float
    narrative: str
    raw_payload: dict[str, Any] | None


@dataclass
class StormEventHistoryRecord:
    id: int | None
    storm_id: str
    site: str
    scan_time: datetime
    event_flags: list[dict]
    lifecycle_state: str | None
    priority_score: float | None
    priority_label: str | None
    severity_level: str | None
    primary_threat: str | None
    threat_scores: dict[str, float]
    storm_mode: str | None
    motion_heading_deg: float | None
    motion_speed_kmh: float | None
    confidence: float | None
    created_at: datetime


@dataclass
class PrecomputedStormSummary:
    storm_id: str
    site: str
    computed_at: datetime
    scan_count: int
    first_seen: datetime | None
    last_seen: datetime | None
    peak_severity: str | None
    peak_threat_scores: dict[str, float]
    peak_reflectivity: float | None
    max_area_km2: float | None
    max_speed_kmh: float | None
    max_priority_score: float | None
    dominant_mode: str | None
    flag_summary: list[dict]       # [{flag, label, occurrence_count}]
    threat_trend: list[dict]       # [{scan_time, max_score}]
    motion_trend: list[dict]       # [{scan_time, speed_kmh, heading_deg}]
    impact_location_ids: list[str]
    summary_narrative: str | None


@dataclass
class ProcessorHistoryStatus:
    id: int | None
    site: str
    last_ingest_time: datetime | None
    last_processing_cycle_time: datetime | None
    last_history_aggregation_time: datetime | None
    last_retention_time: datetime | None
    snapshot_count: int
    event_history_count: int
    precomputed_summary_count: int
    backlog_frame_count: int
    is_caught_up: bool
    history_stale: bool
    updated_at: datetime
