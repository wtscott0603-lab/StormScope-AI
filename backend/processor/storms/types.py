from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np


@dataclass
class StormDetection:
    centroid_lat: float
    centroid_lon: float
    area_km2: float
    max_reflectivity: float
    mean_reflectivity: float
    gate_count: int
    elongation_ratio: float
    radius_km: float
    footprint_geojson: dict[str, Any]
    gate_mask: np.ndarray
    core_gate_count: int = 0
    core_fraction: float = 0.0
    core_max_reflectivity: float = 0.0


@dataclass
class ForecastPoint:
    lat: float
    lon: float
    eta_minutes: int
    label: str


@dataclass
class StormImpact:
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
    details: dict[str, Any] | None = None


@dataclass
class TrackedStorm:
    storm_id: str
    site: str
    frame_id: str | None
    scan_time: datetime
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
    # New fields added in v12
    uncertainty_cone: list[dict[str, Any]] = field(default_factory=list)
    storm_mode: str = "unknown"
    storm_mode_confidence: float = 0.0
    storm_mode_evidence: list[str] = field(default_factory=list)
    track_uncertainty_km: float = 5.0
    associated_signatures: list[dict[str, Any]] = field(default_factory=list)
    environment_summary: dict[str, Any] | None = None
    prediction_summary: dict[str, Any] | None = None
    near_term_expectation: str = ""
    impacts: list[StormImpact] = field(default_factory=list)
    # v13 — enriched threat component scores and lifecycle analysis
    threat_component_breakdown: dict[str, dict[str, float]] = field(default_factory=dict)
    threat_top_reasons: dict[str, list[str]] = field(default_factory=dict)
    threat_limiting_factors: dict[str, list[str]] = field(default_factory=dict)
    lifecycle_summary: dict[str, Any] = field(default_factory=dict)
    # v14 — event flags and operational priority
    event_flags: list[dict[str, Any]] = field(default_factory=list)
    priority_score: float = 0.0
    priority_label: str = "MINIMAL"
    created_at: datetime | None = None
    updated_at: datetime | None = None
