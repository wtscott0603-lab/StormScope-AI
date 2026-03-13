from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    error: str
    detail: str | None = None


class BBoxResponse(BaseModel):
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


class FrameResponse(BaseModel):
    frame_id: str
    site: str
    product: str
    tilt: float
    tilts_available: list[float] = Field(default_factory=list)
    timestamp: datetime
    bbox: BBoxResponse
    url: str


class TiltListResponse(BaseModel):
    site: str
    product: str
    tilts: list[float] = Field(default_factory=list)


class ConfigResponse(BaseModel):
    default_site: str
    enabled_products: list[str]
    update_interval_sec: int
    tile_url: str
    default_center_lat: float
    default_center_lon: float
    default_map_zoom: float
    preferred_units: str
    default_enabled_overlays: list[str] = Field(default_factory=list)
    local_station_priority: list[str] = Field(default_factory=list)


class ProductResponse(BaseModel):
    id: str
    name: str
    description: str
    unit: str
    enabled: bool
    available: bool
    source_kind: str
    source_product: str | None = None


class SiteSummaryResponse(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    state: str
    has_data: bool
    last_frame_time: datetime | None


class SiteDetailResponse(BaseModel):
    id: str
    name: str
    lat: float
    lon: float
    state: str
    elevation_m: float
    range_km: float = Field(default=460.0)
    last_frame_time: datetime | None
    available_products: list[str]


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    version: str
    processor_last_run: datetime | None
    processor_status: Literal["ok", "stale", "error", "never_run"]
    db_ok: bool


class CacheStatusResponse(BaseModel):
    available: bool
    stale: bool
    fetched_at: datetime | None = None
    age_minutes: int | None = None


class SiteHistoryStatus(BaseModel):
    site: str
    last_ingest_time: datetime | None = None
    last_processing_cycle_time: datetime | None = None
    last_history_aggregation_time: datetime | None = None
    snapshot_count: int = 0
    event_history_count: int = 0
    precomputed_summary_count: int = 0
    backlog_frame_count: int = 0
    is_caught_up: bool = True
    history_stale: bool = False


class StatusResponse(BaseModel):
    processor_status: str
    frames_cached: int
    sites_active: int
    active_storms: int | None = None
    processor_last_run: datetime | None = None
    processor_age_minutes: int | None = None
    environment_snapshot_age_minutes: int | None = None
    cache_status: dict[str, CacheStatusResponse] = Field(default_factory=dict)
    data_warnings: list[str] = Field(default_factory=list)
    last_error: str | None
    # v15 — always-on history freshness
    last_ingest_time: datetime | None = None
    last_history_aggregation_time: datetime | None = None
    history_stale: bool = False
    backlog_frame_count: int = 0
    is_caught_up: bool = True
    site_history_statuses: list[SiteHistoryStatus] = Field(default_factory=list)


class AnalysisResultResponse(BaseModel):
    analyzer: str
    ran_at: str
    payload: dict[str, Any]


class FrameAnalysisResponse(BaseModel):
    frame_id: str
    results: list[AnalysisResultResponse]


class SignatureMarkerResponse(BaseModel):
    signature_type: str
    severity: str
    lat: float
    lon: float
    radius_km: float
    label: str
    description: str
    confidence: float
    metrics: dict[str, Any] = Field(default_factory=dict)
    frame_id: str
    analyzer: str
    ran_at: str


class SignaturesResponse(BaseModel):
    site: str
    product: str
    frame_id: str | None
    signatures: list[SignatureMarkerResponse]
    max_severity: str
    generated_at: str


class CrossSectionPoint(BaseModel):
    lat: float
    lon: float


class CrossSectionRequest(BaseModel):
    site: str
    product: str = "REF"
    frame_id: str | None = None
    start: CrossSectionPoint
    end: CrossSectionPoint
    samples: int = Field(default=140, ge=40, le=300)
    altitude_resolution_km: float = Field(default=0.5, ge=0.1, le=2.0)
    max_altitude_km: float = Field(default=18.0, ge=5.0, le=25.0)


class CrossSectionResponse(BaseModel):
    site: str
    product: str
    frame_id: str
    ranges_km: list[float] = Field(default_factory=list)
    altitudes_km: list[float] = Field(default_factory=list)
    values: list[list[float | None]] = Field(default_factory=list)
    start: CrossSectionPoint
    end: CrossSectionPoint
    tilts_used: list[float] = Field(default_factory=list)
    unit: str
    method: str
    limitation: str
    generated_at: str
