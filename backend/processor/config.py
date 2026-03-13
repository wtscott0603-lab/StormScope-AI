from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProcessorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    default_site: str = Field(default="KILN", alias="DEFAULT_SITE")
    default_center_lat: float = Field(default=40.0197, alias="DEFAULT_CENTER_LAT")
    default_center_lon: float = Field(default=-82.8799, alias="DEFAULT_CENTER_LON")
    default_map_zoom: float = Field(default=8.8, alias="DEFAULT_MAP_ZOOM")
    preferred_units: str = Field(default="imperial", alias="PREFERRED_UNITS")
    default_enabled_overlays_raw: str = Field(
        default="alerts,signatures,storms,saved_locations,metars,spc,watch_boxes,storm_trails,range_rings,radar_sites,sweep_animation",
        alias="DEFAULT_ENABLED_OVERLAYS",
    )
    local_station_priority_raw: str = Field(default="KCMH,KOSU,KLCK,KTZR", alias="LOCAL_STATION_PRIORITY")
    default_saved_locations_raw: str = Field(default="", alias="DEFAULT_SAVED_LOCATIONS_JSON")
    enabled_products_raw: str = Field(default="REF,VEL,SRV,CC,ZDR,KDP,ET,VIL,RR,QPE1H,HC", alias="ENABLED_PRODUCTS")
    enabled_tilts_raw: str = Field(default="0.5,1.5", alias="ENABLED_TILTS")
    update_interval_sec: int = Field(default=120, alias="UPDATE_INTERVAL_SEC")
    max_frames_per_site: int = Field(default=20, alias="MAX_FRAMES_PER_SITE")
    retention_hours: int = Field(default=6, alias="RETENTION_HOURS")
    cache_dir: Path = Field(default=Path("/data/cache"), alias="CACHE_DIR")
    db_path: Path = Field(default=Path("/data/db/radar.db"), alias="DB_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    image_size: int = Field(default=1024, alias="IMAGE_SIZE")
    s3_bucket: str = Field(default="noaa-nexrad-level2", alias="S3_BUCKET")
    request_ttl_minutes: int = Field(default=30, alias="REQUEST_TTL_MINUTES")
    storm_reflectivity_threshold_dbz: float = Field(default=40.0, alias="STORM_REFLECTIVITY_THRESHOLD_DBZ")
    storm_min_area_km2: float = Field(default=20.0, alias="STORM_MIN_AREA_KM2")
    storm_track_horizon_min: int = Field(default=60, alias="STORM_TRACK_HORIZON_MIN")
    storm_track_step_min: int = Field(default=10, alias="STORM_TRACK_STEP_MIN")
    metar_cache_ttl_minutes: int = Field(default=10, alias="METAR_CACHE_TTL_MINUTES")
    grid_forecast_cache_ttl_minutes: int = Field(default=30, alias="GRID_FORECAST_CACHE_TTL_MINUTES")
    open_meteo_cache_ttl_minutes: int = Field(default=45, alias="OPEN_METEO_CACHE_TTL_MINUTES")
    overlay_cache_ttl_minutes: int = Field(default=20, alias="OVERLAY_CACHE_TTL_MINUTES")

    @computed_field
    @property
    def enabled_products(self) -> list[str]:
        return [item.strip().upper() for item in self.enabled_products_raw.split(",") if item.strip()]

    @computed_field
    @property
    def enabled_tilts(self) -> list[float]:
        tilts: list[float] = []
        for item in self.enabled_tilts_raw.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                tilts.append(round(float(item), 1))
            except ValueError:
                continue
        return tilts or [0.5]

    @computed_field
    @property
    def default_enabled_overlays(self) -> list[str]:
        return [item.strip().lower() for item in self.default_enabled_overlays_raw.split(",") if item.strip()]

    @computed_field
    @property
    def local_station_priority(self) -> list[str]:
        return [item.strip().upper() for item in self.local_station_priority_raw.split(",") if item.strip()]

    @computed_field
    @property
    def default_saved_locations(self) -> list[dict]:
        if self.default_saved_locations_raw.strip():
            try:
                payload = json.loads(self.default_saved_locations_raw)
                if isinstance(payload, list):
                    return [item for item in payload if isinstance(item, dict)]
            except json.JSONDecodeError:
                pass
        return [
            {
                "location_id": "default-gahanna",
                "name": "Gahanna",
                "lat": 40.0197,
                "lon": -82.8799,
                "kind": "default",
            },
            {
                "location_id": "default-cmh",
                "name": "CMH Airport Area",
                "lat": 39.9980,
                "lon": -82.8919,
                "kind": "default",
            },
            {
                "location_id": "default-columbus",
                "name": "Central Columbus",
                "lat": 39.9612,
                "lon": -82.9988,
                "kind": "default",
            },
        ]

    @computed_field
    @property
    def sounding_cache_dir(self) -> Path:
        return self.cache_dir / "soundings"

    @computed_field
    @property
    def raw_dir(self) -> Path:
        return self.cache_dir / "raw"

    @computed_field
    @property
    def image_dir(self) -> Path:
        return self.cache_dir / "images"

    @computed_field
    @property
    def alerts_cache_path(self) -> Path:
        return self.cache_dir / "alerts" / "active.json"

    @computed_field
    @property
    def metar_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "metars.json"

    @computed_field
    @property
    def metar_stations_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "stations.json"

    @computed_field
    @property
    def environment_cache_dir(self) -> Path:
        return self.cache_dir / "environment"

    @computed_field
    @property
    def spc_overlay_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "spc.json"

    @computed_field
    @property
    def mesoscale_discussions_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "mesoscale_discussions.json"

    @computed_field
    @property
    def local_storm_reports_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "lsr.json"

    @computed_field
    @property
    def watch_overlay_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "watch_boxes.json"

    @computed_field
    @property
    def site_requests_path(self) -> Path:
        return self.db_path.parent / "site_requests.json"


@lru_cache(maxsize=1)
def get_settings() -> ProcessorSettings:
    return ProcessorSettings()
