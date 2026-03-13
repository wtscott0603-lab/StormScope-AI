from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_version: str = "0.4.1"
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
    api_host: str = Field(default="0.0.0.0", alias="API_HOST")
    api_port: int = Field(default=8000, alias="API_PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    cors_allowed_origins_raw: str = Field(default="*", alias="CORS_ALLOWED_ORIGINS")
    cors_allow_credentials: bool = Field(default=False, alias="CORS_ALLOW_CREDENTIALS")
    metar_cache_ttl_minutes: int = Field(default=10, alias="METAR_CACHE_TTL_MINUTES")
    open_meteo_cache_ttl_minutes: int = Field(default=45, alias="OPEN_METEO_CACHE_TTL_MINUTES")
    overlay_cache_ttl_minutes: int = Field(default=20, alias="OVERLAY_CACHE_TTL_MINUTES")
    tile_url: str = Field(default="https://tile.openstreetmap.org/{z}/{x}/{y}.png", alias="VITE_MAP_TILE_URL")
    tile_attribution: str = Field(default="© OpenStreetMap contributors", alias="VITE_MAP_TILE_ATTRIBUTION")
    api_base_url: str = Field(default="http://localhost:8000", alias="VITE_API_BASE_URL")
    storm_track_horizon_min: int = Field(default=60, alias="STORM_TRACK_HORIZON_MIN")

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
    def cors_allowed_origins(self) -> list[str]:
        items = [item.strip() for item in self.cors_allowed_origins_raw.split(",") if item.strip()]
        if not items:
            return ["*"]
        if "*" in items:
            return ["*"]
        return items

    @computed_field
    @property
    def alerts_cache_path(self) -> Path:
        return self.cache_dir / "alerts" / "active.json"

    @computed_field
    @property
    def site_requests_path(self) -> Path:
        return self.db_path.parent / "site_requests.json"

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
    def spc_day2_overlay_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "spc_day2.json"

    @computed_field
    @property
    def spc_day3_overlay_cache_path(self) -> Path:
        return self.cache_dir / "overlays" / "spc_day3.json"

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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
