from __future__ import annotations

from fastapi import APIRouter

from backend.api.config import get_settings
from backend.api.schemas.radar import ConfigResponse


router = APIRouter(tags=["config"])


@router.get("/api/config", response_model=ConfigResponse)
async def get_config() -> ConfigResponse:
    settings = get_settings()
    return ConfigResponse(
        default_site=settings.default_site.upper(),
        enabled_products=settings.enabled_products,
        update_interval_sec=settings.update_interval_sec,
        tile_url=settings.tile_url,
        default_center_lat=settings.default_center_lat,
        default_center_lon=settings.default_center_lon,
        default_map_zoom=settings.default_map_zoom,
        preferred_units=settings.preferred_units,
        default_enabled_overlays=settings.default_enabled_overlays,
        local_station_priority=settings.local_station_priority,
    )
