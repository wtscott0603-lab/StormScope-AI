from __future__ import annotations

from fastapi import APIRouter

from backend.api.config import get_settings
from backend.api.schemas.overlays import OverlayFeatureCollectionResponse
from backend.shared.metar import load_metar_cache
from backend.processor.overlays import load_overlay_cache


router = APIRouter(tags=["overlays-v1"])


@router.get("/api/v1/overlays/spc", response_model=OverlayFeatureCollectionResponse)
async def get_spc_overlays() -> OverlayFeatureCollectionResponse:
    payload = load_overlay_cache(get_settings().spc_overlay_cache_path)
    return OverlayFeatureCollectionResponse(**payload)


@router.get("/api/v1/overlays/spc_day2", response_model=OverlayFeatureCollectionResponse)
async def get_spc_day2_overlays() -> OverlayFeatureCollectionResponse:
    payload = load_overlay_cache(get_settings().spc_day2_overlay_cache_path)
    return OverlayFeatureCollectionResponse(**payload)


@router.get("/api/v1/overlays/spc_day3", response_model=OverlayFeatureCollectionResponse)
async def get_spc_day3_overlays() -> OverlayFeatureCollectionResponse:
    payload = load_overlay_cache(get_settings().spc_day3_overlay_cache_path)
    return OverlayFeatureCollectionResponse(**payload)


@router.get("/api/v1/overlays/md", response_model=OverlayFeatureCollectionResponse)
async def get_mesoscale_discussions() -> OverlayFeatureCollectionResponse:
    payload = load_overlay_cache(get_settings().mesoscale_discussions_cache_path)
    return OverlayFeatureCollectionResponse(**payload)


@router.get("/api/v1/overlays/lsr", response_model=OverlayFeatureCollectionResponse)
async def get_local_storm_reports() -> OverlayFeatureCollectionResponse:
    payload = load_overlay_cache(get_settings().local_storm_reports_cache_path)
    return OverlayFeatureCollectionResponse(**payload)


@router.get("/api/v1/overlays/watch", response_model=OverlayFeatureCollectionResponse)
async def get_watch_boxes() -> OverlayFeatureCollectionResponse:
    payload = load_overlay_cache(get_settings().watch_overlay_cache_path)
    return OverlayFeatureCollectionResponse(**payload)


@router.get("/api/v1/overlays/metar", response_model=OverlayFeatureCollectionResponse)
async def get_metar_overlay() -> OverlayFeatureCollectionResponse:
    payload = load_metar_cache(get_settings().metar_cache_path)
    observations = payload.get("observations", [])
    features = []
    for observation in observations:
        lat = observation.get("lat")
        lon = observation.get("lon")
        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(lon), float(lat)]},
                "properties": {
                    "station_id": observation.get("station_id"),
                    "observation_time": observation.get("observation_time"),
                    "temp_c": observation.get("temp_c"),
                    "dewpoint_c": observation.get("dewpoint_c"),
                    "wind_dir_deg": observation.get("wind_dir_deg"),
                    "wind_speed_kt": observation.get("wind_speed_kt"),
                    "wind_gust_kt": observation.get("wind_gust_kt"),
                    "visibility_mi": observation.get("visibility_mi"),
                    "pressure_hpa": observation.get("pressure_hpa"),
                    "flight_category": observation.get("flight_category"),
                    "raw_text": observation.get("raw_text"),
                },
            }
        )

    return OverlayFeatureCollectionResponse(
        overlay_kind="metar",
        source=payload.get("source"),
        fetched_at=payload.get("fetched_at"),
        features=features,
    )
