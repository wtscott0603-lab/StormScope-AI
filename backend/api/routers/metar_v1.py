from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from backend.api.config import get_settings
from backend.api.schemas.storms import MetarObservationResponse
from backend.processor.analysis.utils import haversine_km
from backend.shared.metar import load_metar_cache
from backend.shared.nexrad_sites import get_site


router = APIRouter(tags=["metar-v1"])


@router.get("/api/v1/metar", response_model=list[MetarObservationResponse])
async def list_metar_observations(
    site: str = Query(..., min_length=4, max_length=4),
    limit: int = Query(default=24, ge=1, le=100),
) -> list[MetarObservationResponse]:
    site_info = get_site(site.upper())
    if site_info is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site.upper()}")

    payload = load_metar_cache(get_settings().metar_cache_path)
    observations = payload.get("observations", [])
    priority = {station_id: index for index, station_id in enumerate(get_settings().local_station_priority)}
    enriched = []
    for observation in observations:
        distance = haversine_km(site_info.lat, site_info.lon, observation["lat"], observation["lon"])
        if distance <= 250.0:
            enriched.append(
                {
                    **observation,
                    "distance_km": round(distance, 1),
                    "_priority": priority.get(str(observation.get("station_id")), 999),
                }
            )
    enriched.sort(key=lambda observation: (observation["_priority"], observation["distance_km"]))

    return [MetarObservationResponse(**{key: value for key, value in observation.items() if key != "_priority"}) for observation in enriched[:limit]]
