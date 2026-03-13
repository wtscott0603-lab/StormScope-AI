from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException

from backend.api.dependencies import get_frame_store
from backend.api.schemas.storms import SavedLocationCreate, SavedLocationResponse


router = APIRouter(tags=["locations-v1"])


@router.get("/api/v1/locations", response_model=list[SavedLocationResponse])
async def list_locations() -> list[SavedLocationResponse]:
    store = get_frame_store()
    locations = await store.list_saved_locations()
    return [
        SavedLocationResponse(
            location_id=location.location_id,
            name=location.name,
            lat=location.lat,
            lon=location.lon,
            kind=location.kind,
            created_at=location.created_at,
            updated_at=location.updated_at,
        )
        for location in locations
    ]


@router.post("/api/v1/locations", response_model=SavedLocationResponse)
async def create_location(payload: SavedLocationCreate) -> SavedLocationResponse:
    store = get_frame_store()
    location_id = f"loc-{uuid.uuid4().hex[:10]}"
    await store.upsert_saved_location(
        location_id=location_id,
        name=payload.name.strip(),
        lat=payload.lat,
        lon=payload.lon,
        kind=payload.kind.strip(),
    )
    location = await store.get_saved_location(location_id)
    if location is None:
        raise HTTPException(status_code=500, detail="Failed to create location")
    return SavedLocationResponse(
        location_id=location.location_id,
        name=location.name,
        lat=location.lat,
        lon=location.lon,
        kind=location.kind,
        created_at=location.created_at,
        updated_at=location.updated_at,
    )


@router.delete("/api/v1/locations/{location_id}")
async def delete_location(location_id: str) -> dict[str, str]:
    store = get_frame_store()
    location = await store.get_saved_location(location_id)
    if location is None:
        raise HTTPException(status_code=404, detail=f"Location not found: {location_id}")
    await store.delete_saved_location(location_id)
    return {"status": "deleted"}
