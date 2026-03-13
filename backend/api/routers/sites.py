from __future__ import annotations

from fastapi import APIRouter, HTTPException

from backend.api.config import get_settings
from backend.api.dependencies import enqueue_site, get_frame_store
from backend.api.schemas.radar import SiteDetailResponse, SiteSummaryResponse
from backend.shared.nexrad_sites import get_site, load_sites
from backend.shared.time import parse_iso_datetime


router = APIRouter(tags=["sites"])


@router.get("/api/sites", response_model=list[SiteSummaryResponse])
async def get_sites() -> list[SiteSummaryResponse]:
    store = get_frame_store()
    last_times = await store.site_last_frame_times()
    responses: list[SiteSummaryResponse] = []
    for site in load_sites():
        last_frame_time = parse_iso_datetime(last_times.get(site.id))
        responses.append(
            SiteSummaryResponse(
                id=site.id,
                name=site.name,
                lat=site.lat,
                lon=site.lon,
                state=site.state,
                has_data=last_frame_time is not None,
                last_frame_time=last_frame_time,
            )
        )
    return responses


@router.get("/api/sites/{site_id}", response_model=SiteDetailResponse)
async def get_site_detail(site_id: str) -> SiteDetailResponse:
    site = get_site(site_id)
    if site is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id.upper()}")

    enqueue_site(site.id)
    store = get_frame_store()
    latest_ref = await store.get_latest_frame(site.id, "REF")
    available_products = await store.available_products(site.id)
    settings = get_settings()
    return SiteDetailResponse(
        id=site.id,
        name=site.name,
        lat=site.lat,
        lon=site.lon,
        state=site.state,
        elevation_m=site.elevation_m,
        range_km=460.0,
        last_frame_time=latest_ref.scan_time if latest_ref else None,
        available_products=available_products or settings.enabled_products,
    )
