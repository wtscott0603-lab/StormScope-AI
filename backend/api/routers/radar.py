from __future__ import annotations

import asyncio
import math
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from backend.api.config import get_settings
from backend.api.dependencies import enqueue_site, get_frame_store
from backend.api.schemas.radar import (
    AnalysisResultResponse,
    BBoxResponse,
    CrossSectionRequest,
    CrossSectionResponse,
    FrameAnalysisResponse,
    FrameResponse,
    SignatureMarkerResponse,
    SignaturesResponse,
    TiltListResponse,
)
from backend.shared.nexrad_sites import get_site
from backend.shared.products import product_is_tilt_dependent
from backend.shared.time import isoformat_utc


router = APIRouter(tags=["radar"])
SEVERITY_RANK = {"TORNADO_EMERGENCY": 5, "TORNADO": 4, "SEVERE": 3, "MARGINAL": 2, "NONE": 1}


def _frame_url(frame_id: str) -> str:
    return f"/api/radar/frames/{frame_id}/image"


def _to_response(frame) -> FrameResponse:
    if frame.min_lat is None or frame.max_lat is None or frame.min_lon is None or frame.max_lon is None:
        raise HTTPException(status_code=500, detail=f"Frame {frame.frame_id} is missing geospatial metadata")
    return FrameResponse(
        frame_id=frame.frame_id,
        site=frame.site,
        product=frame.product,
        tilt=frame.tilt,
        tilts_available=frame.tilts_available,
        timestamp=frame.scan_time,
        bbox=BBoxResponse(
            min_lat=frame.min_lat,
            max_lat=frame.max_lat,
            min_lon=frame.min_lon,
            max_lon=frame.max_lon,
        ),
        url=_frame_url(frame.frame_id),
    )


@router.get("/api/radar/frames", response_model=list[FrameResponse])
@router.get("/api/v1/frames", response_model=list[FrameResponse])
async def list_radar_frames(
    site: str = Query(..., min_length=4, max_length=4),
    product: str = Query(..., min_length=2, max_length=5),
    limit: int = Query(default=20, ge=1, le=100),
    tilt: float | None = Query(default=0.5, ge=0.1, le=25.0),
) -> list[FrameResponse]:
    site_id = site.upper()
    product_id = product.upper()
    if get_site(site_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id}")
    enqueue_site(site_id)

    store = get_frame_store()
    effective_tilt = tilt if product_is_tilt_dependent(product_id) else None
    frames = await store.list_frames(site=site_id, product=product_id, limit=limit, tilt=effective_tilt)
    return [_to_response(frame) for frame in frames]


@router.get("/api/radar/frames/{frame_id}", response_model=FrameResponse)
async def get_radar_frame(frame_id: str) -> FrameResponse:
    store = get_frame_store()
    frame = await store.get_frame(frame_id)
    if frame is None or frame.status != "processed":
        raise HTTPException(status_code=404, detail=f"Radar frame not found: {frame_id}")
    return _to_response(frame)


@router.get("/api/radar/frames/{frame_id}/image")
async def get_radar_frame_image(frame_id: str) -> FileResponse:
    store = get_frame_store()
    frame = await store.get_frame(frame_id)
    if frame is None or frame.status != "processed" or not frame.image_path:
        raise HTTPException(status_code=404, detail=f"Radar frame image not found: {frame_id}")
    image_path = Path(frame.image_path)
    if not image_path.exists():
        raise HTTPException(status_code=404, detail=f"Radar frame image missing on disk: {frame_id}")
    return FileResponse(image_path, media_type="image/png")


@router.get("/api/radar/latest", response_model=FrameResponse)
@router.get("/api/v1/frames/latest", response_model=FrameResponse)
async def get_latest_radar_frame(
    site: str = Query(..., min_length=4, max_length=4),
    product: str = Query(..., min_length=2, max_length=5),
    tilt: float | None = Query(default=0.5, ge=0.1, le=25.0),
) -> FrameResponse:
    site_id = site.upper()
    product_id = product.upper()
    if get_site(site_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id}")
    enqueue_site(site_id)

    store = get_frame_store()
    effective_tilt = tilt if product_is_tilt_dependent(product_id) else None
    frame = await store.get_latest_frame(site=site_id, product=product_id, tilt=effective_tilt)
    if frame is None:
        raise HTTPException(status_code=404, detail=f"No processed frames available for {site_id} {product_id}")
    return _to_response(frame)


@router.get("/api/radar/tilts", response_model=TiltListResponse)
async def get_radar_tilts(
    site: str = Query(..., min_length=4, max_length=4),
    product: str = Query(..., min_length=2, max_length=5),
) -> TiltListResponse:
    site_id = site.upper()
    product_id = product.upper()
    if get_site(site_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id}")
    frame = await get_frame_store().get_latest_frame(site_id, product_id)
    if frame is None:
        return TiltListResponse(site=site_id, product=product_id, tilts=[0.5])
    return TiltListResponse(site=site_id, product=product_id, tilts=frame.tilts_available or [frame.tilt])


@router.get("/api/radar/frames/{frame_id}/analysis", response_model=FrameAnalysisResponse)
async def get_radar_frame_analysis(frame_id: str) -> FrameAnalysisResponse:
    store = get_frame_store()
    frame = await store.get_frame(frame_id)
    if frame is None:
        raise HTTPException(status_code=404, detail=f"Radar frame not found: {frame_id}")
    results = await store.get_analysis_results(frame_id)
    return FrameAnalysisResponse(
        frame_id=frame_id,
        results=[AnalysisResultResponse(**result) for result in results],
    )


@router.get("/api/radar/signatures", response_model=SignaturesResponse)
async def get_radar_signatures(
    site: str = Query(..., min_length=4, max_length=4),
    product: str = Query(default="REF", min_length=2, max_length=5),
    tilt: float | None = Query(default=0.5, ge=0.1, le=25.0),
) -> SignaturesResponse:
    site_id = site.upper()
    product_id = product.upper()
    if get_site(site_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id}")
    enqueue_site(site_id)

    store = get_frame_store()
    settings = get_settings()
    effective_tilt = tilt if product_is_tilt_dependent(product_id) else None
    latest_requested_frame = await store.get_latest_frame(site_id, product_id, tilt=effective_tilt)
    if latest_requested_frame is None:
        return SignaturesResponse(
            site=site_id,
            product=product_id,
            frame_id=None,
            signatures=[],
            max_severity="NONE",
            generated_at=isoformat_utc(),
        )

    # Batch: fetch all product frames for this scan_time in a single query
    other_products = [p for p in settings.enabled_products if p != product_id]
    batch = await store.batch_frames_for_scan(
        site_id,
        other_products,
        latest_requested_frame.scan_time,
        tilt=tilt,
    )
    latest_frames_by_id: dict[str, object] = {latest_requested_frame.frame_id: latest_requested_frame}
    for frame_obj in batch.values():
        if frame_obj is not None:
            latest_frames_by_id[frame_obj.frame_id] = frame_obj

    if not latest_frames_by_id:
        return SignaturesResponse(
            site=site_id,
            product=product_id,
            frame_id=None,
            signatures=[],
            max_severity="NONE",
            generated_at=isoformat_utc(),
        )

    all_signatures: list[dict] = []
    max_severity = "NONE"
    for frame_obj in latest_frames_by_id.values():
        analysis_results = await store.get_analysis_results(frame_obj.frame_id)
        for result in analysis_results:
            payload = result["payload"]
            for signature in payload.get("signatures", []):
                lat = signature.get("lat")
                lon = signature.get("lon")
                if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
                    continue
                if not (math.isfinite(lat) and math.isfinite(lon)):
                    continue
                severity = signature.get("severity", "NONE")
                if SEVERITY_RANK.get(severity, 0) > SEVERITY_RANK.get(max_severity, 0):
                    max_severity = severity
                all_signatures.append(
                    {
                        **signature,
                        "frame_id": frame_obj.frame_id,
                        "analyzer": result["analyzer"],
                        "ran_at": result["ran_at"],
                    }
                )

    all_signatures.sort(
        key=lambda signature: (
            SEVERITY_RANK.get(signature.get("severity", "NONE"), 0),
            signature.get("confidence", 0),
        ),
        reverse=True,
    )

    return SignaturesResponse(
        site=site_id,
        product=product_id,
        frame_id=latest_requested_frame.frame_id if latest_requested_frame else next(iter(latest_frames_by_id)),
        signatures=[SignatureMarkerResponse(**signature) for signature in all_signatures],
        max_severity=max_severity,
        generated_at=isoformat_utc(),
    )


@router.post("/api/v1/cross-section", response_model=CrossSectionResponse)
async def create_cross_section(payload: CrossSectionRequest) -> CrossSectionResponse:
    site_id = payload.site.upper()
    product_id = payload.product.upper()
    if get_site(site_id) is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id}")
    if product_id not in {"REF", "VEL", "CC", "ZDR"}:
        raise HTTPException(
            status_code=400,
            detail="Cross-section currently supports REF, VEL, CC, and ZDR. Other products remain experimental.",
        )

    store = get_frame_store()
    if payload.frame_id:
        frame = await store.get_frame(payload.frame_id)
        if frame is None or frame.site != site_id:
            raise HTTPException(status_code=404, detail=f"Radar frame not found: {payload.frame_id}")
    else:
        frame = await store.get_latest_frame(site_id, product_id)

    if frame is None or not frame.raw_path:
        raise HTTPException(status_code=404, detail=f"No raw volume available for cross-section at {site_id} {product_id}")

    from backend.processor.processing.volume_products import build_cross_section

    result = await asyncio.to_thread(
        build_cross_section,
        frame.raw_path,
        product=product_id,
        frame_id=frame.frame_id,
        site=site_id,
        start_lat=payload.start.lat,
        start_lon=payload.start.lon,
        end_lat=payload.end.lat,
        end_lon=payload.end.lon,
        samples=payload.samples,
        altitude_resolution_km=payload.altitude_resolution_km,
        max_altitude_km=payload.max_altitude_km,
    )
    return CrossSectionResponse(
        site=result.site,
        product=result.product,
        frame_id=result.frame_id,
        ranges_km=result.ranges_km,
        altitudes_km=result.altitudes_km,
        values=result.values,
        start=result.start,
        end=result.end,
        tilts_used=result.tilts_used,
        unit=result.unit,
        method=result.method,
        limitation=result.limitation,
        generated_at=result.generated_at,
    )
