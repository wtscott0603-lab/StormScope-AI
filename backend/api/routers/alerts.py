from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel

from backend.api.config import get_settings
from backend.api.dependencies import get_frame_store, load_alert_cache
from backend.api.schemas.alerts import AlertResponse


router = APIRouter(tags=["alerts"])


class TriggeredAlertResponse(BaseModel):
    id: int
    alert_id: str
    storm_id: str | None
    site: str
    location_id: str | None
    alert_kind: str
    severity_level: str
    title: str
    body: str
    threat_score: float | None
    triggered_at: str
    scan_time: str
    acknowledged: bool
    acknowledged_at: str | None


@router.get("/api/alerts", response_model=list[AlertResponse])
async def get_alerts(state: str | None = Query(default=None, min_length=2, max_length=2)) -> list[AlertResponse]:
    settings = get_settings()
    cached_alerts = load_alert_cache(settings.alerts_cache_path)
    normalized_state = state.upper() if state else None

    responses: list[AlertResponse] = []
    for alert in cached_alerts:
        if normalized_state and normalized_state not in alert.get("state_codes", []):
            continue
        responses.append(
            AlertResponse(
                id=alert["id"],
                event=alert["event"],
                severity=alert["severity"],
                issued=alert.get("issued"),
                expires=alert.get("expires"),
                geometry=alert["geometry"],
            )
        )
    return responses


@router.get("/api/v1/alerts", response_model=list[AlertResponse])
async def get_alerts_v1(state: str | None = Query(default=None, min_length=2, max_length=2)) -> list[AlertResponse]:
    return await get_alerts(state=state)


@router.get("/api/v1/alerts/triggered", response_model=list[TriggeredAlertResponse])
async def get_triggered_alerts(
    site: str | None = Query(default=None),
    unacknowledged_only: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[TriggeredAlertResponse]:
    """
    Return server-side triggered alerts, newest first.
    These persist across page reloads and fire whether or not the browser tab was open.
    """
    frame_store = get_frame_store()
    rows = await frame_store.list_triggered_alerts(
        site=site,
        unacknowledged_only=unacknowledged_only,
        limit=limit,
    )
    return [TriggeredAlertResponse(**row) for row in rows]


@router.post("/api/v1/alerts/triggered/{alert_id}/acknowledge", response_model=dict)
async def acknowledge_triggered_alert(alert_id: str) -> dict:
    """Mark a triggered alert as acknowledged."""
    frame_store = get_frame_store()
    found = await frame_store.acknowledge_alert(alert_id)
    return {"acknowledged": found, "alert_id": alert_id}
