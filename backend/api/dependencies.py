from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from fastapi import HTTPException

from backend.api.config import Settings, get_settings
from backend.processor.cache.frame_store import FrameStore
from backend.shared.nexrad_sites import get_site
from backend.shared.site_requests import enqueue_site_request


@lru_cache(maxsize=1)
def get_frame_store() -> FrameStore:
    settings = get_settings()
    return FrameStore(settings.db_path)


async def ensure_site_exists(site_id: str) -> str:
    site = get_site(site_id)
    if site is None:
        raise HTTPException(status_code=404, detail=f"Unknown radar site: {site_id.upper()}")
    return site.id


def enqueue_site(site_id: str) -> None:
    settings = get_settings()
    enqueue_site_request(settings.site_requests_path, site_id)


def load_alert_cache(path: str | Path) -> list[dict]:
    target = Path(path)
    if not target.exists():
        return []
    try:
        payload = json.loads(target.read_text())
    except json.JSONDecodeError:
        return []
    return list(payload.get("alerts", []))
