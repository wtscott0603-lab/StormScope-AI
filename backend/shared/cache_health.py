from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from backend.shared.time import parse_iso_datetime, utc_now


def cache_health(path: str | Path, *, ttl_minutes: int) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {
            "available": False,
            "stale": True,
            "fetched_at": None,
            "age_minutes": None,
        }

    try:
        payload = json.loads(target.read_text())
    except json.JSONDecodeError:
        return {
            "available": False,
            "stale": True,
            "fetched_at": None,
            "age_minutes": None,
        }

    fetched_at = parse_iso_datetime(payload.get("fetched_at"))
    age_minutes = None
    stale = True
    if fetched_at is not None:
        age_minutes = max(0, int((utc_now() - fetched_at).total_seconds() // 60))
        stale = utc_now() - fetched_at >= timedelta(minutes=ttl_minutes)

    return {
        "available": True,
        "stale": stale,
        "fetched_at": fetched_at,
        "age_minutes": age_minutes,
    }


def cache_is_fresh(path: str | Path, *, ttl_minutes: int) -> bool:
    status = cache_health(path, ttl_minutes=ttl_minutes)
    return bool(status["available"] and not status["stale"])
