from __future__ import annotations

import json
import tempfile
from pathlib import Path

from backend.shared.time import parse_iso_datetime, utc_now


def _read_queue(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key).upper(): str(value) for key, value in payload.items()}


def _write_queue(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def cleanup_requests(path: str | Path, ttl_minutes: int = 30) -> dict[str, str]:
    request_path = Path(path)
    cutoff = utc_now().timestamp() - (ttl_minutes * 60)
    payload = _read_queue(request_path)
    filtered = {
        site_id: timestamp
        for site_id, timestamp in payload.items()
        if (parsed := parse_iso_datetime(timestamp)) and parsed.timestamp() >= cutoff
    }
    if filtered != payload:
        _write_queue(request_path, filtered)
    return filtered


def enqueue_site_request(path: str | Path, site_id: str) -> None:
    request_path = Path(path)
    payload = cleanup_requests(request_path)
    payload[site_id.upper()] = utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _write_queue(request_path, payload)


def requested_sites(path: str | Path, ttl_minutes: int = 30) -> list[str]:
    return sorted(cleanup_requests(path, ttl_minutes))
