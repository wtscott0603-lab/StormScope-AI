from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from backend.processor.overlays import fetcher as overlay_fetcher
from backend.processor.overlays.fetcher import fetch_operational_overlays
from backend.processor.storms.environment import refresh_metar_cache
from backend.shared.time import isoformat_utc, utc_now


@pytest.mark.asyncio
async def test_refresh_metar_cache_falls_back_to_existing_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "metars.json"
    cached_observations = [
        {
            "station_id": "KLOT",
            "observation_time": isoformat_utc(utc_now() - timedelta(hours=2)),
            "lat": 41.6,
            "lon": -88.1,
            "temp_c": 24.0,
        }
    ]
    cache_path.write_text(
        json.dumps(
            {
                "fetched_at": isoformat_utc(utc_now() - timedelta(hours=2)),
                "observations": cached_observations,
            }
        )
    )

    async def fail_get(self, url: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("provider down", request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.AsyncClient, "get", fail_get)

    observations = await refresh_metar_cache(str(cache_path), ttl_minutes=10)

    assert observations == cached_observations


@pytest.mark.asyncio
async def test_fetch_operational_overlays_returns_partial_payloads_on_feed_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_fetch(url: str) -> dict:
        if url == overlay_fetcher.LOCAL_STORM_REPORT_URL:
            raise httpx.HTTPError("lsr unavailable")
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-88.0, 41.0]},
                    "properties": {"label": "5%", "label2": "MRGL", "name": "Test MD"},
                }
            ],
        }

    monkeypatch.setattr(overlay_fetcher, "_fetch_geojson", fake_fetch)

    payloads = await fetch_operational_overlays()

    assert payloads["spc"]["features"]
    assert payloads["md"]["features"]
    assert payloads["watch"]["features"]
    assert payloads["lsr"]["features"] == []
    assert payloads["lsr"]["fetch_failed"] is True
