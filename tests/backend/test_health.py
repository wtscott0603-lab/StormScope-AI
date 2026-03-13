import json
from datetime import timedelta

import pytest

from backend.api.config import Settings
from backend.api.config import get_settings
from backend.shared.time import isoformat_utc, utc_now


@pytest.mark.asyncio
async def test_health_returns_expected_schema(client):
    response = await client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "version" in payload
    assert "processor_last_run" in payload
    assert payload["processor_status"] == "never_run"
    assert payload["db_ok"] is True


@pytest.mark.asyncio
async def test_status_surfaces_cache_and_environment_warnings(client, frame_store):
    settings = get_settings()
    stale_time = isoformat_utc(utc_now() - timedelta(hours=3))
    settings.metar_cache_path.write_text(json.dumps({"fetched_at": stale_time, "observations": []}))
    settings.spc_overlay_cache_path.write_text(
        json.dumps(
            {
                "overlay_kind": "spc",
                "source": "test",
                "type": "FeatureCollection",
                "fetched_at": stale_time,
                "features": [],
            }
        )
    )

    response = await client.get("/api/status")

    assert response.status_code == 200
    payload = response.json()
    assert "cache_status" in payload
    assert payload["cache_status"]["metar"]["stale"] is True
    assert payload["cache_status"]["spc"]["stale"] is True
    assert payload["environment_snapshot_age_minutes"] is None
    assert any("Environment snapshots" in warning for warning in payload["data_warnings"])


def test_api_settings_parse_cors_origins() -> None:
    settings = Settings(
        _env_file=None,
        CORS_ALLOWED_ORIGINS="https://radar.example, https://ops.example",
        CORS_ALLOW_CREDENTIALS=True,
    )

    assert settings.cors_allowed_origins == ["https://radar.example", "https://ops.example"]
    assert settings.cors_allow_credentials is True


@pytest.mark.asyncio
async def test_status_degrades_gracefully_when_db_queries_fail(client, monkeypatch: pytest.MonkeyPatch):
    class BrokenStore:
        async def latest_run(self):
            raise RuntimeError("db unavailable")

        async def count_processed_frames(self):
            raise RuntimeError("db unavailable")

        async def count_sites_with_frames(self):
            raise RuntimeError("db unavailable")

        async def count_active_storms(self):
            raise RuntimeError("db unavailable")

        async def latest_error(self):
            raise RuntimeError("db unavailable")

        async def latest_environment_snapshot_time(self):
            raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.api.routers.status.get_frame_store", lambda: BrokenStore())

    response = await client.get("/api/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["processor_status"] == "error"
    assert payload["frames_cached"] == 0
    assert any("SQLite failed" in warning for warning in payload["data_warnings"])
    assert "db unavailable" in (payload["last_error"] or "")
