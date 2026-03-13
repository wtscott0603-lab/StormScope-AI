from __future__ import annotations

import json
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from backend.api.config import get_settings
from backend.api.dependencies import get_frame_store
from backend.api.main import create_app
from backend.processor.cache.frame_store import FrameStore
from backend.shared.db import init_db
from backend.shared.time import utc_now


@pytest.fixture()
def env_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cache_dir = tmp_path / "cache"
    db_path = tmp_path / "radar.db"
    alerts_dir = cache_dir / "alerts"
    overlays_dir = cache_dir / "overlays"
    alerts_dir.mkdir(parents=True, exist_ok=True)
    overlays_dir.mkdir(parents=True, exist_ok=True)
    (alerts_dir / "active.json").write_text(json.dumps({"fetched_at": utc_now().isoformat(), "alerts": []}))
    (overlays_dir / "metars.json").write_text(json.dumps({"fetched_at": utc_now().isoformat(), "observations": []}))
    for overlay_name in ("spc.json", "mesoscale_discussions.json", "lsr.json", "watch_boxes.json"):
        (overlays_dir / overlay_name).write_text(
            json.dumps(
                {
                    "overlay_kind": overlay_name.removesuffix(".json"),
                    "source": "test",
                    "type": "FeatureCollection",
                    "fetched_at": utc_now().isoformat(),
                    "features": [],
                }
            )
        )

    monkeypatch.setenv("DEFAULT_SITE", "KLOT")
    monkeypatch.setenv("ENABLED_PRODUCTS", "REF,VEL")
    monkeypatch.setenv("UPDATE_INTERVAL_SEC", "120")
    monkeypatch.setenv("CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("VITE_MAP_TILE_URL", "https://tile.openstreetmap.org/{z}/{x}/{y}.png")
    get_settings.cache_clear()
    get_frame_store.cache_clear()
    return db_path


@pytest_asyncio.fixture()
async def frame_store(env_setup: Path) -> FrameStore:
    await init_db(env_setup)
    store = FrameStore(env_setup)
    await store.initialize()
    scan_time = utc_now()
    inserted = await store.insert_raw_frame(
        frame_id=f"KLOT_{scan_time:%Y%m%dT%H%M%S}_REF",
        site="KLOT",
        product="REF",
        tilt=0.5,
        scan_time=scan_time,
        raw_path="/tmp/mock.ar2v",
    )
    assert inserted
    await store.update_frame_status(
        f"KLOT_{scan_time:%Y%m%dT%H%M%S}_REF",
        status="processed",
        image_path="/tmp/mock.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
        tilts_available="0.5,1.5",
    )
    return store


@pytest_asyncio.fixture()
async def client(env_setup: Path, frame_store: FrameStore) -> AsyncClient:
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client
