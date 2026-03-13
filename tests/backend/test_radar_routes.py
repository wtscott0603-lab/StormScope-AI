from datetime import datetime, timedelta, timezone
import json

import pytest

from backend.api.config import get_settings
from backend.processor.processing.volume_products import CrossSectionResult


@pytest.mark.asyncio
async def test_radar_frames_route_returns_list(client):
    response = await client.get("/api/radar/frames", params={"site": "KLOT", "product": "REF", "limit": 5})

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert payload[0]["site"] == "KLOT"
    assert payload[0]["product"] == "REF"
    assert payload[0]["tilts_available"] == [0.5, 1.5]


@pytest.mark.asyncio
async def test_sites_route_returns_site_list(client):
    response = await client.get("/api/sites")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert any(site["id"] == "KLOT" for site in payload)


@pytest.mark.asyncio
async def test_products_route_exposes_enabled_and_source_metadata(client):
    response = await client.get("/api/products")

    assert response.status_code == 200
    payload = response.json()
    ref_product = next(product for product in payload if product["id"] == "REF")
    srv_product = next(product for product in payload if product["id"] == "SRV")

    assert ref_product["enabled"] is True
    assert ref_product["available"] is True
    assert ref_product["source_kind"] == "raw"
    assert srv_product["enabled"] is False or srv_product["source_kind"] == "derived"


@pytest.mark.asyncio
async def test_versioned_alerts_and_metar_overlay_routes(client):
    alerts_response = await client.get("/api/v1/alerts")
    assert alerts_response.status_code == 200
    assert isinstance(alerts_response.json(), list)

    metar_overlay_response = await client.get("/api/v1/overlays/metar")
    assert metar_overlay_response.status_code == 200
    payload = metar_overlay_response.json()
    assert payload["type"] == "FeatureCollection"
    assert payload["overlay_kind"] == "metar"
    assert isinstance(payload["features"], list)


@pytest.mark.asyncio
async def test_signatures_route_aggregates_latest_frame_results(client, frame_store):
    ref_frames = await frame_store.list_frames(site="KLOT", product="REF", limit=1)
    assert ref_frames
    ref_frame = ref_frames[-1]

    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_SIG_VEL",
        site="KLOT",
        product="VEL",
        tilt=0.5,
        scan_time=ref_frame.scan_time,
        raw_path="/tmp/mock-vel.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_SIG_VEL",
        status="processed",
        image_path="/tmp/mock-vel.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
    )

    ref_frame_id = ref_frame.frame_id

    await frame_store.upsert_analysis_result(
        ref_frame_id,
        "hail",
        {
            "status": "ok",
            "max_severity": "SEVERE",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "HAIL_LARGE",
                    "severity": "SEVERE",
                    "lat": 41.7,
                    "lon": -88.2,
                    "radius_km": 3.4,
                    "label": "LARGE HAIL",
                    "description": "Synthetic hail core",
                    "confidence": 0.82,
                    "metrics": {"max_dbz": 62.0},
                }
            ],
        },
    )
    await frame_store.upsert_analysis_result(
        ref_frame_id,
        "debris",
        {
            "status": "ok",
            "max_severity": "TORNADO",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "TDS",
                    "severity": "TORNADO",
                    "lat": 41.68,
                    "lon": -88.18,
                    "radius_km": 1.7,
                    "label": "TDS",
                    "description": "Synthetic debris signature",
                    "confidence": 0.9,
                    "metrics": {"min_cc": 0.52},
                }
            ],
        },
    )
    await frame_store.upsert_analysis_result(
        ref_frame_id,
        "wind",
        {
            "status": "ok",
            "max_severity": "SEVERE",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "BOW_ECHO",
                    "severity": "SEVERE",
                    "lat": 41.55,
                    "lon": -88.35,
                    "radius_km": 11.0,
                    "label": "BOW ECHO",
                    "description": "Synthetic bow echo",
                    "confidence": 0.74,
                    "metrics": {"aspect_ratio": 4.2},
                }
            ],
        },
    )
    await frame_store.upsert_analysis_result(
        "KLOT_SIG_VEL",
        "rotation",
        {
            "status": "ok",
            "max_severity": "TORNADO",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "TVS",
                    "severity": "TORNADO",
                    "lat": 41.69,
                    "lon": -88.21,
                    "radius_km": 2.0,
                    "label": "TVS",
                    "description": "Synthetic velocity couplet",
                    "confidence": 0.88,
                    "metrics": {"shear_per_sec": 0.041},
                }
            ],
        },
    )

    response = await client.get("/api/radar/signatures", params={"site": "KLOT", "product": "REF"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["max_severity"] == "TORNADO"
    assert payload["frame_id"] == ref_frame_id
    analyzers = {signature["analyzer"] for signature in payload["signatures"]}
    assert {"hail", "debris", "wind", "rotation"}.issubset(analyzers)

    analysis_response = await client.get(f"/api/radar/frames/{ref_frame_id}/analysis")

    assert analysis_response.status_code == 200
    analysis_payload = analysis_response.json()
    assert analysis_payload["frame_id"] == ref_frame_id
    assert {result["analyzer"] for result in analysis_payload["results"]} >= {"hail", "debris", "wind"}


@pytest.mark.asyncio
async def test_signatures_route_uses_scan_consistent_frames(client, frame_store):
    ref_frames = await frame_store.list_frames(site="KLOT", product="REF", limit=1)
    assert ref_frames
    ref_frame = ref_frames[-1]

    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_VEL_SAME_SCAN",
        site="KLOT",
        product="VEL",
        tilt=0.5,
        scan_time=ref_frame.scan_time,
        raw_path="/tmp/mock-vel-same.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_VEL_SAME_SCAN",
        status="processed",
        image_path="/tmp/mock-vel-same.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
    )

    newer_time = ref_frame.scan_time + timedelta(minutes=5)
    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_VEL_NEWER_SCAN",
        site="KLOT",
        product="VEL",
        tilt=0.5,
        scan_time=newer_time,
        raw_path="/tmp/mock-vel-new.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_VEL_NEWER_SCAN",
        status="processed",
        image_path="/tmp/mock-vel-new.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
    )

    await frame_store.upsert_analysis_result(
        "KLOT_VEL_SAME_SCAN",
        "rotation",
        {
            "status": "ok",
            "max_severity": "SEVERE",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "ROTATION",
                    "severity": "SEVERE",
                    "lat": 41.6,
                    "lon": -88.1,
                    "radius_km": 2.0,
                    "label": "SCAN-MATCH",
                    "description": "same-scan rotation",
                    "confidence": 0.7,
                    "metrics": {},
                }
            ],
        },
    )
    await frame_store.upsert_analysis_result(
        "KLOT_VEL_NEWER_SCAN",
        "rotation",
        {
            "status": "ok",
            "max_severity": "TORNADO",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "TVS",
                    "severity": "TORNADO",
                    "lat": 41.61,
                    "lon": -88.11,
                    "radius_km": 2.0,
                    "label": "NEWER-SCAN",
                    "description": "newer rotation that should not be mixed into the older reflectivity scan",
                    "confidence": 0.9,
                    "metrics": {},
                }
            ],
        },
    )

    response = await client.get("/api/radar/signatures", params={"site": "KLOT", "product": "REF", "tilt": 0.5})

    assert response.status_code == 200
    payload = response.json()
    labels = {signature["label"] for signature in payload["signatures"]}
    assert "SCAN-MATCH" in labels
    assert "NEWER-SCAN" not in labels


@pytest.mark.asyncio
async def test_signatures_route_respects_requested_tilt_for_matching_products(client, frame_store):
    ref_frames = await frame_store.list_frames(site="KLOT", product="REF", limit=1)
    assert ref_frames
    ref_frame = ref_frames[-1]

    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_REF_TILT_15",
        site="KLOT",
        product="REF",
        tilt=1.5,
        scan_time=ref_frame.scan_time,
        raw_path="/tmp/mock-ref-15.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_REF_TILT_15",
        status="processed",
        image_path="/tmp/mock-ref-15.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
        tilts_available="0.5,1.5",
    )

    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_VEL_TILT_05",
        site="KLOT",
        product="VEL",
        tilt=0.5,
        scan_time=ref_frame.scan_time,
        raw_path="/tmp/mock-vel-05.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_VEL_TILT_05",
        status="processed",
        image_path="/tmp/mock-vel-05.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
        tilts_available="0.5,1.5",
    )

    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_VEL_TILT_15",
        site="KLOT",
        product="VEL",
        tilt=1.5,
        scan_time=ref_frame.scan_time,
        raw_path="/tmp/mock-vel-15.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_VEL_TILT_15",
        status="processed",
        image_path="/tmp/mock-vel-15.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
        tilts_available="0.5,1.5",
    )

    await frame_store.upsert_analysis_result(
        "KLOT_VEL_TILT_05",
        "rotation",
        {
            "status": "ok",
            "max_severity": "SEVERE",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "ROTATION",
                    "severity": "SEVERE",
                    "lat": 41.6,
                    "lon": -88.1,
                    "radius_km": 2.0,
                    "label": "LOW-TILT",
                    "description": "lowest tilt rotation",
                    "confidence": 0.7,
                    "metrics": {},
                }
            ],
        },
    )
    await frame_store.upsert_analysis_result(
        "KLOT_VEL_TILT_15",
        "rotation",
        {
            "status": "ok",
            "max_severity": "TORNADO",
            "signature_count": 1,
            "signatures": [
                {
                    "signature_type": "TVS",
                    "severity": "TORNADO",
                    "lat": 41.61,
                    "lon": -88.11,
                    "radius_km": 2.0,
                    "label": "MID-TILT",
                    "description": "requested tilt rotation",
                    "confidence": 0.9,
                    "metrics": {},
                }
            ],
        },
    )

    response = await client.get("/api/radar/signatures", params={"site": "KLOT", "product": "REF", "tilt": 1.5})

    assert response.status_code == 200
    payload = response.json()
    labels = {signature["label"] for signature in payload["signatures"]}
    assert "MID-TILT" in labels
    assert "LOW-TILT" not in labels


@pytest.mark.asyncio
async def test_radar_tilts_and_overlay_routes(client):
    tilts_response = await client.get("/api/radar/tilts", params={"site": "KLOT", "product": "REF"})

    assert tilts_response.status_code == 200
    assert tilts_response.json()["tilts"] == [0.5, 1.5]

    settings = get_settings()
    settings.spc_overlay_cache_path.parent.mkdir(parents=True, exist_ok=True)
    settings.spc_overlay_cache_path.write_text(
        json.dumps(
            {
                "overlay_kind": "spc",
                "source": "test",
                "type": "FeatureCollection",
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "features": [],
            }
        )
    )
    response = await client.get("/api/v1/overlays/spc")
    assert response.status_code == 200
    assert response.json()["overlay_kind"] == "spc"


@pytest.mark.asyncio
async def test_volume_products_ignore_tilt_filter(client, frame_store):
    scan_time = datetime.now(timezone.utc)
    inserted = await frame_store.insert_raw_frame(
        frame_id="KLOT_ET_SCAN",
        site="KLOT",
        product="ET",
        tilt=0.5,
        scan_time=scan_time,
        raw_path="/tmp/mock-et.ar2v",
    )
    assert inserted is True
    await frame_store.update_frame_status(
        "KLOT_ET_SCAN",
        status="processed",
        image_path="/tmp/mock-et.png",
        min_lat=41.1,
        max_lat=42.1,
        min_lon=-88.9,
        max_lon=-87.3,
        tilts_available="0.5,1.5,2.4",
    )

    response = await client.get("/api/radar/frames", params={"site": "KLOT", "product": "ET", "tilt": 4.5})

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["product"] == "ET"
    assert payload[0]["frame_id"] == "KLOT_ET_SCAN"


@pytest.mark.asyncio
async def test_cross_section_route_returns_payload(client, frame_store, monkeypatch):
    ref_frames = await frame_store.list_frames(site="KLOT", product="REF", limit=1)
    assert ref_frames
    ref_frame = ref_frames[-1]

    def fake_build_cross_section(raw_path, **kwargs):
        return CrossSectionResult(
            site="KLOT",
            product="REF",
            frame_id=kwargs["frame_id"],
            ranges_km=[0.0, 10.0, 20.0],
            altitudes_km=[0.0, 1.0, 2.0],
            values=[[10.0, 20.0, None], [30.0, 40.0, 50.0], [None, 55.0, 60.0]],
            start={"lat": 40.0, "lon": -83.0},
            end={"lat": 40.1, "lon": -82.8},
            tilts_used=[0.5, 1.5],
            unit="dBZ",
            method="test method",
            limitation="test limitation",
            generated_at="2026-03-10T00:00:00Z",
        )

    monkeypatch.setattr("backend.processor.processing.volume_products.build_cross_section", fake_build_cross_section)

    response = await client.post(
        "/api/v1/cross-section",
        json={
            "site": "KLOT",
            "product": "REF",
            "frame_id": ref_frame.frame_id,
            "start": {"lat": 40.0, "lon": -83.0},
            "end": {"lat": 40.1, "lon": -82.8},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["frame_id"] == ref_frame.frame_id
    assert payload["tilts_used"] == [0.5, 1.5]
    assert payload["method"] == "test method"


@pytest.mark.asyncio
async def test_cross_section_route_accepts_velocity_products(client, frame_store, monkeypatch):
    ref_frames = await frame_store.list_frames(site="KLOT", product="REF", limit=1)
    assert ref_frames
    ref_frame = ref_frames[-1]

    def fake_build_cross_section(raw_path, **kwargs):
        return CrossSectionResult(
            site="KLOT",
            product="VEL",
            frame_id=kwargs["frame_id"],
            ranges_km=[0.0, 8.0],
            altitudes_km=[0.0, 1.0],
            values=[[-12.0, 18.0], [-20.0, 25.0]],
            start={"lat": 40.0, "lon": -83.0},
            end={"lat": 40.1, "lon": -82.8},
            tilts_used=[0.5, 1.5],
            unit="m/s",
            method="test method",
            limitation="test limitation",
            generated_at="2026-03-10T00:00:00Z",
        )

    monkeypatch.setattr("backend.processor.processing.volume_products.build_cross_section", fake_build_cross_section)

    response = await client.post(
        "/api/v1/cross-section",
        json={
            "site": "KLOT",
            "product": "VEL",
            "frame_id": ref_frame.frame_id,
            "start": {"lat": 40.0, "lon": -83.0},
            "end": {"lat": 40.1, "lon": -82.8},
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["product"] == "VEL"
    assert payload["unit"] == "m/s"
