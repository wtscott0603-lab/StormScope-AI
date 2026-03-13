from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import re
from typing import Any

import httpx

from backend.processor.analysis.utils import haversine_km
from backend.shared.cache_health import cache_health, cache_is_fresh
from backend.shared.time import isoformat_utc

try:
    from shapely.geometry import Point, shape
except Exception:  # pragma: no cover - optional fallback
    Point = None
    shape = None


USER_AGENT = "radar-platform/0.4 (local-first severe analysis)"
LOGGER = logging.getLogger(__name__)
SPC_CATEGORY_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/1/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
SPC_TORNADO_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/3/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
SPC_HAIL_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/5/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
SPC_WIND_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/7/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
# Day 2 — categorical + individual hazards
SPC_DAY2_CATEGORY_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/9/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
SPC_DAY2_TORNADO_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/11/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
SPC_DAY2_HAIL_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/13/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
SPC_DAY2_WIND_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/15/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
# Day 3 — categorical only (plus combined probabilistic)
SPC_DAY3_CATEGORY_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/SPC_wx_outlks/MapServer/17/"
    "query?where=1%3D1&f=geojson&outFields=*&returnGeometry=true&outSR=4326"
)
MESOSCALE_DISCUSSION_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/outlooks/spc_mesoscale_discussion/MapServer/0/"
    "query?where=1%3D1&f=geojson&outFields=*"
)
LOCAL_STORM_REPORT_URL = (
    "https://mapservices.weather.noaa.gov/vector/rest/services/obs/nws_local_storm_reports/MapServer/0/"
    "query?where=1%3D1&f=geojson&outFields=*"
)
TORNADO_WATCH_URL = "https://api.weather.gov/alerts/active?event=Tornado%20Watch"
SEVERE_THUNDERSTORM_WATCH_URL = "https://api.weather.gov/alerts/active?event=Severe%20Thunderstorm%20Watch"
_PERCENT_RE = re.compile(r"(\d+(?:\.\d+)?)")
_PDS_RE = re.compile(r"\bPDS\b|particularly dangerous situation", re.IGNORECASE)
_SPC_RANK = {
    "General Thunderstorms Risk": 1,
    "TSTM": 1,
    "Marginal Risk": 2,
    "MRGL": 2,
    "Slight Risk": 3,
    "SLGT": 3,
    "Enhanced Risk": 4,
    "ENH": 4,
    "Moderate Risk": 5,
    "MDT": 5,
    "High Risk": 6,
    "HIGH": 6,
}


def write_overlay_cache(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload))


def load_overlay_cache(path: str | Path) -> dict[str, Any]:
    target = Path(path)
    if not target.exists():
        return {"overlay_kind": None, "type": "FeatureCollection", "features": [], "fetched_at": None}
    try:
        return json.loads(target.read_text())
    except json.JSONDecodeError:
        return {"overlay_kind": None, "type": "FeatureCollection", "features": [], "fetched_at": None}


def overlay_cache_status(path: str | Path, *, ttl_minutes: int) -> dict[str, Any]:
    return cache_health(path, ttl_minutes=ttl_minutes)


def overlay_cache_is_fresh(path: str | Path, *, ttl_minutes: int) -> bool:
    return cache_is_fresh(path, ttl_minutes=ttl_minutes)


async def _fetch_geojson(url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=25.0, headers={"User-Agent": USER_AGENT}) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


def _feature_collection(
    overlay_kind: str,
    source: str,
    features: list[dict[str, Any]],
    *,
    fetch_failed: bool = False,
) -> dict[str, Any]:
    return {
        "overlay_kind": overlay_kind,
        "source": source,
        "type": "FeatureCollection",
        "features": features,
        "fetched_at": isoformat_utc(),
        "fetch_failed": fetch_failed,
    }


def _normalize_spc_features(features: list[dict[str, Any]], subtype: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for feature in features:
        properties = dict(feature.get("properties") or {})
        label = str(properties.get("label2") or properties.get("label") or subtype)
        probability = None
        if subtype != "categorical":
            match = _PERCENT_RE.search(str(properties.get("label") or properties.get("label2") or ""))
            probability = float(match.group(1)) if match else None
        normalized.append(
            {
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": {
                    "overlay_subtype": subtype,
                    "category": label,
                    "label": str(properties.get("label") or label),
                    "probability": probability,
                    "valid": properties.get("valid"),
                    "expire": properties.get("expire"),
                    "issue": properties.get("issue"),
                    "stroke": properties.get("stroke"),
                    "fill": properties.get("fill"),
                    "rank": _SPC_RANK.get(label, 0),
                },
            }
        )
    return normalized


def _normalize_md_features(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for feature in features:
        properties = dict(feature.get("properties") or {})
        normalized.append(
            {
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": {
                    "name": properties.get("name") or "Mesoscale Discussion",
                    "discussion_id": properties.get("objectid"),
                },
            }
        )
    return normalized


def _normalize_lsr_features(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for feature in features:
        properties = dict(feature.get("properties") or {})
        normalized.append(
            {
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": {
                    "report_type": properties.get("descript"),
                    "magnitude": properties.get("magnitude"),
                    "units": properties.get("units"),
                    "state": properties.get("state"),
                    "location": properties.get("loc_desc"),
                    "remarks": properties.get("remarks"),
                    "valid_time": properties.get("valid_time"),
                },
            }
        )
    return normalized


def _normalize_watch_features(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for feature in features:
        properties = dict(feature.get("properties") or {})
        event = str(properties.get("event") or "Watch")
        description = str(properties.get("description") or "")
        headline = str(properties.get("headline") or event)
        pds = bool(_PDS_RE.search(headline) or _PDS_RE.search(description))
        rank = 3 if "Tornado Watch" in event else 2 if "Severe Thunderstorm Watch" in event else 1
        if pds:
            rank += 1
        normalized.append(
            {
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": {
                    "watch_type": event,
                    "headline": headline,
                    "description": description,
                    "severity": properties.get("severity"),
                    "certainty": properties.get("certainty"),
                    "urgency": properties.get("urgency"),
                    "sent": properties.get("sent"),
                    "effective": properties.get("effective"),
                    "expires": properties.get("expires"),
                    "watch_rank": rank,
                    "pds": pds,
                },
            }
        )
    return normalized


async def fetch_operational_overlays() -> dict[str, dict[str, Any]]:
    results = await asyncio.gather(
        _fetch_geojson(SPC_CATEGORY_URL),
        _fetch_geojson(SPC_TORNADO_URL),
        _fetch_geojson(SPC_HAIL_URL),
        _fetch_geojson(SPC_WIND_URL),
        _fetch_geojson(SPC_DAY2_CATEGORY_URL),
        _fetch_geojson(SPC_DAY2_TORNADO_URL),
        _fetch_geojson(SPC_DAY2_HAIL_URL),
        _fetch_geojson(SPC_DAY2_WIND_URL),
        _fetch_geojson(SPC_DAY3_CATEGORY_URL),
        _fetch_geojson(MESOSCALE_DISCUSSION_URL),
        _fetch_geojson(LOCAL_STORM_REPORT_URL),
        _fetch_geojson(TORNADO_WATCH_URL),
        _fetch_geojson(SEVERE_THUNDERSTORM_WATCH_URL),
        return_exceptions=True,
    )
    (spc_cat, spc_tor, spc_hail, spc_wind,
     d2_cat, d2_tor, d2_hail, d2_wind,
     d3_cat,
     md, lsr, tor_watch, svr_watch) = results

    def _payload(result: dict[str, Any] | Exception, label: str) -> tuple[dict[str, Any], bool]:
        if isinstance(result, Exception):
            LOGGER.warning("Operational overlay fetch failed for %s: %s", label, result)
            return {"features": []}, True
        return result, False

    spc_cat_payload, spc_cat_failed = _payload(spc_cat, "spc-categorical")
    spc_tor_payload, spc_tor_failed = _payload(spc_tor, "spc-tornado")
    spc_hail_payload, spc_hail_failed = _payload(spc_hail, "spc-hail")
    spc_wind_payload, spc_wind_failed = _payload(spc_wind, "spc-wind")
    d2_cat_payload, d2_cat_failed = _payload(d2_cat, "spc-day2-cat")
    d2_tor_payload, d2_tor_failed = _payload(d2_tor, "spc-day2-tor")
    d2_hail_payload, d2_hail_failed = _payload(d2_hail, "spc-day2-hail")
    d2_wind_payload, d2_wind_failed = _payload(d2_wind, "spc-day2-wind")
    d3_cat_payload, d3_cat_failed = _payload(d3_cat, "spc-day3-cat")
    md_payload, md_failed = _payload(md, "mesoscale-discussions")
    lsr_payload, lsr_failed = _payload(lsr, "local-storm-reports")
    tor_watch_payload, tor_watch_failed = _payload(tor_watch, "tornado-watch")
    svr_watch_payload, svr_watch_failed = _payload(svr_watch, "severe-thunderstorm-watch")
    spc_failed = spc_cat_failed and spc_tor_failed and spc_hail_failed and spc_wind_failed
    watch_failed = tor_watch_failed and svr_watch_failed

    return {
        "spc": _feature_collection(
            "spc",
            "noaa_arcgis_spc",
            _normalize_spc_features(list(spc_cat_payload.get("features", [])), "categorical")
            + _normalize_spc_features(list(spc_tor_payload.get("features", [])), "tornado_probability")
            + _normalize_spc_features(list(spc_hail_payload.get("features", [])), "hail_probability")
            + _normalize_spc_features(list(spc_wind_payload.get("features", [])), "wind_probability"),
            fetch_failed=spc_failed,
        ),
        "spc_day2": _feature_collection(
            "spc_day2",
            "noaa_arcgis_spc",
            _normalize_spc_features(list(d2_cat_payload.get("features", [])), "categorical")
            + _normalize_spc_features(list(d2_tor_payload.get("features", [])), "tornado_probability")
            + _normalize_spc_features(list(d2_hail_payload.get("features", [])), "hail_probability")
            + _normalize_spc_features(list(d2_wind_payload.get("features", [])), "wind_probability"),
            fetch_failed=d2_cat_failed,
        ),
        "spc_day3": _feature_collection(
            "spc_day3",
            "noaa_arcgis_spc",
            _normalize_spc_features(list(d3_cat_payload.get("features", [])), "categorical"),
            fetch_failed=d3_cat_failed,
        ),
        "md": _feature_collection(
            "mesoscale_discussions",
            "noaa_arcgis_spc_mesoscale_discussion",
            _normalize_md_features(list(md_payload.get("features", []))),
            fetch_failed=md_failed,
        ),
        "lsr": _feature_collection(
            "local_storm_reports",
            "noaa_arcgis_nws_local_storm_reports",
            _normalize_lsr_features(list(lsr_payload.get("features", []))),
            fetch_failed=lsr_failed,
        ),
        "watch": _feature_collection(
            "watch_boxes",
            "api_weather_gov_alerts",
            _normalize_watch_features(list(tor_watch_payload.get("features", [])))
            + _normalize_watch_features(list(svr_watch_payload.get("features", []))),
            fetch_failed=watch_failed,
        ),
    }

    def _payload(result: dict[str, Any] | Exception, label: str) -> tuple[dict[str, Any], bool]:
        if isinstance(result, Exception):
            LOGGER.warning("Operational overlay fetch failed for %s: %s", label, result)
            return {"features": []}, True
        return result, False

    spc_cat_payload, spc_cat_failed = _payload(spc_cat, "spc-categorical")
    spc_tor_payload, spc_tor_failed = _payload(spc_tor, "spc-tornado")
    spc_hail_payload, spc_hail_failed = _payload(spc_hail, "spc-hail")
    spc_wind_payload, spc_wind_failed = _payload(spc_wind, "spc-wind")
    md_payload, md_failed = _payload(md, "mesoscale-discussions")
    lsr_payload, lsr_failed = _payload(lsr, "local-storm-reports")
    tor_watch_payload, tor_watch_failed = _payload(tor_watch, "tornado-watch")
    svr_watch_payload, svr_watch_failed = _payload(svr_watch, "severe-thunderstorm-watch")
    spc_failed = spc_cat_failed and spc_tor_failed and spc_hail_failed and spc_wind_failed
    watch_failed = tor_watch_failed and svr_watch_failed

    return {
        "spc": _feature_collection(
            "spc",
            "noaa_arcgis_spc",
            _normalize_spc_features(list(spc_cat_payload.get("features", [])), "categorical")
            + _normalize_spc_features(list(spc_tor_payload.get("features", [])), "tornado_probability")
            + _normalize_spc_features(list(spc_hail_payload.get("features", [])), "hail_probability")
            + _normalize_spc_features(list(spc_wind_payload.get("features", [])), "wind_probability"),
            fetch_failed=spc_failed,
        ),
        "md": _feature_collection(
            "mesoscale_discussions",
            "noaa_arcgis_spc_mesoscale_discussion",
            _normalize_md_features(list(md_payload.get("features", []))),
            fetch_failed=md_failed,
        ),
        "lsr": _feature_collection(
            "local_storm_reports",
            "noaa_arcgis_nws_local_storm_reports",
            _normalize_lsr_features(list(lsr_payload.get("features", []))),
            fetch_failed=lsr_failed,
        ),
        "watch": _feature_collection(
            "watch_boxes",
            "api_weather_gov_alerts",
            _normalize_watch_features(list(tor_watch_payload.get("features", [])))
            + _normalize_watch_features(list(svr_watch_payload.get("features", []))),
            fetch_failed=watch_failed,
        ),
    }


def _feature_contains_point(feature: dict[str, Any], lat: float, lon: float) -> bool:
    if Point is None or shape is None:
        return False
    geometry = feature.get("geometry")
    if not geometry:
        return False
    try:
        return shape(geometry).buffer(0).intersects(Point(lon, lat))
    except Exception:
        return False


def sample_operational_context(
    *,
    lat: float,
    lon: float,
    spc_payload: dict[str, Any] | None,
    md_payload: dict[str, Any] | None,
    lsr_payload: dict[str, Any] | None,
    watch_payload: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    context = {
        "spc": {
            "category": None,
            "category_rank": 0,
            "tornado_probability": None,
            "wind_probability": None,
            "hail_probability": None,
        },
        "md": {
            "active_discussions": 0,
            "discussion_names": [],
        },
        "lsr": {
            "nearby_reports": 0,
            "closest_report_km": None,
            "report_types": [],
        },
        "watch": {
            "count": 0,
            "watch_type": None,
            "pds": False,
            "tornado_watch_rank": 0,
            "wind_watch_rank": 0,
            "labels": [],
        },
    }

    for feature in list((spc_payload or {}).get("features", [])):
        if not _feature_contains_point(feature, lat, lon):
            continue
        props = feature.get("properties") or {}
        subtype = props.get("overlay_subtype")
        if subtype == "categorical":
            rank = int(props.get("rank") or 0)
            if rank >= context["spc"]["category_rank"]:
                context["spc"]["category_rank"] = rank
                context["spc"]["category"] = props.get("category")
        elif subtype == "tornado_probability":
            probability = props.get("probability")
            if probability is not None:
                context["spc"]["tornado_probability"] = max(
                    float(probability),
                    float(context["spc"]["tornado_probability"] or 0.0),
                )
        elif subtype == "hail_probability":
            probability = props.get("probability")
            if probability is not None:
                context["spc"]["hail_probability"] = max(
                    float(probability),
                    float(context["spc"]["hail_probability"] or 0.0),
                )
        elif subtype == "wind_probability":
            probability = props.get("probability")
            if probability is not None:
                context["spc"]["wind_probability"] = max(
                    float(probability),
                    float(context["spc"]["wind_probability"] or 0.0),
                )

    for feature in list((md_payload or {}).get("features", [])):
        if _feature_contains_point(feature, lat, lon):
            context["md"]["active_discussions"] += 1
            name = str((feature.get("properties") or {}).get("name") or "Mesoscale Discussion")
            context["md"]["discussion_names"].append(name)

    nearby_types: set[str] = set()
    closest_report = None
    for feature in list((lsr_payload or {}).get("features", [])):
        geometry = feature.get("geometry") or {}
        coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else None
        if not isinstance(coordinates, list) or len(coordinates) < 2:
            continue
        report_lon = float(coordinates[0])
        report_lat = float(coordinates[1])
        distance = haversine_km(lat, lon, report_lat, report_lon)
        if distance > 90.0:
            continue
        context["lsr"]["nearby_reports"] += 1
        props = feature.get("properties") or {}
        report_type = props.get("report_type")
        if report_type:
            nearby_types.add(str(report_type))
        if closest_report is None or distance < closest_report:
            closest_report = distance
    context["lsr"]["closest_report_km"] = round(closest_report, 1) if closest_report is not None else None
    context["lsr"]["report_types"] = sorted(nearby_types)

    for feature in list((watch_payload or {}).get("features", [])):
        if not _feature_contains_point(feature, lat, lon):
            continue
        props = feature.get("properties") or {}
        watch_type = str(props.get("watch_type") or "Watch")
        rank = int(props.get("watch_rank") or 1)
        context["watch"]["count"] += 1
        context["watch"]["labels"].append(watch_type)
        if props.get("pds"):
            context["watch"]["pds"] = True
        if "Tornado Watch" in watch_type:
            context["watch"]["tornado_watch_rank"] = max(context["watch"]["tornado_watch_rank"], rank)
            if context["watch"]["watch_type"] != "Tornado Watch":
                context["watch"]["watch_type"] = watch_type
        elif "Severe Thunderstorm Watch" in watch_type:
            context["watch"]["wind_watch_rank"] = max(context["watch"]["wind_watch_rank"], rank)
            if context["watch"]["watch_type"] is None:
                context["watch"]["watch_type"] = watch_type
    context["watch"]["labels"] = sorted(set(context["watch"]["labels"]))
    return context
