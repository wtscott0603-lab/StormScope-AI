from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path
import re
from typing import Any

import httpx

from backend.processor.analysis.utils import haversine_km
from backend.processor.storms.geometry import destination_point
from backend.processor.storms.sounding import fetch_sounding_srh
from backend.shared.metar import load_metar_cache, parse_metar_cache_gz, write_metar_cache
from backend.shared.time import isoformat_utc, parse_iso_datetime, utc_now


METAR_CACHE_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
NWS_POINTS_URL = "https://api.weather.gov/points/{lat},{lon}"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
NWS_USER_AGENT = "radar-platform/0.4 (local-first severe analysis)"
_DURATION_RE = re.compile(r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?)?")
_POINTS_CACHE: dict[str, tuple[datetime, dict[str, Any]]] = {}
_GRID_CACHE: dict[str, tuple[datetime, dict[str, Any]]] = {}
_MODEL_CACHE: dict[str, tuple[datetime, dict[str, Any]]] = {}

# Shared async HTTP client — reuses TLS connections and avoids per-call handshake
# overhead. Lazy-created inside the event loop on first use.
_SHARED_CLIENT: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    """Return module-level shared httpx client, creating it on first use."""
    global _SHARED_CLIENT
    if _SHARED_CLIENT is None or _SHARED_CLIENT.is_closed:
        _SHARED_CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0),
            headers={"User-Agent": NWS_USER_AGENT},
            limits=httpx.Limits(max_connections=12, max_keepalive_connections=8),
            follow_redirects=True,
        )
    return _SHARED_CLIENT

_MODEL_FIELDS = (
    "temperature_2m",
    "dew_point_2m",
    "surface_pressure",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "precipitation",
    "cape",
    "convective_inhibition",
    "freezing_level_height",
    "wind_speed_925hPa",
    "wind_direction_925hPa",
    "wind_speed_1000hPa",
    "wind_direction_1000hPa",
    "wind_speed_850hPa",
    "wind_direction_850hPa",
    "wind_speed_500hPa",
    "wind_direction_500hPa",
    "temperature_850hPa",
    "temperature_700hPa",
    "temperature_500hPa",
)
_PROFILE_LEVELS = (
    ("SFC", None, 10.0, None, "wind_speed_10m", "wind_direction_10m"),
    ("1000", 1000, 110.0, None, "wind_speed_1000hPa", "wind_direction_1000hPa"),
    ("925", 925, 760.0, None, "wind_speed_925hPa", "wind_direction_925hPa"),
    ("850", 850, 1450.0, "temperature_850hPa", "wind_speed_850hPa", "wind_direction_850hPa"),
    ("700", 700, 3010.0, "temperature_700hPa", None, None),
    ("500", 500, 5600.0, "temperature_500hPa", "wind_speed_500hPa", "wind_direction_500hPa"),
)


def _cache_path(cache_dir: str | Path | None, namespace: str, key: str) -> Path | None:
    if cache_dir is None:
        return None
    target = Path(cache_dir) / namespace
    target.mkdir(parents=True, exist_ok=True)
    return target / f"{hashlib.sha256(key.encode('utf-8')).hexdigest()}.json"


def _load_persisted_payload(cache_dir: str | Path | None, namespace: str, key: str) -> tuple[datetime, dict[str, Any]] | None:
    cache_path = _cache_path(cache_dir, namespace, key)
    if cache_path is None or not cache_path.exists():
        return None
    try:
        payload = json.loads(cache_path.read_text())
    except json.JSONDecodeError:
        return None
    fetched_at = parse_iso_datetime(payload.get("fetched_at"))
    body = payload.get("payload")
    if fetched_at is None or not isinstance(body, dict):
        return None
    return fetched_at, body


def _write_persisted_payload(cache_dir: str | Path | None, namespace: str, key: str, payload: dict[str, Any], fetched_at: datetime) -> None:
    cache_path = _cache_path(cache_dir, namespace, key)
    if cache_path is None:
        return
    cache_path.write_text(
        json.dumps(
            {
                "url": key,
                "fetched_at": isoformat_utc(fetched_at),
                "payload": payload,
            }
        )
    )


def _scale(value: float | None, low: float, high: float) -> float:
    if value is None:
        return 0.0
    if high <= low:
        return 0.0
    return max(0.0, min(1.0, (value - low) / (high - low)))


def _nearest_observation(lat: float, lon: float, observations: list[dict], max_distance_km: float = 250.0) -> dict | None:
    best = None
    best_distance = max_distance_km
    for observation in observations:
        distance = haversine_km(lat, lon, observation["lat"], observation["lon"])
        if distance < best_distance:
            best_distance = distance
            best = observation
    if best is None:
        return None
    return {**best, "distance_km": round(best_distance, 1)}


def _direction_alignment(storm_heading_deg: float | None, wind_dir_deg: float | None) -> float:
    if storm_heading_deg is None or wind_dir_deg is None:
        return 0.0
    inflow_bearing = (wind_dir_deg + 180.0) % 360.0
    delta = abs(((storm_heading_deg - inflow_bearing + 180.0) % 360.0) - 180.0)
    return max(0.0, 1.0 - (delta / 180.0))


def _parse_duration(value: str) -> timedelta:
    match = _DURATION_RE.fullmatch(value)
    if not match:
        return timedelta(hours=1)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    return timedelta(days=days, hours=hours, minutes=minutes)


def _parse_valid_interval(valid_time: str) -> tuple[datetime, datetime]:
    if "/" not in valid_time:
        start = parse_iso_datetime(valid_time) or utc_now()
        return start, start + timedelta(hours=1)
    start_raw, duration_raw = valid_time.split("/", 1)
    start = parse_iso_datetime(start_raw) or utc_now()
    return start, start + _parse_duration(duration_raw)


def _pick_grid_value(series: dict[str, Any] | None, valid_at: datetime):
    if not isinstance(series, dict):
        return None

    best_future = None
    best_future_time = None
    best_past = None
    best_past_time = None
    for entry in series.get("values", []):
        start, end = _parse_valid_interval(entry.get("validTime", ""))
        value = entry.get("value")
        if start <= valid_at < end:
            return value
        if start > valid_at and (best_future_time is None or start < best_future_time):
            best_future = value
            best_future_time = start
        if end <= valid_at and (best_past_time is None or end > best_past_time):
            best_past = value
            best_past_time = end
    return best_future if best_future_time is not None else best_past


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_model_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _wind_from_to_components(speed_kmh: float | None, direction_deg: float | None) -> tuple[float | None, float | None]:
    if speed_kmh is None or direction_deg is None:
        return None, None
    direction_to_deg = (direction_deg + 180.0) % 360.0
    radians = math.radians(direction_to_deg)
    return speed_kmh * math.sin(radians), speed_kmh * math.cos(radians)


def _storm_motion_components(speed_kmh: float | None, heading_deg: float | None) -> tuple[float | None, float | None]:
    if speed_kmh is None or heading_deg is None:
        return None, None
    radians = math.radians(heading_deg)
    return speed_kmh * math.sin(radians), speed_kmh * math.cos(radians)


def _vector_difference_kt(
    speed_a_kmh: float | None,
    dir_a_deg: float | None,
    speed_b_kmh: float | None,
    dir_b_deg: float | None,
) -> float | None:
    u_a, v_a = _wind_from_to_components(speed_a_kmh, dir_a_deg)
    u_b, v_b = _wind_from_to_components(speed_b_kmh, dir_b_deg)
    if None in (u_a, v_a, u_b, v_b):
        return None
    diff_kmh = math.hypot(u_b - u_a, v_b - v_a)
    return diff_kmh / 1.852


def _estimate_surface_925hpa_helicity(
    *,
    surface_speed_kmh: float | None,
    surface_dir_deg: float | None,
    low_level_speed_kmh: float | None,
    low_level_dir_deg: float | None,
    storm_speed_kmh: float | None,
    storm_heading_deg: float | None,
) -> float | None:
    sfc_u, sfc_v = _wind_from_to_components(surface_speed_kmh, surface_dir_deg)
    low_u, low_v = _wind_from_to_components(low_level_speed_kmh, low_level_dir_deg)
    storm_u, storm_v = _storm_motion_components(storm_speed_kmh, storm_heading_deg)
    if None in (sfc_u, sfc_v, low_u, low_v, storm_u, storm_v):
        return None

    shear_u_ms = (low_u - sfc_u) / 3.6
    shear_v_ms = (low_v - sfc_v) / 3.6
    mean_sr_u_ms = (((sfc_u + low_u) / 2.0) - storm_u) / 3.6
    mean_sr_v_ms = (((sfc_v + low_v) / 2.0) - storm_v) / 3.6
    srh = abs((shear_u_ms * mean_sr_v_ms) - (shear_v_ms * mean_sr_u_ms))
    return srh


def _estimate_dcape_proxy_jkg(
    *,
    surface_temp_c: float | None,
    dewpoint_c: float | None,
    lapse_rate_cpkm: float | None,
    lcl_m: float | None,
    gust_kmh: float | None,
    surface_wind_kmh: float | None,
) -> float | None:
    if all(value is None for value in (surface_temp_c, dewpoint_c, lapse_rate_cpkm, lcl_m, gust_kmh, surface_wind_kmh)):
        return None
    dewpoint_depression = surface_temp_c - dewpoint_c if surface_temp_c is not None and dewpoint_c is not None else None
    dryness = _scale(dewpoint_depression, 4.0, 20.0)
    lapse_component = _scale(lapse_rate_cpkm, 5.5, 8.8)
    subcloud_depth = _scale(lcl_m, 700.0, 2400.0)
    momentum = max(_scale(gust_kmh, 25.0, 90.0), _scale(surface_wind_kmh, 15.0, 55.0))
    estimate = 75.0 + (dryness * 520.0) + (lapse_component * 430.0) + (subcloud_depth * 320.0) + (momentum * 220.0)
    return round(max(0.0, min(1800.0, estimate)), 0)


def _estimate_lapse_rate_cpkm(temp_700_c: float | None, temp_500_c: float | None) -> float | None:
    if temp_700_c is None or temp_500_c is None:
        return None
    return max(0.0, (temp_700_c - temp_500_c) / 2.5)


def _estimate_lcl_m(temp_c: float | None, dewpoint_c: float | None) -> float | None:
    if temp_c is None or dewpoint_c is None:
        return None
    return max(0.0, 125.0 * (temp_c - dewpoint_c))


def _bounded_delta(current: float | None, future: float | None) -> float | None:
    if current is None or future is None:
        return None
    return future - current


def _wind_point(label: str, speed_kmh: float | None, direction_deg: float | None) -> dict[str, Any] | None:
    if speed_kmh is None or direction_deg is None:
        return None
    u_kmh, v_kmh = _wind_from_to_components(speed_kmh, direction_deg)
    if u_kmh is None or v_kmh is None:
        return None
    return {
        "label": label,
        "speed_kt": round(speed_kmh / 1.852, 1),
        "direction_deg": round(direction_deg, 1),
        "u_kt": round(u_kmh / 1.852, 1),
        "v_kt": round(v_kmh / 1.852, 1),
    }


def _storm_motion_point(storm_speed_kmh: float | None, storm_heading_deg: float | None) -> dict[str, Any] | None:
    if storm_speed_kmh is None or storm_heading_deg is None:
        return None
    speed_kt = storm_speed_kmh / 1.852
    radians = math.radians(storm_heading_deg)
    return {
        "label": "Storm Motion",
        "speed_kt": round(speed_kt, 1),
        "direction_deg": round(storm_heading_deg, 1),
        "u_kt": round(speed_kt * math.sin(radians), 1),
        "v_kt": round(speed_kt * math.cos(radians), 1),
    }


def _build_hodograph(
    current_model: dict[str, Any] | None,
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
) -> dict[str, Any] | None:
    if current_model is None:
        return None
    points = [
        _wind_point("SFC", _safe_float(current_model.get("wind_speed_10m")), _safe_float(current_model.get("wind_direction_10m"))),
        _wind_point("925", _safe_float(current_model.get("wind_speed_925hPa")), _safe_float(current_model.get("wind_direction_925hPa"))),
        _wind_point("850", _safe_float(current_model.get("wind_speed_850hPa")), _safe_float(current_model.get("wind_direction_850hPa"))),
        _wind_point("500", _safe_float(current_model.get("wind_speed_500hPa")), _safe_float(current_model.get("wind_direction_500hPa"))),
    ]
    filtered_points = [point for point in points if point is not None]
    if not filtered_points:
        return None
    return {
        "type": "reduced_hodograph",
        "label": "Reduced 4-level hodograph",
        "points": filtered_points,
        "storm_motion": _storm_motion_point(motion_speed_kmh, motion_heading_deg),
    }


def _build_reduced_profile(
    current_model: dict[str, Any] | None,
    *,
    surface_temp_c: float | None,
    surface_dewpoint_c: float | None,
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
) -> dict[str, Any] | None:
    if current_model is None:
        return None

    levels: list[dict[str, Any]] = []
    for label, pressure_hpa, height_m, temperature_field, wind_speed_field, wind_direction_field in _PROFILE_LEVELS:
        temperature_c = surface_temp_c if temperature_field is None and label == "SFC" else _safe_float(current_model.get(temperature_field)) if temperature_field else None
        dewpoint_c = surface_dewpoint_c if label == "SFC" else None
        wind_speed_kmh = _safe_float(current_model.get(wind_speed_field)) if wind_speed_field else None
        wind_direction_deg = _safe_float(current_model.get(wind_direction_field)) if wind_direction_field else None
        if all(value is None for value in (temperature_c, dewpoint_c, wind_speed_kmh, wind_direction_deg)):
            continue
        levels.append(
            {
                "label": label,
                "pressure_hpa": pressure_hpa,
                "height_m_estimate": round(height_m),
                "height_is_estimated": True,
                "temperature_c": round(temperature_c, 1) if temperature_c is not None else None,
                "dewpoint_c": round(dewpoint_c, 1) if dewpoint_c is not None else None,
                "wind_speed_kmh": round(wind_speed_kmh, 1) if wind_speed_kmh is not None else None,
                "wind_direction_deg": round(wind_direction_deg, 1) if wind_direction_deg is not None else None,
                "source": "open_meteo_hourly_model" if label != "SFC" else "open_meteo_hourly_model+surface_context",
            }
        )

    if not levels:
        return None

    return {
        "type": "reduced_model_profile",
        "label": "Reduced model profile",
        "levels": levels,
        "valid_at": current_model.get("valid_at"),
        "storm_motion": _storm_motion_point(motion_speed_kmh, motion_heading_deg),
        "limitation": "Profile uses hourly model levels with standard-atmosphere height estimates. It is useful for local context, not a full observed sounding.",
    }


def _build_field_provenance(current_model: dict[str, Any] | None, *, sounding_srh: bool = False) -> dict[str, str]:
    def model_field(field_name: str) -> str:
        return "direct_model" if current_model and current_model.get(field_name) is not None else "unavailable"

    return {
        "cape_jkg": model_field("cape"),
        "cin_jkg": model_field("convective_inhibition"),
        "freezing_level_m": model_field("freezing_level_height"),
        "bulk_shear_06km_kt": "derived_from_model_winds" if current_model else "unavailable",
        "bulk_shear_01km_kt": "derived_from_model_winds" if current_model else "unavailable",
        "srh_surface_925hpa_m2s2": (
            "sounding_integrated_hodograph" if sounding_srh else "proxy_from_surface_and_925hpa_winds"
        ) if current_model else "unavailable",
        "dcape_jkg": "proxy_from_surface_dryness_lapse_rate_and_wind" if current_model else "unavailable",
        "lapse_rate_midlevel_cpkm": "derived_from_700_500hpa_temperature" if current_model else "unavailable",
        "lcl_m": "derived_from_surface_temperature_and_dewpoint",
        "lfc_m": "unavailable",
        "pwat_mm": "unavailable",
        "profile_summary": "reduced_model_profile" if current_model else "unavailable",
    }


def _build_source_notes(current_model: dict[str, Any] | None, current_obs: dict | None, current_grid: dict | None, *, sounding_meta: dict[str, Any] | None = None) -> list[str]:
    notes: list[str] = []
    if current_model is not None:
        notes.append("CAPE, CIN, and freezing level come directly from hourly Open-Meteo model fields when present.")
        notes.append("Bulk shear and the reduced profile are derived from model wind and temperature levels.")
        if sounding_meta is not None:
            station = sounding_meta.get("station_name") or sounding_meta.get("station_id") or "nearby"
            dist_km = sounding_meta.get("station_distance_km")
            dist_str = f" ({dist_km:.0f} km away)" if dist_km is not None else ""
            notes.append(
                f"0–1 km SRH is integrated from the {station}{dist_str} rawinsonde sounding "
                f"(IEM, {sounding_meta.get('level_count', '?')} levels). "
                "This replaces the model-level proxy with a real hodograph integration."
            )
        else:
            notes.append(
                "Surface-to-925 hPa helicity and DCAPE are proxy estimates derived from model wind levels, "
                "low-level lapse rates, and surface dryness — no observed sounding was available. "
                "Treat these proxy estimates as approximate; they can deviate significantly from a real sounding."
            )
    else:
        notes.append("Model profile fields were unavailable for this storm snapshot.")
    if current_obs is not None:
        notes.append("METAR data adds local surface temperature, dewpoint, wind, pressure, and visibility context.")
    if current_grid is not None:
        notes.append("NWS gridpoint data adds thunder probability, QPF, weather wording, and hazard context ahead of the path.")
    return notes


def _model_url(lat: float, lon: float) -> str:
    fields = ",".join(_MODEL_FIELDS)
    return (
        f"{OPEN_METEO_FORECAST_URL}?latitude={round(lat, 3)}&longitude={round(lon, 3)}"
        f"&hourly={fields}&forecast_hours=8&timezone=UTC"
    )


async def refresh_metar_cache(cache_path: str, *, ttl_minutes: int = 10) -> list[dict]:
    cached = load_metar_cache(cache_path)
    fetched_at = parse_iso_datetime(cached.get("fetched_at"))
    if fetched_at is not None and utc_now() - fetched_at < timedelta(minutes=ttl_minutes):
        return cached["observations"]

    try:
        client = _get_http_client()
        response = await client.get(METAR_CACHE_URL)
        response.raise_for_status()
        observations = parse_metar_cache_gz(response.content)
        write_metar_cache(cache_path, observations)
        return observations
    except Exception:
        cached_observations = list(cached.get("observations", []))
        if cached_observations:
            return cached_observations
        raise


async def _cached_json(
    url: str,
    cache: dict[str, tuple[datetime, dict[str, Any]]],
    *,
    ttl_minutes: int,
    cache_dir: str | Path | None = None,
    namespace: str,
) -> dict[str, Any]:
    cached = cache.get(url)
    now = utc_now()
    if cached is not None and now - cached[0] < timedelta(minutes=ttl_minutes):
        return cached[1]

    persisted = _load_persisted_payload(cache_dir, namespace, url)
    if persisted is not None and now - persisted[0] < timedelta(minutes=ttl_minutes):
        cache[url] = persisted
        return persisted[1]

    # Stale-while-revalidate: serve cached data immediately if it exists but is
    # within 2× the TTL window, then attempt a fresh fetch in the background.
    stale_data: dict[str, Any] | None = None
    stale_ts: datetime | None = None
    if cached is not None:
        stale_data, stale_ts = cached[1], cached[0]
    elif persisted is not None:
        stale_data, stale_ts = persisted[1], persisted[0]

    try:
        client = _get_http_client()
        response = await client.get(url)
        response.raise_for_status()
        payload = response.json()
        cache[url] = (now, payload)
        _write_persisted_payload(cache_dir, namespace, url, payload, now)
        return payload
    except Exception:
        if stale_data is not None:
            # Return stale data as fallback — callers should check freshness
            if stale_ts is not None:
                cache[url] = (stale_ts, stale_data)
            return stale_data
        raise


async def _resolve_grid_metadata(lat: float, lon: float, ttl_minutes: int, *, cache_dir: str | Path | None = None) -> dict[str, Any] | None:
    url = NWS_POINTS_URL.format(lat=round(lat, 4), lon=round(lon, 4))
    payload = await _cached_json(url, _POINTS_CACHE, ttl_minutes=ttl_minutes, cache_dir=cache_dir, namespace="points")
    properties = payload.get("properties") or {}
    forecast_grid_url = properties.get("forecastGridData")
    if not forecast_grid_url:
        return None
    return {
        "gridpoint_id": f"{properties.get('gridId')}/{properties.get('gridX')},{properties.get('gridY')}",
        "forecast_grid_url": forecast_grid_url,
    }


async def _sample_nws_gridpoint(
    lat: float,
    lon: float,
    *,
    valid_at: datetime,
    ttl_minutes: int,
    cache_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    metadata = await _resolve_grid_metadata(lat, lon, ttl_minutes, cache_dir=cache_dir)
    if metadata is None:
        return None

    grid_payload = await _cached_json(
        metadata["forecast_grid_url"],
        _GRID_CACHE,
        ttl_minutes=ttl_minutes,
        cache_dir=cache_dir,
        namespace="grid",
    )
    properties = grid_payload.get("properties") or {}

    weather_raw = _pick_grid_value(properties.get("weather"), valid_at)
    weather_summary = None
    if isinstance(weather_raw, list):
        labels = []
        for item in weather_raw:
            if not isinstance(item, dict):
                continue
            parts = [item.get("coverage"), item.get("weather"), item.get("intensity")]
            label = " ".join(part for part in parts if part)
            if label:
                labels.append(label)
        weather_summary = ", ".join(labels) if labels else None

    hazards_raw = _pick_grid_value(properties.get("hazards"), valid_at)
    hazards: list[str] = []
    if isinstance(hazards_raw, list):
        for item in hazards_raw:
            phenomenon = None
            significance = None
            if isinstance(item, dict):
                phenomenon = item.get("phenomenon")
                significance = item.get("significance")
            if phenomenon:
                hazards.append(f"{phenomenon}:{significance}" if significance else str(phenomenon))

    return {
        "gridpoint_id": metadata["gridpoint_id"],
        "valid_at": isoformat_utc(valid_at),
        "temperature_c": _pick_grid_value(properties.get("temperature"), valid_at),
        "dewpoint_c": _pick_grid_value(properties.get("dewpoint"), valid_at),
        "wind_speed_kmh": _pick_grid_value(properties.get("windSpeed"), valid_at),
        "wind_dir_deg": _pick_grid_value(properties.get("windDirection"), valid_at),
        "probability_of_thunder_pct": _pick_grid_value(properties.get("probabilityOfThunder"), valid_at),
        "quantitative_precip_mm": _pick_grid_value(properties.get("quantitativePrecipitation"), valid_at),
        "weather_summary": weather_summary,
        "hazards": hazards,
    }


def _sample_hourly_fields(payload: dict[str, Any], valid_at: datetime) -> dict[str, Any] | None:
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        return None

    chosen_index = None
    smallest_delta = None
    for index, value in enumerate(times):
        parsed = _parse_model_time(value)
        if parsed is None:
            continue
        delta = abs((parsed - valid_at).total_seconds())
        if smallest_delta is None or delta < smallest_delta:
            smallest_delta = delta
            chosen_index = index
    if chosen_index is None:
        return None

    sampled: dict[str, Any] = {
        "valid_at": isoformat_utc(_parse_model_time(times[chosen_index]) or valid_at),
        "field_count": len(hourly),
    }
    for field_name, values in hourly.items():
        if field_name == "time" or not isinstance(values, list):
            continue
        sampled[field_name] = _safe_float(values[chosen_index]) if chosen_index < len(values) else None
    return sampled


async def _sample_open_meteo_environment(
    lat: float,
    lon: float,
    *,
    valid_at: datetime,
    ttl_minutes: int,
    cache_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    payload = await _cached_json(_model_url(lat, lon), _MODEL_CACHE, ttl_minutes=ttl_minutes, cache_dir=cache_dir, namespace="model")
    sampled = _sample_hourly_fields(payload, valid_at)
    if sampled is None:
        return None
    sampled["source"] = "open_meteo_hourly_model"
    sampled["lat"] = round(lat, 4)
    sampled["lon"] = round(lon, 4)
    return sampled


def _environment_confidence(
    *,
    current_obs: dict | None,
    current_model: dict | None,
    current_grid: dict | None,
) -> float:
    confidence = 0.2
    if current_model is not None:
        confidence += 0.45
    if current_grid is not None:
        confidence += 0.15
    if current_obs is not None:
        confidence += 0.20
        distance = _safe_float(current_obs.get("distance_km"))
        if distance is not None:
            confidence -= min(0.12, distance / 600.0)
    model_fields = int(current_model.get("field_count", 0)) if current_model else 0
    if model_fields >= 10:
        confidence += 0.08
    return max(0.0, min(1.0, confidence))


def _data_freshness_minutes(*timestamps: str | None) -> int | None:
    ages: list[int] = []
    now = utc_now()
    for timestamp in timestamps:
        parsed = parse_iso_datetime(timestamp)
        if parsed is None:
            continue
        ages.append(max(0, int((now - parsed).total_seconds() // 60)))
    return max(ages) if ages else None


async def build_environment_snapshot(
    *,
    site: str,
    storm_id: str,
    centroid_lat: float,
    centroid_lon: float,
    motion_heading_deg: float | None,
    motion_speed_kmh: float | None,
    observations: list[dict],
    grid_cache_ttl_minutes: int = 30,
    open_meteo_cache_ttl_minutes: int = 45,
    cache_dir: str | Path | None = None,
    sounding_cache_dir: str | Path | None = None,
) -> dict | None:
    now = utc_now()
    current_obs = _nearest_observation(centroid_lat, centroid_lon, observations) if observations else None

    future_lat = centroid_lat
    future_lon = centroid_lon
    if motion_heading_deg is not None and motion_speed_kmh is not None and motion_speed_kmh >= 10.0:
        future_lat, future_lon = destination_point(
            centroid_lat,
            centroid_lon,
            motion_heading_deg,
            motion_speed_kmh * 0.75,
        )
    future_obs = _nearest_observation(future_lat, future_lon, observations) if observations else current_obs

    current_grid = None
    future_grid = None
    current_model = None
    future_model = None
    try:
        current_grid = await _sample_nws_gridpoint(
            centroid_lat,
            centroid_lon,
            valid_at=now,
            ttl_minutes=grid_cache_ttl_minutes,
            cache_dir=cache_dir,
        )
        future_grid = await _sample_nws_gridpoint(
            future_lat,
            future_lon,
            valid_at=now + timedelta(minutes=45),
            ttl_minutes=grid_cache_ttl_minutes,
            cache_dir=cache_dir,
        )
    except Exception:
        current_grid = None
        future_grid = None

    try:
        current_model = await _sample_open_meteo_environment(
            centroid_lat,
            centroid_lon,
            valid_at=now,
            ttl_minutes=open_meteo_cache_ttl_minutes,
            cache_dir=cache_dir,
        )
        future_model = await _sample_open_meteo_environment(
            future_lat,
            future_lon,
            valid_at=now + timedelta(minutes=45),
            ttl_minutes=open_meteo_cache_ttl_minutes,
            cache_dir=cache_dir,
        )
    except Exception:
        current_model = None
        future_model = None

    if current_obs is None and current_grid is None and current_model is None:
        return None

    # --- Real sounding SRH (0-1 km hodograph integration) ---
    sounding_result: dict[str, Any] | None = None
    try:
        sounding_result = await fetch_sounding_srh(
            lat=centroid_lat,
            lon=centroid_lon,
            storm_heading_deg=motion_heading_deg,
            storm_speed_kmh=motion_speed_kmh,
            cache_dir=Path(sounding_cache_dir) if sounding_cache_dir is not None else (Path(cache_dir) if cache_dir is not None else None),
            layer_top_m=1000.0,
        )
    except Exception:
        sounding_result = None

    current_temp = _safe_float((current_model or {}).get("temperature_2m")) or _safe_float((current_grid or {}).get("temperature_c")) or _safe_float((current_obs or {}).get("temp_c"))
    current_dewpoint = _safe_float((current_model or {}).get("dew_point_2m")) or _safe_float((current_grid or {}).get("dewpoint_c")) or _safe_float((current_obs or {}).get("dewpoint_c"))
    future_temp = _safe_float((future_model or {}).get("temperature_2m")) or _safe_float((future_grid or {}).get("temperature_c")) or current_temp
    future_dewpoint = _safe_float((future_model or {}).get("dew_point_2m")) or _safe_float((future_grid or {}).get("dewpoint_c")) or current_dewpoint
    current_surface_wind = _safe_float((current_model or {}).get("wind_speed_10m"))
    future_surface_wind = _safe_float((future_model or {}).get("wind_speed_10m"))
    current_surface_wind_kt = (
        current_surface_wind / 1.852 if current_surface_wind is not None else _safe_float((current_obs or {}).get("wind_speed_kt"))
    )
    pressure_hpa = _safe_float((current_model or {}).get("surface_pressure")) or _safe_float((current_obs or {}).get("pressure_hpa"))
    visibility_mi = _safe_float((current_obs or {}).get("visibility_mi"))
    thunder_now = _safe_float((current_grid or {}).get("probability_of_thunder_pct"))
    thunder_future = _safe_float((future_grid or {}).get("probability_of_thunder_pct"))
    qpf_future = _safe_float((future_grid or {}).get("quantitative_precip_mm"))
    precip_now = _safe_float((current_model or {}).get("precipitation"))
    precip_future = _safe_float((future_model or {}).get("precipitation"))

    cape_now = _safe_float((current_model or {}).get("cape"))
    cape_future = _safe_float((future_model or {}).get("cape"))
    cin_now = _safe_float((current_model or {}).get("convective_inhibition"))
    cin_future = _safe_float((future_model or {}).get("convective_inhibition"))
    freezing_level_now = _safe_float((current_model or {}).get("freezing_level_height"))
    freezing_level_future = _safe_float((future_model or {}).get("freezing_level_height"))
    shear_06_now = _vector_difference_kt(
        _safe_float((current_model or {}).get("wind_speed_10m")),
        _safe_float((current_model or {}).get("wind_direction_10m")),
        _safe_float((current_model or {}).get("wind_speed_500hPa")),
        _safe_float((current_model or {}).get("wind_direction_500hPa")),
    )
    shear_06_future = _vector_difference_kt(
        _safe_float((future_model or {}).get("wind_speed_10m")),
        _safe_float((future_model or {}).get("wind_direction_10m")),
        _safe_float((future_model or {}).get("wind_speed_500hPa")),
        _safe_float((future_model or {}).get("wind_direction_500hPa")),
    )
    shear_01_now = _vector_difference_kt(
        _safe_float((current_model or {}).get("wind_speed_10m")),
        _safe_float((current_model or {}).get("wind_direction_10m")),
        _safe_float((current_model or {}).get("wind_speed_925hPa")),
        _safe_float((current_model or {}).get("wind_direction_925hPa")),
    )
    shear_01_future = _vector_difference_kt(
        _safe_float((future_model or {}).get("wind_speed_10m")),
        _safe_float((future_model or {}).get("wind_direction_10m")),
        _safe_float((future_model or {}).get("wind_speed_925hPa")),
        _safe_float((future_model or {}).get("wind_direction_925hPa")),
    )
    srh_proxy_now = _estimate_surface_925hpa_helicity(
        surface_speed_kmh=_safe_float((current_model or {}).get("wind_speed_10m")),
        surface_dir_deg=_safe_float((current_model or {}).get("wind_direction_10m")),
        low_level_speed_kmh=_safe_float((current_model or {}).get("wind_speed_925hPa")),
        low_level_dir_deg=_safe_float((current_model or {}).get("wind_direction_925hPa")),
        storm_speed_kmh=motion_speed_kmh,
        storm_heading_deg=motion_heading_deg,
    )
    srh_proxy_future = _estimate_surface_925hpa_helicity(
        surface_speed_kmh=_safe_float((future_model or {}).get("wind_speed_10m")),
        surface_dir_deg=_safe_float((future_model or {}).get("wind_direction_10m")),
        low_level_speed_kmh=_safe_float((future_model or {}).get("wind_speed_925hPa")),
        low_level_dir_deg=_safe_float((future_model or {}).get("wind_direction_925hPa")),
        storm_speed_kmh=motion_speed_kmh,
        storm_heading_deg=motion_heading_deg,
    )
    # Prefer real sounding SRH when available; fall back to 2-point proxy
    srh_01km_now: float | None = (
        sounding_result["srh_m2s2"] if sounding_result is not None else srh_proxy_now
    )
    srh_01km_future = srh_proxy_future  # sounding is static per launch; future uses proxy delta
    lapse_rate_now = _estimate_lapse_rate_cpkm(
        _safe_float((current_model or {}).get("temperature_700hPa")),
        _safe_float((current_model or {}).get("temperature_500hPa")),
    )
    lapse_rate_future = _estimate_lapse_rate_cpkm(
        _safe_float((future_model or {}).get("temperature_700hPa")),
        _safe_float((future_model or {}).get("temperature_500hPa")),
    )
    lcl_now = _estimate_lcl_m(current_temp, current_dewpoint)
    lcl_future = _estimate_lcl_m(future_temp, future_dewpoint)

    current_wind_dir = _safe_float((current_model or {}).get("wind_direction_10m")) or _safe_float((current_obs or {}).get("wind_dir_deg"))
    gust_now = _safe_float((current_model or {}).get("wind_gusts_10m"))
    gust_future = _safe_float((future_model or {}).get("wind_gusts_10m"))
    dewpoint_depression_now = current_temp - current_dewpoint if current_temp is not None and current_dewpoint is not None else None
    dewpoint_depression_future = future_temp - future_dewpoint if future_temp is not None and future_dewpoint is not None else None
    dcape_proxy_now = _estimate_dcape_proxy_jkg(
        surface_temp_c=current_temp,
        dewpoint_c=current_dewpoint,
        lapse_rate_cpkm=lapse_rate_now,
        lcl_m=lcl_now,
        gust_kmh=gust_now,
        surface_wind_kmh=current_surface_wind,
    )
    dcape_proxy_future = _estimate_dcape_proxy_jkg(
        surface_temp_c=future_temp,
        dewpoint_c=future_dewpoint,
        lapse_rate_cpkm=lapse_rate_future,
        lcl_m=lcl_future,
        gust_kmh=gust_future,
        surface_wind_kmh=future_surface_wind,
    )
    hodograph = _build_hodograph(current_model, motion_heading_deg, motion_speed_kmh)
    reduced_profile = _build_reduced_profile(
        current_model,
        surface_temp_c=current_temp,
        surface_dewpoint_c=current_dewpoint,
        motion_heading_deg=motion_heading_deg,
        motion_speed_kmh=motion_speed_kmh,
    )
    field_provenance = _build_field_provenance(current_model, sounding_srh=sounding_result is not None)
    source_notes = _build_source_notes(current_model, current_obs, current_grid, sounding_meta=sounding_result)

    hail_favorability = (
        (_scale(cape_now, 500.0, 3000.0) * 0.35)
        + (_scale(shear_06_now, 25.0, 60.0) * 0.25)
        + (_scale(lapse_rate_now, 6.3, 8.5) * 0.20)
        + ((1.0 - _scale(freezing_level_now, 3200.0, 5200.0)) * 0.20)
    )
    wind_favorability = (
        (_scale(shear_06_now, 20.0, 55.0) * 0.25)
        + (_scale(cape_now, 400.0, 2500.0) * 0.20)
        + (_scale(gust_future, 30.0, 80.0) * 0.20)
        + (_scale(dcape_proxy_now, 400.0, 1400.0) * 0.15)
        + (_scale(dewpoint_depression_now, 5.0, 18.0) * 0.20)
        + (_scale(thunder_future, 15.0, 70.0) * 0.15)
    )
    tornado_favorability = (
        (_scale(srh_01km_now, 75.0, 250.0) * 0.30)
        + (_scale(shear_01_now, 10.0, 30.0) * 0.25)
        + (_scale(cape_now, 500.0, 2500.0) * 0.20)
        + ((1.0 - _scale(lcl_now, 1200.0, 2400.0)) * 0.15)
        + (_direction_alignment(motion_heading_deg, current_wind_dir) * 0.10)
    )
    heavy_rain_favorability = (
        (_scale(cape_now, 300.0, 2500.0) * 0.25)
        + (_scale(current_dewpoint, 12.0, 23.0) * 0.30)
        + (_scale(qpf_future, 2.0, 18.0) * 0.20)
        + (_scale(precip_future, 0.5, 6.0) * 0.25)
    )

    cape_delta = _bounded_delta(cape_now, cape_future)
    shear_06_delta = _bounded_delta(shear_06_now, shear_06_future)
    shear_01_delta = _bounded_delta(shear_01_now, shear_01_future)
    srh_delta = _bounded_delta(srh_01km_now, srh_01km_future)
    dcape_delta = _bounded_delta(dcape_proxy_now, dcape_proxy_future)
    freezing_delta = _bounded_delta(freezing_level_now, freezing_level_future)
    thunder_delta = _bounded_delta(thunder_now, thunder_future)
    qpf_delta = _bounded_delta(precip_now, precip_future)
    lapse_delta = _bounded_delta(lapse_rate_now, lapse_rate_future)

    trend_bits: list[str] = []
    if cape_delta is not None:
        if cape_delta >= 250.0:
            trend_bits.append("greater instability ahead")
        elif cape_delta <= -250.0:
            trend_bits.append("less instability ahead")
    if shear_06_delta is not None:
        if shear_06_delta >= 6.0:
            trend_bits.append("stronger deep-layer shear ahead")
        elif shear_06_delta <= -6.0:
            trend_bits.append("weaker deep-layer shear ahead")
    if shear_01_delta is not None:
        if shear_01_delta >= 4.0:
            trend_bits.append("stronger low-level shear ahead")
        elif shear_01_delta <= -4.0:
            trend_bits.append("weaker low-level shear ahead")
    if freezing_delta is not None:
        if freezing_delta <= -300.0:
            trend_bits.append("lower freezing levels ahead")
        elif freezing_delta >= 300.0:
            trend_bits.append("higher freezing levels ahead")
    if thunder_delta is not None:
        if thunder_delta >= 10.0:
            trend_bits.append("higher convective coverage ahead")
        elif thunder_delta <= -10.0:
            trend_bits.append("lower convective coverage ahead")

    trend_phrase = ", ".join(trend_bits) if trend_bits else "only modest environmental change ahead"
    convective_signal = max(
        _scale(cape_now, 500.0, 3000.0),
        _scale(cape_future, 500.0, 3000.0),
        _scale(thunder_future, 10.0, 70.0),
    )
    intensification_signal = max(
        0.0,
        min(
            1.0,
            (convective_signal * 0.30)
            + (_scale(cape_delta, 50.0, 600.0) * 0.20)
            + (_scale(shear_06_delta, 2.0, 12.0) * 0.15)
            + (_scale(shear_01_delta, 1.0, 8.0) * 0.15)
            + (_scale(srh_delta, 25.0, 175.0) * 0.10)            + (_scale(dcape_delta, 50.0, 400.0) * 0.05)
            + (_scale(lapse_delta, 0.2, 1.0) * 0.10),
        ),
    )
    weakening_signal = max(
        0.0,
        min(
            1.0,
            (_scale(-1.0 * (cape_delta or 0.0), 100.0, 700.0) * 0.25)
            + (_scale(-1.0 * (shear_06_delta or 0.0), 3.0, 12.0) * 0.15)
            + (_scale(-1.0 * (shear_01_delta or 0.0), 2.0, 8.0) * 0.10)
            + (_scale(-1.0 * (dcape_delta or 0.0), 50.0, 350.0) * 0.05)
            + (_scale(cIN_future := (-1.0 * cin_future) if cin_future is not None else None, 25.0, 150.0) * 0.20)
            + (_scale(dewpoint_depression_future, 10.0, 20.0) * 0.15)
            + (_scale(-1.0 * (thunder_delta or 0.0), 10.0, 40.0) * 0.15),
        ),
    )
    confidence = _environment_confidence(current_obs=current_obs, current_model=current_model, current_grid=current_grid)
    freshness_minutes = _data_freshness_minutes(
        (current_obs or {}).get("observation_time"),
        (current_model or {}).get("valid_at"),
        (current_grid or {}).get("valid_at"),
    )

    raw_payload = {
        "current_station": current_obs,
        "future_station": future_obs,
        "current_grid": current_grid,
        "future_grid": future_grid,
        "current_model": current_model,
        "future_model": future_model,
        "profile_summary": reduced_profile,
        "field_provenance": field_provenance,
        "source_notes": source_notes,
    }
    limitation_bits = []
    if current_model is None or future_model is None:
        limitation_bits.append("model severe-environment fields were unavailable")
    if current_obs is None:
        limitation_bits.append("nearby METAR observations were unavailable")
    if current_grid is None:
        limitation_bits.append("NWS gridpoint forecast context was unavailable")
    if _safe_float((current_model or {}).get("cape")) is None:
        limitation_bits.append("CAPE/CIN coverage is incomplete")
    if dcape_proxy_now is not None:
        limitation_bits.append("DCAPE is an estimated proxy derived from low-level dryness, lapse rates, and wind potential")
    limitation = (
        "; ".join(limitation_bits)
        if limitation_bits
        else "Uses Open-Meteo hourly model fields with METAR and NWS forecast context. Surface-to-925 hPa helicity and DCAPE are proxy estimates, not full sounding diagnostics."
    )

    narrative = (
        "Environment combines Open-Meteo model guidance, AviationWeather METAR observations, and NWS gridpoint forecast context. "
        f"Current analysis suggests {trend_phrase}. "
        "Signals are probabilistic and should be treated as near-term support factors, not deterministic storm outcomes."
    )

    environment_ahead_delta = {
        "cape_jkg": round(cape_delta, 1) if cape_delta is not None else None,
        "bulk_shear_06km_kt": round(shear_06_delta, 1) if shear_06_delta is not None else None,
        "bulk_shear_01km_kt": round(shear_01_delta, 1) if shear_01_delta is not None else None,
        "srh_surface_925hpa_m2s2": round(srh_delta, 1) if srh_delta is not None else None,        "dcape_jkg": round(dcape_delta, 1) if dcape_delta is not None else None,
        "freezing_level_m": round(freezing_delta, 1) if freezing_delta is not None else None,
        "precipitation_mm": round(qpf_delta, 1) if qpf_delta is not None else None,
        "thunder_probability_pct": round(thunder_delta, 1) if thunder_delta is not None else None,
    }

    return {
        "site": site,
        "storm_id": storm_id,
        "snapshot_time": isoformat_utc(),
        "source": "open_meteo_model+aviationweather_metar+nws_gridpoint_forecast",
        "lat": centroid_lat,
        "lon": centroid_lon,
        "station_id": (current_obs or {}).get("station_id"),
        "station_name": (current_obs or {}).get("station_id"),
        "observed_at": (current_obs or {}).get("observation_time"),
        "surface_temp_c": current_temp,
        "dewpoint_c": current_dewpoint,
        "wind_dir_deg": current_wind_dir,
        "wind_speed_kt": current_surface_wind_kt,
        "pressure_hpa": pressure_hpa,
        "visibility_mi": visibility_mi,
        "cape_jkg": cape_now,
        "cin_jkg": cin_now,
        "bulk_shear_06km_kt": shear_06_now,
        "bulk_shear_01km_kt": shear_01_now,
        "helicity_01km": srh_01km_now,
        "dcape_jkg": dcape_proxy_now,
        "freezing_level_m": freezing_level_now,
        "pwat_mm": None,
        "lapse_rate_midlevel_cpkm": round(lapse_rate_now, 1) if lapse_rate_now is not None else None,
        "lcl_m": round(lcl_now, 0) if lcl_now is not None else None,
        "lfc_m": None,
        "environment_confidence": round(confidence, 2),
        "environment_freshness_minutes": freshness_minutes,
        "hail_favorability": round(max(0.0, min(1.0, hail_favorability)), 2),
        "wind_favorability": round(max(0.0, min(1.0, wind_favorability)), 2),
        "tornado_favorability": round(max(0.0, min(1.0, tornado_favorability)), 2),
        "narrative": narrative,
        "raw_payload": raw_payload,
        "summary": {
            "source": "open_meteo_model+metar+nws_grid",
            "current_station_id": (current_obs or {}).get("station_id"),
            "future_station_id": (future_obs or {}).get("station_id") if future_obs else (current_obs or {}).get("station_id"),
            "gridpoint_id": (current_grid or future_grid or {}).get("gridpoint_id"),
            "surface_temp_c": current_temp,
            "dewpoint_c": current_dewpoint,
            "wind_speed_kt": current_surface_wind_kt,
            "forecast_probability_of_thunder": thunder_now,
            "ahead_probability_of_thunder": thunder_future,
            "forecast_qpf_mm": qpf_future,
            "forecast_wind_speed_kmh": _safe_float((future_grid or future_model or current_model or {}).get("wind_speed_kmh")) or future_surface_wind,
            "hail_favorability": round(max(0.0, min(1.0, hail_favorability)), 2),
            "wind_favorability": round(max(0.0, min(1.0, wind_favorability)), 2),
            "tornado_favorability": round(max(0.0, min(1.0, tornado_favorability)), 2),
            "heavy_rain_favorability": round(max(0.0, min(1.0, heavy_rain_favorability)), 2),
            "convective_signal": round(convective_signal, 2),
            "intensification_signal": round(intensification_signal, 2),
            "weakening_signal": round(weakening_signal, 2),
            "ahead_trend": trend_phrase,
            "weather_summary": (future_grid or current_grid or {}).get("weather_summary"),
            "hazards": (future_grid or current_grid or {}).get("hazards", []),
            "environment_confidence": round(confidence, 2),
            "environment_freshness_minutes": freshness_minutes,
            "environment_ahead_delta": environment_ahead_delta,
            "cape_jkg": cape_now,
            "cin_jkg": cin_now,
            "bulk_shear_06km_kt": round(shear_06_now, 1) if shear_06_now is not None else None,
            "bulk_shear_01km_kt": round(shear_01_now, 1) if shear_01_now is not None else None,
            "srh_surface_925hpa_m2s2": round(srh_01km_now, 1) if srh_01km_now is not None else None,
            "srh_is_sounding_integrated": sounding_result is not None,
            "srh_proxy_m2s2": round(srh_proxy_now, 1) if srh_proxy_now is not None else None,
            "sounding_station": {
                "id": sounding_result.get("station_id"),
                "name": sounding_result.get("station_name"),
                "distance_km": sounding_result.get("station_distance_km"),
                "valid_time": sounding_result.get("valid_time"),
                "level_count": sounding_result.get("level_count"),
            } if sounding_result is not None else None,
            "dcape_jkg": dcape_proxy_now,
            "dcape_is_proxy": True,
            "freezing_level_m": freezing_level_now,
            "pwat_mm": None,
            "lapse_rate_midlevel_cpkm": round(lapse_rate_now, 1) if lapse_rate_now is not None else None,
            "lcl_m": round(lcl_now, 0) if lcl_now is not None else None,
            "lfc_m": None,
            "profile_summary": reduced_profile,
            "field_provenance": field_provenance,
            "source_notes": source_notes,
            "model_valid_at": (current_model or {}).get("valid_at"),
            "ahead_model_valid_at": (future_model or {}).get("valid_at"),
            "hodograph": hodograph,
            "limitation": limitation,
        },
    }
