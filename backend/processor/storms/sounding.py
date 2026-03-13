"""
Rawinsonde (upper-air) sounding integration.

Fetches 00Z/12Z radiosonde data from the Iowa Environmental Mesonet (IEM)
JSON API and computes SRH (storm-relative helicity) by hodograph integration.
This replaces the single-layer surface-to-925 hPa cross-product proxy with
a proper integral over the full 0-1 km and 0-3 km layers.

Cache strategy: disk-persisted per station + valid_time, TTL = 12 hours
(soundings launch every 12 hours; a 12-hour TTL ensures we never request
a sounding that is older than one launch cycle).
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from backend.processor.analysis.utils import haversine_km

LOGGER = logging.getLogger(__name__)

IEM_RAOB_URL = "https://mesonet.agron.iastate.edu/json/raob.py"
STATION_LIST_PATH = Path(__file__).parent.parent / "shared" / "upper_air_stations.json"

_STATIONS: list[dict[str, Any]] | None = None


def _load_stations() -> list[dict[str, Any]]:
    global _STATIONS
    if _STATIONS is None:
        try:
            _STATIONS = json.loads(STATION_LIST_PATH.read_text())
        except Exception:
            LOGGER.warning("Could not load upper-air station list; sounding integration disabled")
            _STATIONS = []
    return _STATIONS


def nearest_upper_air_station(lat: float, lon: float) -> dict[str, Any] | None:
    """Return the closest upper-air station to (lat, lon)."""
    stations = _load_stations()
    if not stations:
        return None
    best = min(stations, key=lambda s: haversine_km(lat, lon, s["lat"], s["lon"]))
    return best


def _latest_sounding_time(now: datetime) -> datetime:
    """Return the most recent 00Z or 12Z launch time before now."""
    hour = now.replace(minute=0, second=0, microsecond=0)
    if hour.hour >= 12:
        return hour.replace(hour=12)
    return hour.replace(hour=0)


def _sounding_cache_path(cache_dir: Path, station_id: str, valid_time: datetime) -> Path:
    key = f"sounding_{station_id}_{valid_time:%Y%m%dT%H%M}.json"
    target = cache_dir / "soundings"
    target.mkdir(parents=True, exist_ok=True)
    return target / key


def _load_cached_sounding(cache_dir: Path, station_id: str, valid_time: datetime, ttl_hours: int = 12) -> dict[str, Any] | None:
    path = _sounding_cache_path(cache_dir, station_id, valid_time)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text())
        fetched_at_str = payload.get("_fetched_at")
        if not fetched_at_str:
            return None
        fetched_at = datetime.fromisoformat(fetched_at_str)
        age = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600.0
        if age > ttl_hours:
            return None
        return payload
    except Exception:
        return None


def _write_cached_sounding(cache_dir: Path, station_id: str, valid_time: datetime, payload: dict[str, Any]) -> None:
    path = _sounding_cache_path(cache_dir, station_id, valid_time)
    payload["_fetched_at"] = datetime.now(timezone.utc).isoformat()
    try:
        path.write_text(json.dumps(payload))
    except Exception:
        LOGGER.debug("Failed to write sounding cache to %s", path)


async def _fetch_iem_sounding(station_id: str, valid_time: datetime) -> dict[str, Any] | None:
    """
    Fetch a single sounding from IEM RAOB endpoint.
    Returns the parsed JSON payload or None on failure.
    """
    ts = valid_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(
                IEM_RAOB_URL,
                params={"station": station_id, "ts": ts, "fmt": "json"},
                headers={"User-Agent": "radar-platform/0.4 (local-first weather analysis)"},
            )
            response.raise_for_status()
            return response.json()
    except Exception:
        LOGGER.debug("IEM sounding fetch failed for %s at %s", station_id, ts, exc_info=True)
        return None


def _parse_sounding_levels(payload: dict[str, Any]) -> list[dict[str, Any]] | None:
    """
    Extract level list from IEM JSON response.
    Returns list of dicts with: pres_hpa, height_m (MSL), tmpc, dwpc, drct, sknt.
    """
    try:
        data = payload.get("data", [])
        if not data:
            return None
        profile = data[0]
        levels = profile.get("profile") or profile.get("levels")
        if not levels:
            return None
        parsed: list[dict[str, Any]] = []
        for level in levels:
            try:
                pres = float(level.get("pres") or level.get("pressure") or 0)
                hght = float(level.get("hght") or level.get("height") or 0)
                drct = level.get("drct") or level.get("direction")
                sknt = level.get("sknt") or level.get("speed")
                if pres <= 0 or hght <= 0 or drct is None or sknt is None:
                    continue
                parsed.append({
                    "pres_hpa": pres,
                    "height_msl": hght,
                    "tmpc": float(level.get("tmpc") or level.get("temp") or 0),
                    "dwpc": float(level.get("dwpc") or level.get("dewpoint") or -99),
                    "drct": float(drct),
                    "sknt": float(sknt),
                })
            except (TypeError, ValueError):
                continue
        return parsed if len(parsed) >= 5 else None
    except Exception:
        return None


def _wind_components(direction_from_deg: float, speed_kt: float) -> tuple[float, float]:
    """
    Convert meteorological wind (direction FROM, speed in knots) to
    Cartesian components (u, v) in m/s.
    u = eastward component (positive = from west)
    v = northward component (positive = from south)
    """
    speed_ms = speed_kt * 0.5144
    rad = math.radians(direction_from_deg)
    u = -speed_ms * math.sin(rad)
    v = -speed_ms * math.cos(rad)
    return u, v


def compute_srh_from_levels(
    levels: list[dict[str, Any]],
    *,
    station_elev_m: float,
    storm_heading_deg: float | None,
    storm_speed_kmh: float | None,
    layer_top_m: float = 1000.0,
) -> float | None:
    """
    Compute storm-relative helicity (m²/s²) for the surface-to-layer_top_m AGL layer.

    Uses the shoelace / cross-product method:
        SRH = Σ [ (u_k - cx)(v_{k+1} - cy) - (u_{k+1} - cx)(v_k - cy) ]
    where (cx, cy) is the storm motion vector and (u, v) are layer-mean winds.

    Positive SRH = cyclonic (favors right-moving supercells in Northern Hemisphere).
    """
    if storm_heading_deg is None or storm_speed_kmh is None:
        return None

    # Storm motion components (heading is the direction the storm moves TOWARD)
    storm_speed_ms = storm_speed_kmh / 3.6
    storm_rad = math.radians(storm_heading_deg)
    cx = storm_speed_ms * math.sin(storm_rad)   # eastward component of motion
    cy = storm_speed_ms * math.cos(storm_rad)   # northward component of motion

    # Sort levels by height, convert to AGL
    agl_levels = []
    for level in levels:
        agl = level["height_msl"] - station_elev_m
        if agl < 0:
            agl = 0.0
        u, v = _wind_components(level["drct"], level["sknt"])
        agl_levels.append({"agl_m": agl, "u": u, "v": v})

    agl_levels.sort(key=lambda x: x["agl_m"])

    # Include surface (0 m AGL) by interpolating if first level is above ground
    layer_levels = []
    surface_added = False
    for i, level in enumerate(agl_levels):
        if level["agl_m"] == 0.0:
            layer_levels.append(level)
            surface_added = True
        elif level["agl_m"] > 0.0 and not surface_added and i == 0:
            # First level is above ground; use it as the surface
            layer_levels.append({"agl_m": 0.0, "u": level["u"], "v": level["v"]})
            surface_added = True

        if level["agl_m"] <= layer_top_m:
            if level not in layer_levels:
                layer_levels.append(level)
        elif level["agl_m"] > layer_top_m and layer_levels:
            # Interpolate to exactly layer_top_m
            prev = layer_levels[-1]
            frac = (layer_top_m - prev["agl_m"]) / max(level["agl_m"] - prev["agl_m"], 1.0)
            u_top = prev["u"] + frac * (level["u"] - prev["u"])
            v_top = prev["v"] + frac * (level["v"] - prev["v"])
            layer_levels.append({"agl_m": layer_top_m, "u": u_top, "v": v_top})
            break

    if len(layer_levels) < 2:
        return None

    # Shoelace integration of storm-relative hodograph
    srh = 0.0
    for k in range(len(layer_levels) - 1):
        u_k = layer_levels[k]["u"] - cx
        v_k = layer_levels[k]["v"] - cy
        u_k1 = layer_levels[k + 1]["u"] - cx
        v_k1 = layer_levels[k + 1]["v"] - cy
        srh += (u_k * v_k1) - (u_k1 * v_k)

    return round(srh, 1)


async def fetch_sounding_srh(
    *,
    lat: float,
    lon: float,
    storm_heading_deg: float | None,
    storm_speed_kmh: float | None,
    cache_dir: Path | None = None,
    layer_top_m: float = 1000.0,
) -> dict[str, Any] | None:
    """
    Find the nearest upper-air station, fetch the most recent sounding,
    and return a dict with SRH and metadata.

    Returns:
        {
            "srh_m2s2": float,          # integrated SRH for layer
            "layer_top_m": float,
            "station_id": str,
            "station_name": str,
            "station_distance_km": float,
            "valid_time": str (ISO),
            "level_count": int,
            "source": "sounding_integrated",
        }
    or None if the sounding could not be retrieved.
    """
    station = nearest_upper_air_station(lat, lon)
    if station is None:
        return None

    station_distance_km = haversine_km(lat, lon, station["lat"], station["lon"])

    # Only use soundings within 400 km — beyond that the atmosphere is too different
    if station_distance_km > 400.0:
        LOGGER.debug("Nearest sounding station %s is %.0f km away — skipping", station["id"], station_distance_km)
        return None

    now = datetime.now(timezone.utc)
    valid_time = _latest_sounding_time(now)

    # Don't request a sounding from more than 2 hours after the launch (data may not yet be available)
    time_since_launch = (now - valid_time).total_seconds() / 3600.0
    if time_since_launch < 1.5:
        # Data from this launch cycle may not yet be processed — try the previous one
        valid_time -= timedelta(hours=12)

    # Check disk cache
    cached_payload = None
    if cache_dir is not None:
        cached_payload = _load_cached_sounding(cache_dir, station["id"], valid_time)

    if cached_payload is None:
        raw_payload = await _fetch_iem_sounding(station["id"], valid_time)
        if raw_payload is None:
            return None
        if cache_dir is not None:
            _write_cached_sounding(cache_dir, station["id"], valid_time, raw_payload)
        cached_payload = raw_payload

    levels = _parse_sounding_levels(cached_payload)
    if not levels:
        LOGGER.debug("Could not parse sounding levels for %s at %s", station["id"], valid_time)
        return None

    srh = compute_srh_from_levels(
        levels,
        station_elev_m=station["elev_m"],
        storm_heading_deg=storm_heading_deg,
        storm_speed_kmh=storm_speed_kmh,
        layer_top_m=layer_top_m,
    )
    if srh is None:
        return None

    return {
        "srh_m2s2": srh,
        "layer_top_m": layer_top_m,
        "station_id": station["id"],
        "station_name": station["name"],
        "station_distance_km": round(station_distance_km, 1),
        "valid_time": valid_time.isoformat(),
        "level_count": len(levels),
        "source": "sounding_integrated",
    }
