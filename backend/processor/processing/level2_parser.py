from __future__ import annotations

import os
import threading
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyart

from backend.processor.analysis.base import SweepArrays
from backend.processor.processing.velocity import derive_storm_relative_velocity, quality_control_velocity
from backend.processor.processing.transforms import normalize_longitudes
from backend.shared.products import PRODUCT_CATALOG, source_product_id

# ---------------------------------------------------------------------------
# Radar object LRU cache
# Re-reading a NEXRAD Level-II archive with PyART takes 0.3–1.2 s per call.
# When the processor derives multiple products from the same raw file (REF,
# VEL, CC, ZDR …) it used to hit the disk once per product.  The cache below
# keeps the last MAX_CACHED parsed radar objects in memory, keyed by
# (filepath, mtime_ns).  Thread-safe via a simple lock.
# ---------------------------------------------------------------------------
_MAX_CACHED = 6
_radar_cache: OrderedDict[tuple[str, int], object] = OrderedDict()
_radar_cache_lock = threading.Lock()


def _load_radar_cached(filepath: str | Path) -> object:
    """Return a cached PyART radar object, re-parsing only when the file changes."""
    path = str(filepath)
    try:
        mtime = os.stat(path).st_mtime_ns
    except OSError:
        mtime = 0
    key = (path, mtime)

    with _radar_cache_lock:
        if key in _radar_cache:
            _radar_cache.move_to_end(key)
            return _radar_cache[key]

    radar = pyart.io.read_nexrad_archive(path)

    with _radar_cache_lock:
        _radar_cache[key] = radar
        _radar_cache.move_to_end(key)
        while len(_radar_cache) > _MAX_CACHED:
            _radar_cache.popitem(last=False)

    return radar


def evict_radar_cache() -> None:
    """Clear the in-process radar cache (call after retention purge)."""
    with _radar_cache_lock:
        _radar_cache.clear()


@dataclass
class SweepData:
    product: str
    tilt: float
    values: np.ndarray
    latitudes: np.ndarray
    longitudes: np.ndarray


def resolve_field_name(radar, product: str) -> str:
    available = set(radar.fields.keys())
    source_product = source_product_id(product)
    for candidate in PRODUCT_CATALOG[source_product]["field_aliases"]:
        if candidate in available:
            return candidate
    raise KeyError(f"Field for product {product} is not present in radar file. Available fields: {sorted(available)}")


def _velocity_data_for_sweep(radar, field_name: str, sweep_index: int):
    try:
        corrected_field = pyart.correct.dealias_region_based(radar, vel_field=field_name)
        sweep_slice = slice(radar.get_start(sweep_index), radar.get_end(sweep_index) + 1)
        return corrected_field["data"][sweep_slice]
    except Exception:
        return radar.get_field(sweep_index, field_name, copy=False)


def _mean_elevation_for_sweep(radar, sweep_index: int) -> float:
    start = radar.get_start(sweep_index)
    end = radar.get_end(sweep_index) + 1
    sweep_elevations = radar.elevation["data"][start:end]
    if len(sweep_elevations) == 0:
        fixed_angles = radar.fixed_angle["data"]
        return float(fixed_angles[sweep_index]) if len(fixed_angles) > sweep_index else 0.5
    return float(np.nanmean(sweep_elevations))


def _closest_sweep_index(radar, field_name: str, tilt_deg: float) -> int:
    candidates: list[tuple[float, int]] = []
    for sweep_index in range(int(radar.nsweeps)):
        try:
            radar.get_field(sweep_index, field_name, copy=False)
        except Exception:
            continue
        candidates.append((abs(_mean_elevation_for_sweep(radar, sweep_index) - tilt_deg), sweep_index))
    if not candidates:
        raise KeyError(f"No sweeps are available for field {field_name}")
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def list_available_tilts(filepath: str | Path, product: str) -> list[float]:
    radar = _load_radar_cached(filepath)
    field_name = resolve_field_name(radar, product)
    tilts: list[float] = []
    for sweep_index in range(int(radar.nsweeps)):
        try:
            radar.get_field(sweep_index, field_name, copy=False)
        except Exception:
            continue
        tilts.append(round(_mean_elevation_for_sweep(radar, sweep_index), 1))
    return sorted(set(tilts))


def load_tilt(
    raw_path: str | Path,
    product: str,
    tilt_deg: float,
    *,
    storm_motion_heading_deg: float | None = None,
    storm_motion_speed_kmh: float | None = None,
) -> SweepData:
    radar = _load_radar_cached(raw_path)
    field_name = resolve_field_name(radar, product)
    sweep_index = _closest_sweep_index(radar, field_name, tilt_deg)

    if product.upper() in {"VEL", "SRV"}:
        values = _velocity_data_for_sweep(radar, field_name, sweep_index)
    else:
        values = radar.get_field(sweep_index, field_name, copy=False)
    latitudes, longitudes, _ = radar.get_gate_lat_lon_alt(sweep_index, filter_transitions=True)

    if np.ma.isMaskedArray(values):
        values = values.filled(np.nan)
    values = np.asarray(values, dtype=np.float32)
    site_lat = float(radar.latitude["data"][0])
    site_lon = float(radar.longitude["data"][0])
    nyquist = 0.0
    if product.upper() in {"VEL", "SRV"}:
        try:
            nyquist_data = radar.instrument_parameters.get("nyquist_velocity", {}).get("data")
            if nyquist_data is not None and len(nyquist_data) > 0:
                nyquist = float(np.nanmean(nyquist_data))
        except Exception:
            nyquist = 29.0
        values = quality_control_velocity(values, nyquist_velocity=nyquist)
        if product.upper() == "SRV":
            if storm_motion_heading_deg is None or storm_motion_speed_kmh is None:
                raise ValueError("SRV loading requires storm motion heading and speed")
            values = derive_storm_relative_velocity(
                values,
                np.asarray(latitudes, dtype=np.float32),
                normalize_longitudes(np.asarray(longitudes, dtype=np.float32)),
                site_lat=site_lat,
                site_lon=site_lon,
                motion_heading_deg=storm_motion_heading_deg,
                motion_speed_kmh=storm_motion_speed_kmh,
                nyquist_velocity=nyquist,
            )

    latitudes = np.asarray(latitudes, dtype=np.float32)
    longitudes = normalize_longitudes(np.asarray(longitudes, dtype=np.float32))
    tilt = _mean_elevation_for_sweep(radar, sweep_index)

    return SweepData(
        product=product.upper(),
        tilt=tilt,
        values=values,
        latitudes=latitudes,
        longitudes=longitudes,
    )


def load_lowest_tilt(
    raw_path: str | Path,
    product: str,
    *,
    storm_motion_heading_deg: float | None = None,
    storm_motion_speed_kmh: float | None = None,
) -> SweepData:
    return load_tilt(
        raw_path,
        product,
        tilt_deg=0.5,
        storm_motion_heading_deg=storm_motion_heading_deg,
        storm_motion_speed_kmh=storm_motion_speed_kmh,
    )


def extract_sweep_arrays(
    filepath: str | Path,
    product: str,
    sweep_index: int | None = 0,
    *,
    tilt_deg: float | None = None,
    storm_motion_heading_deg: float | None = None,
    storm_motion_speed_kmh: float | None = None,
) -> SweepArrays:
    """
    Load a sweep and return SweepArrays with full spatial context for analysis.
    The returned arrays are in native polar coordinates mapped to lat/lon.
    """

    radar = _load_radar_cached(filepath)
    field_name = resolve_field_name(radar, product)
    if tilt_deg is not None:
        sweep_index = _closest_sweep_index(radar, field_name, tilt_deg)
    elif sweep_index is None:
        sweep_index = 0

    if product.upper() in {"VEL", "SRV"}:
        values = _velocity_data_for_sweep(radar, field_name, sweep_index)
    else:
        values = radar.get_field(sweep_index, field_name, copy=True)
    latitudes, longitudes, _ = radar.get_gate_lat_lon_alt(sweep_index, filter_transitions=True)

    if np.ma.isMaskedArray(values):
        values = values.filled(np.nan)

    rays_start = radar.get_start(sweep_index)
    rays_end = radar.get_end(sweep_index) + 1
    azimuths = radar.azimuth["data"][rays_start:rays_end].astype(np.float32)
    ranges_km = (radar.range["data"] / 1000.0).astype(np.float32)

    nyquist = 0.0
    if product.upper() in {"VEL", "SRV"}:
        try:
            nyquist_data = radar.instrument_parameters.get("nyquist_velocity", {}).get("data")
            if nyquist_data is not None and len(nyquist_data) > 0:
                nyquist = float(np.nanmean(nyquist_data))
        except Exception:
            nyquist = 29.0

    site_lat = float(radar.latitude["data"][0])
    site_lon = float(radar.longitude["data"][0])
    values_array = np.asarray(values, dtype=np.float32)
    latitudes_array = np.asarray(latitudes, dtype=np.float32)
    longitudes_array = normalize_longitudes(np.asarray(longitudes, dtype=np.float32))
    if product.upper() in {"VEL", "SRV"}:
        values_array = quality_control_velocity(values_array, nyquist_velocity=nyquist)
    if product.upper() == "SRV":
        if storm_motion_heading_deg is None or storm_motion_speed_kmh is None:
            raise ValueError("SRV extraction requires storm motion heading and speed")
        values_array = derive_storm_relative_velocity(
            values_array,
            latitudes_array,
            longitudes_array,
            site_lat=site_lat,
            site_lon=site_lon,
            motion_heading_deg=storm_motion_heading_deg,
            motion_speed_kmh=storm_motion_speed_kmh,
            nyquist_velocity=nyquist,
        )

    return SweepArrays(
        values=values_array,
        latitudes=latitudes_array,
        longitudes=longitudes_array,
        azimuths=azimuths,
        ranges_km=ranges_km,
        site_lat=site_lat,
        site_lon=site_lon,
        nyquist_velocity=nyquist,
    )
