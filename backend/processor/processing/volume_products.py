from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
from backend.processor.processing.transforms import BBox, compute_bbox, normalize_longitudes
from backend.shared.products import PRODUCT_CATALOG
from backend.shared.time import isoformat_utc, utc_now


ECHO_TOP_THRESHOLD_DBZ = 18.0
Z_R_A = 300.0
Z_R_B = 1.4

HC_UNKNOWN = 0.0
HC_RAIN = 1.0
HC_MODERATE_RAIN = 2.0
HC_HEAVY_RAIN = 3.0
HC_LARGE_DROPS = 4.0
HC_HAIL = 5.0
HC_GRAUPEL = 6.0
HC_SNOW = 7.0
HC_MIXED = 8.0
HC_DEBRIS = 9.0
HC_LABELS = {
    1: "rain",
    2: "moderate_rain",
    3: "heavy_rain",
    4: "large_drops",
    5: "hail",
    6: "graupel",
    7: "snow",
    8: "mixed",
    9: "debris_candidate",
}


@dataclass
class GridProduct:
    product: str
    values: np.ndarray
    latitudes: np.ndarray
    longitudes: np.ndarray
    tilt: float = 0.5
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CrossSectionResult:
    site: str
    product: str
    frame_id: str
    ranges_km: list[float]
    altitudes_km: list[float]
    values: list[list[float | None]]
    start: dict[str, float]
    end: dict[str, float]
    tilts_used: list[float]
    unit: str
    method: str
    limitation: str
    generated_at: str


def resolve_field_name(radar, product: str) -> str:
    available = set(radar.fields.keys())
    for candidate in PRODUCT_CATALOG[product.upper()]["field_aliases"]:
        if candidate in available:
            return str(candidate)
    raise KeyError(f"Field for product {product} is not present in radar file. Available fields: {sorted(available)}")


def _mean_elevation_for_sweep(radar, sweep_index: int) -> float:
    start = radar.get_start(sweep_index)
    end = radar.get_end(sweep_index) + 1
    sweep_elevations = radar.elevation["data"][start:end]
    if len(sweep_elevations) == 0:
        fixed_angles = radar.fixed_angle["data"]
        return float(fixed_angles[sweep_index]) if len(fixed_angles) > sweep_index else 0.5
    return float(np.nanmean(sweep_elevations))


def _grid_axes(bbox: BBox, image_size: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    lat_axis = np.linspace(bbox.max_lat, bbox.min_lat, image_size, dtype=np.float32)
    lon_axis = np.linspace(bbox.min_lon, bbox.max_lon, image_size, dtype=np.float32)
    longitudes, latitudes = np.meshgrid(lon_axis, lat_axis)
    return lat_axis, lon_axis, latitudes, longitudes


def _pixel_indices(
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    bbox: BBox,
    image_size: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    finite_mask = np.isfinite(latitudes) & np.isfinite(longitudes)
    finite_mask &= (latitudes >= -90.0) & (latitudes <= 90.0)
    finite_mask &= (longitudes >= -180.0) & (longitudes <= 180.0)
    lon_span = max(bbox.max_lon - bbox.min_lon, 1e-6)
    lat_span = max(bbox.max_lat - bbox.min_lat, 1e-6)
    x = np.clip(((longitudes[finite_mask] - bbox.min_lon) / lon_span * (image_size - 1)).astype(np.int32), 0, image_size - 1)
    y = np.clip(((bbox.max_lat - latitudes[finite_mask]) / lat_span * (image_size - 1)).astype(np.int32), 0, image_size - 1)
    return finite_mask, x, y


def _aggregate_max(values: np.ndarray, x: np.ndarray, y: np.ndarray, image_size: int) -> np.ndarray:
    flat_grid = np.full(image_size * image_size, -np.inf, dtype=np.float32)
    np.maximum.at(flat_grid, (y * image_size) + x, values.astype(np.float32))
    flat_grid[flat_grid == -np.inf] = np.nan
    return flat_grid.reshape(image_size, image_size)


def _aggregate_sum(values: np.ndarray, x: np.ndarray, y: np.ndarray, image_size: int) -> np.ndarray:
    flat_grid = np.zeros(image_size * image_size, dtype=np.float32)
    np.add.at(flat_grid, (y * image_size) + x, values.astype(np.float32))
    return flat_grid.reshape(image_size, image_size)


def _rain_rate_from_dbz(reflectivity_dbz: np.ndarray) -> np.ndarray:
    rain_rate = np.full_like(reflectivity_dbz, np.nan, dtype=np.float32)
    finite_mask = np.isfinite(reflectivity_dbz) & (reflectivity_dbz >= 0.0)
    if not np.any(finite_mask):
        return rain_rate
    z_linear = np.power(10.0, reflectivity_dbz[finite_mask] / 10.0)
    rain_rate[finite_mask] = np.power(z_linear / Z_R_A, 1.0 / Z_R_B)
    return rain_rate


def _apply_dual_pol_rain_adjustment(
    rain_rate: np.ndarray,
    zdr_grid: np.ndarray | None,
    cc_grid: np.ndarray | None,
    kdp_grid: np.ndarray | None = None,
) -> np.ndarray:
    adjusted = rain_rate.copy()
    if zdr_grid is None:
        zdr_grid = np.full(rain_rate.shape, np.nan, dtype=np.float32)
    if cc_grid is None:
        cc_grid = np.full(rain_rate.shape, np.nan, dtype=np.float32)
    if kdp_grid is None:
        kdp_grid = np.full(rain_rate.shape, np.nan, dtype=np.float32)

    valid = np.isfinite(adjusted) & (np.isfinite(zdr_grid) | np.isfinite(cc_grid) | np.isfinite(kdp_grid))
    if not np.any(valid):
        return adjusted

    stratiform_like = valid & (cc_grid >= 0.96) & (zdr_grid >= 0.0) & (zdr_grid <= 3.5)
    hail_contaminated = valid & ((cc_grid < 0.92) | (zdr_grid < -0.5))
    kdp_dominant = valid & np.isfinite(kdp_grid) & (kdp_grid >= 0.3) & (adjusted >= 8.0)
    adjusted[stratiform_like] *= np.clip(1.0 + (zdr_grid[stratiform_like] * 0.08), 0.9, 1.3)
    adjusted[hail_contaminated] *= 0.7
    adjusted[kdp_dominant] = np.maximum(
        adjusted[kdp_dominant],
        40.5 * np.power(np.maximum(kdp_grid[kdp_dominant], 0.01), 0.85),
    )
    return adjusted


def _hydrometeor_classification(
    ref_grid: np.ndarray,
    zdr_grid: np.ndarray | None,
    cc_grid: np.ndarray | None,
    kdp_grid: np.ndarray | None = None,
) -> np.ndarray:
    classes = np.full(ref_grid.shape, HC_UNKNOWN, dtype=np.float32)
    finite_ref = np.isfinite(ref_grid)
    if not np.any(finite_ref):
        return classes

    if zdr_grid is None:
        zdr_grid = np.full(ref_grid.shape, np.nan, dtype=np.float32)
    if cc_grid is None:
        cc_grid = np.full(ref_grid.shape, np.nan, dtype=np.float32)
    if kdp_grid is None:
        kdp_grid = np.full(ref_grid.shape, np.nan, dtype=np.float32)

    valid = finite_ref
    rain = valid & (ref_grid >= 20.0) & (ref_grid < 35.0) & (cc_grid >= 0.96)
    moderate_rain = valid & (ref_grid >= 35.0) & (ref_grid < 48.0) & (zdr_grid >= 0.5) & (zdr_grid < 2.0) & (cc_grid >= 0.94)
    heavy_rain = valid & (
        (
            (ref_grid >= 48.0)
            & (ref_grid < 55.0)
            & (cc_grid >= 0.95)
            & (zdr_grid >= 0.7)
        )
        | (
            (ref_grid >= 42.0)
            & (cc_grid >= 0.95)
            & (kdp_grid >= 0.7)
        )
    )
    large_drops = valid & (ref_grid >= 40.0) & (ref_grid < 58.0) & (zdr_grid >= 2.0) & (cc_grid >= 0.95) & ((~np.isfinite(kdp_grid)) | (kdp_grid < 1.8))
    hail = valid & (ref_grid >= 55.0) & ((zdr_grid <= 0.5) | (cc_grid < 0.95) | (kdp_grid <= 1.5))
    debris = valid & (ref_grid >= 35.0) & (cc_grid < 0.82)
    graupel = valid & (ref_grid >= 35.0) & (ref_grid < 50.0) & (np.abs(zdr_grid) <= 0.5) & (cc_grid >= 0.9) & (cc_grid < 0.97)
    snow = valid & (ref_grid >= 15.0) & (ref_grid < 32.0) & (np.abs(zdr_grid) <= 0.4) & (cc_grid >= 0.97)

    classes[graupel] = HC_GRAUPEL
    classes[snow] = HC_SNOW
    classes[debris] = HC_DEBRIS
    classes[hail] = HC_HAIL
    classes[(classes == HC_UNKNOWN) & large_drops] = HC_LARGE_DROPS
    classes[(classes == HC_UNKNOWN) & heavy_rain] = HC_HEAVY_RAIN
    classes[(classes == HC_UNKNOWN) & moderate_rain] = HC_MODERATE_RAIN
    classes[(classes == HC_UNKNOWN) & rain] = HC_RAIN
    mixed = valid & (classes == HC_UNKNOWN) & (cc_grid >= 0.85) & (cc_grid < 0.96)
    classes[mixed] = HC_MIXED
    return classes


def _derive_kdp_from_phi_proxy(
    radar,
    *,
    sweep_index: int,
    bbox: BBox,
    image_size: int,
) -> np.ndarray | None:
    phi_aliases = (
        "differential_phase",
        "PHIDP",
        "PHI",
        "unfolded_differential_phase",
    )
    phi_field = next((candidate for candidate in phi_aliases if candidate in radar.fields), None)
    if phi_field is None:
        return None

    values = radar.get_field(sweep_index, phi_field, copy=False)
    if np.ma.isMaskedArray(values):
        values = values.filled(np.nan)
    values = np.asarray(values, dtype=np.float32)
    if values.ndim != 2:
        return None

    range_km = np.asarray(radar.range["data"], dtype=np.float32) / 1000.0
    if range_km.size < 2:
        return None
    kdp_proxy = np.full_like(values, np.nan, dtype=np.float32)
    finite = np.isfinite(values)
    filled = np.where(finite, values, 0.0).astype(np.float32)
    window = 9
    try:
        from scipy.ndimage import uniform_filter1d

        smoothed_sum = uniform_filter1d(filled, size=window, axis=1, mode="nearest") * window
        valid_count = uniform_filter1d(finite.astype(np.float32), size=window, axis=1, mode="nearest") * window
    except Exception:
        kernel = np.ones(window, dtype=np.float32)
        smoothed_sum = np.apply_along_axis(lambda row: np.convolve(row, kernel, mode="same"), 1, filled)
        valid_count = np.apply_along_axis(lambda row: np.convolve(row, kernel, mode="same"), 1, finite.astype(np.float32))
    phi_smoothed = np.where(valid_count >= max(3.0, window / 3.0), smoothed_sum / np.maximum(valid_count, 1.0), np.nan)
    forward = phi_smoothed[:, 2:] - phi_smoothed[:, :-2]
    dr = np.maximum(range_km[2:] - range_km[:-2], 0.1)
    kdp_proxy[:, 1:-1] = 0.5 * (forward / dr[np.newaxis, :])
    kdp_proxy = np.clip(kdp_proxy, -2.0, 8.0)

    latitudes, longitudes, _ = radar.get_gate_lat_lon_alt(sweep_index, filter_transitions=True)
    longitudes = normalize_longitudes(np.asarray(longitudes, dtype=np.float32))
    latitudes = np.asarray(latitudes, dtype=np.float32)
    finite_mask, x, y = _pixel_indices(latitudes, longitudes, bbox, image_size)
    finite_values = kdp_proxy[finite_mask]
    if finite_values.size == 0:
        return None
    return _aggregate_max(finite_values, x, y, image_size)


def _extract_field_grid_for_sweep(
    radar,
    field_name: str,
    sweep_index: int,
    bbox: BBox,
    image_size: int,
) -> np.ndarray:
    values = radar.get_field(sweep_index, field_name, copy=False)
    latitudes, longitudes, _ = radar.get_gate_lat_lon_alt(sweep_index, filter_transitions=True)
    if np.ma.isMaskedArray(values):
        values = values.filled(np.nan)
    longitudes = normalize_longitudes(np.asarray(longitudes, dtype=np.float32))
    latitudes = np.asarray(latitudes, dtype=np.float32)
    finite_mask, x, y = _pixel_indices(latitudes, longitudes, bbox, image_size)
    finite_values = np.asarray(values, dtype=np.float32)[finite_mask]
    return _aggregate_max(finite_values, x, y, image_size)


def derive_volume_products(
    raw_path: str | Path,
    *,
    image_size: int,
    accumulation_inputs: list[tuple[str | Path, datetime]] | None = None,
) -> dict[str, GridProduct]:
    import pyart

    radar = pyart.io.read_nexrad_archive(str(raw_path))
    reflectivity_field = resolve_field_name(radar, "REF")
    sweep_records: list[dict[str, Any]] = []

    bbox_latitudes: list[np.ndarray] = []
    bbox_longitudes: list[np.ndarray] = []

    for sweep_index in range(int(radar.nsweeps)):
        try:
            values = radar.get_field(sweep_index, reflectivity_field, copy=False)
        except Exception:
            continue
        latitudes, longitudes, altitudes = radar.get_gate_lat_lon_alt(sweep_index, filter_transitions=True)
        if np.ma.isMaskedArray(values):
            values = values.filled(np.nan)
        latitudes = np.asarray(latitudes, dtype=np.float32)
        longitudes = normalize_longitudes(np.asarray(longitudes, dtype=np.float32))
        altitudes_km = np.asarray(altitudes, dtype=np.float32) / 1000.0
        values = np.asarray(values, dtype=np.float32)
        bbox_latitudes.append(latitudes)
        bbox_longitudes.append(longitudes)
        sweep_records.append(
            {
                "tilt": round(_mean_elevation_for_sweep(radar, sweep_index), 1),
                "values": values,
                "latitudes": latitudes,
                "longitudes": longitudes,
                "altitudes_km": altitudes_km,
                "sweep_index": sweep_index,
                "mean_alt_km": float(np.nanmean(altitudes_km[np.isfinite(altitudes_km)])) if np.isfinite(altitudes_km).any() else 0.0,
            }
        )

    if not sweep_records:
        raise ValueError("No reflectivity sweeps were available for volume products.")

    bbox = compute_bbox(np.concatenate(bbox_latitudes), np.concatenate(bbox_longitudes))
    _, _, lat_grid, lon_grid = _grid_axes(bbox, image_size)
    tilts_available = [record["tilt"] for record in sweep_records]

    echo_top_flat = np.full(image_size * image_size, -np.inf, dtype=np.float32)
    data_mask_flat = np.zeros(image_size * image_size, dtype=bool)
    vil_grid = np.zeros((image_size, image_size), dtype=np.float32)

    mean_altitudes = np.array([record["mean_alt_km"] for record in sweep_records], dtype=np.float32)
    if len(mean_altitudes) == 1:
        thicknesses_m = np.array([1000.0], dtype=np.float32)
    else:
        boundaries = np.empty(len(mean_altitudes) + 1, dtype=np.float32)
        boundaries[0] = max(0.0, mean_altitudes[0] - max(0.3, (mean_altitudes[1] - mean_altitudes[0]) / 2.0))
        boundaries[-1] = mean_altitudes[-1] + max(0.3, (mean_altitudes[-1] - mean_altitudes[-2]) / 2.0)
        for idx in range(1, len(mean_altitudes)):
            boundaries[idx] = (mean_altitudes[idx - 1] + mean_altitudes[idx]) / 2.0
        thicknesses_m = np.maximum(200.0, np.diff(boundaries) * 1000.0)

    lowest_reflectivity_grid: np.ndarray | None = None
    lowest_tilt = min(tilts_available)
    for record, thickness_m in zip(sweep_records, thicknesses_m, strict=False):
        finite_mask, x, y = _pixel_indices(record["latitudes"], record["longitudes"], bbox, image_size)
        finite_values = record["values"][finite_mask]
        if finite_values.size == 0:
            continue
        reflectivity_grid = _aggregate_max(finite_values, x, y, image_size)
        data_mask_flat[(y * image_size) + x] = True

        if lowest_reflectivity_grid is None or abs(record["tilt"] - lowest_tilt) < 0.11:
            lowest_reflectivity_grid = reflectivity_grid

        threshold_mask = finite_mask & np.isfinite(record["values"]) & (record["values"] >= ECHO_TOP_THRESHOLD_DBZ) & np.isfinite(record["altitudes_km"])
        if np.any(threshold_mask):
            threshold_x = np.clip(((record["longitudes"][threshold_mask] - bbox.min_lon) / max(bbox.max_lon - bbox.min_lon, 1e-6) * (image_size - 1)).astype(np.int32), 0, image_size - 1)
            threshold_y = np.clip(((bbox.max_lat - record["latitudes"][threshold_mask]) / max(bbox.max_lat - bbox.min_lat, 1e-6) * (image_size - 1)).astype(np.int32), 0, image_size - 1)
            np.maximum.at(
                echo_top_flat,
                (threshold_y * image_size) + threshold_x,
                record["altitudes_km"][threshold_mask].astype(np.float32),
            )

        finite_ref = np.isfinite(reflectivity_grid) & (reflectivity_grid > 0.0)
        if np.any(finite_ref):
            z_linear = np.power(10.0, reflectivity_grid[finite_ref] / 10.0)
            liquid = 3.44e-6 * np.power(z_linear, 4.0 / 7.0)
            vil_grid[finite_ref] += liquid.astype(np.float32) * float(thickness_m)

    echo_tops_grid = echo_top_flat.reshape(image_size, image_size)
    echo_tops_grid[echo_tops_grid == -np.inf] = np.nan
    vil_grid[~data_mask_flat.reshape(image_size, image_size)] = np.nan

    if lowest_reflectivity_grid is None:
        lowest_reflectivity_grid = np.full((image_size, image_size), np.nan, dtype=np.float32)
    lowest_sweep_index = min(sweep_records, key=lambda record: record["tilt"])["sweep_index"]

    zdr_grid = None
    cc_grid = None
    kdp_grid = None
    try:
        zdr_field = resolve_field_name(radar, "ZDR")
        zdr_grid = _extract_field_grid_for_sweep(radar, zdr_field, lowest_sweep_index, bbox, image_size)
    except Exception:
        zdr_grid = None
    try:
        cc_field = resolve_field_name(radar, "CC")
        cc_grid = _extract_field_grid_for_sweep(radar, cc_field, lowest_sweep_index, bbox, image_size)
    except Exception:
        cc_grid = None
    try:
        kdp_field = resolve_field_name(radar, "KDP")
        kdp_grid = _extract_field_grid_for_sweep(radar, kdp_field, lowest_sweep_index, bbox, image_size)
    except Exception:
        kdp_grid = _derive_kdp_from_phi_proxy(
            radar,
            sweep_index=lowest_sweep_index,
            bbox=bbox,
            image_size=image_size,
        )

    rain_rate_grid = _apply_dual_pol_rain_adjustment(_rain_rate_from_dbz(lowest_reflectivity_grid), zdr_grid, cc_grid, kdp_grid)
    hydrometeor_grid = _hydrometeor_classification(lowest_reflectivity_grid, zdr_grid, cc_grid, kdp_grid)

    products = {
        "KDP": GridProduct(
            product="KDP",
            values=kdp_grid.astype(np.float32) if kdp_grid is not None else np.full_like(lowest_reflectivity_grid, np.nan, dtype=np.float32),
            latitudes=lat_grid,
            longitudes=lon_grid,
            metadata={"method": "direct_field_or_phi_gradient_proxy", "tilts_available": tilts_available},
        ),
        "ET": GridProduct(
            product="ET",
            values=echo_tops_grid,
            latitudes=lat_grid,
            longitudes=lon_grid,
            metadata={"threshold_dbz": ECHO_TOP_THRESHOLD_DBZ, "tilts_available": tilts_available},
        ),
        "VIL": GridProduct(
            product="VIL",
            values=vil_grid,
            latitudes=lat_grid,
            longitudes=lon_grid,
            metadata={"tilts_available": tilts_available},
        ),
        "RR": GridProduct(
            product="RR",
            values=rain_rate_grid.astype(np.float32),
            latitudes=lat_grid,
            longitudes=lon_grid,
            metadata={"method": "z-r_with_dual_pol_adjustment", "tilts_available": tilts_available},
        ),
        "HC": GridProduct(
            product="HC",
            values=hydrometeor_grid,
            latitudes=lat_grid,
            longitudes=lon_grid,
            metadata={
                "classes": {
                    "1": "rain",
                    "2": "moderate_rain",
                    "3": "heavy_rain",
                    "4": "large_drops",
                    "5": "hail",
                    "6": "graupel",
                    "7": "snow",
                    "8": "mixed",
                    "9": "debris_candidate",
                },
                "method": "rules_based_v1",
                "tilts_available": tilts_available,
            },
        ),
    }

    if accumulation_inputs:
        accumulation_grid = derive_hourly_accumulation(
            accumulation_inputs,
            bbox=bbox,
            image_size=image_size,
        )
        products["QPE1H"] = GridProduct(
            product="QPE1H",
            values=accumulation_grid,
            latitudes=lat_grid,
            longitudes=lon_grid,
            metadata={"window_minutes": 60, "method": "rolling_radar_accumulation", "tilts_available": tilts_available},
        )

    return products


def derive_hourly_accumulation(
    frames: list[tuple[str | Path, datetime]],
    *,
    bbox: BBox,
    image_size: int,
) -> np.ndarray:
    import pyart

    if not frames:
        return np.full((image_size, image_size), np.nan, dtype=np.float32)
    ordered = sorted(((Path(raw_path), scan_time) for raw_path, scan_time in frames), key=lambda item: item[1])
    accumulation = np.zeros((image_size, image_size), dtype=np.float32)
    any_data = np.zeros((image_size, image_size), dtype=bool)
    default_dt_hours = 5.0 / 60.0

    for index, (raw_path, scan_time) in enumerate(ordered):
        radar = pyart.io.read_nexrad_archive(str(raw_path))
        field_name = resolve_field_name(radar, "REF")
        sweep_index = min(range(int(radar.nsweeps)), key=lambda candidate: abs(_mean_elevation_for_sweep(radar, candidate) - 0.5))
        reflectivity_grid = _extract_field_grid_for_sweep(radar, field_name, sweep_index, bbox, image_size)
        rain_rate = _rain_rate_from_dbz(reflectivity_grid)
        valid = np.isfinite(rain_rate)
        if not np.any(valid):
            continue
        if index < len(ordered) - 1:
            dt_hours = max(1.0 / 60.0, min(0.5, (ordered[index + 1][1] - scan_time).total_seconds() / 3600.0))
        elif index > 0:
            dt_hours = max(1.0 / 60.0, min(0.5, (scan_time - ordered[index - 1][1]).total_seconds() / 3600.0))
        else:
            dt_hours = default_dt_hours
        accumulation[valid] += rain_rate[valid] * float(dt_hours)
        any_data |= valid

    accumulation[~any_data] = np.nan
    return accumulation


def _latlon_scale(reference_lat: float) -> tuple[float, float]:
    lon_scale = max(0.2, np.cos(np.radians(reference_lat)))
    return 1.0, lon_scale


def _line_points(
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    count: int,
) -> tuple[np.ndarray, np.ndarray]:
    fractions = np.linspace(0.0, 1.0, count, dtype=np.float32)
    return (
        start_lat + ((end_lat - start_lat) * fractions),
        start_lon + ((end_lon - start_lon) * fractions),
    )


def _sample_nearest_gate(
    sample_lat: float,
    sample_lon: float,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    values: np.ndarray,
    altitudes_km: np.ndarray,
) -> tuple[float | None, float | None]:
    valid = np.isfinite(latitudes) & np.isfinite(longitudes) & np.isfinite(values) & np.isfinite(altitudes_km)
    if not np.any(valid):
        return None, None
    lat_scale, lon_scale = _latlon_scale(sample_lat)
    dist2 = ((latitudes[valid] - sample_lat) * lat_scale) ** 2 + ((longitudes[valid] - sample_lon) * lon_scale) ** 2
    nearest = int(np.argmin(dist2))
    return float(values[valid][nearest]), float(altitudes_km[valid][nearest])


def build_cross_section(
    raw_path: str | Path,
    *,
    product: str,
    frame_id: str,
    site: str,
    start_lat: float,
    start_lon: float,
    end_lat: float,
    end_lon: float,
    samples: int = 140,
    altitude_resolution_km: float = 0.5,
    max_altitude_km: float = 18.0,
) -> CrossSectionResult:
    import pyart

    radar = pyart.io.read_nexrad_archive(str(raw_path))
    field_name = resolve_field_name(radar, product)
    sweep_records: list[dict[str, Any]] = []
    for sweep_index in range(int(radar.nsweeps)):
        try:
            values = radar.get_field(sweep_index, field_name, copy=False)
        except Exception:
            continue
        if product.upper() == "VEL":
            from backend.processor.processing.velocity import quality_control_velocity

            nyquist = 29.0
            try:
                nyquist_data = radar.instrument_parameters.get("nyquist_velocity", {}).get("data")
                if nyquist_data is not None and len(nyquist_data) > 0:
                    nyquist = float(np.nanmean(nyquist_data))
            except Exception:
                nyquist = 29.0
            values = quality_control_velocity(np.asarray(values, dtype=np.float32), nyquist_velocity=nyquist)
        latitudes, longitudes, altitudes = radar.get_gate_lat_lon_alt(sweep_index, filter_transitions=True)
        if np.ma.isMaskedArray(values):
            values = values.filled(np.nan)
        sweep_records.append(
            {
                "tilt": round(_mean_elevation_for_sweep(radar, sweep_index), 1),
                "values": np.asarray(values, dtype=np.float32),
                "latitudes": np.asarray(latitudes, dtype=np.float32),
                "longitudes": normalize_longitudes(np.asarray(longitudes, dtype=np.float32)),
                "altitudes_km": np.asarray(altitudes, dtype=np.float32) / 1000.0,
            }
        )

    if not sweep_records:
        raise ValueError(f"No sweeps available for product {product}")

    sample_lats, sample_lons = _line_points(start_lat, start_lon, end_lat, end_lon, samples)
    altitude_bins = np.arange(0.0, max_altitude_km + altitude_resolution_km, altitude_resolution_km, dtype=np.float32)
    section_grid = np.full((len(altitude_bins), samples), np.nan, dtype=np.float32)

    for sample_index, (sample_lat, sample_lon) in enumerate(zip(sample_lats, sample_lons, strict=False)):
        vertical_points: list[tuple[float, float]] = []
        for sweep in sweep_records:
            value, altitude_km = _sample_nearest_gate(
                float(sample_lat),
                float(sample_lon),
                sweep["latitudes"],
                sweep["longitudes"],
                sweep["values"],
                sweep["altitudes_km"],
            )
            if value is None or altitude_km is None:
                continue
            vertical_points.append((altitude_km, value))
        if not vertical_points:
            continue
        vertical_points.sort(key=lambda item: item[0])
        known_alts = np.array([point[0] for point in vertical_points], dtype=np.float32)
        known_vals = np.array([point[1] for point in vertical_points], dtype=np.float32)
        if len(known_alts) == 1:
            altitude_index = int(np.clip(round(known_alts[0] / altitude_resolution_km), 0, len(altitude_bins) - 1))
            section_grid[altitude_index, sample_index] = known_vals[0]
        else:
            interpolated = np.interp(
                altitude_bins,
                known_alts,
                known_vals,
                left=np.nan,
                right=np.nan,
            ).astype(np.float32)
            below = altitude_bins < known_alts.min()
            above = altitude_bins > known_alts.max()
            interpolated[below | above] = np.nan
            section_grid[:, sample_index] = interpolated

    earth_radius_km = 6371.0
    ranges_km = [0.0]
    total_distance = 0.0
    for index in range(1, samples):
        lat1 = np.radians(sample_lats[index - 1])
        lon1 = np.radians(sample_lons[index - 1])
        lat2 = np.radians(sample_lats[index])
        lon2 = np.radians(sample_lons[index])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
        total_distance += earth_radius_km * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(max(1e-12, 1.0 - a)))
        ranges_km.append(round(float(total_distance), 2))

    unit = str(PRODUCT_CATALOG[product.upper()]["unit"])
    return CrossSectionResult(
        site=site,
        product=product.upper(),
        frame_id=frame_id,
        ranges_km=ranges_km,
        altitudes_km=[round(float(value), 2) for value in altitude_bins],
        values=[[None if not np.isfinite(value) else round(float(value), 2) for value in row] for row in section_grid],
        start={"lat": round(float(start_lat), 4), "lon": round(float(start_lon), 4)},
        end={"lat": round(float(end_lat), 4), "lon": round(float(end_lon), 4)},
        tilts_used=[record["tilt"] for record in sweep_records],
        unit=unit,
        method="nearest-gate multi-tilt interpolation",
        limitation=(
            "Cross-sections are generated from available tilts using nearest-gate sampling and vertical interpolation. "
            "They are intended for local analysis, not full RHI replacement."
        ),
        generated_at=isoformat_utc(utc_now()),
    )


def sample_volume_metrics(
    products: dict[str, GridProduct],
    *,
    centroid_lat: float,
    centroid_lon: float,
    radius_km: float,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    if not products:
        return metrics

    search_radius = max(5.0, radius_km * 1.35)
    lat_radius = search_radius / 111.0
    lon_radius = search_radius / max(15.0, 111.0 * float(np.cos(np.radians(centroid_lat))))

    def local_mask(grid: GridProduct) -> np.ndarray:
        base = np.isfinite(grid.values)
        base &= np.abs(grid.latitudes - centroid_lat) <= lat_radius
        base &= np.abs(grid.longitudes - centroid_lon) <= lon_radius
        return base

    echo_tops = products.get("ET")
    echo_top_values = None
    if echo_tops is not None:
        mask = local_mask(echo_tops)
        if np.any(mask):
            echo_top_values = echo_tops.values[mask]
            metrics["max_echo_tops_km"] = round(float(np.nanmax(echo_top_values)), 1)

    vil = products.get("VIL")
    vil_values = None
    if vil is not None:
        mask = local_mask(vil)
        if np.any(mask):
            vil_values = vil.values[mask]
            metrics["max_vil_kgm2"] = round(float(np.nanmax(vil_values)), 1)

    if echo_top_values is not None and vil_values is not None:
        valid_density = np.isfinite(echo_top_values) & np.isfinite(vil_values) & (echo_top_values > 0.5)
        if np.any(valid_density):
            density = vil_values[valid_density] / echo_top_values[valid_density]
            metrics["max_vil_density_gm3"] = round(float(np.nanmax(density)), 2)

    rain_rate = products.get("RR")
    if rain_rate is not None:
        mask = local_mask(rain_rate)
        if np.any(mask):
            metrics["max_rain_rate_mmhr"] = round(float(np.nanmax(rain_rate.values[mask])), 1)

    kdp = products.get("KDP")
    if kdp is not None:
        mask = local_mask(kdp)
        if np.any(mask):
            metrics["max_kdp_degkm"] = round(float(np.nanmax(kdp.values[mask])), 2)

    qpe = products.get("QPE1H")
    if qpe is not None:
        mask = local_mask(qpe)
        if np.any(mask):
            metrics["max_qpe_1h_mm"] = round(float(np.nanmax(qpe.values[mask])), 1)

    hydrometeor = products.get("HC")
    if hydrometeor is not None:
        mask = local_mask(hydrometeor)
        if np.any(mask):
            classes = hydrometeor.values[mask]
            classes = classes[np.isfinite(classes) & (classes >= 1.0)]
            if classes.size:
                rounded = np.rint(classes).astype(int)
                dominant = int(np.bincount(rounded).argmax())
                metrics["dominant_hydrometeor"] = HC_LABELS.get(dominant, "unknown")

    return metrics
