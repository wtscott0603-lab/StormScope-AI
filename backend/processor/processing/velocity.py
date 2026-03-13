from __future__ import annotations

import math

import numpy as np


def _neighbor_median(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    rows, cols = values.shape
    padded = np.pad(values, 1, mode="constant", constant_values=np.nan)
    neighbors = []
    for row_offset in range(3):
        for col_offset in range(3):
            if row_offset == 1 and col_offset == 1:
                continue
            neighbors.append(padded[row_offset : row_offset + rows, col_offset : col_offset + cols])
    stacked = np.stack(neighbors, axis=0)
    finite_counts = np.sum(np.isfinite(stacked), axis=0)
    masked = np.ma.masked_invalid(stacked)
    return np.ma.median(masked, axis=0).filled(np.nan).astype(np.float32), finite_counts


def quality_control_velocity(values: np.ndarray, nyquist_velocity: float = 0.0) -> np.ndarray:
    """
    Apply a conservative QC pass to velocity data.

    This is intentionally lightweight: it clips obviously bad outliers while
    preserving the native sweep structure for downstream analysis.
    """

    qc_values = np.asarray(values, dtype=np.float32).copy()
    finite_mask = np.isfinite(qc_values)
    if not np.any(finite_mask):
        return qc_values

    max_reasonable = 90.0
    if math.isfinite(nyquist_velocity) and nyquist_velocity > 1.0:
        max_reasonable = max(max_reasonable, abs(nyquist_velocity) * 1.35)

    qc_values[finite_mask & (np.abs(qc_values) > max_reasonable)] = np.nan
    finite_mask = np.isfinite(qc_values)
    if np.sum(finite_mask) < 6:
        return qc_values

    neighborhood_median, neighbor_counts = _neighbor_median(qc_values)
    continuity_threshold = 18.0
    if math.isfinite(nyquist_velocity) and nyquist_velocity > 1.0:
        continuity_threshold = max(continuity_threshold, abs(nyquist_velocity) * 0.60)

    isolated_spikes = (
        finite_mask
        & np.isfinite(neighborhood_median)
        & (neighbor_counts >= 3)
        & (np.abs(qc_values - neighborhood_median) >= continuity_threshold)
    )
    qc_values[isolated_spikes] = neighborhood_median[isolated_spikes]
    return qc_values


def project_motion_onto_radial(
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    *,
    site_lat: float,
    site_lon: float,
    motion_heading_deg: float,
    motion_speed_kmh: float,
) -> np.ndarray:
    speed_ms = motion_speed_kmh / 3.6
    heading_rad = math.radians(motion_heading_deg)
    east_ms = speed_ms * math.sin(heading_rad)
    north_ms = speed_ms * math.cos(heading_rad)

    dlat_km = (latitudes - site_lat) * 111.0
    dlon_km = (longitudes - site_lon) * 111.0 * math.cos(math.radians(site_lat))
    norms = np.sqrt((dlat_km ** 2) + (dlon_km ** 2))
    norms = np.where(norms == 0.0, np.nan, norms)
    radial_east = dlon_km / norms
    radial_north = dlat_km / norms
    projected = (east_ms * radial_east) + (north_ms * radial_north)
    return np.where(np.isfinite(projected), projected, 0.0).astype(np.float32)


def derive_storm_relative_velocity(
    values: np.ndarray,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    *,
    site_lat: float,
    site_lon: float,
    motion_heading_deg: float,
    motion_speed_kmh: float,
    nyquist_velocity: float = 0.0,
) -> np.ndarray:
    projected_motion = project_motion_onto_radial(
        latitudes,
        longitudes,
        site_lat=site_lat,
        site_lon=site_lon,
        motion_heading_deg=motion_heading_deg,
        motion_speed_kmh=motion_speed_kmh,
    )
    srv_values = np.asarray(values, dtype=np.float32) - projected_motion
    return quality_control_velocity(srv_values, nyquist_velocity=nyquist_velocity)
