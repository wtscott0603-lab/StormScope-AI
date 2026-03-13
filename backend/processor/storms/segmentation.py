from __future__ import annotations

import math

import numpy as np

from backend.processor.analysis.utils import finite_latlon, label_connected
from backend.processor.storms.geometry import elongation_ratio, make_footprint, polygon_area_km2
from backend.processor.storms.types import StormDetection


def _neighbor_true_count(mask: np.ndarray) -> np.ndarray:
    """Count 8-connected True neighbors for each cell.

    Uses numpy offset slices instead of a Python double loop over offsets —
    roughly 8× faster on typical radar arrays (360 × 460 gates).
    """
    mask_int = np.asarray(mask, dtype=np.int16)
    rows, cols = mask_int.shape
    padded = np.pad(mask_int, 1, mode="constant", constant_values=0)
    counts = (
        padded[0:rows, 0:cols]
        + padded[0:rows, 1 : cols + 1]
        + padded[0:rows, 2 : cols + 2]
        + padded[1 : rows + 1, 0:cols]
        # centre cell [1:rows+1, 1:cols+1] deliberately excluded
        + padded[1 : rows + 1, 2 : cols + 2]
        + padded[2 : rows + 2, 0:cols]
        + padded[2 : rows + 2, 1 : cols + 1]
        + padded[2 : rows + 2, 2 : cols + 2]
    ).astype(np.int16)
    return counts


def _refine_detection_mask(values: np.ndarray, threshold_dbz: float) -> np.ndarray:
    finite_mask = np.isfinite(values)
    base_mask = finite_mask & (values >= threshold_dbz)
    if not base_mask.any():
        return base_mask

    relaxed_mask = finite_mask & (values >= max(30.0, threshold_dbz - 4.0))
    core_threshold = max(threshold_dbz + 8.0, 55.0)
    core_mask = finite_mask & (values >= core_threshold)
    refined = base_mask.copy()

    for _ in range(3):
        neighbor_counts = _neighbor_true_count(refined)
        core_neighbor_counts = _neighbor_true_count(core_mask) if core_mask.any() else neighbor_counts
        additions = (~refined) & relaxed_mask & ((neighbor_counts >= 4) | (core_neighbor_counts >= 2))
        hole_fill = (~refined) & (neighbor_counts >= 6)
        removals = refined & ~core_mask & (neighbor_counts <= 1)
        updated = (refined | additions | hole_fill) & ~removals
        if np.array_equal(updated, refined):
            break
        refined = updated

    return refined


def detect_storm_cells(sweep, *, threshold_dbz: float = 40.0, min_area_km2: float = 20.0) -> list[StormDetection]:
    values = sweep.values
    latitudes = sweep.latitudes
    longitudes = sweep.longitudes
    spatial_mask = np.isfinite(latitudes) & np.isfinite(longitudes)
    mask = _refine_detection_mask(values, threshold_dbz=threshold_dbz) & spatial_mask
    if not mask.any():
        return []

    labeled, region_count = label_connected(mask)
    core_threshold = max(threshold_dbz + 8.0, 55.0)
    core_mask = np.isfinite(values) & (values >= core_threshold)
    detections: list[StormDetection] = []
    for region_id in range(1, region_count + 1):
        region_mask = labeled == region_id
        gate_count = int(region_mask.sum())
        if gate_count < 6:
            continue

        region_values = values[region_mask]
        region_latitudes = latitudes[region_mask]
        region_longitudes = longitudes[region_mask]
        centroid_lat = float(np.nanmean(region_latitudes))
        centroid_lon = float(np.nanmean(region_longitudes))
        if not finite_latlon(centroid_lat, centroid_lon):
            continue

        footprint = make_footprint(zip(region_longitudes.tolist(), region_latitudes.tolist()))
        area_km2 = polygon_area_km2(footprint, centroid_lat, centroid_lon)
        if area_km2 < min_area_km2:
            continue

        core_gate_count = int((region_mask & core_mask).sum())
        core_fraction = core_gate_count / gate_count if gate_count else 0.0
        core_values = region_values[region_values >= core_threshold]
        core_max_reflectivity = float(np.nanmax(core_values)) if core_values.size else float(np.nanmax(region_values))

        detections.append(
            StormDetection(
                centroid_lat=centroid_lat,
                centroid_lon=centroid_lon,
                area_km2=area_km2,
                max_reflectivity=float(np.nanmax(region_values)),
                mean_reflectivity=float(np.nanmean(region_values)),
                gate_count=gate_count,
                elongation_ratio=elongation_ratio(footprint, centroid_lat, centroid_lon),
                radius_km=max(2.0, math.sqrt(area_km2 / math.pi)),
                footprint_geojson=footprint,
                gate_mask=region_mask,
                core_gate_count=core_gate_count,
                core_fraction=round(core_fraction, 3),
                core_max_reflectivity=core_max_reflectivity,
            )
        )

    detections.sort(
        key=lambda detection: (detection.max_reflectivity, detection.core_fraction, detection.area_km2),
        reverse=True,
    )
    return detections
