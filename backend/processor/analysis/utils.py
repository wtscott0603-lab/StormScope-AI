from __future__ import annotations

from collections import deque
import math

import numpy as np

from backend.processor.analysis.base import SignatureMarker


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_km = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return earth_radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def finite_latlon(lat: float, lon: float) -> bool:
    return math.isfinite(lat) and math.isfinite(lon)


def label_connected(mask: np.ndarray) -> tuple[np.ndarray, int]:
    try:
        from scipy.ndimage import label as scipy_label

        return scipy_label(mask)
    except Exception:
        pass

    mask_bool = np.asarray(mask, dtype=bool)
    labeled = np.zeros(mask_bool.shape, dtype=np.int32)
    rows, cols = mask_bool.shape
    region_id = 0
    neighbors = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]

    for row in range(rows):
        for col in range(cols):
            if not mask_bool[row, col] or labeled[row, col] != 0:
                continue

            region_id += 1
            queue: deque[tuple[int, int]] = deque([(row, col)])
            labeled[row, col] = region_id

            while queue:
                current_row, current_col = queue.popleft()
                for row_offset, col_offset in neighbors:
                    next_row = current_row + row_offset
                    next_col = current_col + col_offset
                    if not (0 <= next_row < rows and 0 <= next_col < cols):
                        continue
                    if not mask_bool[next_row, next_col] or labeled[next_row, next_col] != 0:
                        continue
                    labeled[next_row, next_col] = region_id
                    queue.append((next_row, next_col))

    return labeled, region_id


def marker_to_dict(marker: SignatureMarker) -> dict:
    return {
        "signature_type": marker.signature_type,
        "severity": marker.severity,
        "lat": marker.lat,
        "lon": marker.lon,
        "radius_km": marker.radius_km,
        "label": marker.label,
        "description": marker.description,
        "confidence": marker.confidence,
        "metrics": marker.metrics,
        "polygon_latlons": marker.polygon_latlons,
    }
