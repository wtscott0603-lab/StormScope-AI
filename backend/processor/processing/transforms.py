from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from pyproj import Transformer


_WGS84_TO_WEB_MERCATOR = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_WEB_MERCATOR_TO_WGS84 = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)


@dataclass(frozen=True)
class BBox:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


def geodetic_to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
    return _WGS84_TO_WEB_MERCATOR.transform(lon, lat)


def web_mercator_to_geodetic(x: float, y: float) -> tuple[float, float]:
    return _WEB_MERCATOR_TO_WGS84.transform(x, y)


def normalize_longitudes(longitudes: np.ndarray) -> np.ndarray:
    return ((longitudes + 180.0) % 360.0) - 180.0


def compute_bbox(latitudes: np.ndarray, longitudes: np.ndarray) -> BBox:
    finite_mask = np.isfinite(latitudes) & np.isfinite(longitudes)
    if not np.any(finite_mask):
        raise ValueError("Cannot compute a bounding box from empty radar coordinates.")

    valid_lats = latitudes[finite_mask]
    valid_lons = normalize_longitudes(longitudes[finite_mask])
    return BBox(
        min_lat=float(valid_lats.min()),
        max_lat=float(valid_lats.max()),
        min_lon=float(valid_lons.min()),
        max_lon=float(valid_lons.max()),
    )
