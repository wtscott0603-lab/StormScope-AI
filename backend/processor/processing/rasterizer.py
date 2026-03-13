from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
from PIL import Image

from backend.processor.processing.colortables import product_to_rgba
from backend.processor.processing.level2_parser import SweepData
from backend.processor.processing.transforms import BBox, compute_bbox


@dataclass
class RasterizedFrame:
    bbox: BBox
    image_path: Path


def _aggregate_pixels(product: str, values: np.ndarray, x: np.ndarray, y: np.ndarray, size: int) -> np.ndarray:
    pixel_index = (y * size) + x
    flat_size = size * size

    if product == "REF":
        # Bincount-based maximum scatter: sort by (pixel, value) so each pixel's
        # last entry is the maximum; then use np.unique to pick the winner.
        # Avoids the O(n) Python-loop overhead of np.maximum.at for large sweeps.
        order = np.argsort(values, kind="mergesort")          # ascending by value
        sorted_pixels = pixel_index[order]
        sorted_values = values[order]
        # np.unique keeps the *last* occurrence when return_index is not used — we
        # flip so largest is last, then unique takes the first (= largest) position.
        unique_pixels, last_idx = np.unique(sorted_pixels[::-1], return_index=True)
        selected = (len(sorted_pixels) - 1) - last_idx
        flat_grid = np.full(flat_size, np.nan, dtype=np.float32)
        flat_grid[unique_pixels] = sorted_values[selected]
        return flat_grid.reshape(size, size)

    # Velocity and dual-pol products: keep highest-|magnitude| value per pixel.
    order = np.argsort(np.abs(values), kind="mergesort")
    sorted_pixels = pixel_index[order]
    sorted_values = values[order]
    reverse_unique_pixels, reverse_unique_idx = np.unique(sorted_pixels[::-1], return_index=True)
    selected_positions = (len(sorted_pixels) - 1) - reverse_unique_idx
    flat_grid = np.full(flat_size, np.nan, dtype=np.float32)
    flat_grid[reverse_unique_pixels] = sorted_values[selected_positions]
    return flat_grid.reshape(size, size)


def rasterize_sweep(sweep: SweepData, image_path: str | Path, *, image_size: int = 1024) -> RasterizedFrame:
    bbox = compute_bbox(sweep.latitudes, sweep.longitudes)
    image_file = Path(image_path)
    image_file.parent.mkdir(parents=True, exist_ok=True)

    finite_mask = np.isfinite(sweep.values) & np.isfinite(sweep.latitudes) & np.isfinite(sweep.longitudes)
    finite_mask &= (sweep.latitudes >= -90.0) & (sweep.latitudes <= 90.0)
    finite_mask &= (sweep.longitudes >= -180.0) & (sweep.longitudes <= 180.0)

    values = sweep.values[finite_mask]
    latitudes = sweep.latitudes[finite_mask]
    longitudes = sweep.longitudes[finite_mask]

    lon_span = max(bbox.max_lon - bbox.min_lon, 1e-6)
    lat_span = max(bbox.max_lat - bbox.min_lat, 1e-6)

    x = np.clip(((longitudes - bbox.min_lon) / lon_span * (image_size - 1)).astype(np.int32), 0, image_size - 1)
    y = np.clip(((bbox.max_lat - latitudes) / lat_span * (image_size - 1)).astype(np.int32), 0, image_size - 1)

    scalar_grid = _aggregate_pixels(sweep.product, values, x, y, image_size)
    rgba = product_to_rgba(sweep.product, scalar_grid)
    Image.fromarray(rgba, mode="RGBA").save(image_file)

    return RasterizedFrame(bbox=bbox, image_path=image_file)
