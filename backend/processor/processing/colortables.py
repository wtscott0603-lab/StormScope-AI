from __future__ import annotations

from dataclasses import dataclass

import numpy as np


RGBAArray = np.ndarray


@dataclass(frozen=True)
class ColorBin:
    lower: float
    upper: float | None
    rgba: tuple[int, int, int, int]


REFLECTIVITY_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 5.0, (0, 0, 0, 0)),
    ColorBin(5.0, 10.0, (102, 204, 255, 255)),
    ColorBin(10.0, 20.0, (0, 112, 255, 255)),
    ColorBin(20.0, 30.0, (0, 190, 0, 255)),
    ColorBin(30.0, 40.0, (255, 230, 0, 255)),
    ColorBin(40.0, 50.0, (255, 140, 0, 255)),
    ColorBin(50.0, 60.0, (255, 0, 0, 255)),
    ColorBin(60.0, 65.0, (255, 0, 255, 255)),
    ColorBin(65.0, None, (255, 255, 255, 255)),
)

VELOCITY_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), -50.0, (120, 0, 255, 255)),
    ColorBin(-50.0, -30.0, (40, 85, 255, 255)),
    ColorBin(-30.0, -10.0, (0, 185, 255, 255)),
    ColorBin(-10.0, 10.0, (0, 0, 0, 0)),
    ColorBin(10.0, 30.0, (255, 205, 0, 255)),
    ColorBin(30.0, 50.0, (255, 120, 0, 255)),
    ColorBin(50.0, None, (255, 0, 0, 255)),
)

STORM_RELATIVE_VELOCITY_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), -45.0, (138, 43, 226, 255)),
    ColorBin(-45.0, -30.0, (76, 110, 245, 255)),
    ColorBin(-30.0, -18.0, (0, 176, 255, 255)),
    ColorBin(-18.0, -8.0, (0, 224, 255, 210)),
    ColorBin(-8.0, 8.0, (0, 0, 0, 0)),
    ColorBin(8.0, 18.0, (255, 222, 64, 210)),
    ColorBin(18.0, 30.0, (255, 168, 0, 255)),
    ColorBin(30.0, 45.0, (255, 84, 0, 255)),
    ColorBin(45.0, None, (255, 0, 0, 255)),
)

CC_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 0.2, (0, 0, 0, 0)),
    ColorBin(0.2, 0.5, (84, 0, 140, 220)),
    ColorBin(0.5, 0.7, (44, 72, 196, 240)),
    ColorBin(0.7, 0.9, (0, 176, 128, 245)),
    ColorBin(0.9, 0.95, (208, 208, 0, 245)),
    ColorBin(0.95, None, (255, 255, 255, 255)),
)

ZDR_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), -2.0, (0, 0, 0, 0)),
    ColorBin(-2.0, 0.0, (138, 43, 226, 230)),
    ColorBin(0.0, 1.0, (0, 172, 202, 235)),
    ColorBin(1.0, 3.0, (80, 200, 120, 245)),
    ColorBin(3.0, 6.0, (255, 176, 0, 255)),
    ColorBin(6.0, None, (255, 64, 0, 255)),
)

KDP_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 0.1, (0, 0, 0, 0)),
    ColorBin(0.1, 0.5, (96, 165, 250, 220)),
    ColorBin(0.5, 1.0, (34, 197, 94, 235)),
    ColorBin(1.0, 2.0, (250, 204, 21, 245)),
    ColorBin(2.0, 3.5, (249, 115, 22, 250)),
    ColorBin(3.5, 5.0, (239, 68, 68, 255)),
    ColorBin(5.0, None, (190, 24, 93, 255)),
)

ECHO_TOP_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 1.0, (0, 0, 0, 0)),
    ColorBin(1.0, 4.0, (56, 189, 248, 220)),
    ColorBin(4.0, 7.0, (34, 197, 94, 235)),
    ColorBin(7.0, 10.0, (250, 204, 21, 245)),
    ColorBin(10.0, 13.0, (249, 115, 22, 250)),
    ColorBin(13.0, 16.0, (239, 68, 68, 255)),
    ColorBin(16.0, None, (244, 114, 182, 255)),
)

VIL_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 2.0, (0, 0, 0, 0)),
    ColorBin(2.0, 10.0, (59, 130, 246, 210)),
    ColorBin(10.0, 20.0, (16, 185, 129, 225)),
    ColorBin(20.0, 35.0, (234, 179, 8, 240)),
    ColorBin(35.0, 50.0, (249, 115, 22, 250)),
    ColorBin(50.0, 65.0, (239, 68, 68, 255)),
    ColorBin(65.0, None, (236, 72, 153, 255)),
)

RAIN_RATE_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 0.2, (0, 0, 0, 0)),
    ColorBin(0.2, 2.0, (96, 165, 250, 220)),
    ColorBin(2.0, 8.0, (34, 197, 94, 235)),
    ColorBin(8.0, 20.0, (250, 204, 21, 245)),
    ColorBin(20.0, 40.0, (249, 115, 22, 250)),
    ColorBin(40.0, 75.0, (239, 68, 68, 255)),
    ColorBin(75.0, None, (190, 24, 93, 255)),
)

QPE_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 0.5, (0, 0, 0, 0)),
    ColorBin(0.5, 5.0, (125, 211, 252, 220)),
    ColorBin(5.0, 15.0, (34, 197, 94, 235)),
    ColorBin(15.0, 30.0, (250, 204, 21, 245)),
    ColorBin(30.0, 50.0, (249, 115, 22, 250)),
    ColorBin(50.0, 80.0, (239, 68, 68, 255)),
    ColorBin(80.0, None, (168, 85, 247, 255)),
)

HYDROMETEOR_BINS: tuple[ColorBin, ...] = (
    ColorBin(float("-inf"), 0.5, (0, 0, 0, 0)),
    ColorBin(0.5, 1.5, (96, 165, 250, 230)),   # Rain
    ColorBin(1.5, 2.5, (56, 189, 248, 235)),   # Moderate Rain
    ColorBin(2.5, 3.5, (34, 197, 94, 235)),    # Heavy Rain
    ColorBin(3.5, 4.5, (250, 204, 21, 240)),   # Large Drops
    ColorBin(4.5, 5.5, (239, 68, 68, 255)),    # Hail
    ColorBin(5.5, 6.5, (168, 85, 247, 235)),   # Graupel
    ColorBin(6.5, 7.5, (226, 232, 240, 245)),  # Snow
    ColorBin(7.5, 8.5, (148, 163, 184, 235)),  # Mixed
    ColorBin(8.5, None, (236, 72, 153, 255)),  # Debris candidate
)


def apply_color_table(values: np.ndarray, bins: tuple[ColorBin, ...]) -> RGBAArray:
    rgba = np.zeros(values.shape + (4,), dtype=np.uint8)
    finite_mask = np.isfinite(values)
    if not np.any(finite_mask):
        return rgba

    for color_bin in bins:
        if color_bin.upper is None:
            mask = finite_mask & (values >= color_bin.lower)
        else:
            mask = finite_mask & (values >= color_bin.lower) & (values < color_bin.upper)
        if np.any(mask):
            rgba[mask] = color_bin.rgba
    return rgba


def reflectivity_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, REFLECTIVITY_BINS)


def velocity_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, VELOCITY_BINS)


def storm_relative_velocity_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, STORM_RELATIVE_VELOCITY_BINS)


def correlation_coefficient_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, CC_BINS)


def differential_reflectivity_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, ZDR_BINS)


def specific_differential_phase_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, KDP_BINS)


def echo_tops_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, ECHO_TOP_BINS)


def vil_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, VIL_BINS)


def rain_rate_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, RAIN_RATE_BINS)


def qpe_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, QPE_BINS)


def hydrometeor_to_rgba(values: np.ndarray) -> RGBAArray:
    return apply_color_table(values, HYDROMETEOR_BINS)


def product_to_rgba(product: str, values: np.ndarray) -> RGBAArray:
    product_id = product.upper()
    if product_id == "REF":
        return reflectivity_to_rgba(values)
    if product_id == "VEL":
        return velocity_to_rgba(values)
    if product_id == "SRV":
        return storm_relative_velocity_to_rgba(values)
    if product_id == "CC":
        return correlation_coefficient_to_rgba(values)
    if product_id == "ZDR":
        return differential_reflectivity_to_rgba(values)
    if product_id == "KDP":
        return specific_differential_phase_to_rgba(values)
    if product_id == "ET":
        return echo_tops_to_rgba(values)
    if product_id == "VIL":
        return vil_to_rgba(values)
    if product_id == "RR":
        return rain_rate_to_rgba(values)
    if product_id == "QPE1H":
        return qpe_to_rgba(values)
    if product_id == "HC":
        return hydrometeor_to_rgba(values)
    raise ValueError(f"Unsupported radar product: {product}")
