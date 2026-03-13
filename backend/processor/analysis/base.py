from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import numpy as np


@dataclass
class SweepArrays:
    """Raw sweep data arrays passed to analyzers for spatial computations."""

    values: np.ndarray
    latitudes: np.ndarray
    longitudes: np.ndarray
    azimuths: np.ndarray
    ranges_km: np.ndarray
    site_lat: float
    site_lon: float
    nyquist_velocity: float = 0.0


SeverityLevel = Literal["TORNADO_EMERGENCY", "TORNADO", "SEVERE", "MARGINAL", "NONE"]

SEVERITY_RANK: dict[SeverityLevel, int] = {
    "TORNADO_EMERGENCY": 5,
    "TORNADO": 4,
    "SEVERE": 3,
    "MARGINAL": 2,
    "NONE": 1,
}


@dataclass
class SignatureMarker:
    """
    A single detected severe weather signature at a specific geographic location.
    Serialized to JSON and stored in analysis_results.
    """

    signature_type: str
    severity: SeverityLevel
    lat: float
    lon: float
    radius_km: float
    label: str
    description: str
    confidence: float
    metrics: dict[str, Any]
    polygon_latlons: list[list[float]] | None = None


@dataclass
class ProcessedFrame:
    frame_id: str
    site: str
    product: str
    image_path: str
    sweep: SweepArrays | None = None


@dataclass
class AnalysisResult:
    analyzer: str
    payload: dict[str, Any]


class BaseAnalyzer:
    """Interface for all signature analyzers."""

    name = "base"

    def run(self, frame: ProcessedFrame, context: dict | None = None) -> AnalysisResult:
        raise NotImplementedError
