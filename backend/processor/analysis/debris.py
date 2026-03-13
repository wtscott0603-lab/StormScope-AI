from __future__ import annotations

import math

import numpy as np

from backend.processor.analysis.base import AnalysisResult, BaseAnalyzer, ProcessedFrame, SEVERITY_RANK, SignatureMarker
from backend.processor.analysis.utils import finite_latlon, haversine_km, label_connected, marker_to_dict


TDS_CC_MAX = 0.80
TDS_CC_STRONG = 0.70
TDS_REF_MIN = 35.0
TDS_REF_STRONG = 45.0
TDS_MIN_PIXELS = 4


def detect_tds(ref_sweep, cc_sweep) -> list[SignatureMarker]:
    ref_values = ref_sweep.values
    cc_values = cc_sweep.values
    latitudes = ref_sweep.latitudes
    longitudes = ref_sweep.longitudes

    if ref_values.shape != cc_values.shape:
        return []

    low_cc = np.isfinite(cc_values) & (cc_values < TDS_CC_MAX)
    high_ref = np.isfinite(ref_values) & (ref_values >= TDS_REF_MIN)
    tds_mask = low_cc & high_ref & np.isfinite(latitudes) & np.isfinite(longitudes)
    if not tds_mask.any():
        return []

    labeled, region_count = label_connected(tds_mask)
    markers: list[SignatureMarker] = []

    for region_id in range(1, region_count + 1):
        region_mask = labeled == region_id
        gate_count = int(region_mask.sum())
        if gate_count < TDS_MIN_PIXELS:
            continue

        region_cc = cc_values[region_mask]
        region_ref = ref_values[region_mask]
        region_lat = latitudes[region_mask]
        region_lon = longitudes[region_mask]

        centroid_lat = float(np.nanmean(region_lat))
        centroid_lon = float(np.nanmean(region_lon))
        if not finite_latlon(centroid_lat, centroid_lon):
            continue

        mean_cc = float(np.nanmean(region_cc))
        min_cc = float(np.nanmin(region_cc))
        mean_ref = float(np.nanmean(region_ref))
        max_ref = float(np.nanmax(region_ref))

        max_distance = 0.0
        sample_size = min(20, len(region_lat))
        for sample_index in range(sample_size):
            max_distance = max(
                max_distance,
                haversine_km(centroid_lat, centroid_lon, float(region_lat[sample_index]), float(region_lon[sample_index])),
            )
        radius_km = max(0.5, max_distance)

        if min_cc < TDS_CC_STRONG and max_ref >= TDS_REF_STRONG:
            severity = "TORNADO_EMERGENCY"
            confidence = 0.90
        elif min_cc < TDS_CC_MAX and max_ref >= TDS_REF_STRONG:
            severity = "TORNADO"
            confidence = 0.75
        elif min_cc < TDS_CC_MAX:
            severity = "TORNADO"
            confidence = 0.55
        else:
            severity = "SEVERE"
            confidence = 0.40

        label = "TDS" if severity in ("TORNADO_EMERGENCY", "TORNADO") else "DEBRIS?"
        description = (
            f"Debris signature detected | Min CC {min_cc:.2f} | "
            f"Max REF {max_ref:.0f} dBZ | {gate_count} gates"
        )

        markers.append(
            SignatureMarker(
                signature_type="TDS",
                severity=severity,
                lat=centroid_lat,
                lon=centroid_lon,
                radius_km=radius_km,
                label=label,
                description=description,
                confidence=round(confidence, 2),
                metrics={
                    "min_cc": round(min_cc, 3),
                    "mean_cc": round(mean_cc, 3),
                    "max_ref_dbz": round(max_ref, 1),
                    "mean_ref_dbz": round(mean_ref, 1),
                    "gate_count": gate_count,
                },
            )
        )

    markers.sort(key=lambda marker: SEVERITY_RANK[marker.severity], reverse=True)
    return markers


class DebrisAnalyzer(BaseAnalyzer):
    """Tornadic Debris Signature detector. Requires both CC and REF sweep arrays in context."""

    name = "debris"

    def run(self, frame: ProcessedFrame, context: dict | None = None) -> AnalysisResult:
        context_data = context or {}
        ref_sweep = context_data.get("ref_sweep")
        cc_sweep = context_data.get("cc_sweep")

        if ref_sweep is None or cc_sweep is None:
            return AnalysisResult(
                analyzer=self.name,
                payload={
                    "status": "skipped",
                    "reason": "Both REF and CC sweep data required for TDS detection",
                    "max_severity": "NONE",
                    "signature_count": 0,
                    "signatures": [],
                },
            )

        try:
            signatures = detect_tds(ref_sweep, cc_sweep)
            max_severity = "NONE"
            for signature in signatures:
                if SEVERITY_RANK[signature.severity] > SEVERITY_RANK[max_severity]:
                    max_severity = signature.severity

            return AnalysisResult(
                analyzer=self.name,
                payload={
                    "status": "ok",
                    "max_severity": max_severity,
                    "signature_count": len(signatures),
                    "signatures": [marker_to_dict(marker) for marker in signatures],
                },
            )
        except Exception as exc:
            return AnalysisResult(
                analyzer=self.name,
                payload={
                    "status": "error",
                    "reason": str(exc),
                    "max_severity": "NONE",
                    "signature_count": 0,
                    "signatures": [],
                },
            )
