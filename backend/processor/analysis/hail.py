from __future__ import annotations

import math

import numpy as np

from backend.processor.analysis.base import AnalysisResult, BaseAnalyzer, ProcessedFrame, SEVERITY_RANK, SignatureMarker
from backend.processor.analysis.utils import finite_latlon, haversine_km, label_connected, marker_to_dict


HAIL_MARGINAL_DBZ = 50.0
HAIL_LIKELY_DBZ = 55.0
HAIL_LARGE_DBZ = 60.0
HAIL_GIANT_DBZ = 65.0
HAIL_ZDR_MAX = 0.5
MIN_HAIL_GATES = 3


def _estimate_hail_size_label(max_dbz: float) -> str:
    if max_dbz >= HAIL_GIANT_DBZ:
        return 'GIANT HAIL (>=2")'
    if max_dbz >= HAIL_LARGE_DBZ:
        return 'LARGE HAIL (>=1")'
    if max_dbz >= HAIL_LIKELY_DBZ:
        return 'HAIL LIKELY (3/4")'
    return "HAIL POSSIBLE (pea)"


def detect_hail_cores(ref_sweep, zdr_sweep=None) -> list[SignatureMarker]:
    ref_values = ref_sweep.values
    latitudes = ref_sweep.latitudes
    longitudes = ref_sweep.longitudes

    hail_mask = np.isfinite(ref_values) & (ref_values >= HAIL_MARGINAL_DBZ)
    if not hail_mask.any():
        return []

    zdr_confirmed_mask = None
    if zdr_sweep is not None and zdr_sweep.values.shape == ref_values.shape:
        zdr_values = zdr_sweep.values
        low_zdr = np.isfinite(zdr_values) & (zdr_values <= HAIL_ZDR_MAX)
        zdr_confirmed_mask = hail_mask & low_zdr

    labeled, region_count = label_connected(hail_mask)
    markers: list[SignatureMarker] = []

    for region_id in range(1, region_count + 1):
        region_mask = labeled == region_id
        gate_count = int(region_mask.sum())
        if gate_count < MIN_HAIL_GATES:
            continue

        region_ref = ref_values[region_mask]
        region_lat = latitudes[region_mask]
        region_lon = longitudes[region_mask]
        centroid_lat = float(np.nanmean(region_lat))
        centroid_lon = float(np.nanmean(region_lon))
        if not finite_latlon(centroid_lat, centroid_lon):
            continue

        max_dbz = float(np.nanmax(region_ref))
        mean_dbz = float(np.nanmean(region_ref))

        zdr_boost = False
        if zdr_confirmed_mask is not None and int((region_mask & zdr_confirmed_mask).sum()) >= 2:
            zdr_boost = True

        max_distance = 0.0
        sample_size = min(30, len(region_lat))
        for sample_index in range(sample_size):
            max_distance = max(
                max_distance,
                haversine_km(centroid_lat, centroid_lon, float(region_lat[sample_index]), float(region_lon[sample_index])),
            )
        radius_km = max(1.0, max_distance)

        if max_dbz >= HAIL_GIANT_DBZ:
            severity = "TORNADO"
            signature_type = "HAIL_LARGE"
        elif max_dbz >= HAIL_LARGE_DBZ:
            severity = "SEVERE"
            signature_type = "HAIL_LARGE"
        elif max_dbz >= HAIL_LIKELY_DBZ:
            severity = "SEVERE"
            signature_type = "HAIL_CORE"
        else:
            severity = "MARGINAL"
            signature_type = "HAIL_CORE"

        confidence = min(1.0, (max_dbz - HAIL_MARGINAL_DBZ) / 20.0)
        if zdr_boost:
            confidence = min(1.0, confidence + 0.20)

        description = (
            f"Max REF {max_dbz:.0f} dBZ | Mean {mean_dbz:.0f} dBZ | Extent ~{radius_km:.1f} km"
            + (" | ZDR confirmed" if zdr_boost else "")
        )

        markers.append(
            SignatureMarker(
                signature_type=signature_type,
                severity=severity,
                lat=centroid_lat,
                lon=centroid_lon,
                radius_km=radius_km,
                label=_estimate_hail_size_label(max_dbz),
                description=description,
                confidence=round(confidence, 2),
                metrics={
                    "max_dbz": round(max_dbz, 1),
                    "mean_dbz": round(mean_dbz, 1),
                    "gate_count": gate_count,
                    "zdr_confirmed": zdr_boost,
                    "radius_km": round(radius_km, 1),
                },
            )
        )

    markers.sort(key=lambda marker: (SEVERITY_RANK[marker.severity], marker.metrics.get("max_dbz", 0)), reverse=True)
    return markers


class HailAnalyzer(BaseAnalyzer):
    name = "hail"

    def run(self, frame: ProcessedFrame, context: dict | None = None) -> AnalysisResult:
        if frame.product != "REF":
            return AnalysisResult(
                analyzer=self.name,
                payload={
                    "status": "skipped",
                    "reason": "REF product required",
                    "max_severity": "NONE",
                    "signature_count": 0,
                    "signatures": [],
                },
            )

        if frame.sweep is None:
            return AnalysisResult(
                analyzer=self.name,
                payload={
                    "status": "error",
                    "reason": "No sweep arrays available",
                    "max_severity": "NONE",
                    "signature_count": 0,
                    "signatures": [],
                },
            )

        context_data = context or {}
        zdr_sweep = context_data.get("zdr_sweep")

        try:
            signatures = detect_hail_cores(frame.sweep, zdr_sweep)
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
