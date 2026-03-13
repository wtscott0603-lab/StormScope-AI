from __future__ import annotations

import math

import numpy as np

from backend.processor.analysis.base import AnalysisResult, BaseAnalyzer, ProcessedFrame, SEVERITY_RANK, SignatureMarker
from backend.processor.analysis.utils import finite_latlon, haversine_km, marker_to_dict


SHEAR_MARGINAL = 0.005
SHEAR_SEVERE = 0.010
SHEAR_TORNADO = 0.020
SHEAR_TVS = 0.030
TVS_MIN_DELTA_V = 50.0
MIN_RANGE_KM = 10.0
MAX_RANGE_KM = 230.0
AZIMUTH_SEARCH_BINS = 5


def _gate_pair(values: np.ndarray, ray_index: int, neighbor_index: int, gate_index: int) -> tuple[float, float] | None:
    if gate_index < 0 or gate_index >= values.shape[1]:
        return None
    left = float(values[ray_index, gate_index])
    right = float(values[neighbor_index, gate_index])
    if not (np.isfinite(left) and np.isfinite(right)):
        return None
    if left * right >= 0:
        return None
    return left, right


def _couplet_support(values: np.ndarray, ray_index: int, neighbor_index: int, gate_index: int) -> int:
    support = 0
    for gate_offset in (-2, -1, 1, 2):
        pair = _gate_pair(values, ray_index, neighbor_index, gate_index + gate_offset)
        if pair is None:
            continue
        left, right = pair
        if abs(left) >= 6.0 and abs(right) >= 6.0:
            support += 1
    return support


def detect_rotation_couplets(sweep) -> list[SignatureMarker]:
    if sweep is None:
        return []

    values = sweep.values
    lats = sweep.latitudes
    lons = sweep.longitudes
    ranges = sweep.ranges_km
    n_rays, _ = values.shape
    markers: list[dict] = []

    valid_gate_mask = (ranges >= MIN_RANGE_KM) & (ranges <= MAX_RANGE_KM)
    gate_indices = np.where(valid_gate_mask)[0]

    for gate_index in gate_indices:
        velocity_column = values[:, gate_index]
        valid = np.isfinite(velocity_column)
        if valid.sum() < 6:
            continue

        range_km = float(ranges[gate_index])
        for ray_index in range(n_rays):
            if not np.isfinite(velocity_column[ray_index]):
                continue

            v_center = float(velocity_column[ray_index])
            for offset in range(1, AZIMUTH_SEARCH_BINS + 1):
                neighbor_index = (ray_index + offset) % n_rays
                if not np.isfinite(velocity_column[neighbor_index]):
                    continue

                v_neighbor = float(velocity_column[neighbor_index])
                if v_center * v_neighbor >= 0:
                    continue
                if abs(v_center) < 5.0 or abs(v_neighbor) < 5.0:
                    continue

                v_inbound = min(v_center, v_neighbor)
                v_outbound = max(v_center, v_neighbor)
                delta_v = v_outbound - v_inbound

                lat_i = float(lats[ray_index, gate_index])
                lon_i = float(lons[ray_index, gate_index])
                lat_j = float(lats[neighbor_index, gate_index])
                lon_j = float(lons[neighbor_index, gate_index])

                if not (finite_latlon(lat_i, lon_i) and finite_latlon(lat_j, lon_j)):
                    continue

                arc_km = haversine_km(lat_i, lon_i, lat_j, lon_j)
                if arc_km < 0.1:
                    continue

                shear = delta_v / arc_km
                if shear < SHEAR_MARGINAL:
                    continue

                support_count = _couplet_support(values, ray_index, neighbor_index, gate_index)
                centroid_lat = (lat_i + lat_j) / 2
                centroid_lon = (lon_i + lon_j) / 2
                if not finite_latlon(centroid_lat, centroid_lon):
                    continue

                is_tvs = shear >= SHEAR_TVS and delta_v >= TVS_MIN_DELTA_V
                if is_tvs:
                    severity = "TORNADO"
                    signature_type = "TVS"
                    label = "TVS"
                elif shear >= SHEAR_TORNADO:
                    severity = "TORNADO"
                    signature_type = "ROTATION"
                    label = "MESO"
                elif shear >= SHEAR_SEVERE:
                    severity = "SEVERE"
                    signature_type = "ROTATION"
                    label = "ROT"
                else:
                    severity = "MARGINAL"
                    signature_type = "ROTATION"
                    label = "WEAK ROT"

                markers.append(
                    {
                        "lat": centroid_lat,
                        "lon": centroid_lon,
                        "shear": shear,
                        "delta_v": delta_v,
                        "range_km": range_km,
                        "support_count": support_count,
                        "severity": severity,
                        "signature_type": signature_type,
                        "label": label,
                    }
                )
                break

    return _cluster_markers(markers, cluster_radius_km=5.0)


def _cluster_markers(raw_markers: list[dict], cluster_radius_km: float) -> list[SignatureMarker]:
    if not raw_markers:
        return []

    sorted_markers = sorted(raw_markers, key=lambda marker: marker["shear"], reverse=True)
    assigned = [False] * len(sorted_markers)
    clustered: list[SignatureMarker] = []

    for index, anchor in enumerate(sorted_markers):
        if assigned[index]:
            continue

        assigned[index] = True
        cluster = [anchor]
        for candidate_index, candidate in enumerate(sorted_markers):
            if assigned[candidate_index]:
                continue
            if haversine_km(anchor["lat"], anchor["lon"], candidate["lat"], candidate["lon"]) <= cluster_radius_km:
                cluster.append(candidate)
                assigned[candidate_index] = True

        best = cluster[0]
        severity = best["severity"]
        signature_type = best["signature_type"]
        label = best["label"]
        for marker in cluster:
            if SEVERITY_RANK[marker["severity"]] > SEVERITY_RANK[severity]:
                severity = marker["severity"]
                signature_type = marker["signature_type"]
                label = marker["label"]

        range_weight = max(0.45, min(1.0, 1.0 - max(0.0, best["range_km"] - 70.0) / 220.0))
        support_weight = min(1.0, best.get("support_count", 0) / 2.0)
        confidence = min(1.0, ((best["shear"] / SHEAR_TVS) * 0.70) + (range_weight * 0.15) + (support_weight * 0.15))
        radius_km = max(1.5, best["range_km"] * 0.02)
        description = (
            f"Azimuthal shear {best['shear']:.4f} s^-1 | dV {best['delta_v']:.0f} m/s | "
            f"Range {best['range_km']:.0f} km from radar | support {best.get('support_count', 0)} gates"
        )

        clustered.append(
            SignatureMarker(
                signature_type=signature_type,
                severity=severity,
                lat=best["lat"],
                lon=best["lon"],
                radius_km=radius_km,
                label=label,
                description=description,
                confidence=round(confidence, 3),
                metrics={
                    "shear_per_sec": round(best["shear"], 5),
                    "delta_v_ms": round(best["delta_v"], 1),
                    "range_km": round(best["range_km"], 1),
                    "cluster_size": len(cluster),
                    "support_count": int(best.get("support_count", 0)),
                },
            )
        )

    clustered.sort(key=lambda marker: SEVERITY_RANK[marker.severity], reverse=True)
    return clustered


class RotationAnalyzer(BaseAnalyzer):
    name = "rotation"

    def run(self, frame: ProcessedFrame, context: dict | None = None) -> AnalysisResult:
        if frame.product not in {"VEL", "SRV"}:
            return AnalysisResult(
                analyzer=self.name,
                payload={
                    "status": "skipped",
                    "reason": "VEL or SRV product required for rotation analysis",
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
                    "reason": "No sweep data available",
                    "max_severity": "NONE",
                    "signature_count": 0,
                    "signatures": [],
                },
            )

        try:
            signatures = detect_rotation_couplets(frame.sweep)
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
