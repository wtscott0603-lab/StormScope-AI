from __future__ import annotations

import math

import numpy as np

from backend.processor.analysis.base import AnalysisResult, BaseAnalyzer, ProcessedFrame, SEVERITY_RANK, SignatureMarker
from backend.processor.analysis.utils import finite_latlon, label_connected, marker_to_dict


BOW_REF_THRESHOLD = 45.0
BWER_REF_THRESHOLD = 30.0
BWER_SURROUND_DBZ = 50.0
WIND_VEL_THRESHOLD = 25.0


def detect_bow_echo(ref_sweep, vel_sweep=None) -> list[SignatureMarker]:
    ref_values = ref_sweep.values
    latitudes = ref_sweep.latitudes
    longitudes = ref_sweep.longitudes

    convective_mask = np.isfinite(ref_values) & (ref_values >= BOW_REF_THRESHOLD)
    markers: list[SignatureMarker] = []
    if not convective_mask.any():
        return markers

    labeled, region_count = label_connected(convective_mask)
    for region_id in range(1, region_count + 1):
        region_mask = labeled == region_id
        gate_count = int(region_mask.sum())
        if gate_count < 20:
            continue

        region_ref = ref_values[region_mask]
        region_lat = latitudes[region_mask]
        region_lon = longitudes[region_mask]
        centroid_lat = float(np.nanmean(region_lat))
        centroid_lon = float(np.nanmean(region_lon))
        if not finite_latlon(centroid_lat, centroid_lon):
            continue

        max_dbz = float(np.nanmax(region_ref))
        lat_span = float(np.nanmax(region_lat) - np.nanmin(region_lat))
        lon_span = float(np.nanmax(region_lon) - np.nanmin(region_lon))
        lat_km = lat_span * 111.0
        lon_km = lon_span * 111.0 * math.cos(math.radians(centroid_lat))
        major_axis_km = max(lat_km, lon_km)
        minor_axis_km = min(lat_km, lon_km)
        if major_axis_km < 10.0 or minor_axis_km < 2.0:
            continue

        aspect_ratio = major_axis_km / max(minor_axis_km, 0.1)
        if aspect_ratio < 2.5:
            continue

        velocity_confirmed = False
        max_outbound = 0.0
        if vel_sweep is not None and vel_sweep.values.shape == ref_values.shape:
            velocity_values = vel_sweep.values
            forward_mask = region_mask & np.isfinite(velocity_values) & (velocity_values >= WIND_VEL_THRESHOLD)
            if int(forward_mask.sum()) >= 3:
                velocity_confirmed = True
                max_outbound = float(np.nanmax(velocity_values[forward_mask]))

        if velocity_confirmed and max_outbound >= 35.0:
            severity = "SEVERE"
            label = "BOW/WIND"
            confidence = 0.80
        elif velocity_confirmed:
            severity = "SEVERE"
            label = "BOW ECHO"
            confidence = 0.70
        elif aspect_ratio >= 4.0 and max_dbz >= 55.0:
            severity = "SEVERE"
            label = "BOW ECHO"
            confidence = 0.60
        else:
            severity = "MARGINAL"
            label = "CONV LINE"
            confidence = 0.40

        description = (
            f"Convective line | Major axis {major_axis_km:.0f} km | "
            f"Aspect {aspect_ratio:.1f}:1 | Max {max_dbz:.0f} dBZ"
            + (f" | Outbound {max_outbound:.0f} m/s" if velocity_confirmed else "")
        )

        markers.append(
            SignatureMarker(
                signature_type="BOW_ECHO",
                severity=severity,
                lat=centroid_lat,
                lon=centroid_lon,
                radius_km=major_axis_km / 2,
                label=label,
                description=description,
                confidence=round(confidence, 2),
                metrics={
                    "aspect_ratio": round(aspect_ratio, 1),
                    "major_axis_km": round(major_axis_km, 1),
                    "max_dbz": round(max_dbz, 1),
                    "gate_count": gate_count,
                    "vel_confirmed": velocity_confirmed,
                    "max_outbound_ms": round(max_outbound, 1),
                },
            )
        )

    markers.extend(detect_bwer(ref_values, latitudes, longitudes))
    markers.sort(key=lambda marker: SEVERITY_RANK[marker.severity], reverse=True)
    return markers


def detect_bwer(ref_values, latitudes, longitudes) -> list[SignatureMarker]:
    bwer_gates: list[dict[str, float]] = []
    n_rays, n_gates = ref_values.shape

    for ray_index in range(1, n_rays - 1):
        for gate_index in range(1, n_gates - 1):
            value = ref_values[ray_index, gate_index]
            if not np.isfinite(value) or value >= BWER_REF_THRESHOLD:
                continue

            neighbors = [
                ref_values[ray_index - 1, gate_index],
                ref_values[ray_index + 1, gate_index],
                ref_values[ray_index, gate_index - 1],
                ref_values[ray_index, gate_index + 1],
            ]
            if all(np.isfinite(neighbor) and neighbor >= BWER_SURROUND_DBZ for neighbor in neighbors):
                lat = float(latitudes[ray_index, gate_index])
                lon = float(longitudes[ray_index, gate_index])
                if finite_latlon(lat, lon):
                    bwer_gates.append({"lat": lat, "lon": lon})

    if not bwer_gates:
        return []

    centroid_lat = sum(gate["lat"] for gate in bwer_gates) / len(bwer_gates)
    centroid_lon = sum(gate["lon"] for gate in bwer_gates) / len(bwer_gates)
    if not finite_latlon(centroid_lat, centroid_lon):
        return []

    return [
        SignatureMarker(
            signature_type="BWER",
            severity="SEVERE",
            lat=centroid_lat,
            lon=centroid_lon,
            radius_km=max(1.0, len(bwer_gates) * 0.3),
            label="BWER",
            description=f"Bounded Weak Echo Region - strong updraft indicator | {len(bwer_gates)} gates",
            confidence=0.65,
            metrics={"gate_count": len(bwer_gates)},
        )
    ]


class WindAnalyzer(BaseAnalyzer):
    """Bow echo, straight-line wind, and BWER detection from reflectivity."""

    name = "wind"

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

        try:
            context_data = context or {}
            signatures = detect_bow_echo(frame.sweep, context_data.get("vel_sweep"))
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
