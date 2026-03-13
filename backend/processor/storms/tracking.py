from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import math
import uuid

import numpy as np

from backend.processor.analysis.utils import haversine_km
from backend.processor.storms.geometry import bearing_deg, destination_point, motion_vector, polygon_overlap_ratio
from backend.processor.storms.types import StormDetection
from backend.shared.models import StormObjectRecord


def _make_storm_id(site: str, scan_time: datetime) -> str:
    return f"{site.upper()}-{scan_time:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


# ---------------------------------------------------------------------------
# Multi-frame motion estimation
# ---------------------------------------------------------------------------

def estimate_motion_from_history(
    current_lat: float,
    current_lon: float,
    current_time: datetime,
    snapshots: list,
    *,
    max_snapshots: int = 5,
) -> tuple[float | None, float | None, float]:
    """Estimate storm motion using a weighted linear regression over recent centroids.

    Returns (heading_deg, speed_kmh, track_uncertainty_km) where track_uncertainty_km
    is the RMS residual of the fit — a measure of track wobble / direction stability.
    Returns (None, None, 0.0) if there is insufficient history.

    Uses exponential recency weighting (newest points weighted most heavily) to avoid
    giving equal weight to a 3-scan-old observation as to the most recent frame.
    """
    if not snapshots:
        return None, None, 0.0

    recent = list(snapshots[-max_snapshots:])
    # Build time-ordered (oldest → newest) list of (dt_seconds, lat, lon)
    points: list[tuple[float, float, float]] = []
    for snap in recent:
        scan_t = getattr(snap, "scan_time", None)
        if scan_t is None:
            continue
        dt = (current_time - scan_t).total_seconds()
        if dt <= 0:
            continue
        points.append((dt, float(getattr(snap, "centroid_lat", 0.0)), float(getattr(snap, "centroid_lon", 0.0))))

    # Add the current position at dt=0
    points.append((0.0, current_lat, current_lon))
    points.sort(key=lambda p: p[0], reverse=True)  # oldest first by descending dt

    if len(points) < 2:
        return None, None, 0.0

    # Convert to local East-North offsets from the current position in km.
    # This avoids lat/lon distortion for regression.
    cos_lat = math.cos(math.radians(current_lat))
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * cos_lat

    ts = np.array([p[0] for p in points], dtype=np.float64)
    east_km = np.array([(p[2] - current_lon) * km_per_deg_lon for p in points], dtype=np.float64)
    north_km = np.array([(p[1] - current_lat) * km_per_deg_lat for p in points], dtype=np.float64)

    # Exponential weights: weight_i = exp(-0.4 * rank) where rank 0 = current position.
    # Rank increases with age so recent points dominate.
    n = len(ts)
    ranks = np.arange(n - 1, -1, -1, dtype=np.float64)  # current point = rank 0
    weights = np.exp(-0.4 * ranks)

    # Weighted linear regression: position = a + b * t
    # t is negative dt (oldest point has most negative t, current = 0)
    t_arr = -ts  # flip sign so current=0 is the "rightmost" point

    W = np.diag(weights)
    A = np.column_stack([np.ones(n), t_arr])

    try:
        AtW = A.T @ W
        coeffs = np.linalg.lstsq(AtW @ A, AtW @ np.column_stack([east_km, north_km]), rcond=None)[0]
    except np.linalg.LinAlgError:
        return None, None, 0.0

    # coeffs[1] = [d(east)/dt, d(north)/dt] in km/s
    east_rate = float(coeffs[1, 0])   # km/s
    north_rate = float(coeffs[1, 1])  # km/s
    speed_kmh = math.hypot(east_rate, north_rate) * 3600.0

    if speed_kmh < 0.5:
        return None, 0.0, 0.0

    # Convert to meteorological bearing (degrees clockwise from North)
    heading_deg = (math.degrees(math.atan2(east_rate, north_rate)) + 360.0) % 360.0

    # Compute RMS residual as track uncertainty in km
    predicted_east = coeffs[0, 0] + coeffs[1, 0] * t_arr
    predicted_north = coeffs[0, 1] + coeffs[1, 1] * t_arr
    residuals = np.sqrt((east_km - predicted_east) ** 2 + (north_km - predicted_north) ** 2)
    rms_km = float(np.sqrt(np.average(residuals ** 2, weights=weights)))

    return heading_deg, speed_kmh, rms_km


def compute_uncertainty_cone(
    centroid_lat: float,
    centroid_lon: float,
    heading_deg: float | None,
    speed_kmh: float | None,
    *,
    track_uncertainty_km: float = 5.0,
    motion_confidence: float = 0.5,
    horizon_minutes: int = 60,
    step_minutes: int = 10,
    destination_point_func,
) -> list[dict]:
    """Generate an uncertainty cone along the forecast track.

    Each step returns a center point plus left/right edge points that define
    a widening corridor. The corridor width grows linearly from 0 at the storm
    position to a maximum that scales with track_uncertainty_km and the inverse
    of motion_confidence.

    Returns a list of dicts suitable for JSON serialisation and map rendering.
    Returns [] when motion is unknown or too slow to be meaningful.
    """
    if heading_deg is None or speed_kmh is None or speed_kmh < 4.0:
        return []

    # Lateral spread: base on track RMS residual and motion confidence penalty.
    # Anchored at 0 width at t=0, grows linearly to max at t=horizon.
    uncertainty_scale = max(1.0, track_uncertainty_km) * max(1.0, 2.0 - motion_confidence * 2.0)
    max_half_width_km = min(40.0, uncertainty_scale * (horizon_minutes / 20.0))

    # Left/right perpendicular headings
    left_heading = (heading_deg - 90.0) % 360.0
    right_heading = (heading_deg + 90.0) % 360.0

    steps: list[dict] = []
    for eta in range(0, horizon_minutes + step_minutes, step_minutes):
        fraction = eta / max(horizon_minutes, 1)
        half_width_km = max_half_width_km * fraction
        distance_km = speed_kmh * (eta / 60.0)
        center_lat, center_lon = destination_point_func(centroid_lat, centroid_lon, heading_deg, distance_km)
        left_lat, left_lon = destination_point_func(center_lat, center_lon, left_heading, half_width_km)
        right_lat, right_lon = destination_point_func(center_lat, center_lon, right_heading, half_width_km)
        steps.append({
            "eta_minutes": eta,
            "center": {"lat": round(center_lat, 4), "lon": round(center_lon, 4)},
            "left": {"lat": round(left_lat, 4), "lon": round(left_lon, 4)},
            "right": {"lat": round(right_lat, 4), "lon": round(right_lon, 4)},
            "half_width_km": round(half_width_km, 1),
        })
    return steps


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _blend_heading(previous_heading: float | None, new_heading: float | None, previous_weight: float = 0.4) -> float | None:
    if previous_heading is None:
        return new_heading
    if new_heading is None:
        return previous_heading
    previous_rad = math.radians(previous_heading)
    new_rad = math.radians(new_heading)
    x = (math.cos(previous_rad) * previous_weight) + (math.cos(new_rad) * (1.0 - previous_weight))
    y = (math.sin(previous_rad) * previous_weight) + (math.sin(new_rad) * (1.0 - previous_weight))
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def _blend_speed(previous_speed: float | None, new_speed: float | None, previous_weight: float = 0.35) -> float | None:
    if previous_speed is None:
        return new_speed
    if new_speed is None:
        return previous_speed
    return (previous_speed * previous_weight) + (new_speed * (1.0 - previous_weight))


def _match_score(detection: StormDetection, previous: StormObjectRecord, delta_seconds: float) -> float:
    """Multi-factor association score between a new detection and a previous storm object.

    Factors (weights sum to 1.0):
      - footprint overlap      0.28  spatial continuity — most reliable
      - projected position     0.24  extrapolated centroid match
      - raw distance           0.16  fallback spatial proximity
      - motion continuity      0.12  heading + speed consistency
      - intensity similarity   0.10  reflectivity change penalty
      - size similarity        0.10  area ratio penalty (new vs v13)

    Returns a score in [0, 1].  Scores < 0.18 are discarded before calling.
    """
    distance = haversine_km(detection.centroid_lat, detection.centroid_lon, previous.centroid_lat, previous.centroid_lon)
    overlap = polygon_overlap_ratio(detection.footprint_geojson, previous.footprint_geojson)
    intensity_delta = abs(detection.max_reflectivity - previous.max_reflectivity)

    # Size similarity — penalise large area changes relative to the smaller of the two
    # areas.  This helps distinguish splits (new object is much smaller) from continuations.
    smaller_area = min(detection.area_km2, previous.area_km2)
    larger_area = max(detection.area_km2, previous.area_km2)
    area_ratio = smaller_area / max(larger_area, 1.0)  # 1.0 = identical size
    size_score = area_ratio  # already in [0, 1]

    distance_score = max(0.0, 1.0 - (distance / 80.0))
    intensity_score = max(0.0, 1.0 - (intensity_delta / 25.0))

    projected_score = 0.0
    motion_continuity = 0.0
    if previous.motion_heading_deg is not None and previous.motion_speed_kmh is not None and delta_seconds > 0:
        projected_distance_km = previous.motion_speed_kmh * (delta_seconds / 3600.0)
        projected_lat, projected_lon = destination_point(
            previous.centroid_lat,
            previous.centroid_lon,
            previous.motion_heading_deg,
            projected_distance_km,
        )
        predicted_offset = haversine_km(detection.centroid_lat, detection.centroid_lon, projected_lat, projected_lon)
        # Tighter tolerance: scale by how well we trust the previous motion vector.
        # Storms with good prior tracking get a tighter 35 km window; new storms use 45 km.
        motion_trust = min(1.0, max(0.3, getattr(previous, "confidence", 0.5)))
        offset_tolerance = 25.0 + (1.0 - motion_trust) * 20.0
        projected_score = max(0.0, 1.0 - (predicted_offset / offset_tolerance))
        new_heading, new_speed = motion_vector(
            previous.centroid_lat,
            previous.centroid_lon,
            detection.centroid_lat,
            detection.centroid_lon,
            delta_seconds,
        )
        if new_heading is not None and new_speed is not None:
            heading_delta = abs(((new_heading - previous.motion_heading_deg + 180.0) % 360.0) - 180.0)
            speed_delta = abs(new_speed - previous.motion_speed_kmh)
            # Tighter heading tolerance for fast-moving storms (less expected wobble)
            heading_tol = 65.0 if (previous.motion_speed_kmh or 0) < 25.0 else 45.0
            speed_tol = 55.0
            motion_continuity = max(0.0, 1.0 - (heading_delta / heading_tol)) * max(0.0, 1.0 - (speed_delta / speed_tol))

    return (
        (overlap * 0.28)
        + (distance_score * 0.16)
        + (projected_score * 0.24)
        + (motion_continuity * 0.12)
        + (intensity_score * 0.10)
        + (size_score * 0.10)
    )


def _motion_confidence(
    *,
    previous: StormObjectRecord | None,
    detection: StormDetection,
    match_score: float,
    current_candidate_count: int,
    previous_candidate_count: int,
    delta_seconds: float | None,
) -> float:
    confidence = 0.18 if previous is None else 0.32
    confidence += match_score * 0.35
    confidence += min(0.12, max(detection.core_fraction, 0.0) * 0.24)
    if previous is not None:
        overlap = polygon_overlap_ratio(detection.footprint_geojson, previous.footprint_geojson)
        confidence += overlap * 0.10
        confidence += min(0.08, max(previous.confidence, 0.0) * 0.08)
    if delta_seconds is not None:
        confidence += max(0.0, 0.08 - abs(delta_seconds - 600.0) / 9000.0)
    ambiguity_penalty = max(0, current_candidate_count - 1) * 0.08 + max(0, previous_candidate_count - 1) * 0.05
    return round(_clamp(confidence - ambiguity_penalty), 2)


def match_storms(
    site: str,
    scan_time: datetime,
    detections: list[StormDetection],
    previous_storms: list[StormObjectRecord],
) -> list[dict]:
    candidate_pairs: list[tuple[float, int, int]] = []
    candidate_scores: dict[tuple[int, int], float] = {}
    current_candidate_counts: dict[int, int] = defaultdict(int)
    previous_candidate_counts: dict[int, int] = defaultdict(int)

    for detection_index, detection in enumerate(detections):
        for previous_index, previous in enumerate(previous_storms):
            delta_seconds = (scan_time - previous.latest_scan_time).total_seconds()
            if delta_seconds <= 0:
                continue
            distance = haversine_km(
                detection.centroid_lat,
                detection.centroid_lon,
                previous.centroid_lat,
                previous.centroid_lon,
            )
            max_distance = 90.0
            if previous.motion_speed_kmh:
                projected_distance = previous.motion_speed_kmh * (delta_seconds / 3600.0)
                max_distance = max(65.0, projected_distance * 2.2 + 25.0)
            if distance > max_distance:
                continue

            score = _match_score(detection, previous, delta_seconds)
            if score < 0.18:
                continue

            candidate_pairs.append((score, detection_index, previous_index))
            candidate_scores[(detection_index, previous_index)] = score
            current_candidate_counts[detection_index] += 1
            previous_candidate_counts[previous_index] += 1

    candidate_pairs.sort(reverse=True)
    assigned_current: set[int] = set()
    assigned_previous: set[int] = set()
    matched_previous_for_current: dict[int, int] = {}

    for _, detection_index, previous_index in candidate_pairs:
        if detection_index in assigned_current or previous_index in assigned_previous:
            continue
        assigned_current.add(detection_index)
        assigned_previous.add(previous_index)
        matched_previous_for_current[detection_index] = previous_index

    assignments: list[dict] = []
    for detection_index, detection in enumerate(detections):
        previous = None
        storm_id = None
        lifecycle_state = "born"
        status = "active"
        match_score = 0.0
        motion_confidence = 0.18
        if detection_index in matched_previous_for_current:
            previous_index = matched_previous_for_current[detection_index]
            previous = previous_storms[previous_index]
            storm_id = previous.storm_id
            match_score = candidate_scores.get((detection_index, previous_index), 0.0)
            if current_candidate_counts[detection_index] > 1:
                lifecycle_state = "merged"
            elif previous_candidate_counts[previous_index] > 1:
                lifecycle_state = "split"
            else:
                lifecycle_state = "tracked"
        else:
            storm_id = _make_storm_id(site, scan_time)

        heading = None
        speed = None
        delta_seconds = None
        if previous is not None:
            delta_seconds = (scan_time - previous.latest_scan_time).total_seconds()
            heading, speed = motion_vector(
                previous.centroid_lat,
                previous.centroid_lon,
                detection.centroid_lat,
                detection.centroid_lon,
                delta_seconds,
            )
            if heading is None:
                heading = previous.motion_heading_deg
            if speed is None:
                speed = previous.motion_speed_kmh
            heading = _blend_heading(previous.motion_heading_deg, heading)
            speed = _blend_speed(previous.motion_speed_kmh, speed)
        motion_confidence = _motion_confidence(
            previous=previous,
            detection=detection,
            match_score=match_score,
            current_candidate_count=current_candidate_counts.get(detection_index, 1),
            previous_candidate_count=previous_candidate_counts.get(matched_previous_for_current.get(detection_index, -1), 1),
            delta_seconds=delta_seconds,
        )

        assignments.append(
            {
                "storm_id": storm_id,
                "status": status,
                "lifecycle_state": lifecycle_state,
                "detection": detection,
                "previous": previous,
                "motion_heading_deg": heading,
                "motion_speed_kmh": speed,
                "match_score": round(match_score, 3),
                "motion_confidence": motion_confidence,
            }
        )

    for previous_index, previous in enumerate(previous_storms):
        if previous_index not in assigned_previous:
            assignments.append(
                {
                    "storm_id": previous.storm_id,
                    "status": "inactive",
                    "lifecycle_state": "dissipating",
                    "detection": None,
                    "previous": previous,
                    "motion_heading_deg": previous.motion_heading_deg,
                    "motion_speed_kmh": previous.motion_speed_kmh,
                    "match_score": 0.0,
                    "motion_confidence": max(0.0, round(previous.confidence * 0.5, 2)),
                }
            )

    return assignments
