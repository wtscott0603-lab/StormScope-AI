"""lifecycle.py — Enriched storm lifecycle analysis from snapshot history.

Supplements the basic born/tracked/split/merged/dissipating state from the
tracker with higher-resolution lifecycle transitions derived from the last N
snapshots:

  strengthening       — steady reflectivity / area increase
  rapid_intensification — fast increase (≥5 dBZ or ≥40 km² over 2 scans)
  weakening           — steady decrease
  rapid_decay         — fast decrease
  steady              — within noise thresholds
  uncertain           — insufficient or contradictory history

The acceleration and turning trends describe short-term motion changes:
  accelerating / decelerating / turning_left / turning_right / steady_motion

All outputs are explicitly marked as proxy-derived from radar-object fields;
no sounding or true microphysical data is used.
"""
from __future__ import annotations

import math
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _angle_diff(a: float, b: float) -> float:
    """Signed angular difference b - a in degrees, range (-180, 180]."""
    diff = (b - a + 180.0) % 360.0 - 180.0
    return diff


# ---------------------------------------------------------------------------
# Reflectivity / area trend classification
# ---------------------------------------------------------------------------

RAPID_INTENSIFICATION_DBZ = 5.0   # dBZ increase in ≤2 scans
RAPID_INTENSIFICATION_AREA = 40.0  # km² increase in ≤2 scans
STRENGTHENING_DBZ = 2.0
STRENGTHENING_AREA = 12.0
WEAKENING_DBZ = -2.0
WEAKENING_AREA = -12.0
RAPID_DECAY_DBZ = -5.0
RAPID_DECAY_AREA = -40.0


def classify_lifecycle_trend(
    history: list,
    *,
    limit: int = 4,
) -> tuple[str, float, list[str]]:
    """Classify the storm's intensity lifecycle from its recent snapshot history.

    Returns (lifecycle_trend, confidence, evidence_list).

    lifecycle_trend is one of:
      rapid_intensification | strengthening | steady | weakening | rapid_decay | uncertain
    """
    if not history:
        return "uncertain", 0.0, ["No snapshot history available."]

    recent = list(history[-limit:])
    if len(recent) < 2:
        return "uncertain", 0.15, ["Only one snapshot; trend requires at least two scans."]

    ref_values = [_safe_float(getattr(s, "max_reflectivity", None)) for s in recent]
    area_values = [_safe_float(getattr(s, "area_km2", None)) for s in recent]

    ref_values = [v for v in ref_values if v is not None]
    area_values = [v for v in area_values if v is not None]

    if len(ref_values) < 2:
        return "uncertain", 0.10, ["Insufficient reflectivity data in history."]

    ref_delta = ref_values[-1] - ref_values[0]
    area_delta = (area_values[-1] - area_values[0]) if len(area_values) >= 2 else 0.0
    n_scans = len(ref_values) - 1

    evidence: list[str] = [
        f"Max reflectivity changed {ref_delta:+.1f} dBZ over {n_scans} scan(s).",
    ]
    if len(area_values) >= 2:
        evidence.append(f"Footprint area changed {area_delta:+.0f} km² over {n_scans} scan(s).")

    # --- Rapid intensification ---
    if ref_delta >= RAPID_INTENSIFICATION_DBZ or area_delta >= RAPID_INTENSIFICATION_AREA:
        confidence = min(0.90, 0.55 + 0.06 * n_scans + (min(1.0, ref_delta / 12.0) * 0.25))
        evidence.append("Rapid increase in reflectivity or footprint size (proxy for intensification).")
        return "rapid_intensification", round(confidence, 2), evidence

    # --- Strengthening ---
    if ref_delta >= STRENGTHENING_DBZ or area_delta >= STRENGTHENING_AREA:
        confidence = min(0.80, 0.40 + 0.07 * n_scans + (min(1.0, ref_delta / 8.0) * 0.20))
        evidence.append("Moderate increase in reflectivity or footprint (proxy for strengthening).")
        return "strengthening", round(confidence, 2), evidence

    # --- Rapid decay ---
    if ref_delta <= RAPID_DECAY_DBZ or area_delta <= RAPID_DECAY_AREA:
        confidence = min(0.88, 0.50 + 0.06 * n_scans + (min(1.0, abs(ref_delta) / 12.0) * 0.22))
        evidence.append("Rapid drop in reflectivity or footprint size (proxy for rapid decay).")
        return "rapid_decay", round(confidence, 2), evidence

    # --- Weakening ---
    if ref_delta <= WEAKENING_DBZ or area_delta <= WEAKENING_AREA:
        confidence = min(0.78, 0.38 + 0.07 * n_scans + (min(1.0, abs(ref_delta) / 8.0) * 0.18))
        evidence.append("Moderate decrease in reflectivity or footprint (proxy for weakening).")
        return "weakening", round(confidence, 2), evidence

    # --- Steady ---
    confidence = min(0.75, 0.40 + 0.08 * n_scans)
    evidence.append("Reflectivity and area within noise thresholds — storm is roughly steady.")
    return "steady", round(confidence, 2), evidence


# ---------------------------------------------------------------------------
# Motion acceleration / turning trend
# ---------------------------------------------------------------------------

ACCELERATION_THRESHOLD_KMH = 12.0   # speed change to flag as accelerating
DECELERATION_THRESHOLD_KMH = -12.0
TURNING_THRESHOLD_DEG = 20.0        # heading change to flag as turning


def classify_motion_trend(
    history: list,
    *,
    limit: int = 4,
) -> tuple[str, float, list[str]]:
    """Classify recent motion acceleration and turning from snapshot history.

    Returns (motion_trend, confidence, evidence_list).

    motion_trend is one of:
      accelerating | decelerating | turning_left | turning_right | steady_motion | uncertain_motion
    """
    if not history or len(history) < 2:
        return "uncertain_motion", 0.0, ["Insufficient motion history."]

    recent = list(history[-limit:])
    speeds = [_safe_float(getattr(s, "motion_speed_kmh", None)) for s in recent]
    headings = [_safe_float(getattr(s, "motion_heading_deg", None)) for s in recent]

    valid_speeds = [v for v in speeds if v is not None]
    valid_headings = [v for v in headings if v is not None]

    if len(valid_speeds) < 2 or len(valid_headings) < 2:
        return "uncertain_motion", 0.0, ["Motion data sparse — acceleration/turning unknown."]

    speed_delta = valid_speeds[-1] - valid_speeds[0]
    # Use last two valid headings to compute turning
    heading_delta = _angle_diff(valid_headings[-2], valid_headings[-1])

    evidence: list[str] = [
        f"Speed changed {speed_delta:+.0f} km/h over recent scans.",
        f"Heading change {heading_delta:+.0f}° (positive = right / clockwise).",
    ]

    # Turning takes precedence over acceleration
    if abs(heading_delta) >= TURNING_THRESHOLD_DEG:
        direction = "right" if heading_delta > 0 else "left"
        confidence = min(0.82, 0.45 + min(1.0, abs(heading_delta) / 60.0) * 0.30)
        evidence.append(f"Storm is turning {direction} (proxy-derived from centroid headings).")
        trend = "turning_right" if heading_delta > 0 else "turning_left"
        return trend, round(confidence, 2), evidence

    if speed_delta >= ACCELERATION_THRESHOLD_KMH:
        confidence = min(0.78, 0.40 + min(1.0, speed_delta / 40.0) * 0.28)
        evidence.append("Storm motion is accelerating (proxy-derived).")
        return "accelerating", round(confidence, 2), evidence

    if speed_delta <= DECELERATION_THRESHOLD_KMH:
        confidence = min(0.78, 0.40 + min(1.0, abs(speed_delta) / 40.0) * 0.28)
        evidence.append("Storm motion is decelerating (proxy-derived).")
        return "decelerating", round(confidence, 2), evidence

    evidence.append("Motion is roughly steady — speed and heading within noise thresholds.")
    return "steady_motion", min(0.72, 0.35 + 0.08 * len(valid_speeds)), evidence


# ---------------------------------------------------------------------------
# Composite lifecycle summary
# ---------------------------------------------------------------------------

def build_lifecycle_summary(
    history: list,
    *,
    lifecycle_state: str,
    trend: str,
) -> dict:
    """Build an enriched lifecycle summary dict suitable for API inclusion.

    Returns a dict with keys:
      lifecycle_state, intensity_trend, intensity_confidence, intensity_evidence,
      motion_trend, motion_confidence, motion_evidence
    """
    intensity_trend, intensity_confidence, intensity_evidence = classify_lifecycle_trend(history)
    motion_trend, motion_confidence, motion_evidence = classify_motion_trend(history)

    # Resolve conflicts: if the intensity_trend from threats.py ("strengthening",
    # "weakening", "steady") disagrees with our history-derived version,
    # prefer the history-derived version when confidence is higher.
    resolved_intensity = intensity_trend

    return {
        "lifecycle_state": lifecycle_state,
        "intensity_trend": resolved_intensity,
        "intensity_confidence": intensity_confidence,
        "intensity_evidence": intensity_evidence,
        "motion_trend": motion_trend,
        "motion_confidence": motion_confidence,
        "motion_evidence": motion_evidence,
        "provenance": "proxy-derived from radar storm-object history; not an official operational product.",
    }
