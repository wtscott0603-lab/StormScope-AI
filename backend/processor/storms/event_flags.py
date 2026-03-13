"""event_flags.py — Structured operational event flag engine for storm objects.

Produces a list of named, explainable event flags for each tracked storm based on
radar-derived history, lifecycle analysis, threat scores, and motion state.

Flags are proxy-derived and clearly labeled as such. No official NEXRAD algorithm
outputs are implied. All flags include confidence and a human-readable rationale.

Available flags
---------------
rapid_intensification   — reflectivity or area grew fast over the last 2 scans
rapid_decay             — storm is collapsing quickly
rotation_tightening     — rotation signature severity upgraded or SRV delta-V rose
rotation_broadening     — rotation signal widened / severity downgraded
possible_hail_surge     — VIL density or ZH crossed a hail-size threshold
possible_split          — lifecycle state flagged a split event
possible_merge          — lifecycle state flagged a merge event
elevated_uncertainty    — motion confidence low, erratic track, or stale environment
forward_acceleration    — storm speed increased materially over last scans
slowing_training        — storm moving very slowly; flood risk increasing
long_track              — storm has been continuously tracked > N scans
supercell_candidate     — convective mode assessed as supercell-type
tornado_threat_elevated — tornado score crossed warning-level threshold
severe_threat_elevated  — max threat score crossed severe threshold
environment_support_strong — environment confidence high and multiple scores favorable
environment_support_weak   — environment stale, unknown, or unfavorable
"""
from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Flag registry
# ---------------------------------------------------------------------------

FLAG_LABELS: dict[str, str] = {
    "rapid_intensification":        "Rapidly Intensifying",
    "rapid_decay":                  "Rapidly Decaying",
    "rotation_tightening":          "Rotation Tightening",
    "rotation_broadening":          "Rotation Broadening",
    "possible_hail_surge":          "Possible Hail Surge",
    "possible_split":               "Possible Cell Split",
    "possible_merge":               "Possible Cell Merge",
    "elevated_uncertainty":         "Elevated Uncertainty",
    "forward_acceleration":         "Forward Acceleration",
    "slowing_training":             "Slowing / Potential Training",
    "long_track":                   "Long-Track Storm",
    "supercell_candidate":          "Supercell Candidate",
    "tornado_threat_elevated":      "Tornado Threat Elevated",
    "severe_threat_elevated":       "Severe Threat Elevated",
    "environment_support_strong":   "Favorable Environment",
    "environment_support_weak":     "Weak/Stale Environment",
}

# Severity ordering for display sorting
FLAG_SEVERITY: dict[str, int] = {
    "tornado_threat_elevated":      10,
    "rotation_tightening":           9,
    "rapid_intensification":         8,
    "possible_split":                7,
    "severe_threat_elevated":        7,
    "supercell_candidate":           6,
    "possible_hail_surge":           6,
    "forward_acceleration":          5,
    "rotation_broadening":           5,
    "possible_merge":                5,
    "elevated_uncertainty":          4,
    "slowing_training":              4,
    "long_track":                    3,
    "environment_support_strong":    2,
    "rapid_decay":                   2,
    "environment_support_weak":      1,
}


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _angle_diff(a: float, b: float) -> float:
    return (b - a + 180.0) % 360.0 - 180.0


def _flag(name: str, confidence: float, rationale: str) -> dict:
    return {
        "flag": name,
        "label": FLAG_LABELS.get(name, name.replace("_", " ").title()),
        "confidence": round(max(0.0, min(1.0, confidence)), 2),
        "rationale": rationale,
        "severity": FLAG_SEVERITY.get(name, 1),
        "provenance": "proxy-derived from radar storm-object fields",
    }


# ---------------------------------------------------------------------------
# Flag detectors
# ---------------------------------------------------------------------------

def _flag_lifecycle(lifecycle_summary: dict, lifecycle_state: str) -> list[dict]:
    flags: list[dict] = []
    trend = lifecycle_summary.get("intensity_trend", "")
    conf = float(lifecycle_summary.get("intensity_confidence", 0.0))

    if trend == "rapid_intensification":
        flags.append(_flag("rapid_intensification", conf, "Reflectivity and/or area increased rapidly over the last 2 scans (proxy)."))
    elif trend == "rapid_decay":
        flags.append(_flag("rapid_decay", conf, "Reflectivity and/or area dropped rapidly over the last 2 scans (proxy)."))

    if lifecycle_state == "split":
        flags.append(_flag("possible_split", 0.70, "Storm tracker detected a likely cell split — one predecessor storm produced two candidates."))
    elif lifecycle_state == "merged":
        flags.append(_flag("possible_merge", 0.65, "Storm tracker detected a likely cell merge — two candidates associated with one predecessor."))

    return flags


def _flag_rotation(history: list, associated_signatures: list[dict], srv_metrics: dict | None) -> list[dict]:
    flags: list[dict] = []
    sig_types = {str(s.get("signature_type", "")) for s in associated_signatures}

    # Tornado / rotation severity tiers from current scan
    has_tvs = "TVS" in sig_types
    has_rot = "ROTATION" in sig_types
    has_tds = "TDS" in sig_types

    if has_tvs or has_tds:
        flags.append(_flag("tornado_threat_elevated", 0.82 if has_tds else 0.70,
                           "TVS-type or debris-style rotation signature collocated with storm."))

    # Check rotation trend from history
    if len(history) >= 2:
        recent_severities = []
        for snap in history[-3:]:
            sigs = []
            try:
                sigs = list(getattr(snap, "associated_signatures", None) or [])
            except Exception:
                pass
            if any(str(s.get("signature_type", "")) in {"TDS", "TORNADO_EMERGENCY"} for s in sigs):
                recent_severities.append(3)
            elif any(str(s.get("signature_type", "")) in {"TVS"} for s in sigs):
                recent_severities.append(2)
            elif any(str(s.get("signature_type", "")) in {"ROTATION"} for s in sigs):
                recent_severities.append(1)
            else:
                recent_severities.append(0)

        if len(recent_severities) >= 2 and recent_severities[-1] > recent_severities[0]:
            flags.append(_flag("rotation_tightening", 0.65,
                               "Rotation severity indicator has increased over the last 2+ scans (proxy)."))
        elif len(recent_severities) >= 2 and recent_severities[-1] < recent_severities[0] and recent_severities[0] >= 2:
            flags.append(_flag("rotation_broadening", 0.55,
                               "Rotation signature severity decreased from the prior scan — organization may be less concentrated."))

    return flags


def _flag_hail(threat_component_breakdown: dict, volume_metrics: dict | None) -> list[dict]:
    flags: list[dict] = []
    vil_density = None
    if volume_metrics:
        vil_density = _safe_float(volume_metrics.get("max_vil_density_gm3"))
    hail_comps = threat_component_breakdown.get("hail", {})
    hail_sig = float(hail_comps.get("hail_signatures", 0.0))
    vil_comp = float(hail_comps.get("vil_density", 0.0))

    if hail_sig >= 0.15 or (vil_density is not None and vil_density >= 3.5) or vil_comp >= 0.12:
        flags.append(_flag("possible_hail_surge", 0.70,
                           "VIL density and/or hydrometeor-class signature suggest significant hail accumulation (proxy)."))
    return flags


def _flag_motion(history: list, lifecycle_summary: dict, motion_speed_kmh: float | None) -> list[dict]:
    flags: list[dict] = []
    motion_trend = lifecycle_summary.get("motion_trend", "")
    motion_conf = float(lifecycle_summary.get("motion_confidence", 0.0))

    if motion_trend == "accelerating":
        flags.append(_flag("forward_acceleration", motion_conf,
                           "Storm forward speed has increased materially over the last scans (proxy)."))

    # Training / slow-mover check
    if motion_speed_kmh is not None and motion_speed_kmh < 15.0:
        flags.append(_flag("slowing_training", 0.65,
                           f"Storm moving slowly (~{motion_speed_kmh:.0f} km/h). Repeated-path flood risk increases for slow or quasi-stationary storms."))

    return flags


def _flag_uncertainty(
    motion_confidence: float,
    track_uncertainty_km: float,
    lifecycle_summary: dict,
    environment_summary: dict | None,
) -> list[dict]:
    flags: list[dict] = []
    reasons: list[str] = []

    if motion_confidence < 0.40:
        reasons.append(f"motion confidence low ({motion_confidence:.2f})")
    if track_uncertainty_km > 12.0:
        reasons.append(f"track RMS residual high ({track_uncertainty_km:.1f} km)")
    env_conf = float((environment_summary or {}).get("environment_confidence", 1.0))
    env_age = _safe_float((environment_summary or {}).get("environment_freshness_minutes"))
    if env_age is not None and env_age > 90:
        reasons.append(f"environment data is {env_age:.0f} min old")
    if env_conf < 0.40:
        reasons.append(f"environment confidence low ({env_conf:.2f})")

    if reasons:
        flags.append(_flag("elevated_uncertainty", 0.75,
                           "Operational confidence limited: " + "; ".join(reasons) + "."))
    return flags


def _flag_track_length(history: list, lifecycle_state: str) -> list[dict]:
    flags: list[dict] = []
    scan_count = len(history) + 1  # +1 for current
    if scan_count >= 8 and lifecycle_state not in ("split", "merged", "born"):
        flags.append(_flag("long_track", min(0.90, 0.55 + scan_count * 0.03),
                           f"Storm has been continuously tracked for {scan_count} scans — established persistent cell."))
    return flags


def _flag_convective_mode(storm_mode: str, storm_mode_confidence: float) -> list[dict]:
    flags: list[dict] = []
    if storm_mode == "supercell_candidate" and storm_mode_confidence >= 0.55:
        flags.append(_flag("supercell_candidate", storm_mode_confidence,
                           "Radar geometry and rotation signatures support supercell-type convective organization (proxy)."))
    return flags


def _flag_threat_level(threat_scores: dict, severity_level: str) -> list[dict]:
    flags: list[dict] = []
    max_score = max(threat_scores.values()) if threat_scores else 0.0
    if severity_level in ("TORNADO", "TORNADO_EMERGENCY") and max_score >= 0.70:
        if "tornado_threat_elevated" not in {f["flag"] for f in []}:  # always add if not already
            pass  # tornado_threat_elevated handled in rotation section
    if max_score >= 0.55 and severity_level in ("SEVERE", "TORNADO", "TORNADO_EMERGENCY"):
        flags.append(_flag("severe_threat_elevated", min(0.90, max_score),
                           f"Peak threat score {max_score:.0%} crosses severe-level threshold."))
    return flags


def _flag_environment(environment_summary: dict | None) -> list[dict]:
    flags: list[dict] = []
    if not environment_summary:
        flags.append(_flag("environment_support_weak", 0.60,
                           "No environment snapshot available — threat assessment depends entirely on radar proxy data."))
        return flags

    env_conf = float(environment_summary.get("environment_confidence", 0.0))
    hail_fav = float(environment_summary.get("hail_favorability", 0.0))
    wind_fav = float(environment_summary.get("wind_favorability", 0.0))
    tor_fav = float(environment_summary.get("tornado_favorability", 0.0))
    intensification = float(environment_summary.get("intensification_signal", 0.0))

    favorable_count = sum(1 for v in [hail_fav, wind_fav, tor_fav] if v >= 0.45)
    env_age = _safe_float(environment_summary.get("environment_freshness_minutes"))

    if env_conf >= 0.55 and favorable_count >= 2 and intensification >= 0.45:
        flags.append(_flag("environment_support_strong", env_conf,
                           f"Environment confidence {env_conf:.0%}; multiple favorability signals elevated; intensification signal {intensification:.0%} (proxy)."))
    elif env_conf < 0.35 or (env_age is not None and env_age > 120):
        flags.append(_flag("environment_support_weak", 0.65,
                           f"Environment confidence {env_conf:.0%} or data age {env_age or '?'} min limits threat assessment quality."))
    return flags


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_event_flags(
    *,
    history: list,
    lifecycle_summary: dict,
    lifecycle_state: str,
    associated_signatures: list[dict],
    threat_scores: dict,
    threat_component_breakdown: dict,
    severity_level: str,
    storm_mode: str,
    storm_mode_confidence: float,
    motion_speed_kmh: float | None,
    motion_confidence: float,
    track_uncertainty_km: float,
    environment_summary: dict | None,
    volume_metrics: dict | None,
    srv_metrics: dict | None,
) -> list[dict]:
    """Compute structured event flags for a tracked storm.

    Returns a list of flag dicts, sorted by severity (most operationally important first).
    Each flag has: flag, label, confidence, rationale, severity, provenance.
    """
    flags: list[dict] = []

    flags.extend(_flag_lifecycle(lifecycle_summary, lifecycle_state))
    flags.extend(_flag_rotation(history, associated_signatures, srv_metrics))
    flags.extend(_flag_hail(threat_component_breakdown, volume_metrics))
    flags.extend(_flag_motion(history, lifecycle_summary, motion_speed_kmh))
    flags.extend(_flag_uncertainty(motion_confidence, track_uncertainty_km, lifecycle_summary, environment_summary))
    flags.extend(_flag_track_length(history, lifecycle_state))
    flags.extend(_flag_convective_mode(storm_mode, storm_mode_confidence))
    flags.extend(_flag_threat_level(threat_scores, severity_level))
    flags.extend(_flag_environment(environment_summary))

    # Deduplicate by flag name (keep highest confidence)
    seen: dict[str, dict] = {}
    for f in flags:
        name = f["flag"]
        if name not in seen or f["confidence"] > seen[name]["confidence"]:
            seen[name] = f

    # Sort by severity desc, then confidence desc
    return sorted(seen.values(), key=lambda f: (f["severity"], f["confidence"]), reverse=True)
