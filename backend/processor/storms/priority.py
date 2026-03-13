"""priority.py — Operational priority scoring for tracked storm objects.

Produces a single priority_score in [0, 1] representing how urgently a storm
deserves operational attention.  Higher = more urgent.

Inputs considered
-----------------
- Threat severity and scores (tornado > hail = wind > flood)
- Active event flags (rapid_intensification, rotation_tightening, etc.)
- Track persistence and motion confidence
- Location impact — how many saved locations are threatened, and how soon
- Environmental support
- Recent intensification trend
- Storm mode (supercell_candidate ranked highest)
- Short vs long track (new storms get partial credit; long-track gets a bonus)

The priority_score is NOT an official operational product and should be labeled
as a radar-platform heuristic ranking.  It exists to help the user decide which
storm to examine first, not to replace trained meteorological judgment.
"""
from __future__ import annotations

from typing import Any


def _clamp(v: float) -> float:
    return max(0.0, min(1.0, v))


def _score(value: float | None, low: float, high: float) -> float:
    if value is None:
        return 0.0
    if value <= low:
        return 0.0
    if value >= high:
        return 1.0
    return (value - low) / (high - low)


# Threat severity tiers
_SEVERITY_BASE: dict[str, float] = {
    "TORNADO_EMERGENCY": 1.00,
    "TORNADO":           0.85,
    "SEVERE":            0.55,
    "MARGINAL":          0.25,
    "NONE":              0.05,
}

# Primary-threat modifiers
_THREAT_MODIFIER: dict[str, float] = {
    "tornado": 0.12,
    "hail":    0.04,
    "wind":    0.04,
    "flood":   0.02,
}

# Event-flag priority boosts
_FLAG_BOOSTS: dict[str, float] = {
    "tornado_threat_elevated":  0.12,
    "rotation_tightening":      0.10,
    "rapid_intensification":    0.09,
    "possible_split":           0.06,
    "severe_threat_elevated":   0.07,
    "supercell_candidate":      0.07,
    "possible_hail_surge":      0.05,
    "forward_acceleration":     0.04,
    "long_track":               0.04,
    "elevated_uncertainty":    -0.04,
    "rapid_decay":             -0.06,
    "environment_support_weak":-0.03,
}

# Storm mode modifiers
_MODE_MODIFIER: dict[str, float] = {
    "supercell_candidate": 0.08,
    "bow_segment":         0.05,
    "linear_segment":      0.03,
    "training_rain_producer": 0.03,
    "cluster_multicell":   0.01,
    "discrete_cell":       0.0,
    "unknown":             0.0,
}


def compute_priority_score(
    *,
    severity_level: str,
    primary_threat: str,
    threat_scores: dict[str, float],
    event_flags: list[dict],
    motion_confidence: float,
    lifecycle_state: str,
    storm_mode: str,
    environment_summary: dict | None,
    impacts: list[Any],
    history_length: int,
) -> tuple[float, str]:
    """Return (priority_score, priority_label) for a tracked storm.

    priority_label is one of: CRITICAL, HIGH, MODERATE, LOW, MINIMAL
    """
    # Base from severity
    base = _SEVERITY_BASE.get(severity_level, 0.05)

    # Threat score component — top 2 scores weighted
    sorted_scores = sorted(threat_scores.values(), reverse=True)
    threat_component = _score(sorted_scores[0] if sorted_scores else 0.0, 0.20, 0.80) * 0.25
    if len(sorted_scores) >= 2:
        threat_component += _score(sorted_scores[1], 0.20, 0.65) * 0.05

    # Primary threat modifier
    threat_mod = _THREAT_MODIFIER.get(primary_threat, 0.0)

    # Event flag boosts/penalties (cap total at ±0.25)
    flag_names = {f["flag"] for f in event_flags}
    flag_total = sum(_FLAG_BOOSTS.get(name, 0.0) for name in flag_names)
    flag_total = max(-0.25, min(0.25, flag_total))

    # Motion confidence weight
    motion_weight = 0.03 * motion_confidence

    # Storm mode modifier
    mode_mod = _MODE_MODIFIER.get(storm_mode, 0.0)

    # Track persistence bonus
    if lifecycle_state in ("tracked",) and history_length >= 6:
        persistence_bonus = 0.04
    elif lifecycle_state == "born":
        persistence_bonus = -0.02
    else:
        persistence_bonus = 0.0

    # Location impact component
    # Highest-ranked impact drives this; more impacts compound it
    impact_component = 0.0
    if impacts:
        # Sort by impact_rank if available
        sorted_impacts = sorted(
            impacts, key=lambda i: getattr(i, "impact_rank", 0.0), reverse=True
        )
        top_rank = float(getattr(sorted_impacts[0], "impact_rank", 0.0))
        impact_component = _score(top_rank, 0.10, 0.80) * 0.12
        # Bonus for multiple impacted locations
        impact_component += min(0.04, (len(impacts) - 1) * 0.01)

    # Environment support penalty
    env_penalty = 0.0
    if environment_summary:
        env_conf = float(environment_summary.get("environment_confidence", 0.5))
        env_age = environment_summary.get("environment_freshness_minutes")
        if env_age is not None and float(env_age) > 120:
            env_penalty = -0.03
        if env_conf < 0.30:
            env_penalty = min(env_penalty, -0.02)

    priority = _clamp(
        base
        + threat_component
        + threat_mod
        + flag_total
        + motion_weight
        + mode_mod
        + persistence_bonus
        + impact_component
        + env_penalty
    )

    if priority >= 0.80:
        label = "CRITICAL"
    elif priority >= 0.60:
        label = "HIGH"
    elif priority >= 0.38:
        label = "MODERATE"
    elif priority >= 0.18:
        label = "LOW"
    else:
        label = "MINIMAL"

    return round(priority, 3), label
