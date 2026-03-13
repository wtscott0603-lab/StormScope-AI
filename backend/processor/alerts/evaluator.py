"""
Server-side alert evaluator.

Runs after each storm update cycle. For each tracked storm, evaluates a set
of threat thresholds and persists triggered alerts to the DB. Deduplication
is keyed on (storm_id, alert_kind, scan_time) so one alert per storm per
scan per condition fires at most once, even across processor restarts.

Alert kinds:
  tornado_emergency   — tornado_risk ≥ 0.92
  tornado_warning     — tornado_risk ≥ 0.72
  severe_storm        — max_risk ≥ 0.55 (non-tornado primary)
  marginal_storm      — max_risk ≥ 0.30
  tvs_detected        — TVS or TDS signature associated
  location_imminent   — storm within ETA ≤ 20 min of a saved location
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from typing import Any

from backend.processor.storms.types import TrackedStorm
from backend.shared.time import isoformat_utc

LOGGER = logging.getLogger(__name__)


def _alert_id(storm_id: str, alert_kind: str, scan_time: datetime) -> str:
    raw = f"{storm_id}:{alert_kind}:{scan_time:%Y%m%dT%H%M}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _has_signature(storm: TrackedStorm, *kinds: str) -> bool:
    sig_types = {str(s.get("signature_type")) for s in storm.associated_signatures}
    return bool(sig_types & set(kinds))


def _primary_threat_label(storm: TrackedStorm) -> str:
    labels = {
        "tornado": "Tornado",
        "hail": "Hail",
        "wind": "Wind",
        "flood": "Flood",
        "none": "Storm",
    }
    return labels.get(storm.primary_threat, "Storm")


def evaluate_storm_alerts(storm: TrackedStorm, scan_time: datetime) -> list[dict[str, Any]]:
    """
    Return a list of alert payloads for a single storm.
    Caller is responsible for deduplication via INSERT OR IGNORE on alert_id.
    """
    alerts: list[dict[str, Any]] = []
    scan_iso = isoformat_utc(scan_time)
    scores = storm.threat_scores or {}
    tornado_score = scores.get("tornado", 0.0)
    max_risk = max(scores.values(), default=0.0)

    def _build(kind: str, severity: str, title: str, body: str, score: float | None = None) -> dict[str, Any]:
        return {
            "alert_id": _alert_id(storm.storm_id, kind, scan_time),
            "storm_id": storm.storm_id,
            "site": storm.site,
            "location_id": None,
            "alert_kind": kind,
            "severity_level": severity,
            "title": title,
            "body": body,
            "threat_score": round(score, 3) if score is not None else None,
            "triggered_at": isoformat_utc(),
            "scan_time": scan_iso,
        }

    # --- Tornado Emergency ---
    if tornado_score >= 0.92:
        alerts.append(_build(
            "tornado_emergency",
            "TORNADO_EMERGENCY",
            "Tornado Emergency",
            (
                f"Extremely dangerous tornado situation near "
                f"{storm.centroid_lat:.3f}°N {abs(storm.centroid_lon):.3f}°W. "
                f"Tornado threat score {tornado_score:.2f}. "
                "Seek shelter immediately."
            ),
            tornado_score,
        ))
    # --- Tornado Warning ---
    elif tornado_score >= 0.72:
        alerts.append(_build(
            "tornado_warning",
            "TORNADO",
            "Tornado Threat",
            (
                f"Significant tornado threat near "
                f"{storm.centroid_lat:.3f}°N {abs(storm.centroid_lon):.3f}°W. "
                f"Tornado score {tornado_score:.2f}. {storm.near_term_expectation or ''}"
            ).strip(),
            tornado_score,
        ))

    # --- TVS / TDS ---
    if _has_signature(storm, "TVS", "TDS") and tornado_score < 0.72:
        alerts.append(_build(
            "tvs_detected",
            "TORNADO",
            "Tornadic Vortex Signature",
            (
                f"TVS/TDS detected near "
                f"{storm.centroid_lat:.3f}°N {abs(storm.centroid_lon):.3f}°W. "
                f"Tornado score {tornado_score:.2f}."
            ),
            tornado_score,
        ))

    # --- Severe Storm ---
    if max_risk >= 0.55 and tornado_score < 0.72:
        threat_label = _primary_threat_label(storm)
        alerts.append(_build(
            "severe_storm",
            "SEVERE",
            f"Severe {threat_label} Threat",
            (
                f"{threat_label} risk {max_risk:.2f} near "
                f"{storm.centroid_lat:.3f}°N {abs(storm.centroid_lon):.3f}°W. "
                f"{storm.narrative or ''}"
            ).strip()[:280],
            max_risk,
        ))
    # --- Marginal ---
    elif 0.30 <= max_risk < 0.55 and tornado_score < 0.72:
        alerts.append(_build(
            "marginal_storm",
            "MARGINAL",
            "Marginal Storm",
            (
                f"Marginal storm risk {max_risk:.2f} near "
                f"{storm.centroid_lat:.3f}°N {abs(storm.centroid_lon):.3f}°W."
            ),
            max_risk,
        ))

    # --- Location impact alerts ---
    for impact in storm.impacts:
        eta_low = impact.eta_minutes_low
        if eta_low is not None and eta_low <= 20 and impact.impact_rank >= 0.30:
            location_alert_id = _alert_id(storm.storm_id, f"location_{impact.location_id}", scan_time)
            alerts.append({
                "alert_id": location_alert_id,
                "storm_id": storm.storm_id,
                "site": storm.site,
                "location_id": impact.location_id,
                "alert_kind": "location_imminent",
                "severity_level": storm.severity_level,
                "title": f"Storm approaching {impact.location_name}",
                "body": (
                    f"{impact.location_name}: arrival in ~{eta_low}–{impact.eta_minutes_high or eta_low + 5} min. "
                    f"{impact.summary}"
                )[:280],
                "threat_score": round(impact.impact_rank, 3),
                "triggered_at": isoformat_utc(),
                "scan_time": scan_iso,
            })

    return alerts


async def evaluate_and_persist_alerts(
    storms: list[TrackedStorm],
    scan_time: datetime,
    frame_store,  # FrameStore — typed loosely to avoid circular import
) -> int:
    """
    Evaluate all storms and persist any triggered alerts.
    Returns the count of newly inserted alerts.
    """
    inserted = 0
    for storm in storms:
        if storm.status != "active":
            continue
        try:
            alerts = evaluate_storm_alerts(storm, scan_time)
            for alert in alerts:
                await frame_store.insert_triggered_alert(alert)
                inserted += 1
        except Exception:
            LOGGER.exception("Alert evaluation failed for storm %s", storm.storm_id)
    if inserted:
        LOGGER.info("Server-side alerts: %d new triggered alerts written", inserted)
    return inserted
