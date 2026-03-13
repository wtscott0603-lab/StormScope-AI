from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
import logging

from backend.processor.analysis.base import ProcessedFrame
from backend.processor.analysis.rotation import RotationAnalyzer
from backend.processor.cache.frame_store import FrameStore
from backend.processor.config import ProcessorSettings
from backend.processor.overlays import load_overlay_cache, sample_operational_context
from backend.processor.processing.volume_products import sample_volume_metrics
from backend.processor.storms.environment import build_environment_snapshot, refresh_metar_cache
from backend.processor.storms.geometry import destination_point
from backend.processor.storms.segmentation import detect_storm_cells
from backend.processor.storms.threats import (
    build_forecast_path,
    collect_associated_signatures,
    compute_local_srv_rotation_signatures,
    compute_location_impacts,
    compute_srv_metrics,
    compute_threats,
)
from backend.processor.storms.lifecycle import build_lifecycle_summary
from backend.processor.storms.event_flags import compute_event_flags
from backend.processor.storms.priority import compute_priority_score
from backend.processor.storms.tracking import compute_uncertainty_cone, estimate_motion_from_history, match_storms
from backend.processor.storms.types import TrackedStorm
from backend.shared.time import isoformat_utc


LOGGER = logging.getLogger(__name__)


class StormEngine:
    def __init__(self, settings: ProcessorSettings, frame_store: FrameStore) -> None:
        self.settings = settings
        self.frame_store = frame_store

    async def refresh_surface_cache(self) -> None:
        try:
            await refresh_metar_cache(
                self.settings.metar_cache_path,
                ttl_minutes=self.settings.metar_cache_ttl_minutes,
            )
        except Exception:
            LOGGER.exception("Failed to refresh METAR cache")

    async def update_for_frame(
        self,
        *,
        frame,
        ref_sweep,
        vel_sweep,
        ref_analysis_results: list[dict],
        volume_products: dict | None = None,
    ) -> list[TrackedStorm]:
        if ref_sweep is None:
            return []

        detections = detect_storm_cells(
            ref_sweep,
            threshold_dbz=self.settings.storm_reflectivity_threshold_dbz,
            min_area_km2=self.settings.storm_min_area_km2,
        )
        previous_storms = await self.frame_store.list_storm_objects(site=frame.site, include_inactive=False, limit=30)
        assignments = match_storms(frame.site, frame.scan_time, detections, previous_storms)
        saved_locations = await self.frame_store.list_saved_locations()
        observations = await refresh_metar_cache(
            self.settings.metar_cache_path,
            ttl_minutes=self.settings.metar_cache_ttl_minutes,
        )

        combined_signatures: list[dict] = []
        for result in ref_analysis_results:
            combined_signatures.extend(result.get("payload", {}).get("signatures", []))

        spc_payload = load_overlay_cache(self.settings.spc_overlay_cache_path)
        md_payload = load_overlay_cache(self.settings.mesoscale_discussions_cache_path)
        lsr_payload = load_overlay_cache(self.settings.local_storm_reports_cache_path)
        watch_payload = load_overlay_cache(self.settings.watch_overlay_cache_path)

        if vel_sweep is not None:
            rotation_payload = RotationAnalyzer().run(
                ProcessedFrame(
                    frame_id=f"{frame.frame_id}_storm_vel",
                    site=frame.site,
                    product="VEL",
                    image_path=frame.image_path or "",
                    sweep=vel_sweep,
                )
            ).payload
            combined_signatures.extend(rotation_payload.get("signatures", []))

        tracked_storms: list[TrackedStorm] = []
        for assignment in assignments:
            previous = assignment["previous"]
            detection = assignment["detection"]
            if detection is None and previous is not None:
                await self.frame_store.set_storm_status(previous.storm_id, "inactive", assignment["lifecycle_state"])
                continue

            storm_id = assignment["storm_id"]
            # Use more history for multi-frame motion estimation (up to 5 snapshots)
            history = await self.frame_store.list_storm_snapshots(storm_id, limit=5) if previous is not None else []

            # Multi-frame weighted motion estimation using snapshot history.
            # Falls back to single-step assignment if history is sparse.
            track_uncertainty_km = 5.0
            if history and previous is not None:
                hist_heading, hist_speed, track_uncertainty_km = estimate_motion_from_history(
                    detection.centroid_lat,
                    detection.centroid_lon,
                    frame.scan_time,
                    history,
                )
                # Prefer history-based estimate when it provides a heading;
                # blend with assignment's single-step estimate to avoid instability.
                if hist_heading is not None and hist_speed is not None:
                    raw_heading = assignment["motion_heading_deg"]
                    raw_speed = assignment["motion_speed_kmh"]
                    if raw_heading is not None and raw_speed is not None:
                        # Weighted blend: 60% history regression, 40% single-step
                        from backend.processor.storms.tracking import _blend_heading, _blend_speed
                        motion_heading_deg = _blend_heading(hist_heading, raw_heading, previous_weight=0.60)
                        motion_speed_kmh = _blend_speed(hist_speed, raw_speed, previous_weight=0.60)
                    else:
                        motion_heading_deg = hist_heading
                        motion_speed_kmh = hist_speed
                else:
                    motion_heading_deg = assignment["motion_heading_deg"] if assignment["motion_heading_deg"] is not None else (previous.motion_heading_deg if previous else None)
                    motion_speed_kmh = assignment["motion_speed_kmh"] if assignment["motion_speed_kmh"] is not None else (previous.motion_speed_kmh if previous else None)
            else:
                motion_heading_deg = assignment["motion_heading_deg"] if assignment["motion_heading_deg"] is not None else (previous.motion_heading_deg if previous else None)
                motion_speed_kmh = assignment["motion_speed_kmh"] if assignment["motion_speed_kmh"] is not None else (previous.motion_speed_kmh if previous else None)
            associated_signatures = collect_associated_signatures(detection, combined_signatures)
            local_srv_rotation_signatures = compute_local_srv_rotation_signatures(
                detection,
                vel_sweep,
                motion_heading_deg,
                motion_speed_kmh,
            )
            if local_srv_rotation_signatures:
                associated_signatures = [
                    signature
                    for signature in associated_signatures
                    if str(signature.get("signature_type")) not in {"ROTATION", "TVS"}
                ]
                dedupe_keys = {
                    (
                        str(signature.get("signature_type")),
                        round(float(signature.get("lat", 0.0)), 3),
                        round(float(signature.get("lon", 0.0)), 3),
                    )
                    for signature in associated_signatures
                }
                for signature in local_srv_rotation_signatures:
                    key = (
                        str(signature.get("signature_type")),
                        round(float(signature.get("lat", 0.0)), 3),
                        round(float(signature.get("lon", 0.0)), 3),
                    )
                    if key not in dedupe_keys:
                        associated_signatures.append(signature)
                        dedupe_keys.add(key)
            operational_context = sample_operational_context(
                lat=detection.centroid_lat,
                lon=detection.centroid_lon,
                spc_payload=spc_payload,
                md_payload=md_payload,
                lsr_payload=lsr_payload,
                watch_payload=watch_payload,
            )
            forecast_path = build_forecast_path(
                centroid_lat=detection.centroid_lat,
                centroid_lon=detection.centroid_lon,
                motion_heading_deg=motion_heading_deg,
                motion_speed_kmh=motion_speed_kmh,
                horizon_minutes=self.settings.storm_track_horizon_min,
                step_minutes=self.settings.storm_track_step_min,
                destination_point_func=destination_point,
            )
            environment_payload = await build_environment_snapshot(
                site=frame.site,
                storm_id=storm_id,
                centroid_lat=detection.centroid_lat,
                centroid_lon=detection.centroid_lon,
                motion_heading_deg=motion_heading_deg,
                motion_speed_kmh=motion_speed_kmh,
                observations=observations,
                grid_cache_ttl_minutes=self.settings.grid_forecast_cache_ttl_minutes,
                open_meteo_cache_ttl_minutes=self.settings.open_meteo_cache_ttl_minutes,
                cache_dir=self.settings.environment_cache_dir,
                sounding_cache_dir=self.settings.sounding_cache_dir,
            )
            if environment_payload is not None:
                await self.frame_store.insert_environment_snapshot(environment_payload)
            environment_summary = environment_payload.get("summary") if environment_payload else None
            storm_volume_metrics = sample_volume_metrics(
                volume_products or {},
                centroid_lat=detection.centroid_lat,
                centroid_lon=detection.centroid_lon,
                radius_km=detection.radius_km,
            )
            srv_metrics = compute_srv_metrics(
                detection,
                vel_sweep,
                motion_heading_deg,
                motion_speed_kmh,
                motion_confidence=assignment.get("motion_confidence"),
            )
            nearby_storm_count = len([
                a for a in assignments
                if a.get("detection") is not None and a.get("storm_id") != storm_id
            ])
            threat_payload = compute_threats(
                detection=detection,
                history=history,
                associated_signatures=associated_signatures,
                environment_summary=environment_summary,
                volume_metrics=storm_volume_metrics,
                srv_metrics=srv_metrics,
                motion_speed_kmh=motion_speed_kmh,
                motion_heading_deg=motion_heading_deg,
                match_score=assignment.get("match_score"),
                motion_confidence=assignment.get("motion_confidence"),
                operational_context=operational_context,
                nearby_storm_count=nearby_storm_count,
                track_uncertainty_km=track_uncertainty_km,
            )
            if environment_summary is not None:
                environment_summary["projected_trend"] = threat_payload["projected_trend"]
                environment_summary["projection_confidence"] = threat_payload["confidence"]
                environment_summary["operational_context"] = operational_context
                if storm_volume_metrics:
                    environment_summary["volume_metrics"] = storm_volume_metrics
            if srv_metrics.get("available"):
                threat_payload["reasoning_factors"].append(
                    f"Storm-relative velocity delta-V {srv_metrics['delta_v_ms']:.0f} m/s"
                )
                if environment_summary is not None:
                    environment_summary["srv_metrics"] = srv_metrics

            # Build uncertainty cone for map visualization
            uncertainty_cone = compute_uncertainty_cone(
                centroid_lat=detection.centroid_lat,
                centroid_lon=detection.centroid_lon,
                heading_deg=motion_heading_deg,
                speed_kmh=motion_speed_kmh,
                track_uncertainty_km=track_uncertainty_km,
                motion_confidence=assignment.get("motion_confidence", 0.5),
                horizon_minutes=self.settings.storm_track_horizon_min,
                step_minutes=self.settings.storm_track_step_min,
                destination_point_func=destination_point,
            )

            impacts = compute_location_impacts(
                centroid_lat=detection.centroid_lat,
                centroid_lon=detection.centroid_lon,
                radius_km=detection.radius_km,
                forecast_path=forecast_path,
                motion_heading_deg=motion_heading_deg,
                motion_speed_kmh=motion_speed_kmh,
                locations=saved_locations,
                primary_threat=threat_payload["primary_threat"],
                trend=threat_payload["projected_trend"],
                confidence=threat_payload["confidence"],
                threat_scores=threat_payload["threat_scores"],
                prediction_summary=threat_payload.get("prediction_summary"),
                environment_summary=environment_summary,
                operational_context=operational_context,
            )

            # Enriched lifecycle analysis from snapshot history
            lifecycle_summary = build_lifecycle_summary(
                history,
                lifecycle_state=assignment["lifecycle_state"],
                trend=threat_payload["trend"],
            )

            # Structured operational event flags
            event_flags = compute_event_flags(
                history=history,
                lifecycle_summary=lifecycle_summary,
                lifecycle_state=assignment["lifecycle_state"],
                associated_signatures=associated_signatures,
                threat_scores=threat_payload["threat_scores"],
                threat_component_breakdown=threat_payload.get("threat_component_breakdown", {}),
                severity_level=threat_payload["severity_level"],
                storm_mode=threat_payload.get("storm_mode", "unknown"),
                storm_mode_confidence=threat_payload.get("storm_mode_confidence", 0.0),
                motion_speed_kmh=motion_speed_kmh,
                motion_confidence=assignment.get("motion_confidence", 0.0),
                track_uncertainty_km=track_uncertainty_km,
                environment_summary=environment_summary,
                volume_metrics=storm_volume_metrics,
                srv_metrics=srv_metrics,
            )

            # Operational priority score and label
            priority_score, priority_label = compute_priority_score(
                severity_level=threat_payload["severity_level"],
                primary_threat=threat_payload["primary_threat"],
                threat_scores=threat_payload["threat_scores"],
                event_flags=event_flags,
                motion_confidence=assignment.get("motion_confidence", 0.0),
                lifecycle_state=assignment["lifecycle_state"],
                storm_mode=threat_payload.get("storm_mode", "unknown"),
                environment_summary=environment_summary,
                impacts=impacts,
                history_length=len(history),
            )

            now = datetime.now(frame.scan_time.tzinfo)
            tracked_storm = TrackedStorm(
                storm_id=storm_id,
                site=frame.site,
                frame_id=frame.frame_id,
                scan_time=frame.scan_time,
                status="active",
                lifecycle_state=assignment["lifecycle_state"],
                centroid_lat=detection.centroid_lat,
                centroid_lon=detection.centroid_lon,
                area_km2=detection.area_km2,
                max_reflectivity=detection.max_reflectivity,
                mean_reflectivity=detection.mean_reflectivity,
                motion_heading_deg=motion_heading_deg,
                motion_speed_kmh=motion_speed_kmh,
                trend=threat_payload["trend"],
                primary_threat=threat_payload["primary_threat"],
                secondary_threats=threat_payload["secondary_threats"],
                severity_level=threat_payload["severity_level"],
                confidence=threat_payload["confidence"],
                threat_scores=threat_payload["threat_scores"],
                narrative=threat_payload["narrative"],
                reasoning_factors=threat_payload["reasoning_factors"],
                footprint_geojson=detection.footprint_geojson,
                forecast_path=forecast_path,
                uncertainty_cone=uncertainty_cone,
                storm_mode=threat_payload.get("storm_mode", "unknown"),
                storm_mode_confidence=threat_payload.get("storm_mode_confidence", 0.0),
                storm_mode_evidence=threat_payload.get("storm_mode_evidence", []),
                track_uncertainty_km=track_uncertainty_km,
                associated_signatures=associated_signatures,
                environment_summary=environment_summary,
                prediction_summary=threat_payload.get("prediction_summary"),
                near_term_expectation=threat_payload["near_term_expectation"],
                threat_component_breakdown=threat_payload.get("threat_component_breakdown", {}),
                threat_top_reasons=threat_payload.get("threat_top_reasons", {}),
                threat_limiting_factors=threat_payload.get("threat_limiting_factors", {}),
                lifecycle_summary=lifecycle_summary,
                event_flags=event_flags,
                priority_score=priority_score,
                priority_label=priority_label,
                impacts=impacts,
                created_at=previous.created_at if previous else now,
                updated_at=now,
            )
            await self._persist_tracked_storm(tracked_storm)
            tracked_storms.append(tracked_storm)

        return tracked_storms

    async def _persist_tracked_storm(self, storm: TrackedStorm) -> None:
        object_payload = {
            "storm_id": storm.storm_id,
            "site": storm.site,
            "latest_frame_id": storm.frame_id,
            "latest_scan_time": isoformat_utc(storm.scan_time),
            "status": storm.status,
            "lifecycle_state": storm.lifecycle_state,
            "centroid_lat": storm.centroid_lat,
            "centroid_lon": storm.centroid_lon,
            "area_km2": storm.area_km2,
            "max_reflectivity": storm.max_reflectivity,
            "mean_reflectivity": storm.mean_reflectivity,
            "motion_heading_deg": storm.motion_heading_deg,
            "motion_speed_kmh": storm.motion_speed_kmh,
            "trend": storm.trend,
            "primary_threat": storm.primary_threat,
            "secondary_threats": storm.secondary_threats,
            "severity_level": storm.severity_level,
            "confidence": storm.confidence,
            "threat_scores": storm.threat_scores,
            "narrative": storm.narrative,
            "reasoning_factors": storm.reasoning_factors,
            "footprint_geojson": storm.footprint_geojson,
            "forecast_path": storm.forecast_path,
            "uncertainty_cone": storm.uncertainty_cone,
            "storm_mode": storm.storm_mode,
            "storm_mode_confidence": storm.storm_mode_confidence,
            "storm_mode_evidence": storm.storm_mode_evidence,
            "track_uncertainty_km": storm.track_uncertainty_km,
            "associated_signatures": storm.associated_signatures,
            "environment_summary": storm.environment_summary,
            "prediction_summary": storm.prediction_summary,
            "threat_component_breakdown": storm.threat_component_breakdown,
            "threat_top_reasons": storm.threat_top_reasons,
            "threat_limiting_factors": storm.threat_limiting_factors,
            "lifecycle_summary": storm.lifecycle_summary,
            "event_flags": storm.event_flags,
            "priority_score": storm.priority_score,
            "priority_label": storm.priority_label,
            "created_at": isoformat_utc(storm.created_at or storm.scan_time),
            "updated_at": isoformat_utc(storm.updated_at or storm.scan_time),
        }
        snapshot_payload = {
            "storm_id": storm.storm_id,
            "frame_id": storm.frame_id,
            "site": storm.site,
            "scan_time": isoformat_utc(storm.scan_time),
            "centroid_lat": storm.centroid_lat,
            "centroid_lon": storm.centroid_lon,
            "area_km2": storm.area_km2,
            "max_reflectivity": storm.max_reflectivity,
            "mean_reflectivity": storm.mean_reflectivity,
            "motion_heading_deg": storm.motion_heading_deg,
            "motion_speed_kmh": storm.motion_speed_kmh,
            "trend": storm.trend,
            "primary_threat": storm.primary_threat,
            "secondary_threats": storm.secondary_threats,
            "severity_level": storm.severity_level,
            "confidence": storm.confidence,
            "threat_scores": storm.threat_scores,
            "footprint_geojson": storm.footprint_geojson,
            "forecast_path": storm.forecast_path,
            "associated_signatures": storm.associated_signatures,
            "reasoning_factors": storm.reasoning_factors,
            "near_term_expectation": storm.near_term_expectation,
            "prediction_summary": storm.prediction_summary,
            "created_at": isoformat_utc(storm.updated_at or storm.scan_time),
        }
        impact_payloads = [
            {
                "location_id": impact.location_id,
                "computed_at": isoformat_utc(),
                "eta_minutes_low": impact.eta_minutes_low,
                "eta_minutes_high": impact.eta_minutes_high,
                "distance_km": impact.distance_km,
                "threat_at_arrival": impact.threat_at_arrival,
                "trend_at_arrival": impact.trend_at_arrival,
                "confidence": impact.confidence,
                "summary": impact.summary,
                "impact_rank": impact.impact_rank,
                "details": impact.details,
            }
            for impact in storm.impacts
        ]
        await self.frame_store.upsert_storm_object(object_payload)
        await self.frame_store.insert_storm_snapshot(snapshot_payload)
        await self.frame_store.replace_storm_impacts(storm.storm_id, impact_payloads)
