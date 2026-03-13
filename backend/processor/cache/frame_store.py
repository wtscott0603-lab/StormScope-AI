from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
from typing import Any

import aiosqlite

from backend.shared.db import connect, init_db
from backend.shared.models import (
    EnvironmentSnapshotRecord,
    PrecomputedStormSummary,
    ProcessorHistoryStatus,
    ProcessorRunRecord,
    RadarFrameRecord,
    SavedLocationRecord,
    StormEventHistoryRecord,
    StormLocationImpactRecord,
    StormObjectRecord,
    StormSnapshotRecord,
)
from backend.shared.time import isoformat_utc, parse_iso_datetime


class FrameStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    async def initialize(self) -> None:
        await init_db(self.db_path)

    @asynccontextmanager
    async def _connection(self):
        connection = await connect(self.db_path)
        try:
            yield connection
        finally:
            await connection.close()

    @staticmethod
    def _loads_json(value: str | None, default):
        if not value:
            return default
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default

    @staticmethod
    def _loads_float_csv(value: str | None) -> list[float]:
        if not value:
            return []
        items: list[float] = []
        for item in value.split(","):
            item = item.strip()
            if not item:
                continue
            try:
                items.append(float(item))
            except ValueError:
                continue
        return items

    @staticmethod
    def _row_to_frame(row: aiosqlite.Row) -> RadarFrameRecord:
        return RadarFrameRecord(
            frame_id=row["frame_id"],
            site=row["site"],
            product=row["product"],
            tilt=row["tilt"],
            tilts_available=FrameStore._loads_float_csv(row["tilts_available"]),
            scan_time=parse_iso_datetime(row["scan_time"]),
            raw_path=row["raw_path"],
            image_path=row["image_path"],
            min_lat=row["min_lat"],
            max_lat=row["max_lat"],
            min_lon=row["min_lon"],
            max_lon=row["max_lon"],
            status=row["status"],
            error_msg=row["error_msg"],
            created_at=parse_iso_datetime(row["created_at"]),
        )

    @classmethod
    def _row_to_storm_object(cls, row: aiosqlite.Row) -> StormObjectRecord:
        keys = row.keys() if hasattr(row, "keys") else []
        return StormObjectRecord(
            storm_id=row["storm_id"],
            site=row["site"],
            latest_frame_id=row["latest_frame_id"],
            latest_scan_time=parse_iso_datetime(row["latest_scan_time"]),
            status=row["status"],
            lifecycle_state=row["lifecycle_state"],
            centroid_lat=row["centroid_lat"],
            centroid_lon=row["centroid_lon"],
            area_km2=row["area_km2"],
            max_reflectivity=row["max_reflectivity"],
            mean_reflectivity=row["mean_reflectivity"],
            motion_heading_deg=row["motion_heading_deg"],
            motion_speed_kmh=row["motion_speed_kmh"],
            trend=row["trend"],
            primary_threat=row["primary_threat"],
            secondary_threats=cls._loads_json(row["secondary_threats_json"], []),
            severity_level=row["severity_level"],
            confidence=row["confidence"],
            threat_scores=cls._loads_json(row["threat_scores_json"], {}),
            narrative=row["narrative"],
            reasoning_factors=cls._loads_json(row["reasoning_json"], []),
            footprint_geojson=cls._loads_json(row["footprint_geojson"], {"type": "Polygon", "coordinates": []}),
            forecast_path=cls._loads_json(row["forecast_path_json"], []),
            associated_signatures=cls._loads_json(row["signatures_json"], []),
            environment_summary=cls._loads_json(row["environment_json"], None),
            prediction_summary=cls._loads_json(row["prediction_json"], None),
            # v12 fields — safe fallback for older DB rows that lack these columns
            storm_mode=row["storm_mode"] if "storm_mode" in keys and row["storm_mode"] is not None else "unknown",
            storm_mode_confidence=float(row["storm_mode_confidence"]) if "storm_mode_confidence" in keys and row["storm_mode_confidence"] is not None else 0.0,
            storm_mode_evidence=cls._loads_json(row["storm_mode_evidence_json"] if "storm_mode_evidence_json" in keys else None, []),
            uncertainty_cone=cls._loads_json(row["uncertainty_cone_json"] if "uncertainty_cone_json" in keys else None, []),
            track_uncertainty_km=float(row["track_uncertainty_km"]) if "track_uncertainty_km" in keys and row["track_uncertainty_km"] is not None else 5.0,
            # v13 fields — safe fallback for older DB rows
            threat_component_breakdown=cls._loads_json(row["threat_component_breakdown_json"] if "threat_component_breakdown_json" in keys else None, {}),
            threat_top_reasons=cls._loads_json(row["threat_top_reasons_json"] if "threat_top_reasons_json" in keys else None, {}),
            threat_limiting_factors=cls._loads_json(row["threat_limiting_factors_json"] if "threat_limiting_factors_json" in keys else None, {}),
            lifecycle_summary=cls._loads_json(row["lifecycle_summary_json"] if "lifecycle_summary_json" in keys else None, {}),
            # v14 fields — event flags and priority
            event_flags=cls._loads_json(row["event_flags_json"] if "event_flags_json" in keys else None, []),
            priority_score=float(row["priority_score"]) if "priority_score" in keys and row["priority_score"] is not None else 0.0,
            priority_label=row["priority_label"] if "priority_label" in keys and row["priority_label"] is not None else "MINIMAL",
            created_at=parse_iso_datetime(row["created_at"]),
            updated_at=parse_iso_datetime(row["updated_at"]),
        )

    @classmethod
    def _row_to_storm_snapshot(cls, row: aiosqlite.Row) -> StormSnapshotRecord:
        return StormSnapshotRecord(
            id=row["id"],
            storm_id=row["storm_id"],
            frame_id=row["frame_id"],
            site=row["site"],
            scan_time=parse_iso_datetime(row["scan_time"]),
            centroid_lat=row["centroid_lat"],
            centroid_lon=row["centroid_lon"],
            area_km2=row["area_km2"],
            max_reflectivity=row["max_reflectivity"],
            mean_reflectivity=row["mean_reflectivity"],
            motion_heading_deg=row["motion_heading_deg"],
            motion_speed_kmh=row["motion_speed_kmh"],
            trend=row["trend"],
            primary_threat=row["primary_threat"],
            secondary_threats=cls._loads_json(row["secondary_threats_json"], []),
            severity_level=row["severity_level"],
            confidence=row["confidence"],
            threat_scores=cls._loads_json(row["threat_scores_json"], {}),
            footprint_geojson=cls._loads_json(row["footprint_geojson"], {"type": "Polygon", "coordinates": []}),
            forecast_path=cls._loads_json(row["forecast_path_json"], []),
            associated_signatures=cls._loads_json(row["signatures_json"], []),
            reasoning_factors=cls._loads_json(row["reasoning_json"], []),
            near_term_expectation=row["near_term_expectation"],
            prediction_summary=cls._loads_json(row["prediction_json"], None),
            created_at=parse_iso_datetime(row["created_at"]),
        )

    @staticmethod
    def _row_to_run(row: aiosqlite.Row) -> ProcessorRunRecord:
        return ProcessorRunRecord(
            id=row["id"],
            started_at=parse_iso_datetime(row["started_at"]),
            finished_at=parse_iso_datetime(row["finished_at"]),
            status=row["status"],
            frames_added=row["frames_added"],
            error_msg=row["error_msg"],
        )

    @staticmethod
    def _row_to_saved_location(row: aiosqlite.Row) -> SavedLocationRecord:
        return SavedLocationRecord(
            location_id=row["location_id"],
            name=row["name"],
            lat=row["lat"],
            lon=row["lon"],
            kind=row["kind"],
            created_at=parse_iso_datetime(row["created_at"]),
            updated_at=parse_iso_datetime(row["updated_at"]),
        )

    @staticmethod
    def _row_to_impact(row: aiosqlite.Row) -> StormLocationImpactRecord:
        return StormLocationImpactRecord(
            id=row["id"],
            storm_id=row["storm_id"],
            location_id=row["location_id"],
            computed_at=parse_iso_datetime(row["computed_at"]),
            eta_minutes_low=row["eta_minutes_low"],
            eta_minutes_high=row["eta_minutes_high"],
            distance_km=row["distance_km"],
            threat_at_arrival=row["threat_at_arrival"],
            trend_at_arrival=row["trend_at_arrival"],
            confidence=row["confidence"],
            summary=row["summary"],
            impact_rank=row["impact_rank"],
            details=FrameStore._loads_json(row["details_json"], None),
        )

    @classmethod
    def _row_to_environment_snapshot(cls, row: aiosqlite.Row) -> EnvironmentSnapshotRecord:
        return EnvironmentSnapshotRecord(
            id=row["id"],
            site=row["site"],
            storm_id=row["storm_id"],
            snapshot_time=parse_iso_datetime(row["snapshot_time"]),
            source=row["source"],
            lat=row["lat"],
            lon=row["lon"],
            station_id=row["station_id"],
            station_name=row["station_name"],
            observed_at=parse_iso_datetime(row["observed_at"]),
            surface_temp_c=row["surface_temp_c"],
            dewpoint_c=row["dewpoint_c"],
            wind_dir_deg=row["wind_dir_deg"],
            wind_speed_kt=row["wind_speed_kt"],
            pressure_hpa=row["pressure_hpa"],
            visibility_mi=row["visibility_mi"],
            cape_jkg=row["cape_jkg"],
            cin_jkg=row["cin_jkg"],
            bulk_shear_06km_kt=row["bulk_shear_06km_kt"],
            bulk_shear_01km_kt=row["bulk_shear_01km_kt"],
            helicity_01km=row["helicity_01km"],
            dcape_jkg=row["dcape_jkg"],
            freezing_level_m=row["freezing_level_m"],
            pwat_mm=row["pwat_mm"],
            lapse_rate_midlevel_cpkm=row["lapse_rate_midlevel_cpkm"],
            lcl_m=row["lcl_m"],
            lfc_m=row["lfc_m"],
            environment_confidence=row["environment_confidence"],
            environment_freshness_minutes=row["environment_freshness_minutes"],
            hail_favorability=row["hail_favorability"],
            wind_favorability=row["wind_favorability"],
            tornado_favorability=row["tornado_favorability"],
            narrative=row["narrative"],
            raw_payload=cls._loads_json(row["raw_payload_json"], None),
        )

    @staticmethod
    def _json_default(value: Any):
        if hasattr(value, "item"):
            return value.item()
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

    def _dumps_json(self, payload: Any) -> str:
        return json.dumps(payload, default=self._json_default)

    async def insert_raw_frame(
        self,
        *,
        frame_id: str,
        site: str,
        product: str,
        tilt: float,
        scan_time,
        raw_path: str,
    ) -> bool:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                INSERT OR IGNORE INTO radar_frames (
                    frame_id, site, product, tilt, scan_time, raw_path, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'raw', ?)
                """,
                (
                    frame_id,
                    site,
                    product,
                    tilt,
                    isoformat_utc(scan_time),
                    raw_path,
                    isoformat_utc(),
                ),
            )
            await connection.commit()
            return cursor.rowcount > 0

    async def list_frames(
        self,
        *,
        site: str,
        product: str,
        limit: int,
        status: str = "processed",
        tilt: float | None = None,
    ) -> list[RadarFrameRecord]:
        query = """
            SELECT * FROM radar_frames
            WHERE site = ? AND product = ? AND status = ?
        """
        params: list[Any] = [site.upper(), product.upper(), status]
        if tilt is not None:
            query += " AND ABS(tilt - ?) < 0.11"
            params.append(float(tilt))
        query += " ORDER BY scan_time DESC LIMIT ?"
        params.append(limit)
        async with self._connection() as connection:
            cursor = await connection.execute(query, tuple(params))
            rows = await cursor.fetchall()
        records = [self._row_to_frame(row) for row in rows]
        return list(reversed(records))

    async def list_frames_for_window(
        self,
        *,
        site: str,
        product: str,
        start_time,
        end_time,
        status: str = "processed",
        tilt: float | None = None,
        limit: int = 200,
    ) -> list[RadarFrameRecord]:
        query = """
            SELECT * FROM radar_frames
            WHERE site = ? AND product = ? AND status = ? AND scan_time BETWEEN ? AND ?
        """
        params: list[Any] = [
            site.upper(),
            product.upper(),
            status,
            isoformat_utc(start_time),
            isoformat_utc(end_time),
        ]
        if tilt is not None:
            query += " AND ABS(tilt - ?) < 0.11"
            params.append(float(tilt))
        query += " ORDER BY scan_time ASC LIMIT ?"
        params.append(limit)
        async with self._connection() as connection:
            cursor = await connection.execute(query, tuple(params))
            rows = await cursor.fetchall()
        return [self._row_to_frame(row) for row in rows]

    async def get_frame(self, frame_id: str) -> RadarFrameRecord | None:
        async with self._connection() as connection:
            cursor = await connection.execute("SELECT * FROM radar_frames WHERE frame_id = ?", (frame_id,))
            row = await cursor.fetchone()
        return self._row_to_frame(row) if row else None

    async def get_latest_frame(self, site: str, product: str, tilt: float | None = None) -> RadarFrameRecord | None:
        query = """
            SELECT * FROM radar_frames
            WHERE site = ? AND product = ? AND status = 'processed'
        """
        params: list[Any] = [site.upper(), product.upper()]
        if tilt is not None:
            query += " AND ABS(tilt - ?) < 0.11"
            params.append(float(tilt))
            query += " ORDER BY scan_time DESC LIMIT 1"
        else:
            query += " ORDER BY scan_time DESC, ABS(tilt - 0.5) ASC LIMIT 1"
        async with self._connection() as connection:
            cursor = await connection.execute(query, tuple(params))
            row = await cursor.fetchone()
        return self._row_to_frame(row) if row else None

    async def product_has_frames(self, product: str, site: str | None = None) -> bool:
        query = "SELECT 1 FROM radar_frames WHERE product = ? AND status = 'processed'"
        params: list[Any] = [product.upper()]
        if site is not None:
            query += " AND site = ?"
            params.append(site.upper())
        query += " LIMIT 1"
        async with self._connection() as connection:
            cursor = await connection.execute(query, tuple(params))
            row = await cursor.fetchone()
        return row is not None

    async def get_frame_for_scan(
        self,
        site: str,
        product: str,
        scan_time,
        *,
        tilt: float | None = None,
    ) -> RadarFrameRecord | None:
        query = """
            SELECT * FROM radar_frames
            WHERE site = ? AND product = ? AND scan_time = ? AND status = 'processed'
        """
        params: list[Any] = [site.upper(), product.upper(), isoformat_utc(scan_time)]
        if tilt is not None:
            query += " AND ABS(tilt - ?) < 0.11 ORDER BY ABS(tilt - ?) ASC LIMIT 1"
            params.extend([float(tilt), float(tilt)])
        else:
            query += " ORDER BY ABS(tilt - 0.5) ASC LIMIT 1"
        async with self._connection() as connection:
            cursor = await connection.execute(query, tuple(params))
            row = await cursor.fetchone()
        return self._row_to_frame(row) if row else None

    async def list_raw_frames(self, limit: int = 10) -> list[RadarFrameRecord]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM radar_frames
                WHERE status = 'raw'
                ORDER BY scan_time ASC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cursor.fetchall()
        return [self._row_to_frame(row) for row in rows]

    async def update_frame_status(
        self,
        frame_id: str,
        *,
        status: str,
        image_path: str | None = None,
        min_lat: float | None = None,
        max_lat: float | None = None,
        min_lon: float | None = None,
        max_lon: float | None = None,
        error_msg: str | None = None,
        tilts_available: str | None = None,
    ) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                UPDATE radar_frames
                SET status = ?,
                    image_path = COALESCE(?, image_path),
                    min_lat = COALESCE(?, min_lat),
                    max_lat = COALESCE(?, max_lat),
                    min_lon = COALESCE(?, min_lon),
                    max_lon = COALESCE(?, max_lon),
                    tilts_available = COALESCE(?, tilts_available),
                    error_msg = ?
                WHERE frame_id = ?
                """,
                (status, image_path, min_lat, max_lat, min_lon, max_lon, tilts_available, error_msg, frame_id),
            )
            await connection.commit()

    async def begin_processing(self, frame_id: str) -> None:
        async with self._connection() as connection:
            await connection.execute(
                "UPDATE radar_frames SET status = 'processing', error_msg = NULL WHERE frame_id = ?",
                (frame_id,),
            )
            await connection.commit()

    async def frames_older_than(self, cutoff_iso: str) -> list[RadarFrameRecord]:
        async with self._connection() as connection:
            cursor = await connection.execute("SELECT * FROM radar_frames WHERE scan_time < ?", (cutoff_iso,))
            rows = await cursor.fetchall()
        return [self._row_to_frame(row) for row in rows]

    async def delete_frame(self, frame_id: str) -> None:
        async with self._connection() as connection:
            await connection.execute(
                "UPDATE storm_objects SET latest_frame_id = NULL WHERE latest_frame_id = ?",
                (frame_id,),
            )
            await connection.execute("DELETE FROM storm_snapshots WHERE frame_id = ?", (frame_id,))
            await connection.execute("DELETE FROM analysis_results WHERE frame_id = ?", (frame_id,))
            await connection.execute("DELETE FROM radar_frames WHERE frame_id = ?", (frame_id,))
            await connection.commit()

    async def site_last_frame_times(self) -> dict[str, str]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT site, MAX(scan_time) AS last_frame_time
                FROM radar_frames
                WHERE status = 'processed'
                GROUP BY site
                """
            )
            rows = await cursor.fetchall()
        return {row["site"]: row["last_frame_time"] for row in rows}

    async def available_products(self, site: str) -> list[str]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT DISTINCT product FROM radar_frames
                WHERE site = ? AND status = 'processed'
                ORDER BY product ASC
                """,
                (site.upper(),),
            )
            rows = await cursor.fetchall()
        return [row["product"] for row in rows]

    async def create_run(self) -> int:
        async with self._connection() as connection:
            cursor = await connection.execute(
                "INSERT INTO processor_runs (started_at, status, frames_added) VALUES (?, 'running', 0)",
                (isoformat_utc(),),
            )
            await connection.commit()
            return int(cursor.lastrowid)

    async def finish_run(self, run_id: int, *, status: str, frames_added: int, error_msg: str | None = None) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                UPDATE processor_runs
                SET finished_at = ?, status = ?, frames_added = ?, error_msg = ?
                WHERE id = ?
                """,
                (isoformat_utc(), status, frames_added, error_msg, run_id),
            )
            await connection.commit()

    async def latest_run(self) -> ProcessorRunRecord | None:
        async with self._connection() as connection:
            cursor = await connection.execute("SELECT * FROM processor_runs ORDER BY id DESC LIMIT 1")
            row = await cursor.fetchone()
        return self._row_to_run(row) if row else None

    async def latest_error(self) -> str | None:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT error_msg FROM processor_runs
                WHERE status = 'error' AND error_msg IS NOT NULL
                ORDER BY id DESC LIMIT 1
                """
            )
            row = await cursor.fetchone()
        return row["error_msg"] if row else None

    async def count_processed_frames(self) -> int:
        async with self._connection() as connection:
            cursor = await connection.execute("SELECT COUNT(*) AS count FROM radar_frames WHERE status = 'processed'")
            row = await cursor.fetchone()
        return int(row["count"])

    async def count_sites_with_frames(self) -> int:
        async with self._connection() as connection:
            cursor = await connection.execute(
                "SELECT COUNT(DISTINCT site) AS count FROM radar_frames WHERE status = 'processed'"
            )
            row = await cursor.fetchone()
        return int(row["count"])

    async def count_active_storms(self) -> int:
        async with self._connection() as connection:
            cursor = await connection.execute(
                "SELECT COUNT(*) AS count FROM storm_objects WHERE status = 'active'"
            )
            row = await cursor.fetchone()
        return int(row["count"])

    async def upsert_analysis_result(self, frame_id: str, analyzer: str, payload: dict) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO analysis_results (frame_id, analyzer, ran_at, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(frame_id, analyzer)
                DO UPDATE SET ran_at = excluded.ran_at, payload = excluded.payload
                """,
                (frame_id, analyzer, isoformat_utc(), self._dumps_json(payload)),
            )
            await connection.commit()

    async def get_analysis_results(self, frame_id: str) -> list[dict]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT analyzer, ran_at, payload
                FROM analysis_results
                WHERE frame_id = ?
                ORDER BY analyzer ASC
                """,
                (frame_id,),
            )
            rows = await cursor.fetchall()
        return [
            {
                "analyzer": row["analyzer"],
                "ran_at": row["ran_at"],
                "payload": self._loads_json(row["payload"], {}),
            }
            for row in rows
        ]

    async def upsert_storm_object(self, payload: dict[str, Any]) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO storm_objects (
                    storm_id, site, latest_frame_id, latest_scan_time, status, lifecycle_state,
                    centroid_lat, centroid_lon, area_km2, max_reflectivity, mean_reflectivity,
                    motion_heading_deg, motion_speed_kmh, trend, primary_threat, secondary_threats_json,
                    severity_level, confidence, threat_scores_json, narrative, reasoning_json,
                    footprint_geojson, forecast_path_json, signatures_json, environment_json, prediction_json,
                    storm_mode, storm_mode_confidence, storm_mode_evidence_json,
                    uncertainty_cone_json, track_uncertainty_km,
                    threat_component_breakdown_json, threat_top_reasons_json,
                    threat_limiting_factors_json, lifecycle_summary_json,
                    event_flags_json, priority_score, priority_label,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(storm_id) DO UPDATE SET
                    site = excluded.site,
                    latest_frame_id = excluded.latest_frame_id,
                    latest_scan_time = excluded.latest_scan_time,
                    status = excluded.status,
                    lifecycle_state = excluded.lifecycle_state,
                    centroid_lat = excluded.centroid_lat,
                    centroid_lon = excluded.centroid_lon,
                    area_km2 = excluded.area_km2,
                    max_reflectivity = excluded.max_reflectivity,
                    mean_reflectivity = excluded.mean_reflectivity,
                    motion_heading_deg = excluded.motion_heading_deg,
                    motion_speed_kmh = excluded.motion_speed_kmh,
                    trend = excluded.trend,
                    primary_threat = excluded.primary_threat,
                    secondary_threats_json = excluded.secondary_threats_json,
                    severity_level = excluded.severity_level,
                    confidence = excluded.confidence,
                    threat_scores_json = excluded.threat_scores_json,
                    narrative = excluded.narrative,
                    reasoning_json = excluded.reasoning_json,
                    footprint_geojson = excluded.footprint_geojson,
                    forecast_path_json = excluded.forecast_path_json,
                    signatures_json = excluded.signatures_json,
                    environment_json = excluded.environment_json,
                    prediction_json = excluded.prediction_json,
                    storm_mode = excluded.storm_mode,
                    storm_mode_confidence = excluded.storm_mode_confidence,
                    storm_mode_evidence_json = excluded.storm_mode_evidence_json,
                    uncertainty_cone_json = excluded.uncertainty_cone_json,
                    track_uncertainty_km = excluded.track_uncertainty_km,
                    threat_component_breakdown_json = excluded.threat_component_breakdown_json,
                    threat_top_reasons_json = excluded.threat_top_reasons_json,
                    threat_limiting_factors_json = excluded.threat_limiting_factors_json,
                    lifecycle_summary_json = excluded.lifecycle_summary_json,
                    event_flags_json = excluded.event_flags_json,
                    priority_score = excluded.priority_score,
                    priority_label = excluded.priority_label,
                    updated_at = excluded.updated_at
                """,
                (
                    payload["storm_id"],
                    payload["site"],
                    payload.get("latest_frame_id"),
                    payload["latest_scan_time"],
                    payload["status"],
                    payload["lifecycle_state"],
                    payload["centroid_lat"],
                    payload["centroid_lon"],
                    payload["area_km2"],
                    payload["max_reflectivity"],
                    payload["mean_reflectivity"],
                    payload.get("motion_heading_deg"),
                    payload.get("motion_speed_kmh"),
                    payload["trend"],
                    payload["primary_threat"],
                    self._dumps_json(payload.get("secondary_threats", [])),
                    payload["severity_level"],
                    payload["confidence"],
                    self._dumps_json(payload.get("threat_scores", {})),
                    payload["narrative"],
                    self._dumps_json(payload.get("reasoning_factors", [])),
                    self._dumps_json(payload["footprint_geojson"]),
                    self._dumps_json(payload.get("forecast_path", [])),
                    self._dumps_json(payload.get("associated_signatures", [])),
                    self._dumps_json(payload.get("environment_summary")) if payload.get("environment_summary") is not None else None,
                    self._dumps_json(payload.get("prediction_summary")) if payload.get("prediction_summary") is not None else None,
                    payload.get("storm_mode", "unknown"),
                    payload.get("storm_mode_confidence", 0.0),
                    self._dumps_json(payload.get("storm_mode_evidence", [])),
                    self._dumps_json(payload.get("uncertainty_cone", [])) if payload.get("uncertainty_cone") else None,
                    payload.get("track_uncertainty_km"),
                    self._dumps_json(payload.get("threat_component_breakdown", {})) if payload.get("threat_component_breakdown") else None,
                    self._dumps_json(payload.get("threat_top_reasons", {})) if payload.get("threat_top_reasons") else None,
                    self._dumps_json(payload.get("threat_limiting_factors", {})) if payload.get("threat_limiting_factors") else None,
                    self._dumps_json(payload.get("lifecycle_summary", {})) if payload.get("lifecycle_summary") else None,
                    self._dumps_json(payload.get("event_flags", [])) if payload.get("event_flags") else None,
                    payload.get("priority_score", 0.0),
                    payload.get("priority_label", "MINIMAL"),
                    payload["created_at"],
                    payload["updated_at"],
                ),
            )
            await connection.commit()

    async def list_storm_objects(
        self,
        *,
        site: str | None = None,
        include_inactive: bool = False,
        limit: int = 50,
    ) -> list[StormObjectRecord]:
        query = "SELECT * FROM storm_objects"
        params: list[Any] = []
        clauses: list[str] = []
        if site:
            clauses.append("site = ?")
            params.append(site.upper())
        if not include_inactive:
            clauses.append("status = 'active'")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        async with self._connection() as connection:
            cursor = await connection.execute(query, params)
            rows = await cursor.fetchall()
        return [self._row_to_storm_object(row) for row in rows]

    async def get_storm_object(self, storm_id: str) -> StormObjectRecord | None:
        async with self._connection() as connection:
            cursor = await connection.execute("SELECT * FROM storm_objects WHERE storm_id = ?", (storm_id,))
            row = await cursor.fetchone()
        return self._row_to_storm_object(row) if row else None

    async def set_storm_status(self, storm_id: str, status: str, lifecycle_state: str) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                UPDATE storm_objects
                SET status = ?, lifecycle_state = ?, updated_at = ?
                WHERE storm_id = ?
                """,
                (status, lifecycle_state, isoformat_utc(), storm_id),
            )
            await connection.commit()

    async def insert_storm_snapshot(self, payload: dict[str, Any]) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO storm_snapshots (
                    storm_id, frame_id, site, scan_time, centroid_lat, centroid_lon, area_km2,
                    max_reflectivity, mean_reflectivity, motion_heading_deg, motion_speed_kmh, trend,
                    primary_threat, secondary_threats_json, severity_level, confidence, threat_scores_json,
                    footprint_geojson, forecast_path_json, signatures_json, reasoning_json,
                    near_term_expectation, prediction_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(storm_id, frame_id) DO UPDATE SET
                    scan_time = excluded.scan_time,
                    centroid_lat = excluded.centroid_lat,
                    centroid_lon = excluded.centroid_lon,
                    area_km2 = excluded.area_km2,
                    max_reflectivity = excluded.max_reflectivity,
                    mean_reflectivity = excluded.mean_reflectivity,
                    motion_heading_deg = excluded.motion_heading_deg,
                    motion_speed_kmh = excluded.motion_speed_kmh,
                    trend = excluded.trend,
                    primary_threat = excluded.primary_threat,
                    secondary_threats_json = excluded.secondary_threats_json,
                    severity_level = excluded.severity_level,
                    confidence = excluded.confidence,
                    threat_scores_json = excluded.threat_scores_json,
                    footprint_geojson = excluded.footprint_geojson,
                    forecast_path_json = excluded.forecast_path_json,
                    signatures_json = excluded.signatures_json,
                    reasoning_json = excluded.reasoning_json,
                    near_term_expectation = excluded.near_term_expectation,
                    prediction_json = excluded.prediction_json,
                    created_at = excluded.created_at
                """,
                (
                    payload["storm_id"],
                    payload.get("frame_id"),
                    payload["site"],
                    payload["scan_time"],
                    payload["centroid_lat"],
                    payload["centroid_lon"],
                    payload["area_km2"],
                    payload["max_reflectivity"],
                    payload["mean_reflectivity"],
                    payload.get("motion_heading_deg"),
                    payload.get("motion_speed_kmh"),
                    payload["trend"],
                    payload["primary_threat"],
                    self._dumps_json(payload.get("secondary_threats", [])),
                    payload["severity_level"],
                    payload["confidence"],
                    self._dumps_json(payload.get("threat_scores", {})),
                    self._dumps_json(payload["footprint_geojson"]),
                    self._dumps_json(payload.get("forecast_path", [])),
                    self._dumps_json(payload.get("associated_signatures", [])),
                    self._dumps_json(payload.get("reasoning_factors", [])),
                    payload.get("near_term_expectation", ""),
                    self._dumps_json(payload.get("prediction_summary")) if payload.get("prediction_summary") is not None else None,
                    payload["created_at"],
                ),
            )
            await connection.commit()

    async def list_storm_snapshots(self, storm_id: str, limit: int = 12) -> list[StormSnapshotRecord]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM storm_snapshots
                WHERE storm_id = ?
                ORDER BY scan_time DESC
                LIMIT ?
                """,
                (storm_id, limit),
            )
            rows = await cursor.fetchall()
        snapshots = [self._row_to_storm_snapshot(row) for row in rows]
        return list(reversed(snapshots))

    async def upsert_saved_location(
        self,
        *,
        location_id: str,
        name: str,
        lat: float,
        lon: float,
        kind: str = "custom",
    ) -> None:
        now_iso = isoformat_utc()
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO saved_locations (location_id, name, lat, lon, kind, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(location_id) DO UPDATE SET
                    name = excluded.name,
                    lat = excluded.lat,
                    lon = excluded.lon,
                    kind = excluded.kind,
                    updated_at = excluded.updated_at
                """,
                (location_id, name, lat, lon, kind, now_iso, now_iso),
            )
            await connection.commit()

    async def list_saved_locations(self) -> list[SavedLocationRecord]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                "SELECT * FROM saved_locations ORDER BY updated_at DESC, name ASC"
            )
            rows = await cursor.fetchall()
        return [self._row_to_saved_location(row) for row in rows]

    async def get_saved_location(self, location_id: str) -> SavedLocationRecord | None:
        async with self._connection() as connection:
            cursor = await connection.execute("SELECT * FROM saved_locations WHERE location_id = ?", (location_id,))
            row = await cursor.fetchone()
        return self._row_to_saved_location(row) if row else None

    async def delete_saved_location(self, location_id: str) -> None:
        async with self._connection() as connection:
            await connection.execute("DELETE FROM storm_location_impacts WHERE location_id = ?", (location_id,))
            await connection.execute("DELETE FROM saved_locations WHERE location_id = ?", (location_id,))
            await connection.commit()

    async def replace_storm_impacts(self, storm_id: str, impacts: list[dict[str, Any]]) -> None:
        async with self._connection() as connection:
            await connection.execute("DELETE FROM storm_location_impacts WHERE storm_id = ?", (storm_id,))
            for impact in impacts:
                await connection.execute(
                    """
                    INSERT INTO storm_location_impacts (
                        storm_id, location_id, computed_at, eta_minutes_low, eta_minutes_high, distance_km,
                        threat_at_arrival, trend_at_arrival, confidence, summary, impact_rank, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        storm_id,
                        impact["location_id"],
                        impact["computed_at"],
                        impact.get("eta_minutes_low"),
                        impact.get("eta_minutes_high"),
                        impact.get("distance_km"),
                        impact["threat_at_arrival"],
                        impact["trend_at_arrival"],
                        impact["confidence"],
                        impact["summary"],
                        impact["impact_rank"],
                        self._dumps_json(impact.get("details")) if impact.get("details") is not None else None,
                    ),
                )
            await connection.commit()

    async def list_storm_impacts(self, storm_id: str) -> list[StormLocationImpactRecord]:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM storm_location_impacts
                WHERE storm_id = ?
                ORDER BY impact_rank DESC, eta_minutes_low ASC
                """,
                (storm_id,),
            )
            rows = await cursor.fetchall()
        return [self._row_to_impact(row) for row in rows]

    async def batch_latest_snapshots(self, storm_ids: list[str]) -> dict[str, Any]:
        """Return the latest snapshot near_term_expectation for each storm_id (single query)."""
        if not storm_ids:
            return {}
        placeholders = ",".join("?" * len(storm_ids))
        async with self._connection() as connection:
            cursor = await connection.execute(
                f"""
                SELECT s.storm_id, s.near_term_expectation
                FROM storm_snapshots s
                INNER JOIN (
                    SELECT storm_id, MAX(scan_time) AS max_time
                    FROM storm_snapshots
                    WHERE storm_id IN ({placeholders})
                    GROUP BY storm_id
                ) latest ON s.storm_id = latest.storm_id AND s.scan_time = latest.max_time
                """,
                tuple(storm_ids),
            )
            rows = await cursor.fetchall()
        return {row[0]: row[1] for row in rows}

    async def batch_storm_impacts(self, storm_ids: list[str]) -> dict[str, list[StormLocationImpactRecord]]:
        """Return all impacts for a list of storm_ids in a single query."""
        if not storm_ids:
            return {}
        placeholders = ",".join("?" * len(storm_ids))
        async with self._connection() as connection:
            cursor = await connection.execute(
                f"""
                SELECT * FROM storm_location_impacts
                WHERE storm_id IN ({placeholders})
                ORDER BY storm_id, impact_rank DESC, eta_minutes_low ASC
                """,
                tuple(storm_ids),
            )
            rows = await cursor.fetchall()
        result: dict[str, list[StormLocationImpactRecord]] = {sid: [] for sid in storm_ids}
        for row in rows:
            record = self._row_to_impact(row)
            result.setdefault(record.storm_id, []).append(record)
        return result

    async def batch_frames_for_scan(
        self,
        site: str,
        products: list[str],
        scan_time: Any,
        tilt: float | None = None,
    ) -> dict[str, Any]:
        """Return latest frame per product for a given scan_time (single query, replaces N get_frame_for_scan calls)."""
        if not products:
            return {}
        placeholders = ",".join("?" * len(products))
        params: list[Any] = [site.upper(), isoformat_utc(scan_time)]
        params.extend([p.upper() for p in products])
        if tilt is not None:
            params.extend([float(tilt), float(tilt)])
            tilt_clause = "AND ABS(tilt - ?) < 0.11 ORDER BY product, ABS(tilt - ?) ASC"
        else:
            tilt_clause = "ORDER BY product, ABS(tilt - 0.5) ASC"
        async with self._connection() as connection:
            cursor = await connection.execute(
                f"""
                SELECT * FROM radar_frames
                WHERE site = ? AND scan_time = ? AND status = 'processed'
                AND product IN ({placeholders})
                {tilt_clause}
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
        # Keep only first row per product (best tilt match, already ordered)
        seen: set[str] = set()
        result: dict[str, Any] = {}
        for row in rows:
            frame = self._row_to_frame(row)
            if frame.product not in seen:
                seen.add(frame.product)
                result[frame.product] = frame
        return result

    async def insert_environment_snapshot(self, payload: dict[str, Any]) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO environment_snapshots (
                    site, storm_id, snapshot_time, source, lat, lon, station_id, station_name, observed_at,
                    surface_temp_c, dewpoint_c, wind_dir_deg, wind_speed_kt, pressure_hpa, visibility_mi,
                    cape_jkg, cin_jkg, bulk_shear_06km_kt, bulk_shear_01km_kt, helicity_01km, dcape_jkg, freezing_level_m,
                    pwat_mm, lapse_rate_midlevel_cpkm, lcl_m, lfc_m, environment_confidence, environment_freshness_minutes,
                    hail_favorability, wind_favorability, tornado_favorability, narrative, raw_payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["site"],
                    payload.get("storm_id"),
                    payload["snapshot_time"],
                    payload["source"],
                    payload["lat"],
                    payload["lon"],
                    payload.get("station_id"),
                    payload.get("station_name"),
                    payload.get("observed_at"),
                    payload.get("surface_temp_c"),
                    payload.get("dewpoint_c"),
                    payload.get("wind_dir_deg"),
                    payload.get("wind_speed_kt"),
                    payload.get("pressure_hpa"),
                    payload.get("visibility_mi"),
                    payload.get("cape_jkg"),
                    payload.get("cin_jkg"),
                    payload.get("bulk_shear_06km_kt"),
                    payload.get("bulk_shear_01km_kt"),
                    payload.get("helicity_01km"),
                    payload.get("dcape_jkg"),
                    payload.get("freezing_level_m"),
                    payload.get("pwat_mm"),
                    payload.get("lapse_rate_midlevel_cpkm"),
                    payload.get("lcl_m"),
                    payload.get("lfc_m"),
                    payload.get("environment_confidence"),
                    payload.get("environment_freshness_minutes"),
                    payload["hail_favorability"],
                    payload["wind_favorability"],
                    payload["tornado_favorability"],
                    payload["narrative"],
                    self._dumps_json(payload.get("raw_payload")) if payload.get("raw_payload") is not None else None,
                ),
            )
            await connection.commit()

    async def get_latest_environment_snapshot(self, storm_id: str) -> EnvironmentSnapshotRecord | None:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT * FROM environment_snapshots
                WHERE storm_id = ?
                ORDER BY snapshot_time DESC
                LIMIT 1
                """,
                (storm_id,),
            )
            row = await cursor.fetchone()
        return self._row_to_environment_snapshot(row) if row else None

    async def latest_environment_snapshot_time(self) -> datetime | None:
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                SELECT snapshot_time
                FROM environment_snapshots
                ORDER BY snapshot_time DESC
                LIMIT 1
                """
            )
            row = await cursor.fetchone()
        return parse_iso_datetime(row["snapshot_time"]) if row and row["snapshot_time"] else None

    async def cleanup_storm_retention(self, cutoff_iso: str) -> dict[str, int]:
        async with self._connection() as connection:
            now_iso = isoformat_utc()
            expired_cursor = await connection.execute(
                """
                UPDATE storm_objects
                SET status = 'inactive', lifecycle_state = 'expired', updated_at = ?
                WHERE status = 'active' AND latest_scan_time < ?
                """,
                (now_iso, cutoff_iso),
            )

            baseline_changes = connection.total_changes
            await connection.execute(
                "DELETE FROM storm_snapshots WHERE scan_time < ?",
                (cutoff_iso,),
            )
            snapshot_deleted = connection.total_changes - baseline_changes

            baseline_changes = connection.total_changes
            await connection.execute(
                "DELETE FROM environment_snapshots WHERE snapshot_time < ?",
                (cutoff_iso,),
            )
            environment_deleted = connection.total_changes - baseline_changes

            cursor = await connection.execute(
                """
                SELECT storm_id FROM storm_objects
                WHERE status = 'inactive' AND latest_scan_time < ?
                """,
                (cutoff_iso,),
            )
            rows = await cursor.fetchall()
            stale_storm_ids = [row["storm_id"] for row in rows]

            impacts_deleted = 0
            storms_deleted = 0
            if stale_storm_ids:
                placeholders = ", ".join("?" for _ in stale_storm_ids)
                baseline_changes = connection.total_changes
                await connection.execute(
                    f"DELETE FROM storm_location_impacts WHERE storm_id IN ({placeholders})",
                    tuple(stale_storm_ids),
                )
                impacts_deleted = connection.total_changes - baseline_changes

                baseline_changes = connection.total_changes
                await connection.execute(
                    f"DELETE FROM environment_snapshots WHERE storm_id IN ({placeholders})",
                    tuple(stale_storm_ids),
                )
                environment_deleted += connection.total_changes - baseline_changes

                baseline_changes = connection.total_changes
                await connection.execute(
                    f"DELETE FROM storm_snapshots WHERE storm_id IN ({placeholders})",
                    tuple(stale_storm_ids),
                )
                snapshot_deleted += connection.total_changes - baseline_changes

                baseline_changes = connection.total_changes
                await connection.execute(
                    f"DELETE FROM storm_objects WHERE storm_id IN ({placeholders})",
                    tuple(stale_storm_ids),
                )
                storms_deleted = connection.total_changes - baseline_changes

            await connection.commit()
            return {
                "storms_expired": max(0, int(expired_cursor.rowcount or 0)),
                "storms_deleted": int(storms_deleted),
                "snapshots_deleted": int(snapshot_deleted),
                "environment_deleted": int(environment_deleted),
                "impacts_deleted": int(impacts_deleted),
            }

    async def as_dict(self, frame: RadarFrameRecord) -> dict[str, Any]:
        payload = asdict(frame)
        payload["scan_time"] = isoformat_utc(frame.scan_time)
        payload["created_at"] = isoformat_utc(frame.created_at)
        return payload

    async def insert_triggered_alert(self, payload: dict[str, Any]) -> None:
        """Insert a server-side triggered alert. Silently ignores duplicate alert_id."""
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT OR IGNORE INTO triggered_alerts (
                    alert_id, storm_id, site, location_id, alert_kind,
                    severity_level, title, body, threat_score,
                    triggered_at, scan_time, acknowledged, acknowledged_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
                """,
                (
                    payload["alert_id"],
                    payload.get("storm_id"),
                    payload["site"],
                    payload.get("location_id"),
                    payload["alert_kind"],
                    payload["severity_level"],
                    payload["title"],
                    payload["body"],
                    payload.get("threat_score"),
                    payload["triggered_at"],
                    payload["scan_time"],
                ),
            )
            await connection.commit()

    async def list_triggered_alerts(
        self,
        *,
        site: str | None = None,
        unacknowledged_only: bool = False,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return recent triggered alerts, newest first."""
        clauses: list[str] = []
        params: list[Any] = []
        if site is not None:
            clauses.append("site = ?")
            params.append(site)
        if unacknowledged_only:
            clauses.append("acknowledged = 0")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        async with self._connection() as connection:
            cursor = await connection.execute(
                f"""
                SELECT id, alert_id, storm_id, site, location_id, alert_kind,
                       severity_level, title, body, threat_score,
                       triggered_at, scan_time, acknowledged, acknowledged_at
                FROM triggered_alerts
                {where}
                ORDER BY triggered_at DESC
                LIMIT ?
                """,
                tuple(params),
            )
            rows = await cursor.fetchall()
        return [
            {
                "id": row[0],
                "alert_id": row[1],
                "storm_id": row[2],
                "site": row[3],
                "location_id": row[4],
                "alert_kind": row[5],
                "severity_level": row[6],
                "title": row[7],
                "body": row[8],
                "threat_score": row[9],
                "triggered_at": row[10],
                "scan_time": row[11],
                "acknowledged": bool(row[12]),
                "acknowledged_at": row[13],
            }
            for row in rows
        ]

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Mark an alert acknowledged. Returns True if the row existed."""
        async with self._connection() as connection:
            cursor = await connection.execute(
                """
                UPDATE triggered_alerts
                SET acknowledged = 1, acknowledged_at = ?
                WHERE alert_id = ? AND acknowledged = 0
                """,
                (isoformat_utc(), alert_id),
            )
            await connection.commit()
            return (cursor.rowcount or 0) > 0

    async def delete_old_triggered_alerts(self, cutoff_iso: str) -> int:
        """Remove acknowledged alerts older than cutoff."""
        async with self._connection() as connection:
            cursor = await connection.execute(
                "DELETE FROM triggered_alerts WHERE acknowledged = 1 AND triggered_at < ?",
                (cutoff_iso,),
            )
            await connection.commit()
            return cursor.rowcount or 0

    # =========================================================================
    # v15 — Storm Event History
    # =========================================================================

    async def insert_storm_event_history(self, payload: dict[str, Any]) -> None:
        """Persist one event-flag snapshot for a storm scan.  Idempotent on (storm_id, scan_time)."""
        now = isoformat_utc()
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT OR IGNORE INTO storm_event_history
                    (storm_id, site, scan_time, event_flags_json, lifecycle_state,
                     priority_score, priority_label, severity_level, primary_threat,
                     threat_scores_json, storm_mode, motion_heading_deg,
                     motion_speed_kmh, confidence, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    payload["storm_id"],
                    payload["site"],
                    payload["scan_time"],
                    json.dumps(payload.get("event_flags") or []),
                    payload.get("lifecycle_state"),
                    payload.get("priority_score"),
                    payload.get("priority_label"),
                    payload.get("severity_level"),
                    payload.get("primary_threat"),
                    json.dumps(payload.get("threat_scores") or {}),
                    payload.get("storm_mode"),
                    payload.get("motion_heading_deg"),
                    payload.get("motion_speed_kmh"),
                    payload.get("confidence"),
                    now,
                ),
            )
            await connection.commit()

    async def list_storm_event_history(
        self,
        storm_id: str,
        limit: int = 60,
    ) -> list[StormEventHistoryRecord]:
        async with self._connection() as connection:
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                """
                SELECT * FROM storm_event_history
                WHERE storm_id = ?
                ORDER BY scan_time DESC
                LIMIT ?
                """,
                (storm_id, limit),
            )
            rows = await cursor.fetchall()
        result = []
        for row in rows:
            result.append(StormEventHistoryRecord(
                id=row["id"],
                storm_id=row["storm_id"],
                site=row["site"],
                scan_time=parse_iso_datetime(row["scan_time"]),
                event_flags=json.loads(row["event_flags_json"] or "[]"),
                lifecycle_state=row["lifecycle_state"],
                priority_score=row["priority_score"],
                priority_label=row["priority_label"],
                severity_level=row["severity_level"],
                primary_threat=row["primary_threat"],
                threat_scores=json.loads(row["threat_scores_json"] or "{}"),
                storm_mode=row["storm_mode"],
                motion_heading_deg=row["motion_heading_deg"],
                motion_speed_kmh=row["motion_speed_kmh"],
                confidence=row["confidence"],
                created_at=parse_iso_datetime(row["created_at"]),
            ))
        return result

    async def count_storm_event_history(self, site: str | None = None) -> int:
        async with self._connection() as connection:
            if site:
                cursor = await connection.execute(
                    "SELECT COUNT(*) FROM storm_event_history WHERE site = ?", (site,)
                )
            else:
                cursor = await connection.execute("SELECT COUNT(*) FROM storm_event_history")
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def delete_old_event_history(self, cutoff_iso: str) -> int:
        async with self._connection() as connection:
            cursor = await connection.execute(
                "DELETE FROM storm_event_history WHERE scan_time < ?", (cutoff_iso,)
            )
            await connection.commit()
            return cursor.rowcount or 0

    # =========================================================================
    # v15 — Precomputed Storm Summaries
    # =========================================================================

    async def upsert_precomputed_summary(self, summary: PrecomputedStormSummary) -> None:
        now = isoformat_utc()
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO precomputed_storm_summaries
                    (storm_id, site, computed_at, scan_count, first_seen, last_seen,
                     peak_severity, peak_threat_scores_json, peak_reflectivity,
                     max_area_km2, max_speed_kmh, max_priority_score, dominant_mode,
                     flag_summary_json, threat_trend_json, motion_trend_json,
                     impact_location_ids_json, summary_narrative)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(storm_id) DO UPDATE SET
                    computed_at                 = excluded.computed_at,
                    scan_count                  = excluded.scan_count,
                    first_seen                  = excluded.first_seen,
                    last_seen                   = excluded.last_seen,
                    peak_severity               = excluded.peak_severity,
                    peak_threat_scores_json      = excluded.peak_threat_scores_json,
                    peak_reflectivity           = excluded.peak_reflectivity,
                    max_area_km2                = excluded.max_area_km2,
                    max_speed_kmh               = excluded.max_speed_kmh,
                    max_priority_score          = excluded.max_priority_score,
                    dominant_mode               = excluded.dominant_mode,
                    flag_summary_json           = excluded.flag_summary_json,
                    threat_trend_json           = excluded.threat_trend_json,
                    motion_trend_json           = excluded.motion_trend_json,
                    impact_location_ids_json    = excluded.impact_location_ids_json,
                    summary_narrative           = excluded.summary_narrative
                """,
                (
                    summary.storm_id,
                    summary.site,
                    isoformat_utc(summary.computed_at),
                    summary.scan_count,
                    isoformat_utc(summary.first_seen) if summary.first_seen else None,
                    isoformat_utc(summary.last_seen) if summary.last_seen else None,
                    summary.peak_severity,
                    json.dumps(summary.peak_threat_scores),
                    summary.peak_reflectivity,
                    summary.max_area_km2,
                    summary.max_speed_kmh,
                    summary.max_priority_score,
                    summary.dominant_mode,
                    json.dumps(summary.flag_summary),
                    json.dumps(summary.threat_trend),
                    json.dumps(summary.motion_trend),
                    json.dumps(summary.impact_location_ids),
                    summary.summary_narrative,
                ),
            )
            await connection.commit()

    async def get_precomputed_summary(self, storm_id: str) -> PrecomputedStormSummary | None:
        async with self._connection() as connection:
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                "SELECT * FROM precomputed_storm_summaries WHERE storm_id = ?",
                (storm_id,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return PrecomputedStormSummary(
            storm_id=row["storm_id"],
            site=row["site"],
            computed_at=parse_iso_datetime(row["computed_at"]),
            scan_count=row["scan_count"],
            first_seen=parse_iso_datetime(row["first_seen"]) if row["first_seen"] else None,
            last_seen=parse_iso_datetime(row["last_seen"]) if row["last_seen"] else None,
            peak_severity=row["peak_severity"],
            peak_threat_scores=json.loads(row["peak_threat_scores_json"] or "{}"),
            peak_reflectivity=row["peak_reflectivity"],
            max_area_km2=row["max_area_km2"],
            max_speed_kmh=row["max_speed_kmh"],
            max_priority_score=row["max_priority_score"],
            dominant_mode=row["dominant_mode"],
            flag_summary=json.loads(row["flag_summary_json"] or "[]"),
            threat_trend=json.loads(row["threat_trend_json"] or "[]"),
            motion_trend=json.loads(row["motion_trend_json"] or "[]"),
            impact_location_ids=json.loads(row["impact_location_ids_json"] or "[]"),
            summary_narrative=row["summary_narrative"],
        )

    async def count_precomputed_summaries(self, site: str | None = None) -> int:
        async with self._connection() as connection:
            if site:
                cursor = await connection.execute(
                    "SELECT COUNT(*) FROM precomputed_storm_summaries WHERE site = ?", (site,)
                )
            else:
                cursor = await connection.execute("SELECT COUNT(*) FROM precomputed_storm_summaries")
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def list_storm_ids_needing_summary(self, site: str, stale_minutes: int = 10) -> list[str]:
        """Return storm IDs whose precomputed summary is absent or older than stale_minutes."""
        from backend.shared.time import utc_now
        from datetime import timedelta
        threshold = isoformat_utc(utc_now() - timedelta(minutes=stale_minutes))
        async with self._connection() as connection:
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                """
                SELECT so.storm_id FROM storm_objects so
                LEFT JOIN precomputed_storm_summaries ps ON so.storm_id = ps.storm_id
                WHERE so.site = ?
                  AND (ps.storm_id IS NULL OR ps.computed_at < ?)
                ORDER BY so.updated_at DESC
                LIMIT 100
                """,
                (site, threshold),
            )
            rows = await cursor.fetchall()
        return [row["storm_id"] for row in rows]

    async def delete_old_precomputed_summaries(self, cutoff_iso: str) -> int:
        async with self._connection() as connection:
            cursor = await connection.execute(
                "DELETE FROM precomputed_storm_summaries WHERE last_seen < ? AND last_seen IS NOT NULL",
                (cutoff_iso,),
            )
            await connection.commit()
            return cursor.rowcount or 0

    # =========================================================================
    # v15 — Processor History Status
    # =========================================================================

    async def upsert_processor_history_status(self, status: ProcessorHistoryStatus) -> None:
        async with self._connection() as connection:
            await connection.execute(
                """
                INSERT INTO processor_history_status
                    (site, last_ingest_time, last_processing_cycle_time,
                     last_history_aggregation_time, last_retention_time,
                     snapshot_count, event_history_count, precomputed_summary_count,
                     backlog_frame_count, is_caught_up, history_stale, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(site) DO UPDATE SET
                    last_ingest_time                = excluded.last_ingest_time,
                    last_processing_cycle_time      = excluded.last_processing_cycle_time,
                    last_history_aggregation_time   = excluded.last_history_aggregation_time,
                    last_retention_time             = excluded.last_retention_time,
                    snapshot_count                  = excluded.snapshot_count,
                    event_history_count             = excluded.event_history_count,
                    precomputed_summary_count       = excluded.precomputed_summary_count,
                    backlog_frame_count             = excluded.backlog_frame_count,
                    is_caught_up                    = excluded.is_caught_up,
                    history_stale                   = excluded.history_stale,
                    updated_at                      = excluded.updated_at
                """,
                (
                    status.site,
                    isoformat_utc(status.last_ingest_time) if status.last_ingest_time else None,
                    isoformat_utc(status.last_processing_cycle_time) if status.last_processing_cycle_time else None,
                    isoformat_utc(status.last_history_aggregation_time) if status.last_history_aggregation_time else None,
                    isoformat_utc(status.last_retention_time) if status.last_retention_time else None,
                    status.snapshot_count,
                    status.event_history_count,
                    status.precomputed_summary_count,
                    status.backlog_frame_count,
                    1 if status.is_caught_up else 0,
                    1 if status.history_stale else 0,
                    isoformat_utc(status.updated_at),
                ),
            )
            await connection.commit()

    async def get_processor_history_status(self, site: str) -> ProcessorHistoryStatus | None:
        async with self._connection() as connection:
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                "SELECT * FROM processor_history_status WHERE site = ?", (site,)
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        return ProcessorHistoryStatus(
            id=row["id"],
            site=row["site"],
            last_ingest_time=parse_iso_datetime(row["last_ingest_time"]) if row["last_ingest_time"] else None,
            last_processing_cycle_time=parse_iso_datetime(row["last_processing_cycle_time"]) if row["last_processing_cycle_time"] else None,
            last_history_aggregation_time=parse_iso_datetime(row["last_history_aggregation_time"]) if row["last_history_aggregation_time"] else None,
            last_retention_time=parse_iso_datetime(row["last_retention_time"]) if row["last_retention_time"] else None,
            snapshot_count=row["snapshot_count"],
            event_history_count=row["event_history_count"],
            precomputed_summary_count=row["precomputed_summary_count"],
            backlog_frame_count=row["backlog_frame_count"],
            is_caught_up=bool(row["is_caught_up"]),
            history_stale=bool(row["history_stale"]),
            updated_at=parse_iso_datetime(row["updated_at"]),
        )

    async def list_all_processor_history_statuses(self) -> list[ProcessorHistoryStatus]:
        async with self._connection() as connection:
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                "SELECT * FROM processor_history_status ORDER BY site"
            )
            rows = await cursor.fetchall()
        result = []
        for row in rows:
            result.append(ProcessorHistoryStatus(
                id=row["id"],
                site=row["site"],
                last_ingest_time=parse_iso_datetime(row["last_ingest_time"]) if row["last_ingest_time"] else None,
                last_processing_cycle_time=parse_iso_datetime(row["last_processing_cycle_time"]) if row["last_processing_cycle_time"] else None,
                last_history_aggregation_time=parse_iso_datetime(row["last_history_aggregation_time"]) if row["last_history_aggregation_time"] else None,
                last_retention_time=parse_iso_datetime(row["last_retention_time"]) if row["last_retention_time"] else None,
                snapshot_count=row["snapshot_count"],
                event_history_count=row["event_history_count"],
                precomputed_summary_count=row["precomputed_summary_count"],
                backlog_frame_count=row["backlog_frame_count"],
                is_caught_up=bool(row["is_caught_up"]),
                history_stale=bool(row["history_stale"]),
                updated_at=parse_iso_datetime(row["updated_at"]),
            ))
        return result

    async def count_backlog_frames(self, site: str | None = None) -> int:
        """Count pending/unprocessed frames — indicates processing backlog."""
        async with self._connection() as connection:
            if site:
                cursor = await connection.execute(
                    "SELECT COUNT(*) FROM radar_frames WHERE status IN ('pending','processing') AND site = ?",
                    (site,),
                )
            else:
                cursor = await connection.execute(
                    "SELECT COUNT(*) FROM radar_frames WHERE status IN ('pending','processing')"
                )
            row = await cursor.fetchone()
            return int(row[0]) if row else 0

    async def get_latest_ingest_time(self, site: str) -> datetime | None:
        """Return scan_time of the most recently ingested frame for a site."""
        async with self._connection() as connection:
            cursor = await connection.execute(
                "SELECT MAX(scan_time) FROM radar_frames WHERE site = ?", (site,)
            )
            row = await cursor.fetchone()
            if row and row[0]:
                return parse_iso_datetime(row[0])
        return None

    async def get_latest_snapshot_time(self, site: str) -> datetime | None:
        """Return scan_time of the most recent storm snapshot for a site."""
        async with self._connection() as connection:
            cursor = await connection.execute(
                "SELECT MAX(scan_time) FROM storm_snapshots WHERE site = ?", (site,)
            )
            row = await cursor.fetchone()
            if row and row[0]:
                return parse_iso_datetime(row[0])
        return None

    async def get_recent_storm_objects_for_recovery(
        self, site: str, max_age_minutes: int = 90
    ) -> list[StormObjectRecord]:
        """Return recently-active storm objects for tracker in-memory recovery on restart."""
        from backend.shared.time import utc_now
        from datetime import timedelta
        threshold = isoformat_utc(utc_now() - timedelta(minutes=max_age_minutes))
        async with self._connection() as connection:
            connection.row_factory = aiosqlite.Row
            cursor = await connection.execute(
                """
                SELECT * FROM storm_objects
                WHERE site = ? AND updated_at >= ?
                ORDER BY updated_at DESC
                LIMIT 200
                """,
                (site, threshold),
            )
            rows = await cursor.fetchall()
        # Reuse existing _row_to_storm_object helper via list_storm_objects path
        return [self._row_to_storm_object(dict(row)) for row in rows]
