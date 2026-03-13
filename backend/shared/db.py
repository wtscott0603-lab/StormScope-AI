from __future__ import annotations

import sqlite3
from pathlib import Path

import aiosqlite


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS radar_frames (
        frame_id     TEXT PRIMARY KEY,
        site         TEXT NOT NULL,
        product      TEXT NOT NULL,
        tilt         REAL NOT NULL,
        tilts_available TEXT,
        scan_time    TEXT NOT NULL,
        raw_path     TEXT,
        image_path   TEXT,
        min_lat      REAL,
        max_lat      REAL,
        min_lon      REAL,
        max_lon      REAL,
        status       TEXT NOT NULL DEFAULT 'raw',
        error_msg    TEXT,
        created_at   TEXT NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_frames_site_product
    ON radar_frames(site, product, scan_time DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_frames_site_product_tilt_status
    ON radar_frames(site, product, status, tilt, scan_time DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_frames_site_product_status
    ON radar_frames(site, product, status, scan_time DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS processor_runs (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at   TEXT NOT NULL,
        finished_at  TEXT,
        status       TEXT NOT NULL,
        frames_added INTEGER DEFAULT 0,
        error_msg    TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS analysis_results (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        frame_id    TEXT NOT NULL REFERENCES radar_frames(frame_id) ON DELETE CASCADE,
        analyzer    TEXT NOT NULL,
        ran_at      TEXT NOT NULL,
        payload     TEXT NOT NULL,
        UNIQUE(frame_id, analyzer)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_analysis_frame ON analysis_results(frame_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_analysis_site_analyzer ON analysis_results(frame_id, analyzer);
    """,
    """
    CREATE TABLE IF NOT EXISTS storm_objects (
        storm_id               TEXT PRIMARY KEY,
        site                   TEXT NOT NULL,
        latest_frame_id        TEXT REFERENCES radar_frames(frame_id),
        latest_scan_time       TEXT NOT NULL,
        status                 TEXT NOT NULL,
        lifecycle_state        TEXT NOT NULL,
        centroid_lat           REAL NOT NULL,
        centroid_lon           REAL NOT NULL,
        area_km2               REAL NOT NULL,
        max_reflectivity       REAL NOT NULL,
        mean_reflectivity      REAL NOT NULL,
        motion_heading_deg     REAL,
        motion_speed_kmh       REAL,
        trend                  TEXT NOT NULL,
        primary_threat         TEXT NOT NULL,
        secondary_threats_json TEXT NOT NULL,
        severity_level         TEXT NOT NULL,
        confidence             REAL NOT NULL,
        threat_scores_json     TEXT NOT NULL,
        narrative              TEXT NOT NULL,
        reasoning_json         TEXT NOT NULL,
        footprint_geojson      TEXT NOT NULL,
        forecast_path_json     TEXT NOT NULL,
        signatures_json        TEXT NOT NULL,
        environment_json       TEXT,
        prediction_json        TEXT,
        created_at             TEXT NOT NULL,
        updated_at             TEXT NOT NULL
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_storm_objects_site_updated
    ON storm_objects(site, updated_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS storm_snapshots (
        id                     INTEGER PRIMARY KEY AUTOINCREMENT,
        storm_id               TEXT NOT NULL REFERENCES storm_objects(storm_id),
        frame_id               TEXT REFERENCES radar_frames(frame_id),
        site                   TEXT NOT NULL,
        scan_time              TEXT NOT NULL,
        centroid_lat           REAL NOT NULL,
        centroid_lon           REAL NOT NULL,
        area_km2               REAL NOT NULL,
        max_reflectivity       REAL NOT NULL,
        mean_reflectivity      REAL NOT NULL,
        motion_heading_deg     REAL,
        motion_speed_kmh       REAL,
        trend                  TEXT NOT NULL,
        primary_threat         TEXT NOT NULL,
        secondary_threats_json TEXT NOT NULL,
        severity_level         TEXT NOT NULL,
        confidence             REAL NOT NULL,
        threat_scores_json     TEXT NOT NULL,
        footprint_geojson      TEXT NOT NULL,
        forecast_path_json     TEXT NOT NULL,
        signatures_json        TEXT NOT NULL,
        reasoning_json         TEXT NOT NULL,
        near_term_expectation  TEXT NOT NULL,
        prediction_json        TEXT,
        created_at             TEXT NOT NULL,
        UNIQUE(storm_id, frame_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_storm_snapshots_storm_time
    ON storm_snapshots(storm_id, scan_time DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS saved_locations (
        location_id  TEXT PRIMARY KEY,
        name         TEXT NOT NULL,
        lat          REAL NOT NULL,
        lon          REAL NOT NULL,
        kind         TEXT NOT NULL DEFAULT 'custom',
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS storm_location_impacts (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        storm_id          TEXT NOT NULL REFERENCES storm_objects(storm_id),
        location_id       TEXT NOT NULL REFERENCES saved_locations(location_id),
        computed_at       TEXT NOT NULL,
        eta_minutes_low   INTEGER,
        eta_minutes_high  INTEGER,
        distance_km       REAL,
        threat_at_arrival TEXT NOT NULL,
        trend_at_arrival  TEXT NOT NULL,
        confidence        REAL NOT NULL,
        summary           TEXT NOT NULL,
        impact_rank       REAL NOT NULL,
        details_json      TEXT,
        UNIQUE(storm_id, location_id)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_impacts_storm
    ON storm_location_impacts(storm_id, impact_rank DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS environment_snapshots (
        id                    INTEGER PRIMARY KEY AUTOINCREMENT,
        site                  TEXT NOT NULL,
        storm_id              TEXT REFERENCES storm_objects(storm_id),
        snapshot_time         TEXT NOT NULL,
        source                TEXT NOT NULL,
        lat                   REAL NOT NULL,
        lon                   REAL NOT NULL,
        station_id            TEXT,
        station_name          TEXT,
        observed_at           TEXT,
        surface_temp_c        REAL,
        dewpoint_c            REAL,
        wind_dir_deg          REAL,
        wind_speed_kt         REAL,
        pressure_hpa          REAL,
        visibility_mi         REAL,
        cape_jkg              REAL,
        cin_jkg               REAL,
        bulk_shear_06km_kt    REAL,
        bulk_shear_01km_kt    REAL,
        helicity_01km         REAL,
        dcape_jkg             REAL,
        freezing_level_m      REAL,
        pwat_mm               REAL,
        lapse_rate_midlevel_cpkm REAL,
        lcl_m                 REAL,
        lfc_m                 REAL,
        environment_confidence REAL,
        environment_freshness_minutes INTEGER,
        hail_favorability     REAL NOT NULL,
        wind_favorability     REAL NOT NULL,
        tornado_favorability  REAL NOT NULL,
        narrative             TEXT NOT NULL,
        raw_payload_json      TEXT
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_environment_storm_time
    ON environment_snapshots(storm_id, snapshot_time DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS triggered_alerts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id        TEXT NOT NULL UNIQUE,
        storm_id        TEXT,
        site            TEXT NOT NULL,
        location_id     TEXT,
        alert_kind      TEXT NOT NULL,
        severity_level  TEXT NOT NULL,
        title           TEXT NOT NULL,
        body            TEXT NOT NULL,
        threat_score    REAL,
        triggered_at    TEXT NOT NULL,
        scan_time       TEXT NOT NULL,
        acknowledged    INTEGER NOT NULL DEFAULT 0,
        acknowledged_at TEXT
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_triggered_alerts_triggered_at
    ON triggered_alerts(triggered_at DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_triggered_alerts_acknowledged
    ON triggered_alerts(acknowledged, triggered_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS storm_event_history (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        storm_id        TEXT NOT NULL REFERENCES storm_objects(storm_id),
        site            TEXT NOT NULL,
        scan_time       TEXT NOT NULL,
        event_flags_json    TEXT NOT NULL DEFAULT '[]',
        lifecycle_state     TEXT,
        priority_score      REAL,
        priority_label      TEXT,
        severity_level      TEXT,
        primary_threat      TEXT,
        threat_scores_json  TEXT NOT NULL DEFAULT '{}',
        storm_mode          TEXT,
        motion_heading_deg  REAL,
        motion_speed_kmh    REAL,
        confidence          REAL,
        created_at          TEXT NOT NULL,
        UNIQUE(storm_id, scan_time)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_storm_event_history_storm_time
    ON storm_event_history(storm_id, scan_time DESC);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_storm_event_history_site_time
    ON storm_event_history(site, scan_time DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS precomputed_storm_summaries (
        storm_id            TEXT PRIMARY KEY REFERENCES storm_objects(storm_id),
        site                TEXT NOT NULL,
        computed_at         TEXT NOT NULL,
        scan_count          INTEGER NOT NULL DEFAULT 0,
        first_seen          TEXT,
        last_seen           TEXT,
        peak_severity       TEXT,
        peak_threat_scores_json TEXT NOT NULL DEFAULT '{}',
        peak_reflectivity   REAL,
        max_area_km2        REAL,
        max_speed_kmh       REAL,
        max_priority_score  REAL,
        dominant_mode       TEXT,
        flag_summary_json   TEXT NOT NULL DEFAULT '[]',
        threat_trend_json   TEXT NOT NULL DEFAULT '[]',
        motion_trend_json   TEXT NOT NULL DEFAULT '[]',
        impact_location_ids_json TEXT NOT NULL DEFAULT '[]',
        summary_narrative   TEXT
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_precomputed_summaries_site
    ON precomputed_storm_summaries(site, computed_at DESC);
    """,
    """
    CREATE TABLE IF NOT EXISTS processor_history_status (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        site                        TEXT NOT NULL,
        last_ingest_time            TEXT,
        last_processing_cycle_time  TEXT,
        last_history_aggregation_time TEXT,
        last_retention_time         TEXT,
        snapshot_count              INTEGER NOT NULL DEFAULT 0,
        event_history_count         INTEGER NOT NULL DEFAULT 0,
        precomputed_summary_count   INTEGER NOT NULL DEFAULT 0,
        backlog_frame_count         INTEGER NOT NULL DEFAULT 0,
        is_caught_up                INTEGER NOT NULL DEFAULT 0,
        history_stale               INTEGER NOT NULL DEFAULT 0,
        updated_at                  TEXT NOT NULL,
        UNIQUE(site)
    );
    """,
]

MIGRATION_STATEMENTS = [
    "ALTER TABLE radar_frames ADD COLUMN tilts_available TEXT",
    "ALTER TABLE storm_objects ADD COLUMN prediction_json TEXT",
    "ALTER TABLE storm_snapshots ADD COLUMN prediction_json TEXT",
    "ALTER TABLE storm_location_impacts ADD COLUMN details_json TEXT",
    "ALTER TABLE environment_snapshots ADD COLUMN bulk_shear_01km_kt REAL",
    "ALTER TABLE environment_snapshots ADD COLUMN pwat_mm REAL",
    "ALTER TABLE environment_snapshots ADD COLUMN lapse_rate_midlevel_cpkm REAL",
    "ALTER TABLE environment_snapshots ADD COLUMN lcl_m REAL",
    "ALTER TABLE environment_snapshots ADD COLUMN lfc_m REAL",
    "ALTER TABLE environment_snapshots ADD COLUMN environment_confidence REAL",
    "ALTER TABLE environment_snapshots ADD COLUMN environment_freshness_minutes INTEGER",
    # v12 — storm mode, uncertainty cone, track uncertainty
    "ALTER TABLE storm_objects ADD COLUMN storm_mode TEXT",
    "ALTER TABLE storm_objects ADD COLUMN storm_mode_confidence REAL",
    "ALTER TABLE storm_objects ADD COLUMN storm_mode_evidence_json TEXT",
    "ALTER TABLE storm_objects ADD COLUMN uncertainty_cone_json TEXT",
    "ALTER TABLE storm_objects ADD COLUMN track_uncertainty_km REAL",
    # v12 — new index for faster impact-based lookups
    "CREATE INDEX IF NOT EXISTS idx_storm_objects_site_severity ON storm_objects(site, severity_level, updated_at DESC)",
    # v13 — threat component breakdown and lifecycle summary
    "ALTER TABLE storm_objects ADD COLUMN threat_component_breakdown_json TEXT",
    "ALTER TABLE storm_objects ADD COLUMN threat_top_reasons_json TEXT",
    "ALTER TABLE storm_objects ADD COLUMN threat_limiting_factors_json TEXT",
    "ALTER TABLE storm_objects ADD COLUMN lifecycle_summary_json TEXT",
    # v13 — performance: snapshot timeseries query (storm_id, scan_time) is already
    # covered by idx_storm_snapshots_storm_time. Add site+scan_time for cross-site
    # historical queries and for impact index.
    "CREATE INDEX IF NOT EXISTS idx_storm_snapshots_site_time ON storm_snapshots(site, scan_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_impacts_location ON storm_location_impacts(location_id, impact_rank DESC)",
    # v14 — event flags and operational priority
    "ALTER TABLE storm_objects ADD COLUMN event_flags_json TEXT",
    "ALTER TABLE storm_objects ADD COLUMN priority_score REAL",
    "ALTER TABLE storm_objects ADD COLUMN priority_label TEXT",
    "CREATE INDEX IF NOT EXISTS idx_storm_objects_priority ON storm_objects(site, priority_score DESC, updated_at DESC)",
    # v15 — always-on history: event flags persisted per scan, precomputed summaries
    "ALTER TABLE storm_snapshots ADD COLUMN event_flags_json TEXT",
    "ALTER TABLE storm_snapshots ADD COLUMN priority_score REAL",
    "ALTER TABLE storm_snapshots ADD COLUMN priority_label TEXT",
    "ALTER TABLE storm_snapshots ADD COLUMN lifecycle_state TEXT",
    "ALTER TABLE storm_snapshots ADD COLUMN storm_mode TEXT",
    "CREATE INDEX IF NOT EXISTS idx_storm_snapshots_lifecycle ON storm_snapshots(storm_id, lifecycle_state, scan_time DESC)",
]


def _apply_migrations_sync(connection: sqlite3.Connection) -> None:
    for statement in MIGRATION_STATEMENTS:
        try:
            connection.execute(statement)
        except sqlite3.OperationalError:
            continue


async def _apply_migrations_async(connection: aiosqlite.Connection) -> None:
    for statement in MIGRATION_STATEMENTS:
        try:
            await connection.execute(statement)
        except aiosqlite.OperationalError:
            continue


def init_db_sync(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        connection.execute("PRAGMA cache_size = -32000")  # 32 MB
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute("PRAGMA mmap_size = 268435456")  # 256 MB
        connection.execute("PRAGMA foreign_keys = ON")
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)
        _apply_migrations_sync(connection)
        connection.commit()


async def init_db(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(path) as connection:
        await connection.execute("PRAGMA journal_mode = WAL")
        await connection.execute("PRAGMA synchronous = NORMAL")
        await connection.execute("PRAGMA cache_size = -32000")
        await connection.execute("PRAGMA temp_store = MEMORY")
        await connection.execute("PRAGMA mmap_size = 268435456")
        await connection.execute("PRAGMA foreign_keys = ON")
        for statement in SCHEMA_STATEMENTS:
            await connection.execute(statement)
        await _apply_migrations_async(connection)
        await connection.commit()


async def connect(db_path: str | Path) -> aiosqlite.Connection:
    connection = await aiosqlite.connect(db_path)
    await connection.execute("PRAGMA journal_mode = WAL")
    await connection.execute("PRAGMA synchronous = NORMAL")
    await connection.execute("PRAGMA cache_size = -32000")
    await connection.execute("PRAGMA temp_store = MEMORY")
    await connection.execute("PRAGMA foreign_keys = ON")
    connection.row_factory = aiosqlite.Row
    return connection
