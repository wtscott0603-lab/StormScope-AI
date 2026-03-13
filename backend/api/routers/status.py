from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter

from backend.api.config import get_settings
from backend.api.dependencies import get_frame_store
from backend.api.schemas.radar import StatusResponse, SiteHistoryStatus
from backend.processor.overlays import overlay_cache_status
from backend.shared.cache_health import cache_health
from backend.shared.time import utc_now


router = APIRouter(tags=["status"])


@router.get("/api/status", response_model=StatusResponse)
@router.get("/api/v1/status", response_model=StatusResponse)
async def get_status() -> StatusResponse:
    settings = get_settings()
    store = get_frame_store()
    latest_run = None
    frames_cached = 0
    sites_active = 0
    active_storms = 0
    processor_last_run = None
    last_error = None
    db_warning = None
    try:
        latest_run = await store.latest_run()
        processor_last_run = latest_run.finished_at or latest_run.started_at if latest_run else None
        frames_cached = await store.count_processed_frames()
        sites_active = await store.count_sites_with_frames()
        active_storms = await store.count_active_storms()
        last_error = await store.latest_error()
    except Exception as exc:
        processor_status = "error"
        last_error = str(exc)
        db_warning = "Status query against SQLite failed; cache freshness may still be available."
        latest_run = None

    processor_age_minutes = (
        max(0, int((utc_now() - processor_last_run).total_seconds() // 60))
        if processor_last_run is not None
        else None
    )
    if db_warning is not None:
        processor_status = "error"
    elif latest_run is None:
        processor_status = "idle"
    elif latest_run.status == "running" and latest_run.finished_at is None:
        processor_status = "running"
    elif latest_run.status == "error":
        processor_status = "error"
    else:
        processor_status = "idle"

    cache_status = {
        "alerts": cache_health(settings.alerts_cache_path, ttl_minutes=max(2, int((settings.update_interval_sec * 2) / 60) or 2)),
        "metar": cache_health(settings.metar_cache_path, ttl_minutes=max(settings.metar_cache_ttl_minutes, 10)),
        "spc": overlay_cache_status(settings.spc_overlay_cache_path, ttl_minutes=settings.overlay_cache_ttl_minutes),
        "mesoscale_discussions": overlay_cache_status(
            settings.mesoscale_discussions_cache_path,
            ttl_minutes=settings.overlay_cache_ttl_minutes,
        ),
        "local_storm_reports": overlay_cache_status(
            settings.local_storm_reports_cache_path,
            ttl_minutes=settings.overlay_cache_ttl_minutes,
        ),
    }
    latest_environment_snapshot = None
    try:
        latest_environment_snapshot = await store.latest_environment_snapshot_time()
    except Exception as exc:
        if db_warning is None:
            db_warning = "Status query against SQLite failed; cache freshness may still be available."
        if not last_error:
            last_error = str(exc)
    environment_snapshot_age_minutes = (
        max(0, int((utc_now() - latest_environment_snapshot).total_seconds() // 60))
        if latest_environment_snapshot is not None
        else None
    )

    # v15 — history freshness across all sites
    site_history_statuses: list[SiteHistoryStatus] = []
    last_ingest_time_global: object = None
    last_agg_time_global: object = None
    total_backlog = 0
    any_stale = False
    any_not_caught_up = False
    try:
        all_statuses = await store.list_all_processor_history_statuses()
        for hs in all_statuses:
            site_history_statuses.append(SiteHistoryStatus(
                site=hs.site,
                last_ingest_time=hs.last_ingest_time,
                last_processing_cycle_time=hs.last_processing_cycle_time,
                last_history_aggregation_time=hs.last_history_aggregation_time,
                snapshot_count=hs.snapshot_count,
                event_history_count=hs.event_history_count,
                precomputed_summary_count=hs.precomputed_summary_count,
                backlog_frame_count=hs.backlog_frame_count,
                is_caught_up=hs.is_caught_up,
                history_stale=hs.history_stale,
            ))
            total_backlog += hs.backlog_frame_count
            if hs.history_stale:
                any_stale = True
            if not hs.is_caught_up:
                any_not_caught_up = True
            if hs.last_ingest_time and (last_ingest_time_global is None or hs.last_ingest_time > last_ingest_time_global):
                last_ingest_time_global = hs.last_ingest_time
            if hs.last_history_aggregation_time and (last_agg_time_global is None or hs.last_history_aggregation_time > last_agg_time_global):
                last_agg_time_global = hs.last_history_aggregation_time
    except Exception:
        pass  # history status is best-effort

    data_warnings: list[str] = []
    processor_stale_threshold = timedelta(seconds=settings.update_interval_sec * 2)
    if processor_last_run is not None and utc_now() - processor_last_run > processor_stale_threshold:
        data_warnings.append("Processor activity is older than the expected update interval.")
    if processor_last_run is None:
        data_warnings.append("Processor has not completed a successful cycle yet.")
    for label, status in cache_status.items():
        if not status["available"]:
            data_warnings.append(f"{label.replace('_', ' ').title()} cache is unavailable.")
        elif status["stale"]:
            data_warnings.append(f"{label.replace('_', ' ').title()} cache is stale.")
    environment_stale_threshold = max(settings.open_meteo_cache_ttl_minutes * 2, 90)
    if latest_environment_snapshot is None:
        data_warnings.append("Environment snapshots have not been written yet.")
    elif environment_snapshot_age_minutes is not None and environment_snapshot_age_minutes > environment_stale_threshold:
        data_warnings.append("Environment snapshots are stale compared with the configured model cache cadence.")
    if db_warning is not None:
        data_warnings.append(db_warning)

    if any_stale:
        data_warnings.append("History is stale for one or more sites — aggregation may be behind.")
    if any_not_caught_up:
        data_warnings.append("Processing backlog detected — processor may be behind on one or more sites.")

    return StatusResponse(
        processor_status=processor_status,
        frames_cached=frames_cached,
        sites_active=sites_active,
        active_storms=active_storms,
        processor_last_run=processor_last_run,
        processor_age_minutes=processor_age_minutes,
        environment_snapshot_age_minutes=environment_snapshot_age_minutes,
        cache_status=cache_status,
        data_warnings=data_warnings,
        last_error=last_error,
        last_ingest_time=last_ingest_time_global,
        last_history_aggregation_time=last_agg_time_global,
        history_stale=any_stale,
        backlog_frame_count=total_backlog,
        is_caught_up=not any_not_caught_up,
        site_history_statuses=site_history_statuses,
    )
