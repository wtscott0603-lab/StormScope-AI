# Changelog

## 0.8.0

### Critical Architecture Change — Always-On History Processing

The processor is now explicitly documented and wired as a fully headless service.
History accumulates whether or not the frontend is ever opened. The frontend is
a pure consumer of pre-persisted data, never the trigger for creating it.

### New Infrastructure

- **`storm_event_history` table** — one row per storm per radar scan, persisted
  immediately during the processing cycle. Stores event flags, priority score,
  lifecycle state, severity, threat scores, storm mode, and motion for every
  scan of every tracked storm. `(storm_id, scan_time)` unique key makes all
  inserts idempotent — safe to re-run after a crash or restart.

- **`precomputed_storm_summaries` table** — rolled-up per-storm aggregates rebuilt
  by the history aggregator on a ~2-minute cadence: peak severity/threats/
  reflectivity/priority, dominant convective mode, flag occurrence counts,
  24-scan threat trend, 24-scan motion trend, impact location IDs, and a
  plain-text summary narrative.

- **`processor_history_status` table** — per-site freshness tracking: last ingest
  time, last processing cycle time, last history aggregation time, backlog frame
  count, `is_caught_up`, `history_stale`.

- **History Aggregator** (`backend/processor/history/aggregator.py`) — dedicated
  processing pass that runs independently of the ingest cycle. Persists event
  history rows from existing snapshots (idempotent), builds/refreshes precomputed
  summaries for stale storms, and updates the history status table. Runs on a
  separate APScheduler job so a slow aggregation pass never blocks live ingest.

- **Backfill module** (`backend/processor/history/backfill.py`) — three functions:
  - `startup_catchup()` — called on every restart; resets frames stuck in
    `processing` state (from prior crash) to `pending`, then re-runs the
    processing pipeline. No history is lost across restarts.
  - `run_backfill_for_site()` — reprocesses all unprocessed frames in an
    arbitrary time window; safe to re-run.
  - `rebuild_event_history_from_snapshots()` — one-time migration helper that
    derives event history rows from existing storm_snapshots; useful when
    upgrading from pre-v15.

- **Backfill CLI** (`backend/processor/history/backfill_cli.py`):
  ```
  # Process last 4 hours of missed frames for KGRR
  python -m backend.processor.history.backfill_cli --site KGRR --hours 4

  # Process a specific window
  python -m backend.processor.history.backfill_cli \
      --site KDTX --start 2025-06-01T18:00:00 --end 2025-06-01T22:00:00

  # Rebuild event history from existing snapshots (post-upgrade recovery)
  python -m backend.processor.history.backfill_cli --site KGRR --rebuild-history

  # Run aggregation pass only
  python -m backend.processor.history.backfill_cli --site KGRR --aggregate-only
  ```

### Scheduler Overhaul

- Dual independent APScheduler jobs: fast ingest cycle (configurable interval)
  and slower history aggregation cycle (default 120s).
- Both jobs: `max_instances=1`, `coalesce=True`, `misfire_grace_time` set to
  prevent overlap and allow catchup after slow cycles.
- History job failure is fully isolated — never disrupts the ingest cycle.
- `_safe_wrap()` ensures unhandled exceptions are logged but never propagate to
  the scheduler engine.

### Processor Startup

On every start, the processor now:
1. Runs `startup_catchup()` — resets stuck frames, reprocesses last 2 hours
2. Runs an initial history aggregation pass for all active sites
3. Starts both scheduler jobs
4. Logs an explicit message confirming headless operation

### Retention Improvement

Compact history tables (`storm_event_history`, `precomputed_storm_summaries`) are
retained for `4× retention_hours` — the same period during which raw
frames/images are deleted. Historical trend intelligence outlives raw rasters.

### Status API — History Freshness

`GET /api/status` and `GET /api/v1/status` now return:
- `last_ingest_time` — most recent successful data ingest across all sites
- `last_history_aggregation_time` — most recent aggregation pass completion
- `history_stale` — true if any site's history is behind schedule
- `backlog_frame_count` — total pending/unprocessed frames across all sites
- `is_caught_up` — false if any site has a processing backlog
- `site_history_statuses[]` — per-site breakdown of all freshness fields

Data warnings added for stale history and detected backlogs.

### New API Endpoints

- `GET /api/v1/storms/{id}/event-history?limit=60` — paginated per-scan event
  history showing flags, priority, severity, threat scores, mode, and motion
  at each scan time. Newest-first. Built server-side continuously.

- `GET /api/v1/storms/{id}/summary` — precomputed aggregated summary (peak
  severity/threats, flag occurrence counts, threat/motion trend, dominant mode,
  narrative). Returns 404 until first aggregation pass (~2 min after storm
  appears); retry on 404.

### Frontend

- **`EventHistoryPanel`** — collapsible history timeline in the selected-storm
  detail section. Per-scan rows show: time, priority dot (color-coded),
  severity chip, threat color, storm mode, confidence, 4-segment threat bar
  (tornado/hail/wind/flood), and top event flag. Precomputed summary block
  above the timeline shows peak stats, dominant mode, flag occurrence chips.
  Includes explicit provenance note: "History built server-side — continues
  accumulating with no frontend open."

- **StatusBar** — new `hist: ● live` / `hist: ⚠ stale` indicator and backlog
  count, sourced from the v15 history status fields in the API response.

- `ApiStatus` TypeScript type extended with all v15 freshness fields.
- `StormEventHistoryResponse`, `StormPrecomputedSummary`, `SiteHistoryStatus`
  TypeScript interfaces added.
- `fetchStormEventHistory()`, `fetchStormPrecomputedSummary()` API functions.

### Tests

103 tests passing (up from 85). 18 new tests in `test_history_pipeline.py`:
- `TestStormEventHistory` — insert, idempotency, count, delete, missing-storm
- `TestPrecomputedSummaries` — upsert, update, get, missing, count, stale detection
- `TestProcessorHistoryStatus` — upsert, update, list, missing, backlog count
- `TestHistoryAggregator` — status write, full pass completes without error
- `TestBackfill` — stuck-frame reset, time-window backfill frame marking

### How to Run (No Frontend Required)

```bash
# Start the processor — history accumulates indefinitely with no browser open
python -m backend.processor.main

# Backfill missed history after downtime
python -m backend.processor.history.backfill_cli --site KGRR --hours 6

# Check history freshness
curl http://localhost:8000/api/v1/status | jq '.history_stale, .is_caught_up, .backlog_frame_count'
```



### New Features

- **Operational Event Flags** — 16 structured flags per tracked storm with confidence, rationale, severity ranking, and provenance labeling. Flags include: `rapid_intensification`, `rapid_decay`, `rotation_tightening`, `rotation_broadening`, `possible_hail_surge`, `possible_split`, `possible_merge`, `elevated_uncertainty`, `forward_acceleration`, `slowing_training`, `long_track`, `supercell_candidate`, `tornado_threat_elevated`, `severe_threat_elevated`, `environment_support_strong`, `environment_support_weak`. All explicitly labeled as proxy-derived heuristics.

- **Operational Priority Scoring** — Every tracked storm now receives a `priority_score` (0–1) and `priority_label` (CRITICAL/HIGH/MODERATE/LOW/MINIMAL). Score combines: severity tier, threat scores, event flag boosts/penalties, storm mode modifier, motion confidence, location impact count, and track persistence. Persisted to DB with a dedicated index.

- **Hotspots Panel** (`GET /api/v1/storms/hotspots`) — Ranked storm list ordered by priority score. Frontend `HotspotsPanel` shows ranked cards with priority badge, threat color, storm mode chip, motion arrow, top event flag, and location impact count.

- **Location Risk Dashboard** (`GET /api/v1/locations/risk`) — Per-saved-location threat intelligence aggregated across all active storms. Returns risk level (HIGH/MODERATE/LOW/NONE), risk score, soonest ETA window, primary threat, top event flag labels, threatening storm count, and top impact summary. Frontend `LocationRiskPanel` renders this with risk bars, ETA windows, threat color coding, flag chips, and a "focus storm" link.

- **Storm Comparison Endpoint** (`GET /api/v1/storms/compare?storm_a=&storm_b=`) — 21-field side-by-side comparison covering severity, all four threat scores with deltas, reflectivity, area, motion speed/heading, trend, convective mode, priority, track uncertainty, lifecycle state, CAPE, 0–6km shear, SRH proxy, environment confidence, and top event flag.

### Algorithm Improvements

- **Storm Association Scoring** (`tracking.py`) — `_match_score` upgraded from area-delta penalty to area-ratio similarity (smaller/larger), which better distinguishes splits from continuations. Projection tolerance now scales with the previous storm's confidence (tighter for well-tracked storms, looser for new/uncertain ones). Heading tolerance tightened from 80° to 45–65° based on storm speed (faster storms have more constrained heading change expectations). Weight rebalance: motion continuity 0.10→0.12, distance 0.18→0.16.

### UI/UX

- **Storm Cards** — Priority badge (color-coded CRITICAL/HIGH/MODERATE), storm-mode chip, and top event flag now appear on every storm card in the list.
- **Selected Storm Detail** — New "Operational Flags" section shows up to 6 flags with severity-colored icons, confidence percentage, and plain-English rationale text.
- **Hotspots Panel** — Collapsible, sorted by priority. Rank numbers, color-coded dots, threat/mode labels, motion arrows.
- **Location Risk Panel** — Replaces the passive saved-locations list with active threat intelligence. Locations with active threats show risk bars, ETA windows, flag chips.

### Data Model

- `storm_objects` table: three new columns — `event_flags_json`, `priority_score`, `priority_label`
- New index: `idx_storm_objects_priority ON storm_objects(site, priority_score DESC, updated_at DESC)`
- `StormSummaryResponse` schema extended with `event_flags`, `priority_score`, `priority_label`

### Tests

- 85 tests passing (up from 68). New: `TestEventFlags` (10 tests), `TestPriorityScoring` (7 tests).



- **Performance:** Replaced `np.maximum.at` scatter in the rasterizer with an argsort/bincount approach, eliminating GIL-contention overhead on large REF sweeps (96 ms → same throughput with ~40% lower peak GIL hold time).
- **Performance:** Replaced per-call `httpx.AsyncClient` creation in `environment.py` with a process-wide singleton, enabling TCP keepalive across all NWS/model API fetches. Added stale-while-revalidate fallback so network failures return cached data instead of raising.
- **Performance:** Added granular Zustand selectors (`selectRadarKey`, `selectPlayback`, `selectOverlayVisibility`, etc.) so overlay components only rerender when their specific slice changes. Applied `React.memo` to `StormsOverlay`, `OperationalOverlays`, and `AlertsOverlay`.
- **Performance:** Replaced stale-closure `active` flag pattern in `useRadarFrames` with `AbortController`, cancelling in-flight requests when the site, product, or tilt changes before the response arrives.
- **Performance:** Wrapped all GeoJSON builds in `StormsOverlay` with `useMemo` to prevent recomputation on unrelated parent rerenders.
- **Storm tracking:** Added `lifecycle.py` with `classify_lifecycle_trend` (rapid intensification / strengthening / steady / weakening / rapid decay) and `classify_motion_trend` (accelerating / decelerating / turning left / right). Lifecycle summaries are persisted per storm object and exposed via the API.
- **Threat math:** Added `threat_component_breakdown`, `threat_top_reasons`, and `threat_limiting_factors` fields to `compute_threats()`. The API now returns per-component sub-scores and "limiting factor" labels alongside every storm.
- **API:** New `GET /api/v1/storms/{id}/timeseries` endpoint returning per-scan reflectivity, area, and centroid history for a storm. New `GET /api/v1/storms/{id}/breakdown` endpoint returning the full threat-component breakdown.
- **UI — Threat Scores:** StormsPanel now shows a 2×2 threat-component grid with mini score bars, the top 3 contributing sub-components, and missing/zero-score limiting factors for each threat type.
- **UI — Lifecycle:** StormsPanel now shows an intensity trend (color-coded rapid/strengthening/weakening), motion trend, and up to two evidence strings sourced from the lifecycle engine.
- **UI — Sparklines:** StormsPanel now renders inline SVG sparklines of max reflectivity and storm area over the last 20 scans, colour-coded by trend direction.
- **UI — Uncertainty cone:** `StormsOverlay` now renders the storm uncertainty cone as a filled polygon with a dashed border when a storm is selected.
- **UI — Keyboard shortcuts:** Space (play/pause), Arrow keys (step frames), `p` (panel), `c` (county lines), `s` (storms), `a` (alerts).
- **DB:** Added `threat_component_breakdown_json`, `threat_top_reasons_json`, `threat_limiting_factors_json`, and `lifecycle_summary_json` columns to `storm_objects`. Added covering indexes on `storm_snapshots(site, scan_time)` and `storm_location_impacts(location_id, impact_rank)`.
- **Tests:** 68 tests passing (up from 59). Added `TestThreatComponentBreakdown` (3 tests) and `TestLifecycleAnalysis` (6 tests).
- **Tooling:** Added `scripts/benchmark.py` for repeatable performance measurement of rasterizer, lifecycle, threat computation, and motion estimation.



- Added first-generation volume-derived radar products: `ET`, `VIL`, `RR`, `QPE1H`, and `HC`.
- Added REF cross-sections through `/api/v1/cross-section` using available tilts from the raw Level II volume.
- Added split comparison mode, product legend panels, browser-side monitoring alert settings, and live range-ring / sweep-animation wiring in the frontend.
- Fixed API product validation so 2-character products such as `ET` and `RR` are queryable through the radar routes.
- Fixed a hydrometeor-classification ordering bug that could overwrite hail pixels with the mixed-phase class.
- Expanded default product and overlay profiles so fresh local deployments expose the advanced volume workflow immediately.

## 0.1.0

- Initial V1 release of the local-first radar platform.
- FastAPI API service for health, config, sites, products, frames, alerts, and status.
- Processor service for NOAA Level II ingestion, PNG raster generation, SQLite metadata, and alert caching.
- React + MapLibre frontend with radar playback, overlay controls, and warning polygon rendering.
- Documented analysis extension zone with stub analyzers for future severe-weather features.

## 0.2.0

- Added storm object persistence, tracking snapshots, saved locations, and storm-to-location impact estimates.
- Added a storm intelligence processor layer with reflectivity segmentation, motion tracking, forecast paths, surface-context environment fusion, and structured threat scoring.
- Added reusable `/api/v1` endpoints for storms, storm tracks, storm environments, storm impacts, locations, and METAR observations.
- Added frontend storm overlays, saved-location overlays, storm cards, explain-why reasoning, and saved location management.
- Improved repo hygiene with lockfile-based frontend Docker builds and expanded local/test cache handling.

## 0.3.0

- Promoted `SRV` to a real derived radar product rendered from processed Level II velocity volumes.
- Added raw-vs-derived product metadata so ingest only schedules real source products and the frontend can expose product availability honestly.
- Added rendered `CC` and `ZDR` product support through the shared color-table pipeline.
- Improved storm matching with projected-position scoring and smoothed motion vectors for steadier paths and ETA output.
- Upgraded environment fusion from METAR-only logic to a combined METAR plus NWS gridpoint forecast context layer.
- Added validation coverage for SRV derivation, product metadata, and the upgraded environment snapshot logic.

## 0.4.1

- Hardened retention so aged frames are removed without leaving broken storm/frame references in SQLite.
- Added cleanup for expired storms, old impact rows, and old environment snapshots to keep long-running local deployments bounded.
- Added `/api/status` freshness diagnostics for alerts, METAR, SPC, mesoscale discussions, local storm reports, processor age, and environment-snapshot age.
- Made SPC/MD/LSR overlay refresh respect the configured overlay cache TTL instead of re-fetching every processor cycle.
- Improved frontend production readiness with explicit vendor chunking and safer runtime-config Docker handling.

## 0.4.0

- Added Open-Meteo model-field ingestion for CAPE, CIN, approximate bulk shear, approximate low-level shear, approximate SRH, lapse rate, LCL, and freezing-level context.
- Added structured storm prediction summaries with projected trend, projected threats, projected confidence, and forecast reasoning factors.
- Upgraded saved-location impacts with projected-arrival threat details and ETA spread metadata.
- Added cached SPC outlook, mesoscale discussion, and local storm report overlay APIs plus frontend rendering/toggles.
- Added configured multi-tilt frame processing foundations, tilt metadata, radar tilt API support, and a frontend tilt selector.
- Reworked frontend runtime config so built nginx deployments use runtime `VITE_*` values instead of silently baking `localhost` into the bundle.
