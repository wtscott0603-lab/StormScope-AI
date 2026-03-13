# Configuration

Radar Platform uses environment variables for configuration.

- The **documented local defaults** live in `.env.example` (checked in).
- A local `.env` file is **optional**. If present, it overrides `.env.example` for Docker Compose and for local (non-Docker) runs that load `.env`.

## Quick rules

- **If you change `API_PORT`**, also change `VITE_API_BASE_URL` so browsers point at the correct host port.
- **Don’t expose wildcard CORS (`*`) to the internet.** Use explicit origins before any non-local deployment.
- **Data lives in volumes by default.** Removing containers does not remove your cached frames/DB; see the reset commands in `SETUP.md`.

## Environment variable reference

Values shown below match `.env.example` unless noted.

### Radar + UI defaults (shared)

- **`DEFAULT_SITE`**: Default NEXRAD site ID (e.g. `KILN`).
- **`DEFAULT_CENTER_LAT` / `DEFAULT_CENTER_LON` / `DEFAULT_MAP_ZOOM`**: Initial map view.
- **`PREFERRED_UNITS`**: `imperial` or `metric` (frontend uses this for display).
- **`DEFAULT_ENABLED_OVERLAYS`**: Comma-separated overlays enabled on first load.
- **`LOCAL_STATION_PRIORITY`**: Comma-separated METAR station IDs preferred for surface context.
- **`DEFAULT_SAVED_LOCATIONS_JSON`**: Optional JSON list of saved locations to seed on startup.

### Processor / ingest

- **`ENABLED_PRODUCTS`**: Comma-separated radar products to ingest/render (e.g. `REF,VEL,...`).
- **`ENABLED_TILTS`**: Comma-separated elevation tilts (e.g. `0.5,1.5`).
- **`UPDATE_INTERVAL_SEC`**: Processor ingest cadence.
- **`MAX_FRAMES_PER_SITE`**: Limits retained frames per site/product/tilt window in the DB.
- **`RETENTION_HOURS`**: Retention window for raw files and images (structured history is kept longer).
- **`IMAGE_SIZE`**: Rendered raster size (pixels), e.g. `1024`.
- **`S3_BUCKET`**: Level II source bucket (default `noaa-nexrad-level2`).
- **`REQUEST_TTL_MINUTES`**: TTL for requested-sites tracking.

### Storm tracking + environment overlays

- **`STORM_REFLECTIVITY_THRESHOLD_DBZ`**
- **`STORM_MIN_AREA_KM2`**
- **`STORM_TRACK_HORIZON_MIN`**
- **`STORM_TRACK_STEP_MIN`**
- **`METAR_CACHE_TTL_MINUTES`**
- **`GRID_FORECAST_CACHE_TTL_MINUTES`**
- **`OPEN_METEO_CACHE_TTL_MINUTES`**
- **`OVERLAY_CACHE_TTL_MINUTES`**

### Storage

- **`CACHE_DIR`**: Cache root (Docker default: `/data/cache`).
- **`DB_PATH`**: SQLite DB path (Docker default: `/data/db/radar.db`).

### API

- **`API_HOST`**: Bind address in-container (Docker default: `0.0.0.0`).
- **`API_PORT`**: Host port mapped to container `8000` by Compose.
- **`LOG_LEVEL`**: e.g. `INFO`.
- **`CORS_ALLOWED_ORIGINS`**: Comma-separated list or `*` for local development.
- **`CORS_ALLOW_CREDENTIALS`**: Keep `false` unless using explicit trusted origins.

### Frontend runtime config (browser-facing)

The frontend reads a runtime `config.js` generated at container start. It uses `VITE_*` variables when set, otherwise it falls back to the shared defaults (`DEFAULT_*`, `PREFERRED_UNITS`, etc.).

- **`FRONTEND_PORT`**: Host port mapped to nginx `80` by Compose.
- **`VITE_API_BASE_URL`**: Browser → API base URL (e.g. `http://localhost:8000`).
- **`VITE_DEFAULT_SITE`**: Optional frontend-only override for `DEFAULT_SITE`.
- **`VITE_MAP_TILE_URL`** / **`VITE_MAP_TILE_ATTRIBUTION`**: Map tile source and attribution.
- **`VITE_DEFAULT_CENTER_LAT` / `VITE_DEFAULT_CENTER_LON` / `VITE_DEFAULT_MAP_ZOOM`**: Optional frontend-only map overrides.
- **`VITE_PREFERRED_UNITS`**: Optional frontend-only units override.
- **`VITE_DEFAULT_ENABLED_OVERLAYS`**: Optional frontend-only overlay override.

## Where is runtime config generated?

- Frontend runtime config file: `frontend/public/config.js` (placeholder in repo)
- Generated at container start by: `frontend/docker-entrypoint.d/40-runtime-config.sh`

