# Setup

## Local Docker Flow

1. Clone the repository.
2. Optional: create a local override file.

   ```bash
   ./scripts/bootstrap-env.sh
   ```

3. Start the stack.

   ```bash
   docker compose up --build
   ```

4. Open the app:
   - Frontend: `http://localhost:3000`
   - API health: `http://localhost:8000/health`
   - Storm API: `http://localhost:8000/api/v1/storms?site=KILN`
   - API docs (OpenAPI): `http://localhost:8000/docs`

`docker compose` works even when `.env` does not exist. Service containers always load the checked-in defaults from `.env.example`, and a local `.env` overrides those defaults when present.

## Optional Local Overrides

Use `.env` only when you want to change the default local behavior.

Common examples:

- Change ports: `API_PORT`, `FRONTEND_PORT`
- Point the frontend at a different API URL: `VITE_API_BASE_URL`
- Change the default site or map focus: `DEFAULT_SITE`, `DEFAULT_CENTER_LAT`, `DEFAULT_CENTER_LON`, `DEFAULT_MAP_ZOOM`
- Adjust processing cadence or retention: `UPDATE_INTERVAL_SEC`, `MAX_FRAMES_PER_SITE`, `RETENTION_HOURS`

The frontend runtime config inherits the shared `DEFAULT_*`, `DEFAULT_ENABLED_OVERLAYS`, and `PREFERRED_UNITS` values. Set `VITE_*` variants only if you need the UI to differ from the API/processor defaults.

For a full variable reference, see `docs/CONFIGURATION.md`.

## Common Docker Workflows

### Start only backend services (headless)

```bash
docker compose up --build processor api
```

### Rebuild a single service

```bash
docker compose build api
docker compose up api
```

### Reset local data (cache + DB)

By default, Compose stores data in named volumes (`radar-cache`, `radar-db`). To wipe everything:

```bash
docker compose down -v
```

## Production Overrides

Before publishing a deployment or exposing the app outside localhost, review and override:

- `CORS_ALLOWED_ORIGINS`: replace `*` with explicit origins
- `CORS_ALLOW_CREDENTIALS`: keep `false` unless you have trusted explicit origins
- `VITE_API_BASE_URL`: point browsers at the public API URL
- `API_PORT` and `FRONTEND_PORT`: choose the exposed ports intentionally
- `CACHE_DIR` and `DB_PATH`: use deployment-appropriate persistent storage
- Any tile-provider or site defaults you do not want to expose publicly

## Verification Commands

```bash
docker compose config
docker compose build
docker compose up --build
python3 -m pytest -q
npm ci --prefix frontend
npm run build --prefix frontend
```

## Troubleshooting

- Missing `.env`: not required anymore. If you want a local override file, run `./scripts/bootstrap-env.sh`.
- Browser cannot reach the API: if `API_PORT` changed, update `VITE_API_BASE_URL` too.
- Build failures after config edits: run `docker compose config` first, then `docker compose build frontend api processor`.
