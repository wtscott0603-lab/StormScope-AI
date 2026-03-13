# Radar Platform

Radar Platform is a local-first weather stack for NOAA NEXRAD radar analysis. It packages a processor that ingests and renders radar data, a FastAPI API that serves radar and storm metadata, and a Vite/React frontend for playback and map-based monitoring.

## Table of Contents

- [Quick Start](#quick-start)
- [What You Get](#what-you-get)
- [Architecture](#architecture)
- [Configuration](#configuration)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Project Docs](#project-docs)

## Quick Start

```bash
docker compose up --build
```

That works in a fresh clone. `docker compose` now loads the checked-in defaults from `.env.example`, so a local `.env` file is optional.

If you want local overrides:

```bash
./scripts/bootstrap-env.sh
$EDITOR .env
docker compose up --build
```

Open these URLs after the stack is healthy:

- Frontend: `http://localhost:3000`
- API health: `http://localhost:8000/health`
- API example: `http://localhost:8000/api/v1/storms?site=KILN`
- API docs (OpenAPI): `http://localhost:8000/docs`

## What You Get

- **Processor**: Downloads NOAA Level II scans, renders products, runs post-processing analyzers, tracks storms, and persists cache/DB state.
- **API**: Serves frames, overlays, storm metadata, and health/status endpoints.
- **Frontend**: Map-based playback/monitoring UI with runtime-config injection (no rebuild needed to change browser-facing settings).

## Architecture

- `backend/processor/`: ingests NOAA Level II data, renders radar products, computes storm metadata, and writes shared cache/database state.
- `backend/api/`: FastAPI service that exposes radar, storm, overlay, and health endpoints.
- `frontend/`: Vite/React client served by nginx with runtime config injection.
- `docker-compose.yml`: local stack wiring for the processor, API, frontend, and shared volumes.

## Configuration

- `.env.example` is the documented and checked-in development default.
- `.env` is optional and overrides `.env.example` when present.
- Production or public deployments should override at least `CORS_ALLOWED_ORIGINS`, `CORS_ALLOW_CREDENTIALS`, `VITE_API_BASE_URL`, `API_PORT`, `FRONTEND_PORT`, `CACHE_DIR`, and `DB_PATH`.
- If you change `API_PORT`, also update `VITE_API_BASE_URL` so the browser points at the correct host port.

More detail lives in [SETUP.md](SETUP.md) and `docs/CONFIGURATION.md`.

## Development

Run the main local checks without Docker:

```bash
pip install -r backend/api/requirements.txt -r backend/processor/requirements.txt -r requirements-dev.txt
python3 -m pytest -q
npm ci --prefix frontend
npm run build --prefix frontend
```

Useful Docker checks:

```bash
docker compose config
docker compose build
docker compose up --build
```

## Production Notes

- The checked-in defaults are for local development, not hardened production.
- Replace wildcard CORS with explicit origins before exposing the API outside localhost.
- Review data paths, exposed ports, log levels, and any tile-provider/API values before publishing a deployment guide.
- The frontend runtime config is injected at container start, so browser-facing settings can be overridden without rebuilding the frontend image.

## Troubleshooting

- `env file .env not found`: this repo no longer requires `.env` for Docker Compose. If you still see this, make sure you are using the updated `docker-compose.yml` and running from the repository root.
- Frontend cannot reach the API: check `VITE_API_BASE_URL` in `.env` if you changed the API host port.
- Compose validation: run `docker compose config` first to confirm your overrides resolve cleanly.

## Project Docs

- [SETUP.md](SETUP.md)
- `docs/CONFIGURATION.md`
- `docs/DEVELOPMENT.md`
- [CONTRIBUTING.md](CONTRIBUTING.md)
- [SECURITY.md](SECURITY.md)
- [SUPPORT.md](SUPPORT.md)
