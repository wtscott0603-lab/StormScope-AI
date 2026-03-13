# Development

This project is designed to run as a local stack (processor + API + frontend) via Docker Compose, but you can also run checks without Docker.

## Repo layout

- `backend/processor/`: ingestion + rendering + analysis + storm tracking (writes cache + DB)
- `backend/api/`: FastAPI service (reads cache + DB; serves images/metadata)
- `frontend/`: Vite/React UI served by nginx (runtime config injected at start)

## Common workflows

### Run the full stack (recommended)

```bash
docker compose up --build
```

### Run only the backend services (headless)

Useful if you want the processor + API running without the UI.

```bash
docker compose up --build processor api
```

### Run only the frontend container

The UI needs an API to be useful. If you point it at a remote API, set `VITE_API_BASE_URL` in `.env`.

```bash
docker compose up --build frontend
```

## Local checks (without Docker)

```bash
pip install -r backend/api/requirements.txt -r backend/processor/requirements.txt -r requirements-dev.txt
python3 -m pytest -q

npm ci --prefix frontend
npm run build --prefix frontend
```

## API docs

When the API is running locally (Compose default):

- OpenAPI UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`
- Health: `http://localhost:8000/health`

## Testing notes

- Tests live under `tests/` and exercise the FastAPI app and key processing utilities.
- If you add a new route or analyzer, prefer adding at least one focused test.

