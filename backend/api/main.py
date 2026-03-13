from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.api.config import get_settings
from backend.api.routers import alerts, config, health, locations_v1, metar_v1, overlays_v1, products, radar, sites, status, storms_v1
from backend.shared.db import init_db
from backend.shared.logging import configure_logging


LOGGER = logging.getLogger(__name__)


def create_app() -> FastAPI:
    settings = get_settings()
    configure_logging(settings.log_level)
    static_dir = Path(__file__).resolve().parent / "static"
    cors_origins = settings.cors_allowed_origins
    allow_credentials = settings.cors_allow_credentials and cors_origins != ["*"]

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await init_db(settings.db_path)
        yield

    app = FastAPI(title="Radar Platform API", version=settings.app_version, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.exception_handler(HTTPException)
    async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
        detail = exc.detail if isinstance(exc.detail, str) else None
        error = "not_found" if exc.status_code == 404 else "request_error"
        return JSONResponse(status_code=exc.status_code, content={"error": error, "detail": detail})

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"error": "validation_error", "detail": str(exc)})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(_: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("Unhandled API exception")
        return JSONResponse(status_code=500, content={"error": "internal_server_error", "detail": None})

    app.include_router(health.router)
    app.include_router(config.router)
    app.include_router(sites.router)
    app.include_router(products.router)
    app.include_router(radar.router)
    app.include_router(alerts.router)
    app.include_router(status.router)
    app.include_router(storms_v1.router)
    app.include_router(locations_v1.router)
    app.include_router(metar_v1.router)
    app.include_router(overlays_v1.router)
    return app


app = create_app()
