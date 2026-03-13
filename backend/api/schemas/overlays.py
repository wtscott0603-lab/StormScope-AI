from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class OverlayFeatureCollectionResponse(BaseModel):
    overlay_kind: str | None = None
    source: str | None = None
    type: str = "FeatureCollection"
    fetched_at: str | None = None
    features: list[dict[str, Any]] = Field(default_factory=list)
