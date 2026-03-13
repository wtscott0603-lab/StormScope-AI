from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class AlertResponse(BaseModel):
    id: str
    event: str
    severity: str
    issued: datetime | None
    expires: datetime | None
    geometry: dict[str, Any]
