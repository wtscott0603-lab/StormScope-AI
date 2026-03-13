from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from backend.shared.time import utc_now


class FileCache:
    def __init__(self, cache_dir: Path) -> None:
        self.cache_dir = cache_dir
        self.raw_dir = cache_dir / "raw"
        self.image_dir = cache_dir / "images"
        self.alerts_dir = cache_dir / "alerts"
        self.overlays_dir = cache_dir / "overlays"
        self.environment_dir = cache_dir / "environment"
        self.status_dir = cache_dir / "status"

    def ensure_directories(self) -> None:
        for directory in (
            self.cache_dir,
            self.raw_dir,
            self.image_dir,
            self.alerts_dir,
            self.overlays_dir,
            self.environment_dir,
            self.status_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def raw_file_path(self, site: str, filename: str) -> Path:
        date_segment = filename[4:12]
        return self.raw_dir / site.upper() / date_segment / filename

    def image_file_path(self, site: str, product: str, frame_id: str) -> Path:
        return self.image_dir / site.upper() / product.upper() / f"{frame_id}.png"

    @property
    def processor_state_path(self) -> Path:
        return self.status_dir / "processor_state.json"

    def retention_cutoff(self, retention_hours: int):
        return utc_now() - timedelta(hours=retention_hours)
