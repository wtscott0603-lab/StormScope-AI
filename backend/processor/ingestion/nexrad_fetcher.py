from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.processor.cache.file_cache import FileCache
from backend.processor.cache.frame_store import FrameStore
from backend.processor.config import ProcessorSettings
from backend.processor.ingestion.s3_provider import download_key, list_level2_keys, parse_scan_time
from backend.processor.processing.level2_parser import list_available_tilts
from backend.shared.products import raw_enabled_products


LOGGER = logging.getLogger(__name__)


class NexradFetcher:
    def __init__(self, settings: ProcessorSettings, frame_store: FrameStore, file_cache: FileCache) -> None:
        self.settings = settings
        self.frame_store = frame_store
        self.file_cache = file_cache

    def _candidate_prefixes(self, site: str, now: datetime | None = None) -> list[str]:
        current = now or datetime.now(timezone.utc)
        prefixes = []
        for date_value in (current, current - timedelta(days=1)):
            prefixes.append(f"{date_value:%Y/%m/%d}/{site.upper()}/")
        return prefixes

    async def _list_recent_keys(self, site: str) -> list[str]:
        keys: list[str] = []
        for prefix in self._candidate_prefixes(site):
            listed = await asyncio.to_thread(list_level2_keys, self.settings.s3_bucket, prefix)
            keys.extend(listed)
        deduped = sorted(set(keys), key=parse_scan_time)
        return deduped[-self.settings.max_frames_per_site :]

    async def ingest_site(self, site: str) -> int:
        added = 0
        raw_products = raw_enabled_products(self.settings.enabled_products)
        for key in await self._list_recent_keys(site):
            filename = Path(key).name
            destination = self.file_cache.raw_file_path(site, filename)
            await asyncio.to_thread(download_key, self.settings.s3_bucket, key, destination)
            scan_time = parse_scan_time(key)
            for product in raw_products:
                try:
                    available_tilts = await asyncio.to_thread(list_available_tilts, destination, product)
                except Exception:
                    available_tilts = [0.5]
                scheduled_tilts: list[float] = []
                for desired_tilt in self.settings.enabled_tilts:
                    if not available_tilts:
                        continue
                    actual_tilt = min(available_tilts, key=lambda value: abs(value - desired_tilt))
                    if all(abs(existing - actual_tilt) >= 0.11 for existing in scheduled_tilts):
                        scheduled_tilts.append(actual_tilt)
                if not scheduled_tilts:
                    scheduled_tilts = [0.5]
                for tilt in scheduled_tilts:
                    frame_id = f"{site.upper()}_{scan_time:%Y%m%dT%H%M%S}_{product}_T{int(round(tilt * 10)):02d}"
                    inserted = await self.frame_store.insert_raw_frame(
                        frame_id=frame_id,
                        site=site.upper(),
                        product=product,
                        tilt=tilt,
                        scan_time=scan_time,
                        raw_path=str(destination),
                    )
                    added += int(inserted)
        LOGGER.info("Ingested site %s, added %s frame rows", site.upper(), added)
        return added

    async def ingest_sites(self, sites: list[str]) -> int:
        total = 0
        for site in sites:
            try:
                total += await self.ingest_site(site)
            except Exception as exc:
                LOGGER.exception("Failed to ingest site %s", site)
                raise RuntimeError(f"Failed to ingest site {site}: {exc}") from exc
        return total
