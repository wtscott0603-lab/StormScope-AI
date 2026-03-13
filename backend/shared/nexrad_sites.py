from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from backend.shared.models import SiteInfo


SITES_PATH = Path(__file__).with_name("nexrad_sites.json")


@lru_cache(maxsize=1)
def load_sites() -> list[SiteInfo]:
    raw_sites = json.loads(SITES_PATH.read_text())
    return [SiteInfo(**site) for site in raw_sites]


@lru_cache(maxsize=1)
def load_sites_by_id() -> dict[str, SiteInfo]:
    return {site.id: site for site in load_sites()}


def get_site(site_id: str) -> SiteInfo | None:
    return load_sites_by_id().get(site_id.upper())
