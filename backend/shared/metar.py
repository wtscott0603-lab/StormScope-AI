from __future__ import annotations

import csv
import gzip
import io
import json
from pathlib import Path

from backend.shared.time import isoformat_utc


def _to_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_metar_cache_gz(raw_bytes: bytes) -> list[dict]:
    text = gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    observations: list[dict] = []
    for row in reader:
        station_id = (row.get("station_id") or "").strip().upper()
        lat = _to_float(row.get("latitude"))
        lon = _to_float(row.get("longitude"))
        if not station_id or lat is None or lon is None:
            continue
        observations.append(
            {
                "station_id": station_id,
                "observation_time": row.get("observation_time"),
                "lat": lat,
                "lon": lon,
                "temp_c": _to_float(row.get("temp_c")),
                "dewpoint_c": _to_float(row.get("dewpoint_c")),
                "wind_dir_deg": _to_float(row.get("wind_dir_degrees")),
                "wind_speed_kt": _to_float(row.get("wind_speed_kt")),
                "wind_gust_kt": _to_float(row.get("wind_gust_kt")),
                "visibility_mi": _to_float(row.get("visibility_statute_mi")),
                "pressure_hpa": _to_float(row.get("sea_level_pressure_mb")),
                "raw_text": row.get("raw_text"),
                "flight_category": row.get("flight_category"),
            }
        )
    return observations


def write_metar_cache(path: str | Path, observations: list[dict]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"fetched_at": isoformat_utc(), "observations": observations}, indent=2))


def load_metar_cache(path: str | Path) -> dict:
    target = Path(path)
    if not target.exists():
        return {"fetched_at": None, "observations": []}
    try:
        payload = json.loads(target.read_text())
    except json.JSONDecodeError:
        return {"fetched_at": None, "observations": []}
    return {
        "fetched_at": payload.get("fetched_at"),
        "observations": list(payload.get("observations", [])),
    }
