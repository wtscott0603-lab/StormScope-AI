from __future__ import annotations

import json
from pathlib import Path

import geojson
import httpx

from backend.shared.time import isoformat_utc


STATE_FIPS = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "60": "AS",
    "66": "GU",
    "69": "MP",
    "72": "PR",
    "78": "VI",
}


async def fetch_active_alerts() -> list[dict]:
    headers = {
        "Accept": "application/geo+json",
        "User-Agent": "radar-platform/0.1 (local deployment)",
    }
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as client:
        response = await client.get("https://api.weather.gov/alerts/active")
        response.raise_for_status()
        payload = response.json()

    alerts: list[dict] = []
    for feature in payload.get("features", []):
        geometry = feature.get("geometry")
        if geometry is None:
            continue
        properties = feature.get("properties", {})
        same_codes = properties.get("geocode", {}).get("SAME", [])
        states = sorted({STATE_FIPS.get(code[:2]) for code in same_codes if len(code) >= 2 and STATE_FIPS.get(code[:2])})
        feature_obj = geojson.Feature(geometry=geometry, properties={})
        alerts.append(
            {
                "id": properties.get("id") or feature.get("id"),
                "event": properties.get("event", "Unknown"),
                "severity": properties.get("severity", "Unknown"),
                "issued": properties.get("sent") or properties.get("effective"),
                "expires": properties.get("expires"),
                "geometry": feature_obj.geometry,
                "state_codes": states,
            }
        )
    return alerts


def write_alert_cache(path: str | Path, alerts: list[dict]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"fetched_at": isoformat_utc(), "alerts": alerts}, indent=2))
