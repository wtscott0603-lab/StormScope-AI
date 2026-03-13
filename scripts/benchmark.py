#!/usr/bin/env python3
"""benchmark.py — Lightweight profiling for radar11 processor hot paths.

Run from the repo root:
    python3 scripts/benchmark.py

Tests:
  - rasterizer._aggregate_pixels (REF scatter — bincount vs np.maximum.at comparison)
  - lifecycle.classify_lifecycle_trend
  - threats.compute_threats (without environment)
  - tracking.estimate_motion_from_history

All tests use synthetic in-memory data — no network or filesystem I/O required.
"""
from __future__ import annotations

import time
from typing import Callable, Any


def _timer(label: str, fn: Callable[[], Any], iterations: int = 100) -> float:
    """Time fn() over iterations runs and return mean ms."""
    start = time.perf_counter()
    for _ in range(iterations):
        fn()
    elapsed = (time.perf_counter() - start) / iterations * 1000
    print(f"  {label:<55s} {elapsed:8.3f} ms/call  ({iterations} iterations)")
    return elapsed


def bench_rasterizer() -> None:
    import numpy as np
    from backend.processor.processing.rasterizer import _aggregate_pixels

    rng = np.random.default_rng(42)
    n_gates = 300_000
    size = 1024
    values = rng.uniform(-5.0, 75.0, n_gates).astype(np.float32)
    x = rng.integers(0, size, n_gates, dtype=np.int32)
    y = rng.integers(0, size, n_gates, dtype=np.int32)

    print("\n[rasterizer]")
    _timer("_aggregate_pixels(REF, 300k gates, 1024×1024)", lambda: _aggregate_pixels("REF", values, x, y, size))
    _timer("_aggregate_pixels(VEL, 300k gates, 1024×1024)", lambda: _aggregate_pixels("VEL", values, x, y, size))


def bench_lifecycle() -> None:
    from unittest.mock import MagicMock
    from backend.processor.storms.lifecycle import classify_lifecycle_trend, classify_motion_trend

    def make_snaps():
        snaps = []
        for i in range(5):
            s = MagicMock()
            s.max_reflectivity = 50.0 + i * 2.5
            s.area_km2 = 80.0 + i * 12.0
            s.motion_speed_kmh = 40.0 + i * 5.0
            s.motion_heading_deg = 220.0 + i * 3.0
            snaps.append(s)
        return snaps

    snaps = make_snaps()
    print("\n[lifecycle]")
    _timer("classify_lifecycle_trend(5 snapshots)", lambda: classify_lifecycle_trend(snaps), iterations=1000)
    _timer("classify_motion_trend(5 snapshots)", lambda: classify_motion_trend(snaps), iterations=1000)


def bench_threats() -> None:
    from unittest.mock import MagicMock
    import numpy as np
    from backend.processor.storms.threats import compute_threats

    detection = MagicMock()
    detection.max_reflectivity = 58.0
    detection.mean_reflectivity = 48.0
    detection.area_km2 = 120.0
    detection.elongation_ratio = 2.2
    detection.core_fraction = 0.18
    detection.core_max_reflectivity = 62.0
    detection.radius_km = 12.0
    detection.gate_mask = np.ones(100, dtype=bool)

    env = {
        "hail_favorability": 0.65,
        "wind_favorability": 0.55,
        "tornado_favorability": 0.40,
        "heavy_rain_favorability": 0.30,
        "cape_jkg": 2200.0,
        "bulk_shear_06km_kt": 42.0,
        "bulk_shear_01km_kt": 16.0,
        "srh_surface_925hpa_m2s2": 180.0,
        "dcape_jkg": 900.0,
        "freezing_level_m": 3800.0,
        "lapse_rate_midlevel_cpkm": 7.5,
        "lcl_m": 950.0,
        "environment_confidence": 0.72,
        "environment_freshness_minutes": 25,
        "environment_ahead_delta": {},
    }

    print("\n[threats]")
    _timer("compute_threats (no history, with environment)", lambda: compute_threats(
        detection=detection,
        history=[],
        associated_signatures=[],
        environment_summary=env,
        srv_metrics=None,
        motion_speed_kmh=55.0,
        motion_heading_deg=220.0,
    ), iterations=200)


def bench_tracking() -> None:
    from datetime import datetime, timedelta, timezone
    from unittest.mock import MagicMock
    from backend.processor.storms.tracking import estimate_motion_from_history

    now = datetime.now(timezone.utc)
    snaps = []
    for i in range(5):
        s = MagicMock()
        s.scan_time = now - timedelta(minutes=(5 - i) * 6)
        s.centroid_lat = 36.0 + i * 0.04
        s.centroid_lon = -97.0 + i * 0.06
        snaps.append(s)

    print("\n[tracking]")
    _timer("estimate_motion_from_history(5 snapshots)", lambda: estimate_motion_from_history(
        36.2, -96.7, now, snaps
    ), iterations=2000)


if __name__ == "__main__":
    print("=" * 70)
    print("radar11 benchmark — processor hot paths")
    print("=" * 70)
    bench_rasterizer()
    bench_lifecycle()
    bench_threats()
    bench_tracking()
    print("\nDone.")
