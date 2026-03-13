from __future__ import annotations

import numpy as np

from backend.processor.processing.volume_products import (
    GridProduct,
    HC_DEBRIS,
    HC_HAIL,
    HC_HEAVY_RAIN,
    HC_MODERATE_RAIN,
    HC_RAIN,
    _hydrometeor_classification,
    _rain_rate_from_dbz,
    sample_volume_metrics,
)


def test_rain_rate_monotonically_increases_with_reflectivity():
    rates = _rain_rate_from_dbz(np.array([20.0, 35.0, 45.0, 55.0], dtype=np.float32))

    assert np.all(np.isfinite(rates))
    assert list(rates) == sorted(rates.tolist())
    assert rates[-1] > rates[0]


def test_hydrometeor_classification_distinguishes_heavy_rain_hail_and_debris():
    ref = np.array([[28.0, 42.0, 52.0, 58.0, 42.0]], dtype=np.float32)
    zdr = np.array([[0.6, 1.1, 1.4, 0.2, 0.0]], dtype=np.float32)
    cc = np.array([[0.98, 0.97, 0.97, 0.9, 0.78]], dtype=np.float32)
    kdp = np.array([[0.0, 0.0, 1.0, 0.0, 0.0]], dtype=np.float32)

    classes = _hydrometeor_classification(ref, zdr, cc, kdp)

    assert int(classes[0, 0]) == HC_RAIN
    assert int(classes[0, 1]) == HC_MODERATE_RAIN
    assert int(classes[0, 2]) == HC_HEAVY_RAIN
    assert int(classes[0, 3]) == HC_HAIL
    assert int(classes[0, 4]) == HC_DEBRIS


def test_hydrometeor_classification_keeps_moderate_rain_out_of_mixed_bucket():
    ref = np.array([[39.0]], dtype=np.float32)
    zdr = np.array([[1.4]], dtype=np.float32)
    cc = np.array([[0.96]], dtype=np.float32)

    classes = _hydrometeor_classification(ref, zdr, cc)

    assert int(classes[0, 0]) == HC_MODERATE_RAIN


def test_sample_volume_metrics_reports_local_maxima_and_dominant_hydrometeor():
    latitudes = np.array([[40.01, 40.01], [40.02, 40.02]], dtype=np.float32)
    longitudes = np.array([[-82.9, -82.88], [-82.9, -82.88]], dtype=np.float32)

    metrics = sample_volume_metrics(
        {
            "ET": GridProduct(product="ET", values=np.array([[9.0, 11.5], [8.0, np.nan]], dtype=np.float32), latitudes=latitudes, longitudes=longitudes),
            "VIL": GridProduct(product="VIL", values=np.array([[18.0, 34.0], [25.0, np.nan]], dtype=np.float32), latitudes=latitudes, longitudes=longitudes),
            "KDP": GridProduct(product="KDP", values=np.array([[0.5, 1.8], [0.7, np.nan]], dtype=np.float32), latitudes=latitudes, longitudes=longitudes),
            "RR": GridProduct(product="RR", values=np.array([[12.0, 48.0], [15.0, np.nan]], dtype=np.float32), latitudes=latitudes, longitudes=longitudes),
            "QPE1H": GridProduct(product="QPE1H", values=np.array([[6.0, 19.0], [8.5, np.nan]], dtype=np.float32), latitudes=latitudes, longitudes=longitudes),
            "HC": GridProduct(product="HC", values=np.array([[HC_RAIN, HC_HAIL], [HC_HAIL, HC_HAIL]], dtype=np.float32), latitudes=latitudes, longitudes=longitudes),
        },
        centroid_lat=40.015,
        centroid_lon=-82.89,
        radius_km=4.0,
    )

    assert metrics["max_echo_tops_km"] == 11.5
    assert metrics["max_vil_kgm2"] == 34.0
    assert metrics["max_vil_density_gm3"] == 3.12
    assert metrics["max_kdp_degkm"] == 1.8
    assert metrics["max_rain_rate_mmhr"] == 48.0
    assert metrics["max_qpe_1h_mm"] == 19.0
    assert metrics["dominant_hydrometeor"] == "hail"
