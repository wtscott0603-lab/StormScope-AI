import numpy as np

from backend.processor.processing.colortables import (
    correlation_coefficient_to_rgba,
    differential_reflectivity_to_rgba,
    reflectivity_to_rgba,
    specific_differential_phase_to_rgba,
    storm_relative_velocity_to_rgba,
)


def test_reflectivity_color_table_bins():
    values = np.array([[0.0, 7.0, 15.0, 25.0, 35.0, 45.0, 55.0, 62.0, 70.0]], dtype=np.float32)
    rgba = reflectivity_to_rgba(values)[0]

    assert tuple(rgba[0]) == (0, 0, 0, 0)
    assert tuple(rgba[1]) == (102, 204, 255, 255)
    assert tuple(rgba[2]) == (0, 112, 255, 255)
    assert tuple(rgba[3]) == (0, 190, 0, 255)
    assert tuple(rgba[4]) == (255, 230, 0, 255)
    assert tuple(rgba[5]) == (255, 140, 0, 255)
    assert tuple(rgba[6]) == (255, 0, 0, 255)
    assert tuple(rgba[7]) == (255, 0, 255, 255)
    assert tuple(rgba[8]) == (255, 255, 255, 255)


def test_dual_pol_and_srv_color_tables_have_distinct_bins():
    cc_values = np.array([[0.1, 0.45, 0.8, 0.97]], dtype=np.float32)
    zdr_values = np.array([[-3.0, -1.0, 0.5, 2.0, 7.0]], dtype=np.float32)
    kdp_values = np.array([[0.0, 0.3, 0.8, 1.5, 5.5]], dtype=np.float32)
    srv_values = np.array([[-50.0, -15.0, 0.0, 15.0, 50.0]], dtype=np.float32)

    cc_rgba = correlation_coefficient_to_rgba(cc_values)[0]
    zdr_rgba = differential_reflectivity_to_rgba(zdr_values)[0]
    kdp_rgba = specific_differential_phase_to_rgba(kdp_values)[0]
    srv_rgba = storm_relative_velocity_to_rgba(srv_values)[0]

    assert tuple(cc_rgba[0]) == (0, 0, 0, 0)
    assert tuple(cc_rgba[-1]) == (255, 255, 255, 255)
    assert tuple(zdr_rgba[0]) == (0, 0, 0, 0)
    assert tuple(zdr_rgba[-1]) == (255, 64, 0, 255)
    assert tuple(kdp_rgba[0]) == (0, 0, 0, 0)
    assert tuple(kdp_rgba[-1]) == (190, 24, 93, 255)
    assert tuple(srv_rgba[2]) == (0, 0, 0, 0)
    assert tuple(srv_rgba[0]) == (138, 43, 226, 255)
    assert tuple(srv_rgba[-1]) == (255, 0, 0, 255)
