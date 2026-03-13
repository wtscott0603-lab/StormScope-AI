from __future__ import annotations

import math

import numpy as np

from backend.processor.analysis.base import ProcessedFrame, SweepArrays
from backend.processor.analysis.debris import DebrisAnalyzer
from backend.processor.analysis.hail import HailAnalyzer
from backend.processor.analysis.rotation import RotationAnalyzer
from backend.processor.analysis.wind import WindAnalyzer


def make_polar_sweep(
  n_rays: int = 24,
  n_gates: int = 8,
  *,
  site_lat: float = 41.0,
  site_lon: float = -88.0,
) -> SweepArrays:
  azimuths = np.linspace(0.0, 360.0, n_rays, endpoint=False, dtype=np.float32)
  ranges_km = np.linspace(5.0, 5.0 * n_gates, n_gates, dtype=np.float32)
  latitudes = np.zeros((n_rays, n_gates), dtype=np.float32)
  longitudes = np.zeros((n_rays, n_gates), dtype=np.float32)

  for ray_index, azimuth in enumerate(azimuths):
    azimuth_rad = math.radians(float(azimuth))
    for gate_index, range_km in enumerate(ranges_km):
      latitudes[ray_index, gate_index] = site_lat + (range_km * math.cos(azimuth_rad) / 111.0)
      longitudes[ray_index, gate_index] = site_lon + (
        range_km * math.sin(azimuth_rad) / (111.0 * math.cos(math.radians(site_lat)))
      )

  return SweepArrays(
    values=np.full((n_rays, n_gates), np.nan, dtype=np.float32),
    latitudes=latitudes,
    longitudes=longitudes,
    azimuths=azimuths,
    ranges_km=ranges_km,
    site_lat=site_lat,
    site_lon=site_lon,
    nyquist_velocity=29.0,
  )


def make_rect_sweep(
  n_rays: int = 10,
  n_gates: int = 20,
  *,
  site_lat: float = 39.5,
  site_lon: float = -97.5,
) -> SweepArrays:
  latitudes = np.zeros((n_rays, n_gates), dtype=np.float32)
  longitudes = np.zeros((n_rays, n_gates), dtype=np.float32)

  for ray_index in range(n_rays):
    for gate_index in range(n_gates):
      latitudes[ray_index, gate_index] = site_lat + ray_index * 0.015
      longitudes[ray_index, gate_index] = site_lon + gate_index * 0.03

  return SweepArrays(
    values=np.full((n_rays, n_gates), np.nan, dtype=np.float32),
    latitudes=latitudes,
    longitudes=longitudes,
    azimuths=np.linspace(0.0, 360.0, n_rays, endpoint=False, dtype=np.float32),
    ranges_km=np.linspace(10.0, 10.0 * n_gates, n_gates, dtype=np.float32),
    site_lat=site_lat,
    site_lon=site_lon,
    nyquist_velocity=29.0,
  )


def test_rotation_analyzer_detects_tvs() -> None:
  sweep = make_polar_sweep()
  sweep.values[:] = 0.0
  sweep.values[10, 3] = -30.0
  sweep.values[11, 3] = 30.0
  frame = ProcessedFrame(frame_id='VEL_TEST', site='KLOT', product='VEL', image_path='/tmp/vel.png', sweep=sweep)

  result = RotationAnalyzer().run(frame)

  assert result.payload['status'] == 'ok'
  assert result.payload['signatures']
  signature = result.payload['signatures'][0]
  assert signature['signature_type'] == 'TVS'
  assert signature['severity'] == 'TORNADO'
  assert signature['metrics']['shear_per_sec'] >= 0.03


def test_rotation_analyzer_skips_non_velocity() -> None:
  result = RotationAnalyzer().run(
    ProcessedFrame(frame_id='REF_TEST', site='KLOT', product='REF', image_path='/tmp/ref.png', sweep=None)
  )

  assert result.payload['status'] == 'skipped'
  assert result.payload['signatures'] == []


def test_rotation_analyzer_accepts_srv_product() -> None:
  sweep = make_polar_sweep()
  sweep.values[:] = 0.0
  sweep.values[10, 3] = -28.0
  sweep.values[11, 3] = 29.0

  result = RotationAnalyzer().run(
    ProcessedFrame(frame_id='SRV_TEST', site='KLOT', product='SRV', image_path='/tmp/srv.png', sweep=sweep)
  )

  assert result.payload['status'] == 'ok'
  assert result.payload['signatures']


def test_debris_analyzer_detects_tds_with_ref_and_cc() -> None:
  ref_sweep = make_polar_sweep()
  cc_sweep = make_polar_sweep()
  ref_sweep.values[:] = 55.0
  cc_sweep.values[:] = 0.5
  analyzer = DebrisAnalyzer()

  skipped = analyzer.run(
    ProcessedFrame(frame_id='REF_SKIP', site='KLOT', product='REF', image_path='/tmp/ref.png', sweep=None),
    {},
  )
  result = analyzer.run(
    ProcessedFrame(frame_id='REF_TDS', site='KLOT', product='REF', image_path='/tmp/ref.png', sweep=ref_sweep),
    {'ref_sweep': ref_sweep, 'cc_sweep': cc_sweep},
  )

  assert skipped.payload['status'] == 'skipped'
  assert result.payload['status'] == 'ok'
  assert result.payload['signatures']
  signature = result.payload['signatures'][0]
  assert signature['signature_type'] == 'TDS'
  assert signature['severity'] in {'TORNADO', 'TORNADO_EMERGENCY'}


def test_hail_analyzer_detects_large_hail_and_zdr_confirmation() -> None:
  ref_sweep = make_polar_sweep()
  zdr_sweep = make_polar_sweep()
  ref_sweep.values[5:8, 3:6] = 62.0
  zdr_sweep.values[5:8, 3:6] = 0.0
  frame = ProcessedFrame(frame_id='REF_HAIL', site='KLOT', product='REF', image_path='/tmp/ref.png', sweep=ref_sweep)

  result = HailAnalyzer().run(frame, {'zdr_sweep': zdr_sweep})

  assert result.payload['status'] == 'ok'
  assert result.payload['signatures']
  signature = result.payload['signatures'][0]
  assert signature['signature_type'] == 'HAIL_LARGE'
  assert signature['metrics']['zdr_confirmed'] is True
  assert signature['radius_km'] > 0
  assert math.isfinite(signature['lat'])
  assert math.isfinite(signature['lon'])


def test_wind_analyzer_detects_bow_echo_and_bwer() -> None:
  ref_sweep = make_rect_sweep()
  vel_sweep = make_rect_sweep()
  ref_sweep.values[3:7, 2:16] = 50.0
  ref_sweep.values[4:6, 11:16] = 58.0
  ref_sweep.values[5, 8] = 20.0
  ref_sweep.values[4, 8] = 55.0
  ref_sweep.values[6, 8] = 55.0
  ref_sweep.values[5, 7] = 55.0
  ref_sweep.values[5, 9] = 55.0
  vel_sweep.values[3:7, 11:16] = 32.0
  frame = ProcessedFrame(frame_id='REF_WIND', site='KICT', product='REF', image_path='/tmp/ref.png', sweep=ref_sweep)

  result = WindAnalyzer().run(frame, {'vel_sweep': vel_sweep})
  skip = WindAnalyzer().run(
    ProcessedFrame(frame_id='VEL_WIND_SKIP', site='KICT', product='VEL', image_path='/tmp/vel.png', sweep=None)
  )

  assert result.payload['status'] == 'ok'
  signature_types = {signature['signature_type'] for signature in result.payload['signatures']}
  assert 'BOW_ECHO' in signature_types
  assert 'BWER' in signature_types
  assert skip.payload['status'] == 'skipped'
  assert skip.payload['signatures'] == []
