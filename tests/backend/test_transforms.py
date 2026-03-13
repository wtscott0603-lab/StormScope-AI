from backend.processor.processing.transforms import geodetic_to_web_mercator, web_mercator_to_geodetic


def test_transform_round_trip():
    lon, lat = -88.084, 41.604
    x, y = geodetic_to_web_mercator(lon, lat)
    round_trip_lon, round_trip_lat = web_mercator_to_geodetic(x, y)

    assert abs(lon - round_trip_lon) < 1e-6
    assert abs(lat - round_trip_lat) < 1e-6
