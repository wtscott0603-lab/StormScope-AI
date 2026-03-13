from __future__ import annotations

import math
from typing import Iterable

try:
    from pyproj import CRS, Transformer
    from shapely.geometry import LineString, MultiPoint, Point, shape
    from shapely.ops import transform

    HAS_SHAPELY = True
except ImportError:  # pragma: no cover - exercised implicitly in lightweight local envs
    CRS = Transformer = None  # type: ignore[assignment]
    LineString = MultiPoint = Point = None  # type: ignore[assignment]
    shape = transform = None  # type: ignore[assignment]
    HAS_SHAPELY = False

from backend.processor.analysis.utils import haversine_km


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lon = math.radians(lon2 - lon1)
    x = math.sin(delta_lon) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)
    return (math.degrees(math.atan2(x, y)) + 360.0) % 360.0


def destination_point(lat: float, lon: float, bearing: float, distance_km: float) -> tuple[float, float]:
    radius_earth_km = 6371.0
    bearing_rad = math.radians(bearing)
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    angular_distance = distance_km / radius_earth_km

    dest_lat = math.asin(
        math.sin(lat_rad) * math.cos(angular_distance)
        + math.cos(lat_rad) * math.sin(angular_distance) * math.cos(bearing_rad)
    )
    dest_lon = lon_rad + math.atan2(
        math.sin(bearing_rad) * math.sin(angular_distance) * math.cos(lat_rad),
        math.cos(angular_distance) - math.sin(lat_rad) * math.sin(dest_lat),
    )
    return math.degrees(dest_lat), ((math.degrees(dest_lon) + 540.0) % 360.0) - 180.0


def make_footprint(points: Iterable[tuple[float, float]]) -> dict:
    coords = list(points)
    if not coords:
        return {"type": "Polygon", "coordinates": []}

    if HAS_SHAPELY:
        multipoint = MultiPoint(coords)
        hull = multipoint.convex_hull
        if hull.geom_type == "Point":
            hull = hull.buffer(0.02)
        elif hull.geom_type == "LineString":
            hull = hull.buffer(0.01)
        return {
            "type": "Polygon",
            "coordinates": [list(hull.exterior.coords)],
        }

    longitudes = [lon for lon, _ in coords]
    latitudes = [lat for _, lat in coords]
    min_lon, max_lon = min(longitudes), max(longitudes)
    min_lat, max_lat = min(latitudes), max(latitudes)
    return {
        "type": "Polygon",
        "coordinates": [[[min_lon, min_lat], [max_lon, min_lat], [max_lon, max_lat], [min_lon, max_lat], [min_lon, min_lat]]],
    }


_TRANSFORMER_CACHE: dict[tuple[int, int], "Transformer"] = {}  # keyed by (lat*10, lon*10)


def _local_transformer(center_lat: float, center_lon: float) -> Transformer:
    if not HAS_SHAPELY:
        raise RuntimeError("Projected transformer requested without shapely/pyproj support")
    # Quantize to 0.1° grid — sufficient accuracy for storm polygons, avoids
    # rebuilding a CRS for every polygon-overlap call (which is O(N×M) per scan).
    key = (round(center_lat * 10), round(center_lon * 10))
    cached = _TRANSFORMER_CACHE.get(key)
    if cached is not None:
        return cached
    local_crs = CRS.from_proj4(f"+proj=aeqd +lat_0={center_lat} +lon_0={center_lon} +datum=WGS84 +units=m +no_defs")
    transformer = Transformer.from_crs("EPSG:4326", local_crs, always_xy=True)
    # Cap cache size at 512 entries (~most radar domains ever seen in one session).
    if len(_TRANSFORMER_CACHE) >= 512:
        _TRANSFORMER_CACHE.clear()
    _TRANSFORMER_CACHE[key] = transformer
    return transformer


def polygon_area_km2(footprint_geojson: dict, center_lat: float, center_lon: float) -> float:
    if not HAS_SHAPELY:
        coordinates = footprint_geojson.get("coordinates", [[]])[0]
        if len(coordinates) < 4:
            return 0.0
        longitudes = [point[0] for point in coordinates]
        latitudes = [point[1] for point in coordinates]
        lon_span_km = max(longitudes) - min(longitudes)
        lat_span_km = max(latitudes) - min(latitudes)
        return abs(lon_span_km * 111.0 * math.cos(math.radians(center_lat)) * lat_span_km * 111.0)
    transformer = _local_transformer(center_lat, center_lon)
    projected = transform(transformer.transform, shape(footprint_geojson))
    return projected.area / 1_000_000.0


def polygon_overlap_ratio(left_geojson: dict, right_geojson: dict) -> float:
    if not HAS_SHAPELY:
        left = left_geojson.get("coordinates", [[]])[0]
        right = right_geojson.get("coordinates", [[]])[0]
        if len(left) < 4 or len(right) < 4:
            return 0.0
        left_min_lon = min(point[0] for point in left)
        left_max_lon = max(point[0] for point in left)
        left_min_lat = min(point[1] for point in left)
        left_max_lat = max(point[1] for point in left)
        right_min_lon = min(point[0] for point in right)
        right_max_lon = max(point[0] for point in right)
        right_min_lat = min(point[1] for point in right)
        right_max_lat = max(point[1] for point in right)
        intersection_lon = max(0.0, min(left_max_lon, right_max_lon) - max(left_min_lon, right_min_lon))
        intersection_lat = max(0.0, min(left_max_lat, right_max_lat) - max(left_min_lat, right_min_lat))
        intersection = intersection_lon * intersection_lat
        left_area = (left_max_lon - left_min_lon) * (left_max_lat - left_min_lat)
        right_area = (right_max_lon - right_min_lon) * (right_max_lat - right_min_lat)
        union = left_area + right_area - intersection
        return intersection / union if union > 0 else 0.0
    left_shape = shape(left_geojson)
    right_shape = shape(right_geojson)
    if left_shape.is_empty or right_shape.is_empty:
        return 0.0
    center = left_shape.centroid
    transformer = _local_transformer(center.y, center.x)
    left_projected = transform(transformer.transform, left_shape)
    right_projected = transform(transformer.transform, right_shape)
    intersection = left_projected.intersection(right_projected).area
    union = left_projected.union(right_projected).area
    if union <= 0:
        return 0.0
    return intersection / union


def elongation_ratio(footprint_geojson: dict, center_lat: float, center_lon: float) -> float:
    if not HAS_SHAPELY:
        coordinates = footprint_geojson.get("coordinates", [[]])[0]
        if len(coordinates) < 4:
            return 1.0
        lon_span = max(point[0] for point in coordinates) - min(point[0] for point in coordinates)
        lat_span = max(point[1] for point in coordinates) - min(point[1] for point in coordinates)
        major = max(lon_span * 111.0 * math.cos(math.radians(center_lat)), lat_span * 111.0)
        minor = max(0.1, min(lon_span * 111.0 * math.cos(math.radians(center_lat)), lat_span * 111.0))
        return major / minor
    transformer = _local_transformer(center_lat, center_lon)
    projected = transform(transformer.transform, shape(footprint_geojson))
    rectangle = projected.minimum_rotated_rectangle
    if isinstance(rectangle, Point):
        return 1.0
    if isinstance(rectangle, LineString):
        return max(1.0, rectangle.length / 1000.0)
    coords = list(rectangle.exterior.coords)
    edges = []
    for index in range(4):
        x1, y1 = coords[index]
        x2, y2 = coords[index + 1]
        edges.append(math.hypot(x2 - x1, y2 - y1))
    edges.sort(reverse=True)
    if not edges or edges[-1] <= 0:
        return 1.0
    return edges[0] / edges[-1]


def motion_vector(previous_lat: float, previous_lon: float, current_lat: float, current_lon: float, delta_seconds: float) -> tuple[float | None, float | None]:
    if delta_seconds <= 0:
        return None, None
    distance_km = haversine_km(previous_lat, previous_lon, current_lat, current_lon)
    speed_kmh = distance_km / (delta_seconds / 3600.0)
    heading = bearing_deg(previous_lat, previous_lon, current_lat, current_lon)
    return heading, speed_kmh


def motion_to_components(speed_kmh: float, heading_deg_value: float) -> tuple[float, float]:
    speed_ms = speed_kmh / 3.6
    heading_rad = math.radians(heading_deg_value)
    east = speed_ms * math.sin(heading_rad)
    north = speed_ms * math.cos(heading_rad)
    return east, north
