import math
import time
from pathlib import Path

import ee
import geemap
import geopandas as gpd
from shapely import GeometryType
from shapely.geometry import box

from src.constants import ASSETS_PATH, DATA_PATH
from src.utils.gee import asset_exists, create_folders_recursively, init_gee
from src.utils.geo import load_country_boundaries
from src.utils.time import timeit

init_gee()

# Define constants
EARTH_RADIUS = 6378137
TILE_SIZE = 256
MIN_LAT = -85.05112878
MAX_LAT = 85.05112878
MIN_LON = -180
MAX_LON = 180


def load_ukraine_quadkeys_gee(zoom: int) -> ee.FeatureCollection:
    """
    Load quadkeys grid for Ukraine at a specified zoom level from a GEE asset.

    Args:
        zoom (int): The zoom level.

    Returns:
        ee.FeatureCollection: The quadkeys grid.
    """

    asset_id = ASSETS_PATH + f"quadkeys_grid/ukraine_zoom{zoom}"
    if not asset_exists(asset_id):
        print(f"Asset {asset_id} does not exist. Creating it now...")

        # Make sure folder exists
        create_folders_recursively(asset_id, last_one_is_asset=True)

        # Transform to ee.FeatureCollection and export to asset
        fc = geemap.geopandas_to_ee(load_ukraine_quadkeys(zoom))
        ee.batch.Export.table.toAsset(
            collection=fc,
            description=f"Ukraine quadkeys grid zoom {zoom}",
            assetId=asset_id,
        ).start()
        print(f"Exporting Ukraine quadkeys grid zoom {zoom}. Waiting for it to be done...")

        while not asset_exists(asset_id):
            time.sleep(5)
        print(f"Asset {asset_id} created.")

    return ee.FeatureCollection(asset_id)


def load_ukraine_quadkeys(zoom: int, clip_to_border=True) -> gpd.GeoDataFrame:
    """
    Load quadkeys grid for Ukraine at a specified zoom level.

    Args:
        zoom (int): The zoom level.

    Returns:
        gpd.GeoDataFrame: The quadkeys grid.
    """

    fp_qk_grid = DATA_PATH / f"ukraine_qk_grid_zoom{zoom}.geojson"
    if not fp_qk_grid.exists():
        print(f"File {fp_qk_grid} does not exist. Creating it now...")
        create_ukraine_quadkeys_grid(zoom, fp_to_save=fp_qk_grid)

    grid = gpd.read_file(fp_qk_grid)

    if clip_to_border:
        grid = grid.clip(load_country_boundaries("Ukraine"))

    return grid


@timeit
def create_ukraine_quadkeys_grid(zoom: int, fp_to_save: str | Path):
    ukraine = load_country_boundaries("Ukraine")
    quadkeys = get_intersecting_quadkeys(ukraine, zoom)
    quadkeys["area_in_ukraine"] = quadkeys.geometry.apply(lambda geo: geo.intersection(ukraine).area / geo.area)
    quadkeys.to_file(fp_to_save, driver="GeoJSON")
    print(f"Saved quadkeys grid for Ukraine at zoom level {zoom}.")


def get_intersecting_quadkeys(polygon: GeometryType, zoom: int) -> gpd.GeoDataFrame:
    minx, miny, maxx, maxy = polygon.bounds
    top_left_x, top_left_y = global_pixel_to_tile(*position_to_global_pixel(minx, maxy, zoom))
    bottom_right_x, bottom_right_y = global_pixel_to_tile(*position_to_global_pixel(maxx, miny, zoom))

    quadkeys = []
    geoms = []

    for x in range(top_left_x, bottom_right_x + 1):
        for y in range(top_left_y, bottom_right_y + 1):
            quadkey = tile_to_quadkey(x, y, zoom)
            tile_geom = tile_to_bbox(x, y, zoom)

            if tile_geom.intersects(polygon):
                quadkeys.append(quadkey)
                geoms.append(tile_geom)

    return gpd.GeoDataFrame({"qk": quadkeys, "geometry": geoms}, crs="EPSG:4326")


# Convert global pixel to tile coordinates
def global_pixel_to_tile(pixel_x, pixel_y):
    return int(pixel_x // TILE_SIZE), int(pixel_y // TILE_SIZE)


# Convert lat/lon to global pixel coordinates at a specified zoom level
def position_to_global_pixel(lon, lat, zoom):
    lat = clip(lat, MIN_LAT, MAX_LAT)
    lon = clip(lon, MIN_LON, MAX_LON)

    x = (lon + 180) / 360
    sin_lat = math.sin(lat * math.pi / 180)
    y = 0.5 - math.log((1 + sin_lat) / (1 - sin_lat)) / (4 * math.pi)

    map_size = TILE_SIZE * (2**zoom)
    pixel_x = clip(x * map_size + 0.5, 0, map_size - 1)
    pixel_y = clip(y * map_size + 0.5, 0, map_size - 1)

    return pixel_x, pixel_y


# Clipping utility
def clip(n, min_value, max_value):
    return min(max(n, min_value), max_value)


# Convert tile XY to quadkey
def tile_to_quadkey(tile_x, tile_y, zoom):
    quadkey = []
    for i in range(zoom, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if (tile_x & mask) != 0:
            digit += 1
        if (tile_y & mask) != 0:
            digit += 2
        quadkey.append(str(digit))
    return "".join(quadkey)


# Convert tile XY to bounding box in EPSG:4326
def tile_to_bbox(tile_x, tile_y, zoom):
    map_size = TILE_SIZE * (2**zoom)
    x1, y1 = tile_x * TILE_SIZE, tile_y * TILE_SIZE
    x2, y2 = x1 + TILE_SIZE, y1 + TILE_SIZE

    lon1, lat1 = global_pixel_to_latlon(x1, y1, map_size)
    lon2, lat2 = global_pixel_to_latlon(x2, y2, map_size)

    return box(min(lon1, lon2), min(lat1, lat2), max(lon1, lon2), max(lat1, lat2))


# Helper function to convert global pixel to lat/lon
def global_pixel_to_latlon(pixel_x, pixel_y, map_size):
    x = (clip(pixel_x, 0, map_size - 1) / map_size) - 0.5
    y = 0.5 - (clip(pixel_y, 0, map_size - 1) / map_size)

    lon = 360 * x
    lat = 90 - 360 * math.atan(math.exp(-y * 2 * math.pi)) / math.pi
    return lon, lat
