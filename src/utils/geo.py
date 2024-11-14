import io
import zipfile

import geopandas as gpd
import osmnx as ox
import requests
from pyproj import Transformer
from shapely import Geometry
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import transform

from src.constants import DATA_PATH


def load_country_boundaries(country: str) -> tuple[Polygon, MultiPolygon]:
    """
    Load shapefile with country boundaries.

    If file does not exist, download it from OSM.

    Args:
        country (str): Name of the country (eg Ukraine, Iraq, ...)

    Returns:
        Tuple[Polygon, MultiPolygon]: The boundaries
    """

    folder = DATA_PATH / "countries"
    folder.mkdir(exist_ok=True)

    fp = folder / f"{country}.shp"

    if not fp.exists():
        print(f"The file with {country} boundaries does not exist. Downloading it now...")
        gdf = ox.geocode_to_gdf(country)
        gdf[["geometry"]].to_file(fp)
        print("Done")

    return gpd.read_file(fp).iloc[0].geometry


def load_ukraine_admin_polygons(adm_level=4):
    assert adm_level in [1, 2, 3, 4]
    try:
        ukraine_admin_path = sorted((DATA_PATH / "UKR_admin_boundaries").glob(f"*_adm{adm_level}*.shp"))[0]
    except IndexError:
        print("Admin boundaries not found. Downloading them now...")
        download_admin_boundaries()
        ukraine_admin_path = sorted((DATA_PATH / "UKR_admin_boundaries").glob(f"*_adm{adm_level}*.shp"))[0]
    columns = [f"ADM{i}_EN" for i in range(1, adm_level + 1)] + ["geometry"]
    ukr_admin = gpd.read_file(ukraine_admin_path)[columns]
    ukr_admin.index.name = "admin_id"
    ukr_admin.reset_index(inplace=True)
    ukr_admin["admin_id"] = ukr_admin["admin_id"].apply(lambda x: f"{adm_level}_{x}")
    return ukr_admin


def reproject_geo(geo: Geometry, current_crs: str, target_crs: str) -> Geometry:
    """Reprojects a Shapely geometry from the current CRS to a new CRS."""
    transformer = Transformer.from_crs(current_crs, target_crs, always_xy=True)
    return transform(transformer.transform, geo)


def download_admin_boundaries():
    """Download admin boundaries for Ukraine from HDX"""

    url = "https://data.humdata.org/dataset/d23f529f-31e4-4021-a65b-13987e5cfb42/resource/4105bb4d-5a9d-4824-a1d7-53141cf47c44/download/ukr_admbnd_sspe_20240416_ab_shp.zip"  # noqa E501
    folder = DATA_PATH / "UKR_admin_boundaries"
    folder.mkdir(exist_ok=True)

    print("Downloading admin boundaries...")
    r = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(folder)
    print("Done")


def get_best_utm_crs_from_gdf(gdf: gpd.GeoDataFrame) -> str:
    """Get the best UTM CRS for the given GeoDataFrame."""
    mean_lon = gdf.geometry.unary_union.centroid.x
    mean_lat = gdf.geometry.unary_union.centroid.y
    return get_best_utm_crs_from_lon_lat(mean_lon, mean_lat)


def get_best_utm_crs_from_lon_lat(lon: float, lat: float) -> str:
    """Get the best UTM CRS for the given lon and lat."""
    utm_zone = int(((lon + 180) / 6) % 60) + 1
    utm_crs = f"EPSG:326{utm_zone}" if lat > 0 else f"EPSG:327{utm_zone}"
    return utm_crs
