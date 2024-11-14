import duckdb
import geopandas as gpd
import pandas as pd
from shapely.wkb import loads as wkb_loads
from tqdm.auto import tqdm

from src.constants import OVERTURE_PATH
from src.data.unosat import load_unosat_geo, load_unosat_labels
from src.data.utils import get_all_aois
from src.utils.geo import get_best_utm_crs_from_gdf, load_country_boundaries, load_ukraine_admin_polygons
from src.utils.time import timeit

OVERTURE_RAW_FP = OVERTURE_PATH / "raw_ukraine_buildings.parquet"
OVERTURE_PROCESSED_FP = OVERTURE_PATH / "ukraine_buildings.parquet"


@timeit
def process_overture():
    """
    Iteratively process the Overture dataset

    1. Keep only buildings within Ukraine, and relevant properties

    2. Add UNOSAT info

    3. Add admin info
    """

    # Keep only buildings within Ukraine, and relevant properties
    print("Keeping only buildings within Ukraine and relevant properties...")
    only_in_ukraine_and_relevant_properties()

    # Add UNOSAT info
    print("Adding UNOSAT info...")
    add_unosat_info()

    # Add admin info
    print("Adding admin info...")
    add_admin_info()


@timeit
def only_in_ukraine_and_relevant_properties() -> None:
    """Includes dataset, OSM class, centroid (lon/lat) and area (in meters)"""

    db = duckdb.connect()
    db.execute("INSTALL spatial; INSTALL httpfs; LOAD spatial; LOAD httpfs; SET s3_region='us-west-2';")

    geo = load_country_boundaries("Ukraine").simplify(0.001)  # important to simplify

    query = f"""
        COPY (
            SELECT
                JSON(sources)[0].dataset as dataset,
                id as building_id,
                class as osm_class,
                geometry as geometry_wkb,
                ST_Area_Spheroid(geometry) as area,
                ST_X(ST_Centroid(geometry)) as lon,
                ST_Y(ST_Centroid(geometry)) as lat,

            FROM
                read_parquet('{OVERTURE_RAW_FP}')
            WHERE
                ST_WITHIN(ST_Centroid(geometry), ST_GeomFromText('{geo.wkt}'))
        ) TO '{OVERTURE_PROCESSED_FP}'
        WITH (FORMAT 'Parquet')
    """
    db.execute(query)
    print("Done")


@timeit
def add_unosat_info(buffer: int = 5) -> None:
    """
    Add UNOSAT info to the Overture dataset.

    Use a buffer of 5m. Also, probably we could use duckdb for that.

    Args:
        buffer (int): Buffer around buildings. Defaults to 5.
    """

    # Read Overture data
    df = pd.read_parquet(OVERTURE_PROCESSED_FP)
    df["geometry"] = df.geometry_wkb.apply(lambda x: wkb_loads(x))
    gdf_all = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    del df
    print("Overture data loaded")

    # Loop over AOIs
    gdf_buildings_with_unosat = []

    for aoi in tqdm(get_all_aois()):
        # Labels for the given AOI
        points = load_unosat_labels(aoi, labels_to_keep=None).reset_index()

        # Keep only buildigns within the AOI
        geo = load_unosat_geo(aoi)
        gdf = gdf_all[gdf_all.intersects(geo)].copy()

        # Buffer around buildings
        crs_proj = get_best_utm_crs_from_gdf(points)
        gdf.geometry = gdf.to_crs(crs_proj).buffer(buffer).to_crs("EPSG:4326")

        # overlay buildings with UNOSAT labels
        pts_in = gpd.overlay(points, gdf[["geometry", "building_id"]], how="intersection")

        # Keep lowest damage level (most severe) - easy trick by sorting
        pts_sorted = pts_in.sort_values(by=["damage"])
        buildings_with_unosat = pts_sorted.groupby("building_id").agg(
            {"damage": "first", "unosat_id": "first", "date": "first"}
        )
        buildings_with_unosat["aoi"] = aoi

        gdf_buildings_with_unosat.append(buildings_with_unosat)

    gdf_buildings_with_unosat = pd.concat(gdf_buildings_with_unosat)
    gdf_buildings_with_unosat.rename(
        columns={"damage": "unosat_damage", "date": "unosat_date", "aoi": "unosat_aoi"},
        inplace=True,
    )

    gdf_all = gdf_all.merge(gdf_buildings_with_unosat, on="building_id", how="left")
    gdf_all.drop(columns=["geometry"], inplace=True)

    # Save to parquet
    gdf_all.to_parquet(OVERTURE_PROCESSED_FP)
    print("Done")


@timeit
def add_admin_info() -> None:
    """Merge based on admin_level = 3"""

    # Load admin dataframes
    adm1 = load_ukraine_admin_polygons(1).rename(columns={"admin_id": "adm1_id"})[["ADM1_EN", "adm1_id"]]
    adm2 = load_ukraine_admin_polygons(2).rename(columns={"admin_id": "adm2_id"})[["ADM2_EN", "adm2_id"]]
    adm3 = load_ukraine_admin_polygons(3).rename(columns={"admin_id": "adm3_id"})
    adm = adm3.merge(adm1, on="ADM1_EN").merge(adm2, on="ADM2_EN")
    adm = adm[["ADM1_EN", "ADM2_EN", "ADM3_EN", "adm1_id", "adm2_id", "adm3_id", "geometry"]]
    adm["geometry_wkt"] = adm.geometry.to_wkt()
    adm.drop(columns=["geometry"], inplace=True)

    # Load Overture data as table
    db = duckdb.connect()
    db.execute("INSTALL spatial; INSTALL httpfs; LOAD spatial; LOAD httpfs; SET s3_region='us-west-2';")
    db.execute(f"CREATE TABLE buildings AS SELECT * FROM read_parquet('{OVERTURE_PROCESSED_FP}')")

    # Add columns to the table
    add_column(db, "ADM1_EN", "STRING")
    add_column(db, "ADM2_EN", "STRING")
    add_column(db, "ADM3_EN", "STRING")
    add_column(db, "adm1_id", "STRING")
    add_column(db, "adm2_id", "STRING")
    add_column(db, "adm3_id", "STRING")

    # Iterate over admin regions and update the table (slow! -> look at multiprocessing for that)
    # Take all buildings whose centroid is within the admin region
    for _, row in tqdm(adm.iterrows(), total=len(adm)):
        db.execute(
            f"""
            UPDATE buildings
            SET ADM1_EN = '{row.ADM1_EN}',
                ADM2_EN = '{row.ADM2_EN}',
                ADM3_EN = '{row.ADM3_EN}',
                adm1_id = '{row.adm1_id}',
                adm2_id = '{row.adm2_id}',
                adm3_id = '{row.adm3_id}'

            WHERE ST_Contains(ST_GeomFromText('{row.geometry_wkt}'), ST_Point(lon, lat))
            """
        )

    # Save to parquet
    db.execute(f"COPY buildings TO '{OVERTURE_PROCESSED_FP}' WITH (FORMAT 'Parquet')")


def add_column(db: duckdb.DuckDBPyConnection, name: str, type: str, table_name: str = "buildings") -> None:
    """Add a column to a table in a DuckDB database."""
    try:
        db.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {type}")
        print(f"Column {name} added to table {table_name}")
    except Exception:
        print(f"Column {name} already exists in table {table_name}")


if __name__ == "__main__":
    # Either everything or one by one
    process_overture()

    # only_in_ukraine_and_relevant_properties()
    # add_unosat_info()
    # add_admin_info()
