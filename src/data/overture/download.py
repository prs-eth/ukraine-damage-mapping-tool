"""Download all buildings from Overture Maps in Ukraine using DuckDB"""

from pathlib import Path

import duckdb

from src.constants import OVERTURE_PATH
from src.utils.geo import load_country_boundaries
from src.utils.time import timeit

# release used in this project
OVERTURE_RELEASE = "s3://overturemaps-us-west-2/release/2024-02-15-alpha.0"


def download_overture_buildings(bbox: list[float], filepath: str | Path):
    """
    Download all buildings from Overture Maps in the given bounding box using DuckDB. Save as parquet

    To test if a building is in ukraine, we test if its centroid is within the country bounding box.

    Args:
        bbox (list[float]): The bounding box of the country.
        filepath (str | Path): The path to save the buildings.
    """

    minx, miny, maxx, maxy = bbox

    db = duckdb.connect()
    db.execute("INSTALL spatial; INSTALL httpfs; LOAD spatial; LOAD httpfs; SET s3_region='us-west-2';")

    db.execute(
        f"""
            COPY (
                SELECT
                    *
                FROM
                    read_parquet('{OVERTURE_RELEASE}/theme=buildings/type=building/*')
                WHERE
                    bbox.minX >= {minx}
                AND bbox.minY >= {miny}
                AND bbox.maxX <= {maxx}
                AND bbox.maxY <= {maxy}
            ) TO '{filepath}'
            WITH (FORMAT 'Parquet');
        """
    )


@timeit
def main():
    ukraine_geo = load_country_boundaries("Ukraine")
    bbox = ukraine_geo.bounds
    filepath = OVERTURE_PATH / "raw_ukraine_buildings.parquet"
    filepath.parent.mkdir(exist_ok=True, parents=True)

    print("Downloading Overture buildings in Ukraine...")
    download_overture_buildings(bbox, filepath)
    print("Overture buildings in Ukraine downloaded.")


if __name__ == "__main__":
    main()
