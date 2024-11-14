import ee
import geopandas as gpd
from shapely.geometry import Polygon

from src.constants import DATA_PATH, OLD_ASSETS_PATH

# ==================== LOCAL DATA ====================


def load_unosat_labels(
    aoi: str | list[str] | None = None,
    labels_to_keep: list[int] = [1, 2],
    combine_epoch: bool = "last",
) -> gpd.GeoDataFrame:
    """
    Load UNOSAT labels processed.

    Args:
        aoi (str | list[str] | None): Which AOIs to keep. Default to None (all)
        labels_to_keep (list[int]): Which labels to keep. Default to [1,2] (destroyed, major damage)
        combine_epoch (bool): For points that have multiple observations, we keep only one label.
            Either the 'last' one or the 'min' one (eg the strongest label). Default to 'last'

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame with all UNOSAT labels
    """

    labels_fp = DATA_PATH / "unosat_labels.geojson"
    assert labels_fp.exists(), "The GeoDataFrame has not been created yet."

    gdf = gpd.read_file(labels_fp).set_index("unosat_id")

    if combine_epoch is not None:
        if combine_epoch == "last":
            # Only keep most recent epoch for each point
            gdf = gdf.loc[gdf.groupby(gdf.geometry.to_wkt())["ep"].idxmax()]
        elif combine_epoch == "min":
            # Only keep strongest label for each point
            gdf = gdf.loc[gdf.groupby(gdf.geometry.to_wkt())["damage"].idxmin()]
        else:
            raise ValueError("combine_epoch must be 'last' or 'min'")

    if labels_to_keep is not None:
        # Only keep some labels
        gdf = gdf[gdf.damage.isin(labels_to_keep)]

    if aoi is not None:
        # Only keep some AOIs
        aoi = [aoi] if isinstance(aoi, str) else aoi
        gdf = gdf[gdf.aoi.isin(aoi)]

    return gdf


def load_unosat_aois() -> gpd.GeoDataFrame:
    """
    Load GeoDataFrame with all AOIs from UNOSAT.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame with column 'aoi' and 'geometry'
    """

    aoi_fp = DATA_PATH / "unosat_aois.geojson"
    assert aoi_fp.exists(), "The GeoDataFrame has not been created yet."
    return gpd.read_file(aoi_fp)


def load_unosat_geo(aoi: str) -> Polygon:
    """
    Get the geometry of the given AOI.

    Args:
        aoi (str): The area of interest.

    Returns:
        Polygon: The geometry of the AOI
    """
    aois = load_unosat_aois().set_index("aoi")
    geo = aois.loc[aoi].geometry
    return geo


# ==================== GEE DATA ====================


def load_unosat_labels_gee(aoi: str, all_labels: bool = False) -> ee.FeatureCollection:
    """
    Get the UNOSAT labels for the given AOI.

    Args:
        aoi (str): The area of interest.
        all_labels (bool, optional): If True, return all labels. Otherwise only 1 and 2.
            Defaults to False.

    Returns:
        ee.FeatureCollection: The UNOSAT labels.
    """
    if all_labels:
        return ee.FeatureCollection(OLD_ASSETS_PATH + f"UNOSAT_labels/{aoi}_full")
    else:
        return ee.FeatureCollection(OLD_ASSETS_PATH + f"UNOSAT_labels/{aoi}")


def load_unosat_geo_gee(aoi: str) -> ee.FeatureCollection:
    """
    Get the AOI geometry in GEE.

    Args:
        aoi (str): The area of interest.

    Returns:
        ee.FeatureCollection: The AOI geometry.
    """

    return ee.FeatureCollection(OLD_ASSETS_PATH + f"AOIs/{aoi}").geometry()
