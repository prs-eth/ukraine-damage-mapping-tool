import geopandas as gpd
import pandas as pd

from src.constants import DATA_PATH


def get_valid_orbits(aoi: str) -> list[int]:
    """
    Get the valid orbits for a given AOI.

    Args:
        aoi (str): The AOI name

    Returns:
        List[int]: The list of valid orbits
    """
    df_orbits = load_df_orbits()
    return df_orbits.loc[aoi, "valid_orbits"]


def load_df_orbits() -> gpd.GeoDataFrame:
    """
    Load the GeoDataFrame with the valid orbits (and the best) for each AOI.

    If the file does not exist, it is created.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame
    """

    fp = DATA_PATH / "s1_aoi_orbits.csv"
    assert fp.exists(), "The file with the orbits per AOI for Sentinel-1 does not exist. It should be on github."

    df_orbits = pd.read_csv(fp)

    # Cast back into list and set index
    df_orbits.valid_orbits = df_orbits.valid_orbits.apply(lambda x: [int(i) for i in x.split(",")])
    df_orbits.set_index("aoi", inplace=True)

    # orbit 109 for UKR8 causes some bugs (both locally and in GEE)
    df_orbits.at["UKR8", "valid_orbits"] = [i for i in df_orbits.loc["UKR8", "valid_orbits"] if i != 109]
    return df_orbits
