import re
import warnings

import geopandas as gpd
import numpy as np
import xarray as xr

from src.constants import DATA_PATH


def vectorize_xarray_3d(xa: xr.DataArray, dates: list[str]) -> gpd.GeoDataFrame:
    """
    Vectorize a 3D xarray.DataArray to a GeoDataFrame.

    Essentially, creates a square polygon around each pixel, and assign the values of each band to the polygon.
    Probably not the most optimal way to do it, but it works.

    Args:
        xa (xr.DataArray): The 3D xarray.
        dates (list[str]): The dates to vectorize.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame.
    """

    assert "date" in xa.dims, "xarray should have a 'date' dimension"
    if len(xa.shape) > 3:
        xa = xa.squeeze(dim="band")
        assert len(xa.shape) == 3, "xarray should be 3D"

    # Comptue grid from coordinates
    x, y = xa.x.values, xa.y.values
    x, y = np.meshgrid(x, y)
    x, y = x.flatten(), y.flatten()

    # Flatten the values of each band
    vs = {d: xa.sel(date=d).values.flatten() for d in dates}
    gdf_pixels = gpd.GeoDataFrame(vs, geometry=gpd.GeoSeries.from_xy(x, y), columns=vs.keys(), crs=xa.rio.crs)
    gdf_pixels.index.name = "pixel_id"
    gdf_pixels.reset_index(inplace=True)

    # Buffer the pixels to get one polygon per pixel
    res = xa.rio.resolution()
    buffer = res[0] / 2  # half the pixel size, assuming square pixels
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        gdf_pixels["geometry"] = gdf_pixels.buffer(buffer, cap_style=3)
    return gdf_pixels


def find_post_dates(run_name: str) -> list[tuple[str, str]]:
    """Find post dates from the file names in the folder."""
    local_folder = DATA_PATH / run_name
    post_dates = []
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})")  # 'YYYY-MM-DD_YYYY-MM-DD'

    for file in local_folder.glob("ukraine_*.tif"):
        match = date_pattern.search(file.stem)
        if match:
            post_dates.append((match.group(1), match.group(2)))

    return post_dates
