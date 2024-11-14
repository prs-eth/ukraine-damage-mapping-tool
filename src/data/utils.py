from pathlib import Path

import rasterio
import rioxarray as rxr
import shapely
import xarray as xr

from src.data.sentinel1.orbits import get_valid_orbits


def aoi_orbit_iterator():
    """Iterator over all AOIs and valid orbits"""
    for aoi in get_all_aois():
        orbits = get_valid_orbits(aoi)
        for orbit in orbits:
            yield aoi, orbit


def get_all_aois():
    """Return all AOIs"""
    return [f"UKR{i}" for i in range(1, 19)]


def read_fp_within_geo(fp: Path, geo: shapely.GeometryType) -> xr.DataArray:
    """Read a raster file within a given geometry."""
    fp = Path(fp) if isinstance(fp, str) else fp
    assert fp.exists(), f"File {fp} does not exist"

    with rasterio.open(fp) as src:
        wind = rasterio.windows.from_bounds(*geo.bounds, src.transform)

        # data = src.read(window=wind)
    xa = rxr.open_rasterio(fp).rio.isel_window(wind)
    return xa
