import multiprocessing as mp
import tempfile
import warnings
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
import xarray as xr
from osgeo import gdal
from shapely.geometry import box
from shapely.wkb import loads as wkb_loads
from tqdm.auto import tqdm

from src.constants import DATA_PATH
from src.data.overture.preprocessing import OVERTURE_PROCESSED_FP as OVERTURE_FP
from src.data.utils import read_fp_within_geo
from src.postprocessing.utils import find_post_dates, vectorize_xarray_3d
from src.utils.gdrive import drive_to_local, get_files_in_folder
from src.utils.geo import load_ukraine_admin_polygons
from src.utils.time import timeit


@timeit
def drive_to_result(run_name: str, post_dates: list[tuple[str, str]] = None) -> None:
    """
    Download predictions from drive and preprocess to results.

    Includes assigning a prediction to each building.

    Args:
        run_name (str): The name of the run.
        post_dates (list[tuple[str, str]], optional): The post disaster dates to download.
            Defaults to None. (used to debug)
    """

    # 1. download to local
    download_and_merge_all_dates(run_name, post_dates)

    # 2. Prediction per building (this might crash from memory and requires a few attempts)
    # Creates a geojson per admin unit with predictions for each building
    # create_all_gdf_overture_with_preds(run_name)
    create_all_gdf_overture_with_preds_mp(run_name, cpu=3)

    # 3. Aggregate all preds
    aggregate_all_preds(run_name)


# ====================== 1. Download and merge ======================
def download_and_merge_all_dates(run_name: str, post_dates: list[tuple[str, str]] = None) -> None:
    """
    Download all predictions from drive to local and merge them into a single file.

    Args:
        run_name (str): The name of the run.
        post_dates (list[tuple[str, str]], optional): The post disaster dates to download.
            Defaults to None. (used to debug)
    """

    local_folder = DATA_PATH / run_name
    local_folder.mkdir(exist_ok=True, parents=True)
    drive_folders = get_files_in_folder(f"{run_name}_quadkeys_predictions", return_names=True)

    if post_dates is not None:
        # filter folders to download
        post_dates_ = [f"{p[0]}_{p[1]}" for p in post_dates]
        drive_folders = [f for f in drive_folders if f in post_dates_]

    print(f"Downloading {len(drive_folders)} folders")

    for drive_folder in drive_folders:
        if drive_folder == "cfg.yaml":
            continue

        name_file = f"ukraine_{drive_folder}.tif"
        download_and_merge(drive_folder, local_folder, name_file, save_individual_files=False)


@timeit
def download_and_merge(
    drive_folder: str,
    local_folder: Path,
    name_file: str,
    save_individual_files: bool = False,
) -> None:
    """
    Download predictions from drive and merge them into a single file.

    If save_individual_files not set, use tmeporary directory to download all individual files.

    Args:
        drive_folder (str): The name of the folder in drive.
        local_folder (Path): The local folder to save the files.
        name_file (str): The name of the file to save.
        save_individual_files (bool, optional): Whether to save individual files. Defaults to False.
    """

    if (local_folder / name_file).exists():
        print(f"{name_file} already exists")
        return

    print(f"Downloading {drive_folder}")

    with tempfile.TemporaryDirectory() as tmp:
        if save_individual_files:
            local_folder_indiv = local_folder / drive_folder.split("/")[-1]
        else:
            local_folder_indiv = Path(tmp)

        drive_to_local(drive_folder, local_folder_indiv, delete_in_drive=False, verbose=0)
        print(f"Finished downloading {drive_folder}")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            tif_files = [str(fp) for fp in local_folder_indiv.glob("*.tif")]
            output_file = str(local_folder / name_file)
            print(f"Merging {len(tif_files)} files into {name_file}")
            gdal.Warp(output_file, tif_files, format="GTiff")
            print(f"Finished merging {name_file}")


# ====================== 2. Preds per building ======================
@timeit
def create_all_gdf_overture_with_preds_mp(run_name: str, cpu: int = 5, verbose: int = 0) -> None:
    """
    Create GeoDataFrame with predictions for each building in parallel.

    To not overload memory, we do this for each admin unit (level=3) separately.

    Args:
        run_name (str): The name of the run.
        cpu (int): The number of CPUs to use. Defaults to 5.
        verbose (int): The verbosity level. Defaults to 0.
    """

    gdf_admin = load_ukraine_admin_polygons(adm_level=3)
    folder_preds = DATA_PATH / run_name / "admin_preds"
    folder_preds.mkdir(exist_ok=True, parents=True)

    args = [
        (admin_id, run_name, folder_preds, verbose)
        for admin_id in gdf_admin.admin_id
        if not (folder_preds / f"{admin_id}.geojson").exists()
    ]
    print(f"Processing {len(args)} admin units...")
    with mp.Pool(cpu) as pool:
        pool.starmap(create_gdf_overture_with_preds, args)


@timeit
def create_all_gdf_overture_with_preds(run_name: str, verbose: int = 1) -> None:
    """
    Create GeoDataFrame with predictions for each building.

    Do not overload memory, do this for each admin unit (level=3) separately.

    Args:
        run_name (str): The name of the run.
        verbose (int): The verbosity level. Defaults to 1.
    """

    gdf_admin = load_ukraine_admin_polygons(adm_level=3)
    folder_preds = DATA_PATH / run_name / "admin_preds"
    folder_preds.mkdir(exist_ok=True, parents=True)

    for admin_id in gdf_admin.admin_id:
        if (folder_preds / f"{admin_id}.geojson").exists():
            continue
        print(f"Processing {admin_id}...")
        create_gdf_overture_with_preds(admin_id, run_name, folder_preds, verbose)


def create_gdf_overture_with_preds(admin_id: str, run_name: str, folder_preds: Path, verbose: int = 0) -> None:
    """
    Create GeoDataFrame with predictions for each building for a given admin ID.

    For each building, we assign the weighted mean and max value of the predictions, for each date.

    Args:
        admin_id (str): The admin ID.
        run_name (str): The name of the run.
        folder_preds (Path): The folder to save the results.
        verbose (int): The verbosity level. Defaults to 0.
    """

    # Get filepath and check if it already exists
    fp = folder_preds / f"{admin_id}.geojson"
    if fp.exists():
        print(f"{fp.name} already exists")
        return

    # Get buildings for the given admin, if don't exist yet, create it first (that can take a loong time,
    # might be better to run src/data/overture/admin.py first)
    gdf_buildings = get_overture_buildings_for_admin(admin_id).set_index("building_id")
    total_bounds = box(*gdf_buildings.total_bounds)
    if verbose:
        print(f"{gdf_buildings.shape[0]} buildings for admin {admin_id} loaded")

    # Find post dates
    post_dates = find_post_dates(run_name)
    post_dates_ = [p[0] for p in post_dates]  # first date for reference (eg 2023-02-24_2023-05-24 will be 2023-02-24)

    # Read and stack preds for each date
    fp_preds = [DATA_PATH / run_name / f'ukraine_{"_".join(post_date)}.tif' for post_date in post_dates]
    dates = xr.Variable("date", pd.to_datetime(post_dates_))
    preds = xr.concat([read_fp_within_geo(fp, total_bounds) for fp in fp_preds], dim=dates).squeeze(dim="band")
    if verbose:
        print(f"Raster with preds read and stacked ({preds.shape})")

    # Vectorize pixels (the heavy part of the code)
    gdf_pixels = vectorize_xarray_3d(preds, post_dates_)
    if verbose:
        print(f"Pixels vectorized ({gdf_pixels.shape})")

    # Overlap with buildings
    overlap = gpd.overlay(gdf_buildings.reset_index(), gdf_pixels, how="intersection").set_index("building_id")
    if verbose:
        print(f"Overlap computed ({overlap.shape})")

    # Add area of overlap
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        overlap["polygon_area"] = overlap.area

    # Compute weighted mean for each building and date
    cols = [f"{d}_weighted_value" for d in post_dates_]  # eg 2023-02-24_weighted_value
    overlap[cols] = overlap[post_dates_].multiply(overlap["polygon_area"], axis=0)
    grps = overlap.groupby("building_id")
    gdf_weighted_mean = grps[cols].sum().divide(grps["polygon_area"].sum(), axis=0)
    gdf_weighted_mean = gdf_weighted_mean.stack().reset_index(level=1)
    gdf_weighted_mean.columns = ["post_date", "weighted_mean"]
    gdf_weighted_mean["post_date"] = gdf_weighted_mean["post_date"].apply(lambda x: x.split("_")[0])
    gdf_weighted_mean.set_index("post_date", append=True, inplace=True)

    # Compute max value for each building and date
    gdf_max = overlap.groupby("building_id")[post_dates_].max().stack().to_frame(name="max")
    gdf_max.index.names = ["building_id", "post_date"]

    # Merge with original buildings
    gdf_buildings_with_preds = gdf_buildings.join(gdf_weighted_mean).join(gdf_max).sort_index()
    if verbose:
        print("Weighted mean and max extracted for each building.")

    # Save to file
    gdf_buildings_with_preds.to_file(fp, driver="GeoJSON")
    print(f"Finished creating gdf with preds for admin {admin_id}")


def get_overture_buildings_for_admin(admin_id: str) -> gpd.GeoDataFrame:
    """
    Get Overture buildings for the given admin ID.

    Args:
        admin_id (str): The admin ID.

    Returns:
        gpd.GeoDataFrame: The GeoDataFrame with the buildings.
    """

    db = duckdb.connect()
    df = db.execute(
        f"""
        SELECT *
        FROM read_parquet('{OVERTURE_FP}')
        WHERE adm3_id = '{admin_id}'
    """
    ).fetchdf()
    # Cast back to geometry and geodataframe
    df["geometry"] = df.geometry_wkb.apply(lambda x: wkb_loads(bytes(x)))
    return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")


# ====================== 3. Aggregate everything ======================


def aggregate_all_preds(run_name: str) -> None:
    gdf_admin = load_ukraine_admin_polygons(adm_level=3).set_index("admin_id")
    folder_preds_admin = DATA_PATH / run_name / "admin_preds"

    # Create a dataframe with building_id as index and post_date as columns. Values are the weighted mean
    adm3_ids = list(gdf_admin.index)
    args = [(adm3_id, run_name) for adm3_id in adm3_ids if (folder_preds_admin / f"{adm3_id}.geojson").exists()]
    print(f"Aggregating {len(args)} admin units...")
    with mp.Pool(mp.cpu_count()) as pool:
        df_preds = list(tqdm(pool.imap(process_file_args, args), total=len(adm3_ids)))

    # Filter out any None results due to errors
    df_preds = [df for df in df_preds if df is not None]

    # Concatenate all DataFrames into one
    df_preds = pd.concat(df_preds, axis=0)

    # Merge with building metadata from overture
    df_buildings = pd.read_parquet(OVERTURE_FP).set_index("building_id")
    df_buildings_with_preds = df_buildings.join(df_preds, how="left")

    # Save to file
    fp = DATA_PATH / run_name / "buildings_preds.parquet"
    df_buildings_with_preds.to_parquet(fp)
    print("Finished creating buildings_preds.parquet")


def process_file_args(args: tuple[str, str]) -> pd.DataFrame:
    return process_file(*args)


def process_file(adm3_id: str, run_name: str) -> pd.DataFrame:
    """Process a single admin file."""

    folder_preds_admin = DATA_PATH / run_name / "admin_preds"

    try:
        gdf = gpd.read_file(folder_preds_admin / f"{adm3_id}.geojson", driver="GeoJSON")
        return gdf.reset_index().pivot_table(index="building_id", columns="post_date", values="weighted_mean")
    except Exception as e:
        print(f"Error processing {adm3_id}: {e}")
        return None
