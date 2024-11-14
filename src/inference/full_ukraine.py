import ee
from omegaconf import DictConfig, OmegaConf
from tqdm import tqdm

from src.classification.main import load_or_create_classifier
from src.data.quadkeys import load_ukraine_quadkeys_gee
from src.inference.dense_inference import predict_geo
from src.utils.gdrive import create_drive_folder, create_yaml_file_in_drive_from_config_dict, get_files_in_folder
from src.utils.gee import init_gee
from src.utils.time import timeit

init_gee()


@timeit
def ukraine_full_inference(cfg: DictConfig) -> None:
    # Load classifier trained on UNOSAT data
    classifier = load_or_create_classifier(cfg)

    # Prepare folder in Drive and save config
    try:
        # Create drive folder and save config
        create_drive_folder(cfg.run_name)
        create_yaml_file_in_drive_from_config_dict(cfg, cfg.run_name)
    except Exception as e:
        # get input from user to be sure they want to continue
        print(f"Folder already exists. {e} Continue? (y/n)")
        user_input = input()
        if user_input != "y":
            raise ValueError("Interrupted")

    # Case where post period is given as a tuple (start, end) instead of a list of tuples
    if isinstance(cfg.inference.time_periods.post[0], str):
        cfg.inference.time_periods.post = [cfg.inference.time_periods.post]

    # Loop over all post periods, predict and export
    post_periods = cfg.inference.time_periods.post
    for post_period in post_periods:
        folder_name = f"{cfg.run_name}/{'_'.join(post_period)}"

        # Get dict with only one start and end date for pre and post
        time_periods = cfg.inference.time_periods
        time_periods.post = post_period

        try:
            # Create drive folder
            create_drive_folder(folder_name)
        except Exception:
            pass  # folder exists, but user already confirmed they want to continue

        # Launch predictions
        predict_and_export_all_grids(
            run_name=cfg.run_name,
            classifier=classifier,
            extract_window=cfg.data.extract_winds,
            reducer_names=cfg.reducer_names,
            time_periods=time_periods,
            aggregation_method=cfg.aggregation_method,
            drive_folder=folder_name,
            zoom_level=cfg.inference.quadkey_zoom,
            target_ids=None,
            n_limit=None,
        )


def predict_and_export_all_grids(
    run_name: str,
    classifier: ee.Classifier,
    extract_window: str,
    reducer_names: list[str],
    time_periods: dict[tuple[str]],
    aggregation_method: str,
    drive_folder: str,
    zoom_level: int,
    target_ids: list[str] | None = None,
    n_limit: int | None = None,
) -> None:
    # Load quadkey grid
    grids = load_ukraine_quadkeys_gee(zoom=zoom_level)

    # Get all quadkeys to predict (target_ids and n_limit are used for debugging)
    if target_ids is None:
        ids = grids.aggregate_array("qk").getInfo()
        if n_limit is not None:
            ids = ids[:n_limit]
    else:
        ids = target_ids

    # Filter IDs that have already been predicted or that are running
    ids = filter_ids(ids, drive_folder, run_name, time_periods)

    print(f"Predicting and exporting {len(ids)} grids")
    for id_ in tqdm(ids):
        # Get geometry from grid
        geo = grids.filter(ee.Filter.eq("qk", id_)).geometry()

        # Predict
        preds = predict_geo(
            geometry=geo,
            classifier=classifier,
            time_periods=time_periods,
            extract_window=extract_window,
            reducer_names=reducer_names,
            orbits=None,
            aggregate_method=aggregation_method,
            verbose=0,
        )
        preds = preds.set("qk", id_)

        # Export to Drive in Uint8 format
        task = ee.batch.Export.image.toDrive(
            image=preds.multiply(2**8 - 1).toUint8(),  # multiply by 255 and convert to uint8
            description=get_description(id_, run_name, time_periods),
            folder=drive_folder,
            fileNamePrefix=f"qk_{id_}",
            region=geo,
            scale=10,
            maxPixels=1e13,
        )
        task.start()


def filter_ids(ids: list[str], drive_folder: str, run_name: str, time_periods: dict[tuple[str]]) -> list[str]:
    """Filter out quadkeys that already exist in Drive folder, or that are still running in GEE operations."""

    initial_len = len(ids)

    # Filter IDs that have already been predicted (names are qk_12345678.tif for instance)
    files = get_files_in_folder(drive_folder, return_names=True)
    existing_names = [f.split("qk_")[-1].split(".")[0] for f in files if f.startswith("qk_")]
    ids = [id_ for id_ in ids if id_ not in existing_names]
    after_len = len(ids)
    already_existing = initial_len - after_len
    if already_existing:
        print(f"{already_existing} IDs already predicted.")

    # filter IDs that are still running (based on description)
    ops = [o for o in ee.data.listOperations() if o["metadata"]["state"] in ["PENDING", "RUNNING"]]
    ids_running = [o["metadata"]["description"] for o in ops]
    ids = [id_ for id_ in ids if get_description(id_, run_name, time_periods) not in ids_running]
    running_len = after_len - len(ids)
    if running_len:
        print(f"{running_len} IDs still running.")

    return ids


def get_description(id_, run_name, time_periods) -> str:
    return f"{run_name}_qk{id_}_{'_'.join(time_periods.post)}"


if __name__ == "__main__":
    from src.constants import AOIS_TEST, ASSETS_PATH
    from src.utils.gee import create_folders_recursively

    run_name = "ukraine_full_inference"
    GEE_FOLDER = ASSETS_PATH + f"predictions/{run_name}"
    create_folders_recursively(GEE_FOLDER)

    cfg = OmegaConf.create(
        dict(
            aggregation_method="mean",
            model_name="random_forest",
            model_kwargs=dict(numberOfTrees=50, minLeafPopulation=3, maxNodes=1e4),
            data=dict(
                s1=dict(subset_bands=None),
                s2=None,
                aois_test=AOIS_TEST,
                damages_to_keep=[1, 2],
                extract_winds="1x1",
                time_periods=dict(pre=("2020-02-24", "2021-02-23"), post="3months"),
            ),
            inference=dict(
                time_periods={
                    "pre": ("2020-02-24", "2021-02-23"),
                    "post": [
                        ("2021-02-24", "2021-05-23"),
                        ("2021-05-24", "2021-08-23"),
                        ("2021-08-24", "2021-11-23"),
                        ("2021-11-24", "2022-02-23"),
                        ("2022-02-24", "2022-05-23"),
                        ("2022-05-24", "2022-08-23"),
                        ("2022-08-24", "2022-11-23"),
                        ("2022-11-24", "2023-02-23"),
                    ],
                },
                quadkey_zoom=8,  # zoom level for quadkey grid, balance between number of grids and size of each grid
            ),
            reducer_names=[
                "mean",
                "stdDev",
                "median",
                "min",
                "max",
                "skew",
                "kurtosis",
            ],
            seed=0,
            gee_folder=GEE_FOLDER,
            run_name=run_name,
        )
    )
