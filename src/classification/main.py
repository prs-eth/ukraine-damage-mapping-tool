"""Script to perform classification on UNOSAT labels using a config dict."""

import time
from pathlib import Path

import ee
import geemap
import geopandas as gpd
from omegaconf import DictConfig

from src.classification.dataset import get_dataset_ready
from src.classification.metrics import get_metrics
from src.classification.models import classifier_factory, export_classifier, load_classifier
from src.classification.utils import get_features_names, get_run_name, get_sat_from_cfg
from src.data.unosat import load_unosat_labels
from src.utils.gee import asset_exists, create_folder, init_gee
from src.utils.time import print_sec, timeit

init_gee()


@timeit
def full_pipeline(cfg: DictConfig, force_recreate=False) -> dict:
    """
    Full pipeline to go from a config dict to metrics on the UNOSAT dataset

    1) The Classifier is loaded if it already exists, otherwise created, trained and saved.
    2) The test dataset is loaded and classified.
    3) The predictions are exported from GEE to a local GeoDataFrame and saved.
    4) The metrics are computed.

    Args:
        cfg (DictConfig): The config dict
        force_recreate (bool, optional): If True, recreates even if already exists. Defaults to False.

    Returns:
        dict: The metrics
    """

    # Run name serves as unique identifier for the run
    run_name = get_run_name(cfg)
    print(f"Running pipeline for {run_name}")

    local_folder = Path(cfg.local_folder) if isinstance(cfg.local_folder, str) else cfg.local_folder
    fp_preds_local = local_folder / f"{run_name}.geojson"

    if not fp_preds_local.exists() or force_recreate:
        # Check folder exists
        folder_preds_asset_id = cfg.gee_folder + "/preds"
        if not asset_exists(folder_preds_asset_id):
            create_folder(folder_preds_asset_id)
        preds_asset_id = folder_preds_asset_id + f"/{run_name}"

        print(f"Predictions asset ID: {preds_asset_id}")
        if not asset_exists(preds_asset_id) or force_recreate:
            # Compute predictions on GEE

            # Load classifier
            classifier = load_or_create_classifier(cfg)
            classifier = classifier.setOutputMode("PROBABILITY")

            # Get features ready
            sat = get_sat_from_cfg(cfg)
            fc_test = get_dataset_ready(
                sat,
                "test",
                cfg.data.time_periods["post"],
                extract_wind=cfg.data.extract_winds,
            )
            print(f"Test set for {sat}, {cfg.data.time_periods['post']}: {fc_test.size().getInfo()} features")

            # Classify and export preds
            print("Classifying and exporting predictions")
            preds = fc_test.classify(classifier)

            task = ee.batch.Export.table.toAsset(
                collection=preds,
                description=f"preds for {run_name}",
                assetId=preds_asset_id,
            )
            task.start()

            print("Waiting for predictions to be exported...")
            time_start = time.time()
            while not asset_exists(preds_asset_id):
                time.sleep(5)
            print(f"Predictions exported in {print_sec(time.time() - time_start)}")

        else:
            print(f"Predictions for {run_name} already exist")

        # Reading predictions as GeoDataFrame
        time_start = time.time()
        gdf = geemap.ee_to_df(ee.FeatureCollection(preds_asset_id))
        print(f"GeoDataFrame created in {print_sec(time.time() - time_start)} ({gdf.shape})")

        # Aggregate predictions, add date and geometry for UNOSAT and change format to match get_metrics function
        all_labels = load_unosat_labels().reset_index()
        gdf = (
            gdf.groupby(["unosat_id", "aoi", "start_post"])
            .classification.mean()
            .mul(255)
            .astype(int)
            .reset_index()
            .pivot(
                index=["unosat_id", "aoi"],
                columns="start_post",
                values="classification",
            )
            .sort_values(["aoi", "unosat_id"])
            .join(
                all_labels[["aoi", "unosat_id", "date", "geometry"]].set_index(["aoi", "unosat_id"]),
                on=["aoi", "unosat_id"],
            )
        )
        gdf.rename(
            columns={c: f"pred_{c}" for c in gdf.columns if c.startswith("202")},
            inplace=True,
        )
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry")
        fp_preds_local.parent.mkdir(exist_ok=True, parents=True)
        gdf.to_file(fp_preds_local, driver="GeoJSON")
        print(f"Geodataframe saved to {fp_preds_local}")
    else:
        gdf = gpd.read_file(fp_preds_local).set_index(["unosat_id", "aoi"])

    # Get metrics and print classification report
    return get_metrics(
        gdf,
        threshold=0.5,
        method="date-wise",
        print_classification_report=True,
        only_2022_for_pos=True,
        digits=3,
        return_preds=False,
    )


def load_or_create_classifier(cfg: DictConfig) -> ee.Classifier:
    """
    Load the classifier if it exists, otherwise create, train and save it.

    Args:
        cfg (DictConfig): The config dict

    Returns:
        ee.Classifier: The classifier trained
    """

    # Check folder exists
    classifier_folder_asset_id = cfg.gee_folder + "/classifiers"
    if not asset_exists(classifier_folder_asset_id):
        create_folder(classifier_folder_asset_id)
    asset_id = classifier_folder_asset_id + f"/{get_run_name(cfg)}"

    if not asset_exists(asset_id):
        print("training and exporting classifier")
        classifier = get_classifier_trained(cfg, verbose=1)
        export_classifier(classifier, asset_id)

        print("Waiting for classifier to be exported...")
        time_start = time.time()
        while not asset_exists(asset_id):
            time.sleep(3)
        print(f"Classifier exported in {print_sec(time.time() - time_start)}")

    print(f"Loading classifier from {asset_id}")
    return load_classifier(asset_id)


def get_classifier_trained(cfg: DictConfig, verbose: int = 1) -> ee.Classifier:
    """
    Train the classifier from the config.

    Args:
        cfg (DictConfig): The config dict
        verbose (int, optional): Verbosity. Defaults to 1.

    Returns:
        ee.Classifier: The classifier trained
    """

    # Load classifier
    classifier = classifier_factory(cfg.model_name, seed=cfg.seed, verbose=verbose, **cfg.model_kwargs)

    # Load features ready
    sat = get_sat_from_cfg(cfg)
    fc = get_dataset_ready(sat, "train", cfg.data.time_periods["post"], extract_wind=cfg.data.extract_winds)
    if verbose:
        print(f"Training set for {sat}, {cfg.data.time_periods['post']}: {fc.size().getInfo()} features")

    # Get names of features
    features_names = get_features_names(cfg)
    if verbose:
        print(f"Number of features: {len(features_names)}")

    # Train classifier
    classifier = classifier.train(features=fc, classProperty="label", inputProperties=features_names)

    # Train also on test set if required
    if cfg.train_on_all_data:
        print("Training also on test set")
        fc_test = get_dataset_ready(sat, "test", cfg.data.time_periods["post"], extract_wind=cfg.data.extract_winds)
        if verbose:
            print(f"Test set for {sat}, {cfg.data.time_periods['post']}: {fc_test.size().getInfo()} features")
        classifier = classifier.train(features=fc_test, classProperty="label", inputProperties=features_names)

    return classifier


if __name__ == "__main__":
    from omegaconf import OmegaConf

    from src.constants import AOIS_TEST, ASSETS_PATH, DATA_PATH

    LOCAL_FOLDER = DATA_PATH / "ablation_runs"
    GEE_FOLDER = ASSETS_PATH + "ablation_runs"
    if not asset_exists(GEE_FOLDER):
        create_folder(GEE_FOLDER)

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
            local_folder=LOCAL_FOLDER,
        )
    )

    metrics = full_pipeline(cfg, force_recreate=False)
