import ee

from src.classification.reducers import get_reducers
from src.constants import AOIS_TEST, AOIS_TRAIN, ASSETS_PATH, S2_BANDS, UKRAINE_WAR_START
from src.data.unosat import load_unosat_labels_gee
from src.data.utils import get_all_aois
from src.utils.gee import init_gee

init_gee()


def create_dataset_ready_all_dates_s2(
    split: str,
    damages_to_keep: list[int] | None,
    d_periods: dict[str, list[tuple[str]]],
    extract_window: str,
    reducer_names: list[str],
    export: bool = False,
) -> ee.FeatureCollection:
    """
    Create the feature collection with the extracted features ready for classification for Sentinel-2.

    For each UNOSAT point, each orbit and each combination pre/post periods, we extract the features from the time
    series with the given reducers.

    Args:
        split (str): The split to use (train or test).
        damages_to_keep (list[int] | None): The damages to keep. If None, keep all damages.
        d_periods (dict[str, list[tuple[str]]): The time periods to use. The keys are "pre" and "post".
        extract_window (str): The extraction window to use. Can be "1x1" or "3x3".
        reducer_names (list[str]): The reducers to use.
        export (bool): Whether to export the feature collection as asset. Default to False

    Returns:
        ee.FeatureCollection: The feature collection.
    """

    aois = AOIS_TRAIN if split == "train" else AOIS_TEST
    fs = []
    for pre in d_periods["pre"]:
        for post in d_periods["post"]:
            d_periods_ = dict(pre=pre, post=post)
            fs += create_dataset_s2(aois, damages_to_keep, d_periods_, extract_window, reducer_names)
    fc = ee.FeatureCollection(fs).flatten()

    if export:
        period = "1year" if len(d_periods["post"]) == 2 else "3months"
        ee.batch.Export.table.toAsset(
            collection=fc,
            description=f"{split} data all {period} dates",
            assetId=ASSETS_PATH + f"features_ready/s2_{extract_window}_{period}_{split}",
        ).start()
        print(f"Exporting {split} data all {period} dates")

    return fc


def create_dataset_s2(
    aois: list[str],
    damages_to_keep: list[int] | None,
    d_periods: dict[str, tuple[str, str]],
    extract_window: str,
    reducer_names: list[str],
) -> list[ee.FeatureCollection]:
    """
    Create the feature collection with the extracted features ready for classification.

    For each UNOSAT point, each orbit and each combination pre/post periods, we extract the features from the time
    series with the given reducers.

    Args:
        aois (list[str]): The areas of interest.
        damages_to_keep (list[int] | None): The damages to keep. If None, keep all damages.
        d_periods (dict[str, tuple[str, str]]): The time periods to use. The keys are "pre" and "post" and values are
            date tuples (start, end).
        extract_window (str): The extraction window to use. Can be "1x1" or "3x3".
        reducer_names (list[str]): The reducers to use.

    Returns:
        list[ee.FeatureCollection]: The feature collection
    """

    # Function to extract features (mean, std, ...) from the collection
    reducer_names = list(reducer_names)  # make sure it is not a ListConfig
    reducer = get_reducers(reducer_names)

    end_post_period = d_periods["post"][1]
    if end_post_period <= UKRAINE_WAR_START:
        label = 0
    else:
        label = 1  # but need extra filtering to remove unknown ones

    features = []
    for aoi in get_all_aois():
        if aoi not in aois:
            continue

        points = load_unosat_labels_gee(aoi, True)
        points = points.filter(ee.Filter.inList("damage", list(damages_to_keep)))
        points = points.map(
            lambda f: f.set(
                {
                    "label": label,
                    "aoi": aoi,
                    "start_pre": d_periods["pre"][0],
                    "end_pre": d_periods["pre"][1],
                    "start_post": d_periods["post"][0],
                    "end_post": d_periods["post"][1],
                }
            )
        )
        if label == 1:
            # Only keep rows for which we know the label for sure
            # (the analysis was done before the end of the post period
            points = points.filter(ee.Filter.lte("date", end_post_period))

        fc = get_fc_ts_s2(aoi, extract_window, damages_to_keep)
        for name_period, (start, end) in d_periods.items():
            fc_dates = fc.filterDate(start, end)
            prefix = f"{name_period}_{extract_window}"

            def extract_features_per_point(point):
                point = ee.Feature(point)
                for column in S2_BANDS:
                    # Filter based on "unosat_id" and reduce for the current column
                    fc_dates_points = fc_dates.filter(ee.Filter.eq("unosat_id", point.get("unosat_id")))
                    n_dates = fc_dates_points.size()
                    stats = fc_dates_points.reduceColumns(reducer, [column])
                    stats = stats.rename(reducer_names, [f"{column}_{prefix}_{c}" for c in reducer_names])
                    point = point.set(stats).set(f"n_tiles_{name_period}", n_dates)
                return point

            points = points.map(extract_features_per_point)
        features.append(points)
    return features


def get_fc_ts_s2(aoi: str, extract: str, damages_to_keep: list[int] | None = [1, 2]) -> ee.FeatureCollection:
    """Load precomputed features for training or testing given the aoi."""

    fc_path = ASSETS_PATH + f"intermediate_features/ts_s2_{extract}/{aoi}"
    fc = ee.FeatureCollection(fc_path)

    if damages_to_keep is not None:
        fc = fc.filter(ee.Filter.inList("damage", damages_to_keep))
    return fc


if __name__ == "__main__":
    damages_to_keep = [1, 2]
    extract_winds = "1x1"  # Extract features for 1x1 windows (ie no spatial mean)
    time_periods = {
        "pre": [("2020-02-24", "2021-02-23")],  # always use only this one for Ukraine
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
        # "post": [("2021-02-24", "2022-02-23"), ("2022-02-24", "2023-02-23")], # 1 year post period
    }
    reducer_names = ["mean", "stdDev", "median", "min", "max", "skew", "kurtosis"]

    create_dataset_ready_all_dates_s2(
        "train",
        damages_to_keep,
        time_periods,
        extract_winds,
        reducer_names,
        export=True,
    )
    create_dataset_ready_all_dates_s2("test", damages_to_keep, time_periods, extract_winds, reducer_names, export=True)
