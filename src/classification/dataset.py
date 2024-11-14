import ee

from src.constants import ASSETS_PATH
from src.utils.gee import asset_exists


def get_dataset_ready(
    sat: str = "s1",
    split: str = "train",
    post_dates: str = "3months",
    extract_wind: str | list[str] = "1x1",
) -> ee.FeatureCollection:
    """
    Get the feature collection with the extracted features ready for classification.

    Args:
        sat (str): The satellite to use (s1, s2 or both). Default to s1.
        split (str): The split to use (train or test).
        post_dates (str): The post dates to use (3months or 6months).
        extract_wind (str | list[str]): The extraction window to use (1x1, 3x3 or both).

    Returns:
        ee.FeatureCollection: The feature collection.
    """

    if isinstance(extract_wind, str):
        extract_wind = [extract_wind]

    # TODO: Clean assets folder, remove old ones and update the paths
    fcs = []
    for wind in extract_wind:
        asset_id = ASSETS_PATH + f"features_ready/{sat}_{wind}_{post_dates}_{split}"
        assert asset_exists(asset_id), f"Asset {asset_id} does not exist. run src/data/gee/extract_features.py first."
        fc_wind = ee.FeatureCollection(asset_id)
        fcs.append(fc_wind)

    if len(fcs) > 1:
        fc = join_fcs(fcs[0], fcs[1], subset1=None, subset2=None)
    else:
        fc = fcs[0]
    return fc


def join_fcs(
    fc1: ee.FeatureCollection,
    fc2: ee.FeatureCollection,
    subset1: list[str] | None = None,
    subset2: list[str] | None = None,
) -> ee.FeatureCollection:
    """
    Join two feature collections based on the 'unosat_id', 'start_post' and 'orbit' properties.

    Args:
        fc1 (ee.FeatureCollection): The primary feature collection.
        fc2 (ee.FeatureCollection): The secondary feature collection.
        subset1 (list[str] | None): The properties to keep from the primary collection. Default to all properties.
        subset2 (list[str] | None): The properties to keep from the secondary collection. Default to all properties.

    Returns:
        ee.FeatureCollection: The joined feature collection.
    """

    # Prepare join
    join = ee.Join.inner()
    filter = ee.Filter.And(
        ee.Filter.equals(leftField="unosat_id", rightField="unosat_id"),
        ee.Filter.equals(leftField="start_post", rightField="start_post"),
        ee.Filter.equals(leftField="orbit", rightField="orbit"),
    )
    fc_joined = join.apply(fc1, fc2, filter)

    # Merge properties from both collections (based on subset)
    def merge_features(joined_feature):
        primary = ee.Feature(joined_feature.get("primary"))
        secondary = ee.Feature(joined_feature.get("secondary"))
        return primary.set(primary.toDictionary(subset1).combine(secondary.toDictionary(subset2)))

    return fc_joined.map(merge_features)
