import ee

from src.classification.reducers import get_reducers
from src.data.sentinel1.collection import get_s1_collection


def predict_geo(
    geometry: ee.Geometry,
    classifier: ee.Classifier,
    time_periods: dict[tuple[str]],
    extract_window: str,
    reducer_names: list[str],
    orbits: list[int] | None = None,
    aggregate_method: str = "mean",
    verbose: int = 1,
) -> ee.Image:
    """
    Predict the geometry using the classifier.

    Args:
        geometry (ee.Geometry): The geometry to predict.
        classifier (ee.Classifier): The classifier to use.
        time_periods (dict[tuple[str]]): The time periods to extract.
        extract_window (str): The window to extract.
        reducer_names (list[str]): The reducer names.
        orbits (list[int], optional): The orbits to extract. Defaults to None.
        aggregate_method (str, optional): The aggregation method. Defaults to "mean".
        verbose (int, optional): Verbosity. Defaults to 1.

    Returns:
        ee.Image: The predicted damage heatmap.
    """

    # Make sure the classifier is in the correct mode
    classifier = classifier.setOutputMode("PROBABILITY")

    # Get Sentinel-1 collection for the given orbit
    s1 = get_s1_collection(geometry)
    if orbits is None:
        orbits = find_orbits(s1, time_periods)
        if verbose:
            print(f"Found orbits: {orbits.getInfo()}")

    def predict_s1_orbit(orbit: int) -> ee.FeatureCollection:
        # Filter by orbit
        s1_orbit = s1.filter(ee.Filter.eq("relativeOrbitNumber_start", orbit))

        # Collection of image to ee.Image where each band is a feature
        s1_features = col_to_features(s1_orbit, reducer_names, time_periods, extract_window)

        # Predict
        return s1_features.classify(classifier)

    # Predict for each orbit individually
    preds_all = ee.ImageCollection(orbits.map(predict_s1_orbit))

    # Aggregate
    if aggregate_method == "mean":
        preds = preds_all.mean()
    elif aggregate_method == "max":
        preds = preds_all.max()
    elif aggregate_method == "min":
        preds = preds_all.min()
    elif aggregate_method == "median":
        preds = preds_all.median()
    else:
        raise ValueError(f"Unknown aggregation method: {aggregate_method}")
    return preds


def col_to_features(
    col: ee.ImageCollection,
    reducer_names: list[str],
    time_periods: dict[str, tuple[str, str]],
    extract_window: str,
) -> ee.Image:
    """
    Convert an ImageCollection to a single ee.Image where each band is a feature.

    Args:
        col (ee.ImageCollection): The collection to convert.
        reducer_names (list[str]): The reducer names.
        time_periods (dict[str, tuple[str, str]]): The time periods. ({pre: (start, end), post: (start, end)})
        extract_window (str): The window to extract, eg 1x1.

    Returns:
        ee.Image: The image with all features.
    """
    s1_features = None

    reducer_names = list(reducer_names)  # GEE does not like ListConfig
    reducer = get_reducers(reducer_names)
    original_col_names = [f"{b}_{r}" for b in ["VV", "VH"] for r in reducer_names]

    if int(extract_window[0]) > 1:
        # convolve (similar to looking at a larger window) with radius (eg 15m for 3x3 window)
        col = convolve_collection(col, 10 * int(extract_window[0]) // 2, "square", "meters")

    # Extract features for each time period
    for name_period, (start, end) in time_periods.items():
        s1_dates = col.filterDate(start, end)
        prefix = f"{name_period}_{extract_window}"

        # Reduce to features, and rename the bands
        _s1_features = s1_dates.reduce(reducer)
        _s1_features = _s1_features.select(original_col_names, get_new_names(original_col_names, prefix))
        s1_features = _s1_features if s1_features is None else s1_features.addBands(_s1_features)

    return s1_features


def find_orbits(
    s1: ee.FeatureCollection,
    time_periods: dict[str, tuple[str, str]],
    min_number: int = 5,
) -> ee.List:
    """Find all orbits that appear at least min_number in each time period."""
    list_orbits = []
    for _, (start, end) in time_periods.items():
        s1_ = s1.filterDate(start, end)
        orbits_counts = s1_.aggregate_histogram("relativeOrbitNumber_start")
        # At least 5 images per orbit (two months of data)
        orbits_counts = orbits_counts.map(lambda k, v: ee.Algorithms.If(ee.Number(v).gte(min_number), k, None))
        orbits_inference = orbits_counts.keys().map(lambda k: ee.Number.parse(k))  # cast keys back to number
        list_orbits.append(orbits_inference)
    return list_orbits[0].filter(ee.Filter.inList("item", list_orbits[1]))


def convolve_collection(
    img_col: ee.ImageCollection,
    radius: int,
    kernel_type: str = "square",
    units: str = "meters",
) -> ee.ImageCollection:
    """Convolve each image in the collection with a focal mean of radius `radius`"""

    def _convolve_mean(img):
        return img.focalMean(radius, kernel_type, units=units).set("system:time_start", img.get("system:time_start"))

    return img_col.map(_convolve_mean)


def get_new_names(bands: list[str], prefix: str) -> list[str]:
    """Add the prefix (pre or post and window) between the band and the reducer name."""
    new_bands = []
    for b in bands:
        b_, r = b.split("_")
        new_bands.append(f"{b_}_{prefix}_{r}")
    return new_bands
