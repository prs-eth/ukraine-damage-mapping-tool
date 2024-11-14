import ee

from src.utils.gee import asset_exists


def classifier_factory(model_name: str, seed: int = 0, verbose: int = 1, **kwargs) -> ee.Classifier:
    """
    Factory function to create a classifier object based on the model_name.

    Args:
        model_name (str): Name of the model to be created.
        seed (int, optional): The random seed. Defaults to 0.
        verbose (int, optional): Verbosity. Defaults to 1.

    Returns:
        ee.Classifier: The GEE classifier object.
    """
    if model_name == "random_forest":
        classifier = ee.Classifier.smileRandomForest(**kwargs, seed=seed)
    elif model_name == "svm":
        classifier = ee.Classifier.libsvm()
    elif model_name == "boosted_trees":
        classifier = ee.Classifier.smileGradientTreeBoost(**kwargs)
    else:
        raise NotImplementedError(f"Model {model_name} not implemented.")

    classifier = classifier.setOutputMode("CLASSIFICATION")
    if verbose:
        print(f"Classifier {model_name} created and set to CLASSIFICATION mode.")
    return classifier


def export_classifier(classifier: ee.Classifier, asset_id: str):
    """
    Export the classifier to GEE as an asset.

    There is a bug in GEE that prevents exporting classifiers directly. For now, we serialize it to
    a JSON string and export it as a FeatureCollection with a single feature.

    See: https://groups.google.com/g/google-earth-engine-developers/c/WePgEdN6F0w/m/k5QheFBbCQAJ

    Args:
        classifier (ee.Classifier): The classifier
        asset_id (str): The asset ID
    """
    description = asset_id.split("/")[-1]
    if len(description) > 100:
        print("Asset description too long, truncating to 100 characters.")
        description = description[:100]

    classifier_serialized = ee.serializer.toJSON(classifier)
    col = ee.FeatureCollection(ee.Feature(ee.Geometry.Point((0, 0))).set("classifier", classifier_serialized))

    ee.batch.Export.table.toAsset(
        collection=col,
        description=asset_id.split("/")[-1],
        assetId=asset_id,
    ).start()
    print("Starting export of classifier to asset ", asset_id)


def load_classifier(asset_id: str) -> ee.Classifier:
    """
    Load a classifier from GEE.

    Args:
        asset_id (str): The asset ID.

    Returns:
        ee.Classifier: The classifier.
    """
    assert asset_exists(asset_id), f"Asset {asset_id} does not exist."
    json = ee.Feature(ee.FeatureCollection(asset_id).first()).get("classifier").getInfo()
    return ee.deserializer.fromJSON(json)
