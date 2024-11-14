import ee

from src.constants import S2_BANDS


def get_s2_collection(
    geo=None,
    start="1900-01-01",
    end="2099-12-31",
    QA_BAND="cs",
    CLEAR_THRESHOLD=0.6,
    bands=None,
):
    """From https://developers.google.com/earth-engine/datasets/catalog/GOOGLE_CLOUD_SCORE_PLUS_V1_S2_HARMONIZED"""

    # Load S2 collection
    s2 = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")

    # Load S2 cloud mask
    csPlus = ee.ImageCollection("GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED")

    # Join two collections
    s2_with_cs = s2.filterBounds(geo).filterDate(ee.Date(start), ee.Date(end)).linkCollection(csPlus, [QA_BAND])

    # Mask out cloudy pixels
    s2_with_cs = s2_with_cs.map(lambda img: img.updateMask(img.select(QA_BAND).gte(CLEAR_THRESHOLD)))

    # Keep only certain bands
    if bands == "all":
        return s2_with_cs
    elif bands is not None:
        bands = bands + [QA_BAND]
        return s2_with_cs.select(bands)
    else:
        bands = S2_BANDS + [QA_BAND]
        return s2_with_cs.select(bands)
