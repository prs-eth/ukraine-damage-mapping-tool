import ee

from src.constants import S1_BANDS


def get_s1_collection(geo=None, start="1900-01-01", end="2099-12-31", orbit=None):
    # Load Sentinel-1 collection
    s1 = (
        ee.ImageCollection("COPERNICUS/S1_GRD")
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
        .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VH"))
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filter(ee.Filter.eq("platform_number", "A"))
        .filterDate(ee.Date(start), ee.Date(end))
    )

    if geo:
        s1 = s1.filterBounds(geo)

    if orbit:
        s1 = s1.filter(ee.Filter.eq("relativeOrbitNumber_start", orbit))

    # Only keep VV and VH (discard angle)
    return s1.select(S1_BANDS)
