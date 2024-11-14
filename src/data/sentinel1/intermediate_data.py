import ee

from src.constants import ASSETS_PATH
from src.data.sentinel1.collection import get_s1_collection
from src.data.unosat import load_unosat_geo_gee, load_unosat_labels_gee
from src.utils.gee import asset_exists, create_folders_recursively, fill_nan_with_mean, init_gee

init_gee()

# In the original code, it was slightly different: '2020-03-01' and '2023-03-01'
START_DATE = "2020-02-24"
END_DATE = "2024-02-24"


def create_fc_aoi_orbit(
    aoi: str,
    orbit: str,
    scale: int = 10,
    export: bool = True,
) -> ee.FeatureCollection:
    """
    Creates a feature collection with all bands values for each date and each point.

    i.e., we stack all time series for the given AOI.

    Args:
        aoi (str): The area of interest.
        orbit (str): The orbit number.
        scale (int, optional): The scale in meters. Defaults to 10.
        export (bool, optional): Whether to export the feature collection as asset. Defaults to True.

    Returns:
        ee.FeatureCollection: The feature collection.
    """

    # Check if asset id exists
    extract = f"{scale // 10}x{scale // 10}"
    folder = ASSETS_PATH + f"intermediate_features/ts_s1_{extract}"
    asset_id = folder + f"/{aoi}_orbit{orbit}"
    if asset_exists(asset_id):
        print(f"Asset {asset_id} already exists.")
        return
    create_folders_recursively(folder)

    # Load UNOSAT labels and geometry
    labels = load_unosat_labels_gee(aoi, True)
    # labels = labels.filter(ee.Filter.inList("damage", [1,2])) # now we extract for all labels in case we need them
    geo = load_unosat_geo_gee(aoi)

    # Load S1 collection
    s1 = get_s1_collection(geo, START_DATE, END_DATE).filterMetadata("relativeOrbitNumber_start", "equals", orbit)
    s1 = fill_nan_with_mean(s1)

    def extract_all_imgs(img_col, feat_col, scale, img_bands=["VV", "VH"]):
        def extract_img(img):
            def extract_point(point):
                bands = img.select(img_bands).reduceRegion(
                    reducer=ee.Reducer.mean(), geometry=point.geometry(), scale=scale
                )
                return point.set(bands).set("system:time_start", img.get("system:time_start"))

            return feat_col.map(extract_point)

        return img_col.map(extract_img).flatten()

    fc_extracted = extract_all_imgs(s1, labels, scale)

    if export:
        # Export as asset
        ee.batch.Export.table.toAsset(
            collection=fc_extracted,
            description=f"{aoi}_orbit{orbit}_{scale}m",
            assetId=asset_id,
        ).start()
        print(f"Exporting {aoi}_orbit{orbit}_{scale}m")
    return fc_extracted


if __name__ == "__main__":
    from src.data.utils import aoi_orbit_iterator

    scale = 10
    for aoi, orbit in aoi_orbit_iterator():
        create_fc_aoi_orbit(aoi, orbit, scale=scale, export=True)
