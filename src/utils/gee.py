import ee


def init_gee(project: str = "rmac-ethz") -> None:
    """
    Initialize GEE. Works also when working through ssh

    Args:
        project (str, optional): Name of the project. Defaults to "rmac-ethz".
    """
    try:
        ee.Initialize(project=project)
    except Exception:
        ee.Authenticate(auth_mode="localhost")
        ee.Initialize(project=project)


def fc_to_list(fc: ee.FeatureCollection) -> ee.List:
    """Transforms a feature collection to a list."""
    return fc.toList(fc.size())


def fill_nan_with_mean(col: ee.FeatureCollection) -> ee.FeatureCollection:
    """Fill NaN values with the mean of the column."""
    col_mean = col.reduce(ee.Reducer.mean())

    def _fill_nan_with_mean(img):
        mask = img.mask().Not()
        filled_img = img.unmask().add(col_mean.multiply(mask))
        filled_img = filled_img.copyProperties(img, img.propertyNames())
        return filled_img

    return col.map(_fill_nan_with_mean)


# ======= ASSET MANAGEMENT =======
def asset_exists(asset_id: str) -> bool:
    """Check if an asset exists."""
    try:
        ee.data.getAsset(asset_id)
        return True
    except ee.ee_exception.EEException:
        return False


def delete_asset(asset_id: str) -> bool:
    """Delete an asset."""
    try:
        ee.data.deleteAsset(asset_id)
        print(f"{asset_id} deleted")
        return True
    except ee.ee_exception.EEException:
        return False


def rename_asset(original_path: str, new_path: str) -> None:
    """Rename an asset."""
    try:
        ee.data.renameAsset(original_path, new_path)
        print(f"Asset renamed from {original_path} to {new_path}")
    except Exception as e:
        print(f"Error renaming asset: {e}")


def create_folder(folder_path: str, verbose: int = 1) -> None:
    """Create a folder in GEE."""
    try:
        ee.data.createAsset({"type": "FOLDER"}, folder_path)
        if verbose:
            print(f"Folder created at {folder_path}")
    except Exception as e:
        if verbose:
            print(f"Error creating folder: {e}")


def list_assets(folder_path: str, print_list: bool = False) -> list[str]:
    """List all assets in a folder."""
    try:
        asset_list = [a["id"] for a in ee.data.getList({"id": folder_path})]
        if print_list:
            print(f"Assets in {folder_path}: {asset_list}")
        return asset_list
    except Exception as e:
        print(f"Error listing assets: {e}")


def create_folders_recursively(full_path: str, last_one_is_asset: bool = False):
    """Create folders recursively."""

    if last_one_is_asset:
        # ignore asset_id
        full_path = "/".join(full_path.split("/")[:-1])

    folders_to_create = []
    current_path = full_path

    # Traverse up until we find an existing folder
    while not asset_exists(current_path):
        folders_to_create.append(current_path)
        current_path = "/".join(current_path.split("/")[:-1])
        # Stop if there's no more parent (root reached)
        assert current_path != "projects", "Problem, we should never reach the root !"

    if not folders_to_create:
        return

    # Create the folders from top to bottom
    for folder in reversed(folders_to_create):
        create_folder(folder)
