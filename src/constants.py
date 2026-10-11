import os
from pathlib import Path

# Paths to repositories and parameters
SRC_PATH = Path(__file__).resolve().parent
PROJECT_PATH = SRC_PATH.parent

SECRETS_PATH = PROJECT_PATH / "secrets"
DATA_PATH = PROJECT_PATH / "data"
OVERTURE_PATH = DATA_PATH / "overture_buildings"

# ---------------------------- GEE PATH CONSTANTS ----------------------------
# Dynamically retrieve the currently registered user's GEE cloud project path
try:
    import ee
    data_mod = ee.data
    # Safe methods across different versions of earthengine-api
    if hasattr(data_mod, 'get_project') and data_mod.get_project():
        current_project = data_mod.get_project()
    elif hasattr(data_mod, '_cloud_project') and data_mod._cloud_project:
        current_project = data_mod._cloud_project
    else:
        raise ValueError(
            "The cloud project path returned by Earth Engine is empty. "
            "This usually happens when ee.Initialize() was called without a specific 'project' ID."
        )
    ASSETS_PATH = f"projects/{current_project}/assets/"
except Exception as e:
    raise RuntimeError(
        f"\n\n[ERROR] Failed to dynamically retrieve the Google Earth Engine project path!\n"
        f"UNDERLYING ERROR DETAILS: {e}\n"
        "RESOLUTION: Please re-run the authentication and initialization steps in your environment "
        "and explicitly provide your valid Earth Engine Project ID, for example:\n"
        "    import ee\n"
        "    ee.Authenticate()\n"
        "    ee.Initialize(project='YOUR-PROJECT-ID-HERE')\n"
    ) from e
