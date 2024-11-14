from pathlib import Path

# ------------------- PROJECT CONSTANTS -------------------
AOIS_TRAIN = [f"UKR{i}" for i in range(1, 5)]
AOIS_TEST = [f"UKR{i}" for i in range(5, 19)]
AOIS = AOIS_TRAIN + AOIS_TEST

S1_BANDS = ["VV", "VH"]
S2_BANDS = ["B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8A", "B9", "B11", "B12"]

UKRAINE_WAR_START = "2022-02-24"


# ------------------- LOCAL PATH CONSTANTS -------------------
constants_path = Path(__file__)
SRC_PATH = constants_path.parent
PROJECT_PATH = SRC_PATH.parent

SECRETS_PATH = PROJECT_PATH / "secrets"

DATA_PATH = PROJECT_PATH / "data"

OVERTURE_PATH = DATA_PATH / "overture_buildings"

# ------------------- GEE PATH CONSTANTS -------------------
ASSETS_PATH = "projects/rmac-ethz/assets/ukraine-mapping-tool/"
OLD_ASSETS_PATH = "projects/rmac-ethz/assets/"
