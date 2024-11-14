from multiprocessing import Process
from pathlib import Path

from omegaconf import DictConfig, OmegaConf
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import GoogleDriveFile
from tqdm import tqdm

from src.constants import SECRETS_PATH

# Change path to secret files
GoogleAuth.DEFAULT_SETTINGS["client_config_file"] = SECRETS_PATH / "client_secrets.json"
gauth = GoogleAuth(settings_file=SECRETS_PATH / "pydrive_settings.yaml")
drive = GoogleDrive(gauth)


def drive_to_local(
    folder_name: str,
    local_folder: Path,
    use_multiprocess: bool = False,
    delete_in_drive: bool = False,
    verbose: int = 1,
):
    """
    Download all files present in the drive folder into the local one.

    Delete files in drive when downloaded if necessary.

    Args:
        folder_name (str): The name of the drive folder
        local_folder (Path): Path to the local folder
        use_multiprocess (bool): Whether to use multiprocess or not. For now, there is
            still a bug with multiprocess, so don't do it ! TODO: debug.
            Defaults to False
        delete_in_drive (bool): If true, delete file in Drive when downloaded.
            Defaults to True
        verbose (int): If 0, no print. If 1, print file names.
    """

    # Make sure local folder exists
    local_folder.mkdir(exist_ok=True, parents=True)

    all_files = get_files_in_folder(folder_name)

    if use_multiprocess:
        # NOT WORKING YET
        processes = []
        for f in all_files:
            if (local_folder / f["title"]).exists():
                print(f'{f["title"]} already downloaded')
                continue
            p = Process(
                target=download_file_to_local,
                args=[f["id"], f["title"], local_folder, delete_in_drive],
            )
            p.start()

        for process in processes:
            process.join()

        print(f"All files moved to {local_folder}.")
    else:
        for f in tqdm(all_files):
            if (local_folder / f["title"]).exists():
                if verbose:
                    print(f'{f["title"]} already downloaded')
                continue
            download_file_to_local(
                file_id=f["id"],
                file_name=f["title"],
                local_folder=local_folder,
                delete_in_drive=delete_in_drive,
                verbose=verbose,
            )
        print(f"All files moved to {local_folder}.")


def download_file_to_local(
    file_id: str,
    file_name: str,
    local_folder: Path,
    delete_in_drive: bool = True,
    verbose: int = 1,
) -> None:
    """Download and delete a file from the drive"""
    try:
        ref = drive.CreateFile({"id": file_id})
        ref.GetContentFile(local_folder / file_name)
        if delete_in_drive:
            ref.Delete()  # permanently delete a file from the Drive (not in the trash)
            if verbose:
                print(f"File {file_name} downloaded and deleted.")
        else:
            if verbose:
                print(f"File {file_name} downloaded")
    except Exception as e:
        print(e)
        pass


def get_folder_id(folder_name: str, create_if_missing: bool = False) -> str:
    """
    Get the ID associated with the folder in the drive.

    If the folder does not exist, create it first.

    Args:
        folder_name (str): The name of the folder.
        create_if_missing (bool, optional): If true, create the folder if it does not exist.

    Returns:
        str: The folder ID
    """

    existing_folders = drive.ListFile({"q": f"title='{folder_name}'"}).GetList()
    n_folder = len(existing_folders)
    if n_folder:
        folder_id = existing_folders[0]["id"]
    elif create_if_missing:
        folder_id = create_drive_folder(folder_name)
    else:
        raise Exception(f"Folder '{folder_name}' does not exist in Drive.")
    return folder_id


def get_files_in_folder(folder_name: str, return_names: bool = False) -> list[GoogleDriveFile | str]:
    """
    Returns list of all files inside the folder.

    Args:
        folder_name (str): name of the folder
        return_names (bool, optional): If true, return names instead of GoogleDriveFile.
            Defaults to False.

    Returns:
        list[GoogleDriveFile | str]: List of files (or filenames)
    """

    folder_id = get_folder_id(folder_name)

    # Get all files in the folder.
    list_files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()
    if return_names:
        # Only returns filenames.
        return [f["title"] for f in list_files]
    else:
        return list_files


def create_drive_folder(folder_name: str) -> None:
    """
    Create a new folder in Google Drive. If folder has a '/', creates it recursively.

    Args:
        folder_name (str): The name of the folder to create.

    Raises:
        Exception: If the folder already exists.
    """

    folders = folder_name.split("/")
    parent_id = "root"

    for i, folder in enumerate(folders):
        query = f"title='{folder}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"  # noqa E501
        existing_folders = drive.ListFile({"q": query}).GetList()
        if i < len(folders) - 1:
            if existing_folders:
                # The parent folder exists, just update the parent_id
                parent_id = existing_folders[0]["id"]
            else:
                # The parent folder does not exist, create it
                file_metadata = {
                    "title": folder,
                    "parents": [{"id": parent_id}],
                    "mimeType": "application/vnd.google-apps.folder",
                }
                new_folder = drive.CreateFile(file_metadata)
                new_folder.Upload()
                parent_id = new_folder["id"]
                print(f"Folder '{folder}' created in Drive.")
        else:
            # Last folder
            if existing_folders:
                raise Exception(f"Folder '{folder_name}' already exists in Drive.")
            else:
                # Folder does not exist, create it
                file_metadata = {
                    "title": folder,
                    "parents": [{"id": parent_id}],
                    "mimeType": "application/vnd.google-apps.folder",
                }
                new_folder = drive.CreateFile(file_metadata)
                new_folder.Upload()
                print(f"Folder '{folder}' created in Drive.")

    return new_folder["id"]  # id of last folder created


def create_yaml_file_in_drive_from_config_dict(config_dict: DictConfig, folder_name: str) -> None:
    """
    Creates a yaml file in Google Drive from an OmegaConf.ConfigDict.

    Args:
        config_dict: The configuration dictionary to convert.
        folder_name (str): The name of the folder where the file will be created.
    """
    # Convert the OmegaConf.ConfigDict to a YAML string
    yaml_str = OmegaConf.to_yaml(config_dict)

    # Get the folder ID where the file will be created
    folder_id = get_folder_id(folder_name)

    # Create a new file in Google Drive with the YAML content
    file_metadata = {
        "title": "cfg.yaml",
        "parents": [{"id": folder_id}],
        "mimeType": "application/x-yaml",
    }
    file = drive.CreateFile(file_metadata)
    file.SetContentString(yaml_str)
    file.Upload()
    print(f"File '{file['title']}' created in Drive folder '{folder_name}'.")
