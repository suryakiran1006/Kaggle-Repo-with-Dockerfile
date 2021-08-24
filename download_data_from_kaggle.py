import warnings
import fire
import os
import zipfile
import tempfile
from pathlib import Path
import pathlib
import shutil


def create_kaggle_api(kaggle_username:str = None,kaggle_key:str = None):
    """
    Reads user credentials from env,authenticates them, and returns the api object
    """
    os.environ["KAGGLE_USERNAME"] = kaggle_username
    os.environ["KAGGLE_KEY"] = kaggle_key
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    return api

def download_competition_files(kaggle_api=None,competition_name:str = None,path:pathlib.PurePath = None,force:bool = False,quiet:bool = True):
    """
    Download competition files to a local path
    """
    kaggle_api.competition_download_files(competition=competition_name,path=path,force=force,quiet=quiet)

def move_files_from_src_to_dest(src_path:pathlib.PurePath = None,dest_path:pathlib.PurePath = None):
    """
    Move all the files and directories from src folder to dest folder
    """

    # Unzip and move all zip files from src_path to dest_path
    if src_path.suffix == ".zip":
        if len(src_path.stem.split(".")) > 1:
            # Format of the file is not zip. Rename the file to <filename>_zip.<frmt>
            frmt = src_path.stem.split(".")[-1]
            filename = ".".join(src_path.stem.split(".")[:-1])+"_zip."+str(frmt)
            dest_filename = dest_path.joinpath(filename)
            # Providing a staging area is necessary or else files might be overwirtten
            with zipfile.ZipFile(src_path,"r") as zip_file:
                zip_file.extractall(path=Path.cwd())
            shutil.move(str(src_path.joinpath(src_path.name)),str(dest_filename))
        else:
            # File is a zip file. Just unzip it
            with zipfile.ZipFile(src_path,"r") as zip_file:
                zip_file.extractall(path=dest_path)
                src_path.unlink()
    # Move all csv files from src_path to dest_path
    elif src_path.suffix == ".csv":
        shutil.move(str(src_path),str(dest_path.joinpath(src_path.name)))
    # No file should have a format other than zip or csv
    else:
        warnings.warn("We found a file that is neither zip nor csv")

def unzip_files(src_path:pathlib.PurePath = None,dest_path:pathlib.PurePath = None):
    """
    Unzip the files in src_path and move them to tempdir
    """

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir_path = Path(tempdir)
        # Move all files from src_path to tempdir
        for f in src_path.glob("*"):
            move_files_from_src_to_dest(f,tempdir_path)

        # Move every file in tempdir tp dest_dir
        for f in tempdir_path.glob("*"):
            move_files_from_src_to_dest(f,dest_path)


def main(kaggle_username:str = None,kaggle_key:str = None,competition_name:str=None,src_path:pathlib.PurePath = None,dest_path:pathlib.PurePath = None,force:bool = False,quiet:bool = True):
    """
    main function that binds the entire program
    """
    src_path = Path(src_path)
    dest_path = Path(dest_path)
    api = create_kaggle_api(kaggle_username,kaggle_key)
    download_competition_files(kaggle_api=api,competition_name=competition_name,path=src_path,force=force,quiet=quiet)
    unzip_files(src_path=src_path,dest_path=dest_path)


if __name__ == "__main__":
    fire.Fire(main)
