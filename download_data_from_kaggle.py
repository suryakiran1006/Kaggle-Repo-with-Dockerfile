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

def unzip_files(src_path:pathlib.PurePath = None,dest_path:pathlib.PurePath = None):
    """
    Unzip the files in src_path and move them to tempdir
    """

    with tempfile.TemporaryDirectory() as tempdir:
        for f in src_path.glob("*"):
            # Unzipping all zip files from src_path to tempdir
            if f.suffix == ".zip":
                with zipfile.ZipFile(f,"r") as f:
                    f.extractall(tempdir)
            # Copying all csv files from src_path to tempdir
            elif f.suffix == ".csv":
                dest_filename = Path(tempdir).joinpath(f.name)
                shutil.copy(f,dest_filename)
                f.unlink()
            # Not doing anything if the file isn't zip or csv
            else:
                pass

        # For every file in tempdir, unzip zipped files
        for f in Path(tempdir).glob("*"):
            if f.suffix == ".zip":
                if len(f.stem.split(".")) > 1:
                    # Format of the file is not zip. Rename the file as <filename>_zip.<fmrt>
                    frmt = f.stem.split(".")[-1]
                    filename = ".".join(f.stem.split(".")[:-1])
                    src_filename = Path(f.stem)
                    dest_filename = Path(filename + "_zip." + frmt)
                    with zipfile.ZipFile(f,"r") as zip_file:
                        zip_file.extractall(path=Path.cwd())
                    shutil.copy(src_filename,dest_filename)
                    src_filename.unlink()
                else:
                    # File is a zip file. Just unzip it
                    with zipfile.ZipFile(f,"r") as zip_file:
                        zip_file.extractall(path=Path.cwd())
                f.unlink()

        # For every file in tempdir, copy it to dsc_path
        for f in Path(tempdir).glob("*"):
            shutil.move(str(f),str(dest_path))



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
