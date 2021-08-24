import pdb
from tqdm import tqdm
import warnings
import fire
import os
import zipfile
import tempfile
from pathlib import Path
import pathlib
import shutil
import boto3


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
    elif src_path.is_dir():
        shutil.move(str(src_path),str(dest_path))
    # No file should have a format other than zip or csv
    else:
        warnings.warn("We found a file that is neither zip nor csv")

def create_boto3_session(aws_access_key_id:str = None,aws_secret_access_key:str = None,region_name:str = None):
    """
    Create and return boto3 session
    """
    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)
    return session.resource("s3")

def move_from_local_to_s3(bucket_name:str = None,local_path:pathlib.Path = None):
    """
    Move all the files and directories from local to S3
    Retrieve aws_access_key_id,aws_secret_access_key,region_name to environment
    """
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    region_name = os.environ["REGION_NAME"]

    s3_resource = create_boto3_session(aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key,
                                        region_name=region_name)

    for f in tqdm(local_path.rglob("*")):
        if f.is_dir():
            continue
        s3_resource.meta.client.upload_file(Filename=str(f),Bucket=bucket_name,Key=str(f))


def unzip_files(competition_zipfile:zipfile.ZipFile = None,dest_path:pathlib.PurePath = None):
    """
    Unzip the files in src_path and move them to tempdir
    """

    with tempfile.TemporaryDirectory() as tempdir:
        # Move all files from src_path to tempdir
        with zipfile.ZipFile(competition_zipfile,"r") as zip_file:
            zip_file.extractall(tempdir)

        # Move every file in tempdir tp dest_dir
        for f in Path(tempdir).glob("*"):
            print(f)
            move_files_from_src_to_dest(f,dest_path)


def main(kaggle_username:str = None,kaggle_key:str = None,competition_name:str=None,src_path:pathlib.PurePath = None,dest_path:pathlib.PurePath = None,force:bool = False,quiet:bool = True,bucket_name:str = None):
    """
    main function that binds the entire program
    """
    src_path = Path(src_path)
    dest_path = Path(dest_path)
    api = create_kaggle_api(kaggle_username,kaggle_key)
    download_competition_files(kaggle_api=api,competition_name=competition_name,path=src_path,force=force,quiet=quiet)
    unzip_files(competition_zipfile=competition_name+".zip",dest_path=dest_path)
    move_from_local_to_s3(bucket_name=bucket_name,local_path=dest_path)


if __name__ == "__main__":
    fire.Fire(main)
