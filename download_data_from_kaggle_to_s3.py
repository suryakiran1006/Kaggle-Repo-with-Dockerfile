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

def move_files_from_src(src_path:pathlib.PurePath = None):
    """
    Move all the files and directories from src folder to dest folder
    """

    # Unzip and move all zip files from src_path to dest_path
    if src_path.suffix == ".zip":
        if len(src_path.stem.split(".")) > 1:
            # Format of the file is not zip. Rename the file to <filename>_zip.<frmt>
            frmt = src_path.stem.split(".")[-1]
            filename = ".".join(src_path.stem.split(".")[:-1])+"_zip."+str(frmt)
            src_new_filename = src_path.parent.joinpath(filename)
            # Providing a staging area is necessary or else files might be overwirtten
            with zipfile.ZipFile(src_path,"r") as zip_file:
                zip_file.extractall(path=src_path.parent)
            shutil.move(str(src_path),str(src_new_filename))
        else:
            # File is a zip file. Just unzip it
            with zipfile.ZipFile(src_path,"r") as zip_file:
                zip_file.extractall(path=src_path)

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
        key = f.parts[3:]
        key = Path(*key)
        if f.is_dir():
            continue
        s3_resource.meta.client.upload_file(Filename=str(f),Bucket=bucket_name,Key=str(key))


def unzip_files(kaggle_api=None,competition_name:str = None,force:bool = False,quiet:bool = True,bucket_name:str = None):
    """
    Unzip the files in src_path and move them to tempdir
    kwargs are all the arguments for download_competition_files
    """

    with tempfile.TemporaryDirectory() as tempdir:
        
        download_competition_files(kaggle_api = kaggle_api,competition_name = competition_name,force = force,quiet = quiet,path = Path(tempdir))

        # Move all files from src_path to tempdir
        with zipfile.ZipFile(Path(tempdir).joinpath(competition_name+".zip"),"r") as zip_file:
            zip_file.extractall(tempdir)
        Path(tempdir).joinpath(competition_name+".zip").unlink()

        # Move every file in tempdir tp dest_dir
        for f in Path(tempdir).glob("*"):
            move_files_from_src(f)

        move_from_local_to_s3(bucket_name=bucket_name,local_path=Path(tempdir))


def main(kaggle_username:str = None,kaggle_key:str = None,competition_name:str=None,force:bool = False,quiet:bool = True,bucket_name:str = None):
    """
    main function that binds the entire program
    """
    api = create_kaggle_api(kaggle_username,kaggle_key)
    unzip_files(kaggle_api = api,competition_name = competition_name,force = force,quiet = quiet,bucket_name = bucket_name)


if __name__ == "__main__":
    fire.Fire(main)
