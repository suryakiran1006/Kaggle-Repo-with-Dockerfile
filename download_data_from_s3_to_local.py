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


def create_boto3_session(aws_access_key_id:str = None,aws_secret_access_key:str = None,region_name:str = None):
    """
    Create and return boto3 session
    """
    session = boto3.session.Session(aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,region_name=region_name)
    return session.resource("s3")

def move_from_s3_to_local(bucket_name:str = None,dest_path:pathlib.Path = None):
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
    
    bucket_object = s3_resource.Bucket(name=bucket_name)
    for f in tqdm(bucket_object.objects.all()):
        print(f)
        s3_resource.Object(bucket_name,f.key).download_file(f"{dest_path.joinpath(f.key)}")


def main(bucket_name:str = None,dest_path:str = None):
    """
    main function that binds the entire program
    """
    dest_path = Path(dest_path)
    move_from_s3_to_local(bucket_name=bucket_name,dest_path=dest_path)


if __name__ == "__main__":
    fire.Fire(main)
