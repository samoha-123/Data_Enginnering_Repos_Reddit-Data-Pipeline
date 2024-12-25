"""
This module provides a pipeline to upload files to AWS S3.

Functions:
    upload_s3_pipeline(file_path):
        Connects to AWS S3, creates a bucket if it does not exist, and uploads a file to the specified bucket.

Usage:
    Run this script with the file name as the first command line argument.
    Example: python aws_s3_pipeline.py <file_name>

Modules:
    etls.aws_etl:
        - connect_to_s3: Establishes a connection to AWS S3.
        - create_bucket_if_not_exist: Creates an S3 bucket if it does not already exist.
        - upload_to_s3: Uploads a file to the specified S3 bucket.
    utils.constants:
        - AWS_BUCKET_NAME: The name of the S3 bucket.
        - AWS_S3_RAW_FILE_NAME: The base name for the raw file to be uploaded to S3.
"""
import sys
import os

# Add the parent directory to the sys.path
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.aws_etl import connect_to_s3, create_bucket_if_not_exist, upload_to_s3
from utils.constants import AWS_BUCKET_NAME, AWS_S3_RAW_FILE_NAME

def upload_s3_pipeline(file_path):
    """
    Uploads a file to an S3 bucket.
    This function connects to an S3 service, creates a bucket if it does not exist,
    and uploads the specified file to the bucket with a predefined name.
    Args:
        file_path (str): The local path to the file that needs to be uploaded.
    Raises:
        Exception: If there is any error during the upload process, the exception
                   is caught, an error message is printed, and the program exits
                   with a status code of 1.
    """
    
    print("Starting upload_s3_pipeline")
    try:
        s3 = connect_to_s3()
        create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
        upload_to_s3(s3, file_path, AWS_BUCKET_NAME, f'{AWS_S3_RAW_FILE_NAME}.csv')
        print("upload_s3_pipeline completed successfully")
    except Exception as e:
        print(f'Error in upload_s3_pipeline: {e}')
        sys.exit(1)

if __name__ == '__main__':
    # get the file name from the command line first argument
    file_name = sys.argv[1]
    file_path = f'/home/glue_user/workspace/data/output/{file_name}.csv'
    print("Starting script with file: %s", file_path)
    upload_s3_pipeline(file_path)
    print("Script completed")