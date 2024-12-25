"""
This script manages S3 logs by deleting log folders older than one month from an S3 bucket.
Modules:
    boto3: AWS SDK for Python to interact with AWS services.
    datetime: Provides classes for manipulating dates and times.
    re: Provides regular expression matching operations.
    sys: Provides access to some variables used or maintained by the interpreter.
    os: Provides a way of using operating system dependent functionality.
Functions:
    delete_old_log_folders(): Deletes log folders in the specified S3 bucket that are older than one month.
Constants:
    AWS_S3_AIRFLOW_LOG_BUCKET: The name of the S3 bucket containing the log folders.
    AWS_S3_AIRFLOW_LOG_KEY: The base folder in the S3 bucket containing the log folders.
Usage:
    Run this script directly to delete old log folders from the specified S3 bucket.
"""
import boto3
from datetime import datetime, timedelta
import re
import sys
import os

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import AWS_S3_AIRFLOW_LOG_BUCKET, AWS_S3_AIRFLOW_LOG_KEY

# Initialize S3 client
s3_client = boto3.client('s3')

# S3 bucket and base folder
bucket_name = AWS_S3_AIRFLOW_LOG_BUCKET
base_folder = AWS_S3_AIRFLOW_LOG_KEY

# Calculate the date one month ago from today
one_month_ago = datetime.now() - timedelta(days=30)
one_month_ago_date_str = one_month_ago.strftime('%Y-%m-%d')

# Regular expression to match the log folder pattern
log_folder_pattern = re.compile(r'dag_id=.*?/run_id=(manual__|scheduled__)(\d{4}-\d{2}-\d{2})T.*')

def delete_old_log_folders():
    """
    Deletes log folders in an S3 bucket that are older than one month.
    This function performs the following steps:
    1. Checks if the specified S3 bucket exists.
    2. Lists all objects in the specified base folder within the bucket.
    3. Iterates through the objects and identifies log folders based on a predefined pattern.
    4. Parses the date from the log folder name and deletes the folder if it is older than one month.
    Raises:
        Exception: If there is an error during the S3 operations, the exception is printed and re-raised.
    Note:
        - The function assumes the existence of the following variables:
            - s3_client: An initialized boto3 S3 client.
            - bucket_name: The name of the S3 bucket.
            - base_folder: The base folder in the S3 bucket where log folders are stored.
            - log_folder_pattern: A regex pattern to identify log folders and extract dates.
            - one_month_ago: A datetime object representing the cutoff date for deletion.
    """
    
    try:
        # Check if the bucket exists
        s3_client.head_bucket(Bucket=bucket_name)
                # List all objects in the base folder
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=base_folder)
        
        if 'Contents' not in response:
            print("No log folders found.")
            return
    
        for obj in response['Contents']:
            key = obj['Key']
            match = log_folder_pattern.match(key)
            
            if match:
                log_date_str = match.group(2)
                log_date = datetime.strptime(log_date_str, '%Y-%m-%d')
                
                if log_date < one_month_ago:
                    print(f"Deleting log folder: {key}")
                    s3_client.delete_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        print(f"Error: {e}")
        raise e
    
if __name__ == '__main__':
    try:
        delete_old_log_folders()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)