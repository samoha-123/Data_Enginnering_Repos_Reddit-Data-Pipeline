"""
This module provides functions to interact with Amazon S3 using the s3fs library.
It includes functions to connect to S3, create a bucket if it does not exist, and upload files to S3.
Functions:
        connect_to_s3():
                Returns an S3FileSystem object if the connection is successful.
                Raises an exception if there is an error connecting to S3.
        create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket: str):
                Creates an S3 bucket if it does not already exist.
                Raises an exception if there is an error during the bucket creation process.
        upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket: str, s3_file_name: str):
                Raises an exception if there is an error during the file upload.
"""
import s3fs
from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY

def connect_to_s3():
        """
        Establishes a connection to Amazon S3 using s3fs.
        This function attempts to create a connection to Amazon S3 using the provided
        AWS access key ID and secret access key. If the connection is successful, it
        returns an S3FileSystem object. If the connection fails, it prints an error
        message and raises the exception.
        Returns:
                s3fs.S3FileSystem: An S3FileSystem object if the connection is successful.
        Raises:
                Exception: If there is an error connecting to S3.
        """
        try:
                s3 = s3fs.S3FileSystem(anon=False,
                                       key= AWS_ACCESS_KEY_ID,
                                       secret=AWS_ACCESS_KEY)

                print('Connected to s3')
                return s3
        except Exception as e:
                print(f'Error connecting to s3: {e}')
                raise e

def create_bucket_if_not_exist(s3: s3fs.S3FileSystem, bucket:str):
        """
        Create an S3 bucket if it does not already exist.
        This function checks if the specified S3 bucket exists. If it does not,
        it creates the bucket. If the bucket already exists, it prints a message
        indicating so.
        Args:
                s3 (s3fs.S3FileSystem): An instance of the s3fs.S3FileSystem class used to interact with S3.
                bucket (str): The name of the S3 bucket to check or create.
        Raises:
                Exception: If there is an error during the bucket creation process, an exception is raised and the error message is printed.
        """
        
        try:        
                if not s3.exists(bucket):
                    s3.mkdir(bucket)
                    print("Bucket created")
                else:
                    print("Bucket already exists")
        except Exception as e:
                print(f'Error creating bucket: {e}')
                raise e


def upload_to_s3(s3: s3fs.S3FileSystem, file_path: str, bucket:str, s3_file_name: str):
        """
        Uploads a file to an S3 bucket.
        Parameters:
        s3 (s3fs.S3FileSystem): The S3 filesystem object.
        file_path (str): The local path to the file to be uploaded.
        bucket (str): The name of the S3 bucket.
        s3_file_name (str): The name to be assigned to the file in the S3 bucket.
        Raises:
        Exception: If there is an error during the file upload.
        """
        
        try:
                s3.put(file_path, bucket+'/raw/'+ s3_file_name)
                print('File uploaded to s3 at ', bucket+'/raw/'+ s3_file_name)
        except Exception as e:
                print(f'Error uploading file to s3: {e}')
                raise e