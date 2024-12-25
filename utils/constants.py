"""
This module contains constants and configuration settings for the Reddit Data Pipeline.

Attributes:
    SECRET (str): Reddit API secret key.
    CLIENT_ID (str): Reddit API client ID.
    DATABASE_HOST (str): Hostname for the database.
    DATABASE_NAME (str): Name of the database.
    DATABASE_PORT (str): Port number for the database.
    DATABASE_USER (str): Username for the database.
    DATABASE_PASSWORD (str): Password for the database.
    AWS_ACCESS_KEY_ID (str): AWS access key ID.
    AWS_ACCESS_KEY (str): AWS secret access key.
    AWS_REGION (str): AWS region.
    AWS_BUCKET_NAME (str): AWS S3 bucket name.
    AWS_GLUE_JOB_NAME (str): AWS Glue job name.
    AWS_GLUE_CRAWLER_NAME (str): AWS Glue crawler name.
    AWS_S3_RAW_FILE_NAME (str): AWS S3 raw file name.
    AWS_S3_TRANSFORMED_FILE_NAME (str): AWS S3 transformed file name.
    USE_LOACL_GLUE_TRANSFORM_SCRIPT (bool): Flag indicating whether to use a local Glue transform script.
    AWS_S3_AIRFLOW_LOG_BUCKET (str): AWS S3 bucket for Airflow logs.
    AWS_S3_AIRFLOW_LOG_KEY (str): AWS S3 key for Airflow logs.
    INPUT_PATH (str): Input file path.
    OUTPUT_PATH (str): Output file path.
    REDDIT_OUTPUT_FILE_NAME (str): Output file name for Reddit data.
    POST_FIELDS (tuple): Fields returned by the Reddit API for posts.
"""
import configparser
import os

parser = configparser.ConfigParser()
parser.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))

# REDDIT API
SECRET = parser.get('api_keys', 'reddit_secret_key')
CLIENT_ID = parser.get('api_keys', 'reddit_client_id')

# DATABASE
DATABASE_HOST =  parser.get('database', 'database_host')
DATABASE_NAME =  parser.get('database', 'database_name')
DATABASE_PORT =  parser.get('database', 'database_port')
DATABASE_USER =  parser.get('database', 'database_username')
DATABASE_PASSWORD =  parser.get('database', 'database_password')

# AWS
AWS_ACCESS_KEY_ID = parser.get('aws', 'aws_access_key_id')
AWS_ACCESS_KEY = parser.get('aws', 'aws_secret_access_key')
AWS_REGION = parser.get('aws', 'aws_region')
AWS_BUCKET_NAME = parser.get('aws', 'aws_bucket_name')
AWS_GLUE_JOB_NAME = parser.get('aws', 'aws_glue_job_name')
AWS_GLUE_CRAWLER_NAME = parser.get('aws', 'aws_glue_crawler_name')
AWS_S3_RAW_FILE_NAME = parser.get('aws', 'aws_s3_raw_file_name')
AWS_S3_TRANSFORMED_FILE_NAME = parser.get('aws', 'aws_s3_transformed_file_name')
USE_LOACL_GLUE_TRANSFORM_SCRIPT = parser.get('aws', 'use_local_glue_transform_script').lower() == 'true'
AWS_S3_AIRFLOW_LOG_BUCKET = parser.get('aws', 'aws_s3_airflow_log_bucket')
AWS_S3_AIRFLOW_LOG_KEY = parser.get('aws', 'aws_s3_airflow_log_key')

# FILE PATHS
INPUT_PATH = parser.get('file_paths', 'input_path')
OUTPUT_PATH = parser.get('file_paths', 'output_path')

# REDDIT FILES CONFIGS
REDDIT_OUTPUT_FILE_NAME = parser.get('file_paths', 'reddit_output_file_name')

# REDDIT API RETURNEED POST FIELDS
POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)