"""
This script triggers an AWS Glue job and monitors its status until completion.
Functions:
    trigger_glue_job(job_name):
        Triggers an AWS Glue job and monitors its status.
        Args:
            job_name (str): The name of the AWS Glue job to trigger.
        Raises:
            NoCredentialsError: If AWS credentials are not provided.
            PartialCredentialsError: If incomplete AWS credentials are provided.
            Exception: For any other exceptions that occur during job triggering or status monitoring.
Usage:
    This script is intended to be run as a standalone script. It retrieves the AWS Glue job name from the constants module and triggers the job.
    Example:
        python reddit_glue_transform.py
"""
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import sys
import os
import time

# must add the parent directory to the sys.path in order to import the modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import AWS_REGION ,AWS_GLUE_JOB_NAME

def trigger_glue_job(job_name):
    """
    Triggers an AWS Glue job and monitors its status until completion.
    Args:
        job_name (str): The name of the AWS Glue job to trigger.
    Raises:
        NoCredentialsError: If AWS credentials are not found.
        PartialCredentialsError: If incomplete AWS credentials are provided.
        Exception: For any other errors that occur during job triggering or status monitoring.
    The function performs the following steps:
    1. Initializes a Glue client using boto3.
    2. Starts the Glue job specified by `job_name`.
    3. Prints the JobRunId of the triggered job.
    4. Enters a loop to check the job status every 5 seconds.
    5. Prints the job status until the job either succeeds or fails.
    6. Raises an exception if the job fails or if any error occurs during status checking.
    """
    
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments={}
        )
        print(f"Triggered AWS Glue job '{job_name}' with JobRunId: {response['JobRunId']}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Error: {e}")
        raise e
    except Exception as e:
        print(f"An error occurred: {e}")
        raise e
    
    while True:
        time.sleep(5)
        try:
            job_run = glue_client.get_job_run(JobName=job_name, RunId=response['JobRunId'])
            status = job_run['JobRun']['JobRunState'].upper()
            if status == 'SUCCEEDED':
                print(f"AWS Glue job '{job_name}' succeeded")
                break
            elif status == 'FAILED':
                raise Exception(f"AWS Glue job '{job_name}' failed")
            else:
                print(f"AWS Glue job '{job_name}' is still running")
        except Exception as e:
            print(f"An error occurred while checking job status: {e}")
            raise e

if __name__ == "__main__":
    try:
        job_name = AWS_GLUE_JOB_NAME  # Replace with your AWS Glue job name
        trigger_glue_job(job_name)
    except Exception as e:
        print(f"An error occurred while triggering the AWS Glue job: {e}")
        sys.exit(1)