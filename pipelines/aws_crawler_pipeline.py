

"""
This script initializes an AWS Glue client and defines a function to start and monitor an AWS Glue crawler.
Modules:
    os: Provides a way of using operating system dependent functionality.
    sys: Provides access to some variables used or maintained by the interpreter.
    boto3: The AWS SDK for Python, which allows Python developers to write software that makes use of Amazon services.
    time: Provides various time-related functions.
Functions:
    run_and_monitor_crawler():
        Starts the AWS Glue crawler specified by AWS_GLUE_CRAWLER_NAME and monitors its status.
        Raises an exception if the crawler's last run status is None or FAILED.
        Polls the crawler status every 5 seconds until the crawler is ready and the last run status is SUCCEEDED.
Constants:
    AWS_REGION: The AWS region where the Glue client is initialized.
    AWS_GLUE_CRAWLER_NAME: The name of the AWS Glue crawler to be started and monitored.
Usage:
    This script is intended to be run as a standalone module. When executed, it will start the specified AWS Glue crawler and monitor its status until completion.
"""
import os
import sys
import boto3
import time

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
from utils.constants import AWS_REGION, AWS_GLUE_CRAWLER_NAME

# Initialize the Glue client
glue_client = boto3.client('glue', region_name=AWS_REGION)

# Function to start the crawler and monitor its run status
def run_and_monitor_crawler():
    """
    Starts an AWS Glue crawler and monitors its status until completion.
    This function initiates the specified AWS Glue crawler and continuously 
    polls its status. It checks for the crawler's state and the last run status 
    to determine if the crawler has completed successfully, failed, or is still running.
    Raises:
        Exception: If the crawler's last run status is None or if the crawler run fails.
        Exception: If any other error occurs during the process.
    Prints:
        Status messages indicating the start of the crawler, its current status, 
        and the final outcome of the crawler run.
    """
    
    try:
        # Start the crawler
        glue_client.start_crawler(Name=AWS_GLUE_CRAWLER_NAME)
        print(f"Started crawler '{AWS_GLUE_CRAWLER_NAME}'")

        # Poll for crawler status
        while True:
            response = glue_client.get_crawler(Name=AWS_GLUE_CRAWLER_NAME)
            status = response['Crawler']['State']
            last_run_status = response['Crawler'].get('LastCrawl', {}).get('Status')

            # Check for None or failure in last_run_status
            if last_run_status is None:
                raise Exception(f"Error: Crawler run status is None for '{AWS_GLUE_CRAWLER_NAME}'. Exiting.")
            elif last_run_status == 'FAILED':
                raise Exception(f"Error: Crawler run failed for '{AWS_GLUE_CRAWLER_NAME}'. Exiting.")
            elif status == 'READY' and last_run_status == 'SUCCEEDED':
                print(f"Crawler run completed successfully with status: {last_run_status}")
                break
            else:
                print(f"Crawler '{AWS_GLUE_CRAWLER_NAME}' is still running... Current status: {status}")
                time.sleep(5)  # Poll every 5 seconds

    except Exception as e:
        print(f"Error occurred while running and monitoring the crawler: {e}")
        sys.exit(1)
        
if __name__ == '__main__':
    run_and_monitor_crawler()