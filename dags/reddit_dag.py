"""
This module defines an Apache Airflow DAG for an ETL pipeline that extracts data from Reddit,
uploads it to S3, processes it using Spark, and runs an AWS Glue crawler.
Modules:
    os: Provides a way of using operating system dependent functionality.
    sys: Provides access to some variables used or maintained by the interpreter.
    datetime: Supplies classes for manipulating dates and times.
    airflow: Provides the core components for creating and managing workflows.
    airflow.decorators: Provides decorators for defining tasks in a DAG.
    airflow.operators.bash: Provides an operator to execute bash commands.
Functions:
    extract: Extracts data from Reddit using the reddit_pipeline function.
Operators:
    s3_upload: Executes a bash command to upload the extracted data to S3 using a Docker container.
    spark_submit: Executes a bash command to run a Spark job using a Docker container.
    run_glue_crawler: Executes a bash command to run an AWS Glue crawler using a Docker container.
DAG:
    etl_reddit_pipeline: Defines the ETL pipeline with tasks for extraction, S3 upload, Spark processing, and Glue crawling.
Constants:
    USE_LOACL_GLUE_TRANSFORM_SCRIPT: Determines whether to use a local Glue transform script.
    REDDIT_OUTPUT_FILE_NAME: The base name for the output file from the Reddit extraction.
"""


import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
from pipelines.reddit_pipeline import reddit_pipeline
from utils.constants import USE_LOACL_GLUE_TRANSFORM_SCRIPT, REDDIT_OUTPUT_FILE_NAME

if USE_LOACL_GLUE_TRANSFORM_SCRIPT:
    glue_transform_file = 'local_reddit_glue_transform.py'
else:
    glue_transform_file = 'reddit_glue_transform.py'

file_postfix = datetime.now().strftime("_%Y-%m-%d_%H-%M-%S")

default_args = {
    'owner': 'omar mohamed'
}

with DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit', 'etl', 'pipeline'],
    is_paused_upon_creation=True
    ) as dag:

    # extraction from reddit
    @task()
    def extract():
        """
        Extracts data from the 'dataengineering' subreddit using the reddit_pipeline function.
        Returns:
            dict: The result of the reddit_pipeline function, which contains the extracted data.
        """
        
        return reddit_pipeline(
            file_name=f'{REDDIT_OUTPUT_FILE_NAME}{file_postfix}',
            subreddit='dataengineering',
            time_filter='day',
            limit=100
        )
    
    # run python script in existing docker container
    s3_upload = BashOperator(
        task_id='s3_upload',
        bash_command='docker exec -i glue_pyspark python3 /home/glue_user/workspace/pipelines/aws_s3_pipeline.py {{ ti.xcom_pull(task_ids="extract", key="return_value") }}',
        dag=dag
    )
    
    # run spark-submit in existing docker container
    spark_submit = BashOperator(
        task_id='spark_submit',
        bash_command=f'docker exec -i glue_pyspark /home/glue_user/spark/bin/spark-submit  /home/glue_user/workspace/pipelines/{glue_transform_file}',
        dag=dag
    )
    
    # run the aws glue crawler
    run_glue_crawler = BashOperator(
        task_id='run_glue_crawler',
        bash_command=f'docker exec -i glue_pyspark python3  /home/glue_user/workspace/pipelines/aws_crawler_pipeline.py',
        dag=dag
    )

extract() >> s3_upload >> spark_submit >> run_glue_crawler