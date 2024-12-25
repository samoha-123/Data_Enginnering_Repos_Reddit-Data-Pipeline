
"""
This module defines an Apache Airflow DAG for managing S3 logs.
The DAG is scheduled to run daily at 1:00 AM and uses a BashOperator to execute
a Python script inside a Docker container for managing S3 logs.
Attributes:
    default_args (dict): Default arguments for the DAG.
    dag (DAG): The DAG instance for managing S3 logs.
    manage_logs_task (BashOperator): Task to manage S3 logs using a Bash command.
Modules:
    os: Provides a way of using operating system dependent functionality.
    sys: Provides access to some variables used or maintained by the interpreter.
    datetime: Supplies classes for manipulating dates and times.
    airflow: Provides the core components of Apache Airflow.
    airflow.operators.bash: Provides the BashOperator to execute bash commands.
DAG:
    manage_s3_logs_pipeline:
        - dag_id: manage_s3_logs_pipeline
        - default_args: {'owner': 'omar mohamed'}
        - start_date: 2024-10-01
        - schedule_interval: '0 1 * * *'
        - catchup: False
        - tags: ['s3', 'logs', 'management']
        - is_paused_upon_creation: False
Tasks:
    manage_logs_task:
        - task_id: manage_logs_task
        - bash_command: 'docker exec -i glue_pyspark python3 /home/glue_user/workspace/plugins/manage_s3_logs.py'
"""


import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the function to manage S3 logs

default_args = {
    'owner': 'omar mohamed',
}

with DAG(
    dag_id='manage_s3_logs_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    # Runs every day at 1:00 AM
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['s3', 'logs', 'management'],
    is_paused_upon_creation=True
) as dag:

    # Task to manage S3 logs using BashOperator
    manage_logs_task = BashOperator(
        task_id='manage_logs_task',
        bash_command='docker exec -i glue_pyspark python3 /home/glue_user/workspace/plugins/manage_s3_logs.py',
        dag=dag
    )
    
manage_logs_task