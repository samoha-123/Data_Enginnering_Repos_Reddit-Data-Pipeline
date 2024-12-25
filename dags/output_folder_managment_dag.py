"""
This module defines an Airflow DAG for managing output files using a BashOperator.
The DAG is scheduled to run daily at 1:00 AM and is responsible for executing a 
Bash command within a Docker container to manage output files.
Attributes:
    default_args (dict): Default arguments for the DAG.
    dag (DAG): The DAG instance for managing output files.
    manage_files_task (BashOperator): Task to manage output files using a Bash command.
DAG Configuration:
    dag_id: 'manage_output_files_pipeline'
    default_args: {'owner': 'omar mohamed'}
    start_date: datetime(2024, 10, 1)
    schedule_interval: '0 1 * * *'
    catchup: False
    tags: ['output', 'files', 'management']
    is_paused_upon_creation: False
Tasks:
    manage_files_task: Executes a Bash command within a Docker container to manage output files.
"""
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

default_args = {
    'owner': 'omar mohamed',
}

with DAG(
    dag_id='manage_output_files_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 10, 1),
    # Runs every day at 1:00 AM
    schedule_interval='0 1 * * *',
    catchup=False,
    tags=['output', 'files', 'management'],
    is_paused_upon_creation=True
) as dag:
    
    # Task to manage output files using BashOperator
    manage_files_task = BashOperator(
        task_id='manage_files_task',
        bash_command='docker exec -i glue_pyspark python3 /home/glue_user/workspace/plugins/manage_output_files.py',
        dag=dag
    )
    
manage_files_task