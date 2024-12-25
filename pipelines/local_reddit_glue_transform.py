"""
This script is an AWS Glue job that reads data from an S3 bucket, transforms it using PySpark, and writes the transformed data back to S3.

Functions:
    get_job_name():
        Retrieves the job name from the command-line arguments or sets it to a default value if not provided.

    initialize_spark_and_glue(args):
        Initializes Spark and Glue contexts and starts the Glue job.

    read_source_data(glueContext):
        Reads source data from an S3 bucket into a DynamicFrame.

    transform_data(dynamic_frame, glueContext):
        Transforms the data by combining specific columns and dropping the original columns.

    write_target_data(glueContext, dynamic_frame):
        Writes the transformed data to a temporary location in an S3 bucket.

    move_file_in_s3():
        Moves the transformed file from the temporary location to the final destination in the S3 bucket.

    main():
        Orchestrates the entire ETL process by calling the above functions in sequence.
"""
import sys
import boto3
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat_ws
from awsglue import DynamicFrame
import os

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.constants import AWS_GLUE_JOB_NAME, AWS_S3_RAW_FILE_NAME, AWS_S3_TRANSFORMED_FILE_NAME

def get_job_name():
    """
    Retrieves the job name from the command line arguments or sets it to a default value.
    This function attempts to get the 'JOB_NAME' argument from the command line arguments using 
    the `getResolvedOptions` function. If the 'JOB_NAME' argument is not provided, it catches 
    the `GlueArgumentError` and sets the job name to a default value specified by `AWS_GLUE_JOB_NAME`.
    Returns:
        dict: A dictionary containing the 'JOB_NAME' key with its corresponding value.
    """
    
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    except GlueArgumentError:
        args = {'JOB_NAME': AWS_GLUE_JOB_NAME}
        print("JOB_NAME argument is missing. Setting JOB_NAME to default value: ", args['JOB_NAME'])
    return args

def initialize_spark_and_glue(args):
    """
    Initializes Apache Spark and AWS Glue contexts and job.
    This function sets up the SparkContext, GlueContext, and Job for an AWS Glue ETL job.
    It extracts the job name from the provided arguments, initializes the job, and returns
    the GlueContext and Job objects.
    Args:
        args (dict): A dictionary of arguments required for initializing the Glue job.
                     Must contain the key 'JOB_NAME' for the job name.
    Returns:
        tuple: A tuple containing the GlueContext and Job objects.
    Raises:
        Exception: If there is an error during the initialization of Spark or Glue.
    """
    
    print("Initializing Spark and Glue")
    try:
        sc = SparkContext()
        glueContext = GlueContext(sc)
        job = Job(glueContext)
        job_name = args['JOB_NAME']
        args.pop('JOB_NAME')
        job.init(job_name, args)
        print("Spark and Glue initialized")
        return glueContext, job
    except Exception as e:
        print(f'Error initializing Spark and Glue: {e}')
        raise e

def read_source_data(glueContext):
    """
    Reads source data from an S3 bucket using AWS Glue.
    Args:
        glueContext (GlueContext): The GlueContext object used to create the dynamic frame.
    Returns:
        DynamicFrame: A dynamic frame containing the data read from the specified S3 path.
    Raises:
        Exception: If there is an error reading the source data from S3.
    Example:
        glueContext = GlueContext(SparkContext.getOrCreate())
        dynamic_frame = read_source_data(glueContext)
    """
    
    print("Reading source data from S3")
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={"paths": [f"s3://3mar-reddit-bucket/raw/{AWS_S3_RAW_FILE_NAME}.csv"]}
        )
        print("Source data read successfully")
        return dynamic_frame
    except Exception as e:
        print(f'Error reading source data from S3: {e}')
        raise e

def transform_data(dynamic_frame, glueContext):
    """
    Transforms the given DynamicFrame by combining the 'edited', 'spoiler', and 'stickied' columns 
    into a single column 'ESS_updated' and dropping the original columns.
    Args:
        dynamic_frame (DynamicFrame): The input DynamicFrame to be transformed.
        glueContext (GlueContext): The GlueContext object used for the transformation.
    Returns:
        DynamicFrame: The transformed DynamicFrame with the new 'ESS_updated' column.
    Raises:
        Exception: If an error occurs during the transformation process.
    """
    
    print("Transforming data")
    try:
        df = dynamic_frame.toDF()
        df_combined = df.withColumn("ESS_updated", concat_ws("-", df["edited"], df["spoiler"], df["stickied"]))
        df_combined = df_combined.drop("edited", "spoiler", "stickied")
        print("Data transformed successfully")
        return DynamicFrame.fromDF(df_combined, glueContext, "transformed")
    except Exception as e:
        print(f'Error transforming data: {e}')
        raise e

def write_target_data(glueContext, dynamic_frame):
    """
    Writes the transformed data to an S3 bucket in CSV format.
    Args:
        glueContext (GlueContext): The GlueContext object used to interact with AWS Glue.
        dynamic_frame (DynamicFrame): The DynamicFrame containing the transformed data to be written.
    Raises:
        Exception: If there is an error while writing the data to S3, the exception is caught and re-raised.
    """
    
    print("Writing transformed data to S3")
    try:
        glueContext.write_dynamic_frame.from_options(
            frame=dynamic_frame,
            connection_type="s3",
            format="csv",
            connection_options={"path": "s3://3mar-reddit-bucket/transformed/temp"}
        )
        print("Transformed data written to S3 successfully")
    except Exception as e:
        print(f'Error writing transformed data to S3: {e}')
        raise e

def move_file_in_s3():
    """
    Moves a file from the 'transformed/temp/' directory to the 'transformed/' directory within the same S3 bucket.
    This function performs the following steps:
    1. Connects to the S3 resource.
    2. Retrieves the first file found in the 'transformed/temp/' directory.
    3. Copies the file to the 'transformed/' directory with a new name specified by the AWS_S3_TRANSFORMED_FILE_NAME environment variable.
    4. Deletes the original file from the 'transformed/temp/' directory.
    Raises:
        Exception: If there is any error during the file move operation.
    Prints:
        - A message indicating the start of the file move operation.
        - The name of the file being moved.
        - A success message if the file is moved successfully.
        - An error message if there is an exception.
    """
    
    print("Moving file in S3")
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('3mar-reddit-bucket')
        file_name = None
        for obj in bucket.objects.filter(Prefix='transformed/temp/'):
            file_name = obj.key
            break
        print(f'File name: {file_name}')
        if file_name:
            s3.Object('3mar-reddit-bucket', f'transformed/{AWS_S3_TRANSFORMED_FILE_NAME}.csv').copy_from(CopySource='3mar-reddit-bucket/' + file_name)
            s3.Object('3mar-reddit-bucket', file_name).delete()
            print(f'File {file_name} moved to transformed/{AWS_S3_TRANSFORMED_FILE_NAME}.csv successfully')
    except Exception as e:
        print(f'Error moving file in S3: {e}')  
        raise e

def main():
    """
    Main function to execute the ETL job.
    This function performs the following steps:
    1. Prints a starting message.
    2. Retrieves job arguments.
    3. Initializes Spark and Glue contexts.
    4. Reads source data.
    5. Transforms the source data.
    6. Writes the transformed data to the target location.
    7. Moves the file in S3.
    8. Commits the Glue job.
    9. Prints a success message.
    If any exception occurs during the process, it prints an error message and exits the program.
    Raises:
        Exception: If any error occurs during the execution of the job.
    """
    
    try:
        print("Starting main job")
        args = get_job_name()
        glueContext, job = initialize_spark_and_glue(args)
        source_data = read_source_data(glueContext)
        transformed_data = transform_data(source_data, glueContext)
        write_target_data(glueContext, transformed_data)
        move_file_in_s3()
        job.commit()
        print("Job finished successfully")
    except Exception as e:
        print(f'Error in main job: {e}')
        sys.exit(1)

if __name__ == "__main__":
    main()