import sys
import boto3
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import concat_ws
from awsglue import DynamicFrame

def get_job_name():
    try:
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    except GlueArgumentError:
        args = {'JOB_NAME': '3mar_glue_ETL'}
        print("JOB_NAME argument is missing. Setting JOB_NAME to default value: ", args['JOB_NAME'])
    return args

def initialize_spark_and_glue(args):
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    return glueContext, job

def read_source_data(glueContext):
    aws_s3_raw_file_name = "your reddit output file name"
    return glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [f"s3://3mar-reddit-bucket/raw/{aws_s3_raw_file_name}.csv"]},
        transformation_ctx="Reddit_3mar_ETL_glue_source_node1729426594830"
    )

def transform_data(dynamic_frame, glueContext):
    df = dynamic_frame.toDF()
    df_combined = df.withColumn("ESS_updated", concat_ws("-", df["edited"], df["spoiler"], df["stickied"]))
    df_combined = df_combined.drop("edited", "spoiler", "stickied")
    return DynamicFrame.fromDF(df_combined, glueContext, "final_Reddit_DF")

def write_target_data(glueContext, dynamic_frame):
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="csv",
        connection_options={"path": "s3://3mar-reddit-bucket/transformed/temp"},
        transformation_ctx="Reddit_3mar_ETL_glue_target_node1729426703126"
    )

def move_file_in_s3():
    aws_s3_transformed_file_name = "your transformed file name"
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('3mar-reddit-bucket')
    file_name = None
    for obj in bucket.objects.filter(Prefix='transformed/temp/'):
        file_name = obj.key
        break
    if file_name:
        print("Moving file: ", file_name , " to transformed folder")
        s3.Object('3mar-reddit-bucket', f'transformed/{aws_s3_transformed_file_name}.csv').copy_from(CopySource='3mar-reddit-bucket/' + file_name)
        s3.Object('3mar-reddit-bucket', file_name).delete()

def main():
    args = get_job_name()
    glueContext, job = initialize_spark_and_glue(args)
    source_data = read_source_data(glueContext)
    transformed_data = transform_data(source_data, glueContext)
    write_target_data(glueContext, transformed_data)
    move_file_in_s3()
    job.commit()
    print("Job finished successfully!")

if __name__ == "__main__":
    main()