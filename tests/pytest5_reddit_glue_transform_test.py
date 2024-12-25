"""
This module contains unit tests for the `transform_data` function in the 
`pipelines.local_reddit_glue_transform` module using PyTest and AWS Glue.

Fixtures:
    spark (pytest.fixture): A session-scoped fixture that provides a SparkSession.
    glue_context (pytest.fixture): A session-scoped fixture that provides a GlueContext.

Functions:
    test_transform_data(spark, glue_context): Tests the `transform_data` function by 
    creating a sample DataFrame, converting it to a DynamicFrame, applying the 
    transformation, and asserting the results.
"""
import pytest
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import os
import sys
# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.local_reddit_glue_transform import transform_data

@pytest.fixture(scope="session", autouse=True)
def spark():
    return SparkSession.builder \
        .appName("pytest") \
        .master("local[*]") \
        .getOrCreate()

@pytest.fixture(scope="session", autouse=True)
def glue_context(spark):
    return GlueContext(spark.sparkContext)

def test_transform_data(spark, glue_context):
    """
    Test the transform_data function.
    This test performs the following steps:
    1. Creates a sample DataFrame with test data.
    2. Converts the DataFrame to a DynamicFrame.
    3. Calls the transform_data function with the DynamicFrame.
    4. Converts the resulting DynamicFrame back to a DataFrame.
    5. Collects the results from the DataFrame.
    6. Defines the expected data.
    7. Asserts that the length of the result data matches the expected data.
    8. Asserts that each row in the result data matches the corresponding row in the expected data.
    Args:
        spark (SparkSession): The Spark session object.
        glue_context (GlueContext): The AWS Glue context object.
    Raises:
        AssertionError: If the result data does not match the expected data.
    """
    
    # Create a sample DataFrame
    data = [
        {"edited": "edit1", "spoiler": "spoiler1", "stickied": "stickied1"},
        {"edited": "edit2", "spoiler": "spoiler2", "stickied": "stickied2"}
    ]
    df = spark.createDataFrame(data)

    # Convert DataFrame to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glue_context, "test_dynamic_frame")

    # Call the transform_data function
    result_dynamic_frame = transform_data(dynamic_frame, glue_context)

    # Convert the result DynamicFrame back to DataFrame
    result_df = result_dynamic_frame.toDF()

    # Collect the results
    result_data = result_df.collect()

    # Expected data
    expected_data = [
        {"ESS_updated": "edit1-spoiler1-stickied1"},
        {"ESS_updated": "edit2-spoiler2-stickied2"}
    ]

    # Assert the results
    assert len(result_data) == len(expected_data)
    for row, expected_row in zip(result_data, expected_data):
        assert row["ESS_updated"] == expected_row["ESS_updated"]