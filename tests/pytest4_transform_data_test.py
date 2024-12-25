"""
This module contains unit tests for the `transform_data` function from the `etls.reddit_etl` module.

Fixtures:
    sample_data: A pytest fixture that provides a sample DataFrame for testing.

Tests:
    test_transform_data: Tests the `transform_data` function to ensure it correctly transforms the input DataFrame.
        - Checks if 'created_utc' is converted to datetime.
        - Checks if 'over_18' is boolean.
        - Checks if 'author' is string.
        - Checks if 'edited' is boolean.
        - Checks if 'num_comments' is integer.
        - Checks if 'score' is integer.
        - Checks if 'title' is string.
        - Checks if the mode of 'edited' is correctly applied.
        - Checks if the values are correctly transformed.
"""
import pytest
import pandas as pd
import numpy as np
import os
import sys
# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.reddit_etl import transform_data

@pytest.fixture
def sample_data():
    data = {
        'created_utc': [1609459200, 1609459201],
        'over_18': [True, False],
        'author': ['author1', 'author2'],
        'edited': [True, '2021-01-01T00:00:00Z'],
        'num_comments': [10, 20],
        'score': [100, 200],
        'title': ['title1', 'title2']
    }
    return pd.DataFrame(data)

def test_transform_data(sample_data):
    """
    Test the `transform_data` function to ensure it correctly transforms the input data.
    Args:
        sample_data (pd.DataFrame): A sample DataFrame containing the data to be transformed.
    Asserts:
        - 'created_utc' column is converted to datetime.
        - 'over_18' column is of boolean type.
        - 'author' column is of string type.
        - 'edited' column is of boolean type.
        - 'num_comments' column is of integer type.
        - 'score' column is of integer type.
        - 'title' column is of string type.
        - The mode of 'edited' column is correctly applied.
        - The values in the transformed DataFrame are correctly transformed.
    """
    
    transformed_df = transform_data(sample_data)

    # Check if 'created_utc' is converted to datetime
    assert pd.api.types.is_datetime64_any_dtype(transformed_df['created_utc'])

    # Check if 'over_18' is boolean
    assert transformed_df['over_18'].dtype == np.bool_

    # Check if 'author' is string
    assert transformed_df['author'].dtype == object

    # Check if 'edited' is boolean
    assert transformed_df['edited'].dtype == np.bool_

    # Check if 'num_comments' is integer
    assert transformed_df['num_comments'].dtype == np.int64

    # Check if 'score' is integer
    assert transformed_df['score'].dtype == np.int64

    # Check if 'title' is string
    assert transformed_df['title'].dtype == object

    # Check if the mode of 'edited' is correctly applied
    assert transformed_df['edited'].iloc[1] == transformed_df['edited'].mode().iloc[0]

    # Check if the values are correctly transformed
    assert transformed_df['created_utc'].iloc[0] == pd.to_datetime(1609459200, unit='s') and transformed_df['created_utc'].iloc[1] == pd.to_datetime(1609459201, unit='s')
    assert transformed_df['over_18'].iloc[0] == True and transformed_df['over_18'].iloc[1] == False
    assert transformed_df['author'].iloc[0] == 'author1' and transformed_df['author'].iloc[1] == 'author2'
    assert transformed_df['edited'].iloc[0] == True
    assert transformed_df['num_comments'].iloc[0] == 10 and transformed_df['num_comments'].iloc[1] == 20
    assert transformed_df['score'].iloc[0] == 100 and transformed_df['score'].iloc[1] == 200
    assert transformed_df['title'].iloc[0] == 'title1' and transformed_df['title'].iloc[1] == 'title2'