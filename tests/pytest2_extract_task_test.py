"""
Test suite for the Reddit data pipeline.
This module contains pytest fixtures and a test function to test the `reddit_pipeline` function
from the `pipelines.reddit_pipeline` module. The tests use the `unittest.mock.patch` method to 
mock the dependencies of the `reddit_pipeline` function.
Fixtures:
    mock_connect_reddit: Mocks the `connect_reddit` function from the `pipelines.reddit_pipeline` module.
    mock_extract_posts: Mocks the `extract_posts` function from the `pipelines.reddit_pipeline` module.
    mock_transform_data: Mocks the `transform_data` function from the `pipelines.reddit_pipeline` module.
    mock_load_data_to_csv: Mocks the `load_data_to_csv` function from the `pipelines.reddit_pipeline` module.
Test Functions:
    test_reddit_pipeline: Tests the `reddit_pipeline` function by asserting the calls and return values of the mocked functions.
"""
import pytest
from unittest.mock import patch
import pandas as pd
import os
import sys
# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
from utils.constants import CLIENT_ID, SECRET    
from pipelines.reddit_pipeline import reddit_pipeline

@pytest.fixture
def mock_connect_reddit():
    with patch('pipelines.reddit_pipeline.connect_reddit') as mock:
        yield mock

@pytest.fixture
def mock_extract_posts():
    with patch('pipelines.reddit_pipeline.extract_posts') as mock:
        yield mock

@pytest.fixture
def mock_transform_data():
    with patch('pipelines.reddit_pipeline.transform_data') as mock:
        yield mock

@pytest.fixture
def mock_load_data_to_csv():
    with patch('pipelines.reddit_pipeline.load_data_to_csv') as mock:
        yield mock

def test_reddit_pipeline(mock_connect_reddit, mock_extract_posts, mock_transform_data, mock_load_data_to_csv):
    """
    Test the `reddit_pipeline` function.
    This test function uses mock objects to simulate the behavior of the `reddit_pipeline` function's dependencies.
    It verifies that the function correctly calls its dependencies with the expected arguments and returns the expected result.
    Mocks:
        mock_connect_reddit: Mock object for the `connect_reddit` function.
        mock_extract_posts: Mock object for the `extract_posts` function.
        mock_transform_data: Mock object for the `transform_data` function.
        mock_load_data_to_csv: Mock object for the `load_data_to_csv` function.
    Test Steps:
        1. Set up the mock return values for each dependency.
        2. Call the `reddit_pipeline` function with test arguments.
        3. Assert that each mock function is called once with the expected arguments.
        4. Assert that the result of the `reddit_pipeline` function is as expected.
    Asserts:
        - `connect_reddit` is called once with CLIENT_ID, SECRET, and '3mar Airscholar Agent'.
        - `extract_posts` is called once with 'mock_instance', 'test_subreddit', 'day', and 10.
        - `transform_data` is called once.
        - `load_data_to_csv` is called once.
        - The result of the `reddit_pipeline` function is 'test_file'.
    """
    
    # Mock the return values
    mock_connect_reddit.return_value = 'mock_instance'
    mock_extract_posts.return_value = [{'id': 'test_id', 'title': 'test_title'}]
    mock_transform_data.return_value = pd.DataFrame([{'id': 'test_id', 'title': 'test_title'}])
    mock_load_data_to_csv.return_value = None

    # Call the reddit_pipeline function
    result = reddit_pipeline(file_name='test_file', subreddit='test_subreddit', time_filter='day', limit=10)

    # Assert the function calls
    mock_connect_reddit.assert_called_once_with(CLIENT_ID, SECRET, '3mar Airscholar Agent')
    mock_extract_posts.assert_called_once_with('mock_instance', 'test_subreddit', 'day', 10)
    mock_transform_data.assert_called_once()
    mock_load_data_to_csv.assert_called_once()

    # Assert the result
    assert result == 'test_file'