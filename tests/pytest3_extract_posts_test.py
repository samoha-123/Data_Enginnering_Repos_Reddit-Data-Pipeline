"""
Unit tests for the extract_posts function in the reddit_etl module.

This module contains the following fixtures and test functions:
- mock_reddit_instance: Creates a mock Reddit instance with a mock subreddit.
- mock_submissions: Creates a list of mock submissions.
- test_extract_posts: Tests the extract_posts function to ensure it returns the expected output.

Fixtures:
    mock_reddit_instance: A pytest fixture that returns a mock Reddit instance.
    mock_submissions: A pytest fixture that returns a list of mock submissions.

Test Functions:
    test_extract_posts(mock_reddit_instance, mock_submissions): 
        Mocks the top method of the subreddit to return mock submissions, 
        defines the expected output, calls the extract_posts function, 
        and asserts that the result matches the expected output.
"""
import pytest
from unittest.mock import MagicMock
from praw.models import Subreddit, Submission
import os
import sys
# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.reddit_etl import extract_posts

@pytest.fixture
def mock_reddit_instance():
    # Create a mock Reddit instance
    mock_reddit = MagicMock()
    mock_subreddit = MagicMock(spec=Subreddit)
    mock_reddit.subreddit.return_value = mock_subreddit
    return mock_reddit

@pytest.fixture
def mock_submissions():
    # Create a list of mock submissions
    mock_submission = MagicMock(spec=Submission)
    mock_submission.id = "test_id"
    mock_submission.title = "test_title"
    mock_submission.score = 100
    mock_submission.num_comments = 10
    mock_submission.created_utc = 1609459200
    mock_submission.over_18 = False
    mock_submission.author = "test_author"
    mock_submission.edited = False
    mock_submission.url = "test_url"
    mock_submission.spoiler = False
    mock_submission.stickied = False

    return [mock_submission]

def test_extract_posts(mock_reddit_instance, mock_submissions):
    """
    Test the extract_posts function.
    This test verifies that the extract_posts function correctly extracts post data
    from a given subreddit using a mock Reddit instance and mock submissions.
    Args:
        mock_reddit_instance (Mock): A mock instance of the Reddit API.
        mock_submissions (list): A list of mock submissions to be returned by the
                                 mock Reddit instance.
    Steps:
    1. Mock the `top` method of the subreddit to return the mock submissions.
    2. Define the expected output as a list of dictionaries containing post data.
    3. Call the extract_posts function with the mock Reddit instance, subreddit name,
       time filter, and limit.
    4. Assert that the result matches the expected output.
    Expected Output:
        A list of dictionaries containing the extracted post data, matching the
        expected output.
    """
    
    # Mock the top method to return the mock submissions
    mock_reddit_instance.subreddit.return_value.top.return_value = mock_submissions

    # Define the expected output
    expected_output = [{
        'id': 'test_id',
        'title': 'test_title',
        'score': 100,
        'num_comments': 10,
        'created_utc': 1609459200,
        'over_18': False,
        'author': 'test_author',
        'edited': False,
        'url': 'test_url',
        'spoiler': False,
        'stickied': False
    }]

    # Call the extract_posts function
    result = extract_posts(mock_reddit_instance, 'test_subreddit', 'day', limit=1)

    # Assert the result matches the expected output
    assert result == expected_output