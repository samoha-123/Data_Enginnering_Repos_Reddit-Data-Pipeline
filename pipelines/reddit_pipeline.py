"""
Reddit Data Pipeline

This module provides a function to extract, transform, and load Reddit posts data into a CSV file.

Functions:
    reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None) -> str:
        Executes the Reddit data pipeline which includes connecting to Reddit, extracting posts,
        transforming the data, and loading it into a CSV file.

        Parameters:
            file_name (str): The name of the output CSV file.
            subreddit (str): The subreddit to extract posts from.
            time_filter (str, optional): The time filter for extracting posts (e.g., 'day', 'week', 'month'). Defaults to 'day'.
            limit (int, optional): The maximum number of posts to extract. Defaults to None.

        Returns:
            str: The name of the output file.

        Raises:
            Exception: If there is an error during any step of the pipeline, the exception is caught,
                       an error message is printed, and the program exits with status code 1.
"""
import os
import sys
import pandas as pd

# must add the parent directory to the sys.path in order to import the modules
if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.reddit_etl import connect_reddit, extract_posts, load_data_to_csv, transform_data
from utils.constants import CLIENT_ID, OUTPUT_PATH, SECRET

def reddit_pipeline(file_name: str, subreddit: str, time_filter='day', limit=None):
    """
    Executes a Reddit data pipeline that extracts posts from a specified subreddit,
    transforms the data, and loads it into a CSV file.
    Args:
        file_name (str): The name of the output CSV file.
        subreddit (str): The name of the subreddit to extract posts from.
        time_filter (str, optional): The time filter for the posts (e.g., 'day', 'week', 'month'). Defaults to 'day'.
        limit (int, optional): The maximum number of posts to extract. Defaults to None.
    Returns:
        str: The name of the output file.
    Raises:
        Exception: If an error occurs during the pipeline execution.
    """
    
    print("Starting reddit_pipeline with file_name: %s, subreddit: %s, time_filter: %s, limit: %s", file_name, subreddit, time_filter, limit)
    try:
        # connecting to reddit instance
        instance = connect_reddit(CLIENT_ID, SECRET, '3mar Airscholar Agent')
        # extraction
        posts = extract_posts(instance, subreddit, time_filter, limit)
        posts_df = pd.DataFrame(posts)
        # transformation
        posts_df = transform_data(posts_df)
        # loading to csv
        file_path = f'{OUTPUT_PATH}/{file_name}.csv'
        load_data_to_csv(posts_df, file_path)
        return f'{file_name}'
    except Exception as e:
        print(f'Error in reddit_pipeline: {e}')
        sys.exit(1)