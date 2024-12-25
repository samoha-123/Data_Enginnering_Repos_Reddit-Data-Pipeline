"""
This module provides functions to connect to Reddit, extract posts from a subreddit,
transform the extracted data, and load the data into a CSV file.
Functions:
    connect_reddit(client_id, client_secret, user_agent) -> Reddit:
        Connects to Reddit using the provided credentials and returns a Reddit instance.
    extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
        Extracts posts from the specified subreddit using the given time filter and limit.
        Returns a list of dictionaries containing the extracted post data.
    transform_data(post_df: pd.DataFrame):
        Transforms the extracted post data by converting data types and handling missing values.
        Returns the transformed DataFrame.
    load_data_to_csv(data: pd.DataFrame, path: str):
        Loads the transformed data into a CSV file at the specified path.
"""

import numpy as np
import pandas as pd
import praw
from praw import Reddit

from utils.constants import POST_FIELDS


def connect_reddit(client_id, client_secret, user_agent) -> Reddit:
    """
    Connect to the Reddit API using the provided credentials.
    Args:
        client_id (str): The client ID of your Reddit application.
        client_secret (str): The client secret of your Reddit application.
        user_agent (str): A unique identifier that helps Reddit identify the application.
    Returns:
        Reddit: An instance of the Reddit class from the PRAW library.
    Raises:
        Exception: If there is an error connecting to Reddit, the exception is raised with an error message.
    """
    
    try:
        reddit = praw.Reddit(client_id=client_id,
                             client_secret=client_secret,
                             user_agent=user_agent)
        print("connected to reddit!")
        return reddit
    except Exception as e:
        print(f'Error connecting to reddit: {e}')
        raise e
        
def extract_posts(reddit_instance: Reddit, subreddit: str, time_filter: str, limit=None):
    """
    Extracts posts from a specified subreddit using the provided Reddit instance.
    Args:
        reddit_instance (Reddit): An instance of the Reddit API client.
        subreddit (str): The name of the subreddit to extract posts from.
        time_filter (str): The time filter to apply (e.g., 'day', 'week', 'month', 'year', 'all').
        limit (int, optional): The maximum number of posts to extract. Defaults to None, which means no limit.
    Returns:
        list: A list of dictionaries, each containing data for a single post.
    Raises:
        Exception: If an error occurs during the extraction process.
    """
    
    try:
        subreddit = reddit_instance.subreddit(subreddit)
        posts = subreddit.top(time_filter=time_filter, limit=limit)
        post_lists = []
        for post in posts:
            post_dict = vars(post)
            post = {key: post_dict[key] for key in POST_FIELDS}
            post_lists.append(post)

        print(f"Extracted {len(post_lists)} posts from {subreddit} subreddit")    
        return post_lists
    except Exception as e:
        print(f'Error extracting posts: {e}')
        raise e

def transform_data(post_df: pd.DataFrame):
    """
    Transforms the given DataFrame by performing the following operations:
    1. Converts the 'created_utc' column from Unix timestamp to datetime.
    2. Ensures the 'over_18' column is boolean.
    3. Converts the 'author' column to string.
    4. Replaces non-boolean values in the 'edited' column with the mode of the column and converts it to boolean.
    5. Converts the 'num_comments' column to integer.
    6. Converts the 'score' column to integer.
    7. Converts the 'title' column to string.
    Args:
        post_df (pd.DataFrame): DataFrame containing Reddit post data.
    Returns:
        pd.DataFrame: Transformed DataFrame.
    Raises:
        Exception: If any error occurs during the transformation process.
    """
    
    try:
        post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
        post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
        post_df['author'] = post_df['author'].astype(str)
        edited_mode = post_df['edited'].mode()
        post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                     post_df['edited'], edited_mode).astype(bool)
        post_df['num_comments'] = post_df['num_comments'].astype(int)
        post_df['score'] = post_df['score'].astype(int)
        post_df['title'] = post_df['title'].astype(str)
        print("Transformed data")
        return post_df
    except Exception as e:
        print(f'Error transforming data: {e}')
        raise e

def load_data_to_csv(data: pd.DataFrame, path: str):
    """
    Saves the given DataFrame to a CSV file at the specified path.
    Args:
        data (pd.DataFrame): The DataFrame to be saved.
        path (str): The file path where the CSV will be saved.
    Raises:
        Exception: If there is an error during the saving process, it will be raised after being printed.
    """
    
    try:
        data.to_csv(path, index=False)
        print(f"Data loaded to CSV at {path}")
    except Exception as e:
        print(f'Error loading data to CSV: {e}')
        raise e
    
