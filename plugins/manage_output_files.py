"""
This script manages output files by deleting old files and shipping files to a shared folder.
Functions:
    delete_old_files(folder_path=output_dir):
        Deletes files older than one month from the specified folder.
        Args:
            folder_path (str): Path to the folder containing files to be deleted. Defaults to 'data/output'.
    ship_files_to_shared_folder():
        Copies files from the output directory to a shared folder, skipping files that already exist in the shared folder.
Usage:
    Run the script directly to delete old files and ship files to the shared folder.
    Example:
        python manage_output_files.py
Constants:
    output_dir (str): Directory containing the output files.
    one_month_ago (datetime): Date one month ago from today.
    one_month_ago_date_str (str): Date string representing one month ago.
    file_pattern (Pattern): Regular expression to match the file pattern.
Exceptions:
    Raises exceptions if there are errors during file deletion or copying.
"""
import os
from datetime import datetime, timedelta
import re
import sys

# Directory containing the output files in parent directory of the parent directory of the current file
output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data/output')

# Calculate the date one month ago from today
one_month_ago = datetime.now() - timedelta(days=30)
one_month_ago_date_str = one_month_ago.strftime('%Y-%m-%d')

# Regular expression to match the file pattern
file_pattern = re.compile(r'.*_(\d{4}-\d{2}-\d{2})_\d{2}-\d{2}-\d{2}\..*')

def delete_old_files(folder_path = output_dir):
    """
        Deletes files in the specified folder that match a certain pattern and are older than one month.
        Args:
            folder_path (str): The path to the folder containing the files to be deleted. Defaults to 'output_dir'.
        Raises:
            Exception: If an error occurs during the file deletion process, it is printed and re-raised.
        Note:
            - The function assumes that `output_dir`, `file_pattern`, `one_month_ago`, `os`, and `datetime` are defined elsewhere in the code.
            - Files are identified for deletion based on a date extracted from their filenames using `file_pattern`.
            - The date format in the filenames is expected to be '%Y-%m-%d'.
    """
    
    try:
        for filename in os.listdir(folder_path):
            match = file_pattern.match(filename)
            
            if match:
                file_date_str = match.group(1)
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                
                if file_date < one_month_ago:
                    file_path = os.path.join(folder_path, filename)
                    print(f"Deleting file: {file_path}")
                    os.remove(file_path)
    except Exception as e:
        print(f"Error: {e}")
        raise e

def ship_files_to_shared_folder():
    """
        Copies files from the output directory to a shared folder, skipping files that already exist in the shared folder.
        Raises:
            Exception: If an error occurs during the file copying process, it is printed and re-raised.
        Note:
            - The function assumes that `output_dir` and `os` are defined elsewhere in the code.
            - The shared folder is assumed to be in the parent directory of the parent directory of the current file.
    """
    try:
        for filename in os.listdir(output_dir):
            file_path = os.path.join(output_dir, filename)
            # in the shared_folder directory adjacent to the current directory of the script
            shared_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shared_folder', filename)
            # check if file already exists in shared folder
            if os.path.exists(shared_file_path):
                print(f"File {shared_file_path} already exists in shared folder.")
                continue
            print(f"Copying file from {file_path} to {shared_file_path}")
            os.system(f"cp {file_path} {shared_file_path}")
    except Exception as e:
        print(f"Error: {e}")
        raise e
    
if __name__ == '__main__':
    try:
        delete_old_files()
        delete_old_files(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'shared_folder'))
        ship_files_to_shared_folder()
        print("Script completed")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)