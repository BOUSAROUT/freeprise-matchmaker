import os
import sys

from data_processing_functions import upload_all_files_in_directory

def load_to_bucket():
    local_directory = 'data/cleaned'
    project_id = os.getenv('PROJECT_ID')
    bucket = os.getenv('BUCKET')
    if not project_id or not bucket:
        print("Environment variables PROJECT_ID and BUCKET must be set.", file=sys.stderr)
        return

    bucket_path = f'gs://{bucket}/raw'
    upload_all_files_in_directory(local_directory, bucket_path)

if __name__ == "__main__":
    load_to_bucket()
