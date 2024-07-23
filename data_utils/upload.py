from datetime import datetime
from google.cloud import storage
from typing import List
import os

def get_data_files(data_dir: str = 'raw') -> List[str]:
    """
    Gets the list of data files in your local dir.
    Assumes the structure <current_directory>/data/<data_dir>
    The default is 'raw'

    Args:
    - file_name (str): The name of the file to upload.

    Returns:
    None
    """
    current_dir = os.getcwd()
    folder = os.path.join(current_dir, 'data', data_dir)
    try:
        return os.listdir(folder)
    except Exception as e:
        print('Error message: {}'.format(e))
        return None


def upload_to_lake(file_name: str, bucket_dir: str = 'raw') -> None:
    """
    Uploads a file to a GCP bucket, organizing it in a data lake's raw zone with a date-based structure.

    Args:
    - bucket_dir: raw or silver
    - file_name (str): The name of the file to upload.

    Returns:
    None
    """
    date_today = datetime.now()
    year = date_today.strftime("%Y")
    month = date_today.strftime("%m")
    day = date_today.strftime("%d")
    ifile_path = f"data/{bucket_dir}/{file_name}"

    blob_path = f"{bucket_dir}/freelancer/{year}/{month}/{day}/{file_name}"

    storage_client = storage.Client()

    bucket_name = os.environ["LAKE_BUCKET"]
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(blob_path)
    # This need to take the full path
    blob.upload_from_filename(ifile_path)
    print(f"File {file_name} uploaded to {blob_path}.")
