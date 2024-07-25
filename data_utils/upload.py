from datetime import datetime
from google.cloud import storage
from typing import List
from dotenv import load_dotenv
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
        if not os.path.exists(folder):
            os.makedirs(folder)
        return os.listdir(folder)
    except Exception as e:
        print('Error message: {}'.format(e))
        return None


def upload_to_lake(datasource: str, file_name: str) -> None:
    """
    Uploads a file to a GCP bucket, organizing it in a data lake's raw zone with a date-based structure.

    Args:
    - datasource: The source of the data e.g. freelancer.com, kaggle
    - file_name (str): The name of the file to upload.

    Returns:
    - None
    """
    date_today = datetime.now()
    year = date_today.strftime("%Y")
    month = date_today.strftime("%m")
    day = date_today.strftime("%d")
    ifile_path = f"data/raw/{file_name}"

    blob_path = f"{datasource}/{year}_{month}_{day}/{file_name}"

    storage_client = storage.Client()

    bucket_name = os.environ["LAKE_BUCKET"]
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(blob_path)

    try:
        blob.upload_from_filename(ifile_path)
        print(f"File {file_name} uploaded to {blob_path}.")
    except Exception as e:
        print('Error message: {}'.format(e))



if __name__ == '__main__':
    # Load environment variables from .env file
    load_dotenv()
    print(get_data_files())
    upload_to_lake('test', 'test.txt')
