import os
import logging
from data_processing_functions import upload_all_files_in_directory

def load_to_bucket():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting load_to_bucket task")

    local_directory = 'data/cleaned'
    project_id = os.getenv('PROJECT_ID')
    bucket = os.getenv('BUCKET')

    if not project_id or not bucket:
        logging.error("Environment variables PROJECT_ID and BUCKET must be set.")
        return

    logging.info(f"PROJECT_ID: {project_id}")
    logging.info(f"BUCKET: {bucket}")

    bucket_path = f'gs://{bucket}/raw'

    try:
        upload_all_files_in_directory(local_directory, bucket_path)
        logging.info(f"Files from {local_directory} have been successfully uploaded to {bucket_path}")
    except Exception as e:
        logging.error(f"Error during upload: {str(e)}")
        raise

if __name__ == "__main__":
    load_to_bucket()
