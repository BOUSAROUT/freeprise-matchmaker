import json
import re
import hashlib
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Generalized function to clean and parse the JSON string
def clean_and_parse_json(json_str):
    if isinstance(json_str, str) and json_str.strip():
        try:
            json_str = json_str.replace("'", "\"")
            json_str = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', json_str)
            json_str = json_str.replace('\n', ' ').replace('\r', ' ')
            json_str = re.sub(r'}\s*{', '},{', json_str)
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return []
    return []

# Function to flatten and filter experience data
def extract_experience_data(parsed_data, profile_id):
    extracted_data = []
    for item in parsed_data:
        company = item.get('company')
        company_id = item.get('company_id')
        industry = item.get('industry')
        location = item.get('location')
        url = item.get('url')
        positions = item.get('positions', [])

        for position in positions:
            filtered_item = {
                'profile_id': profile_id,
                'company': company,
                'company_id': company_id,
                'industry': industry,
                'location': location,
                'url': url,
                'description': position.get('description'),
                'duration': position.get('duration'),
                'duration_short': position.get('duration_short'),
                'end_date': position.get('end_date'),
                'start_date': position.get('start_date'),
                'subtitle': position.get('subtitle'),
                'title': position.get('title')
            }
            extracted_data.append(filtered_item)
    return extracted_data

# Function to flatten and filter education data
def extract_education_data(parsed_data, profile_id):
    extracted_data = []
    for item in parsed_data:
        filtered_item = {
            'profile_id': profile_id,
            'degree': item.get('degree'),
            'end_year': item.get('end_year'),
            'field': item.get('field'),
            'meta': item.get('meta'),
            'start_year': item.get('start_year'),
            'title': item.get('title'),
            'url': item.get('url')
        }
        extracted_data.append(filtered_item)
    return extracted_data

# Function to flatten and filter certification data
def extract_certifications_data(parsed_data, profile_id):
    extracted_data = []
    for item in parsed_data:
        filtered_item = {
            'profile_id': profile_id,
            'meta': item.get('meta'),
            'subtitle': item.get('subtitle'),
            'title': item.get('title')
        }
        extracted_data.append(filtered_item)
    return extracted_data

# Function to add hash ID to each row based on the job_link and save the file using PySpark
def add_hash_id_and_save(input_file_path, output_dir, output_file_name):
    # Load the CSV file using PySpark
    df = spark.read.csv(input_file_path, header=True, inferSchema=True)

    # Generate a new hash ID for each row based on the job_link
    def generate_hash_id(job_link):
        if job_link is None:
            return None
        return hashlib.sha256(job_link.encode('utf-8')).hexdigest()

    hash_udf = udf(generate_hash_id, StringType())

    # Add the hash_id column
    df = df.withColumn("hash_id", hash_udf(df["job_link"]))

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Save the cleaned data to the specified file path
    output_file_path = os.path.join(output_dir, output_file_name)
    df.write.csv(output_file_path, header=True, mode="overwrite")

    print(f"Data with hash IDs saved to {output_file_path}")
