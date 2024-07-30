import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf, concat_ws, when
from pyspark.sql.types import StringType, ArrayType, FloatType, StructType, StructField
from Levenshtein import ratio
from google.cloud import storage
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Get the project_id and bucket name from the environment variables
project_id = os.getenv('PROJECT_ID')
bucket_name = os.getenv('BUCKET')

# Initialize Google Cloud Storage client
storage_client = storage.Client(project=project_id)
bucket = storage_client.bucket(bucket_name)

def download_blob_to_file(blob_name, file_path):
    """Downloads a blob from the bucket and saves it to a local file if it doesn't exist."""
    if not os.path.exists(file_path):
        blob = bucket.blob(blob_name)
        blob.download_to_filename(file_path)
        logging.info(f"Downloaded {blob_name} to {file_path}")
    else:
        logging.info(f"File {file_path} already exists. Skipping download.")

# Ensure the local directory exists
os.makedirs('data/raw', exist_ok=True)

# Define local file paths
local_profiles_path = 'data/raw/raw_profiles.csv'
local_job_summary_path = 'data/raw/raw_job_summary.csv'
local_job_skills_path = 'data/raw/raw_job_skills.csv'

# Download source files from Google Cloud Storage
download_blob_to_file('raw/raw_profiles.csv', local_profiles_path)
download_blob_to_file('raw/raw_job_summary.csv', local_job_summary_path)
download_blob_to_file('raw/raw_job_skills.csv', local_job_skills_path)

# Initialize Spark session with more memory
spark = SparkSession.builder \
    .appName("Job Matching") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

logging.info("Spark session initialized")

# Define UDFs for calculating match metrics
def calculate_match_udf(profile_description, job_summary, job_skills):
    lev_ratio = ratio(profile_description, job_summary)
    profile_skills_list = [skill.strip() for skill in profile_description.split(',')] if profile_description else []
    job_skills_list = job_skills if job_skills else []
    skill_match_ratio = len(set(profile_skills_list).intersection(set(job_skills_list))) / len(set(job_skills_list)) if job_skills_list else 0
    missing_skills = list(set(job_skills_list) - set(profile_skills_list))
    extra_skills = list(set(profile_skills_list) - set(job_skills_list))
    return lev_ratio, skill_match_ratio, missing_skills, extra_skills

schema = StructType([
    StructField("lev_ratio", FloatType(), True),
    StructField("skill_match_ratio", FloatType(), True),
    StructField("missing_skills", ArrayType(StringType()), True),
    StructField("extra_skills", ArrayType(StringType()), True)
])

calculate_match = udf(calculate_match_udf, schema)

# Define matching states based on the match score
def define_match_state(match_score):
    if match_score is None:
        return 'No Match'
    if match_score >= 0.8:
        return 'High Match'
    elif match_score >= 0.5:
        return 'Moderate Match'
    elif match_score >= 0.2:
        return 'Low Match'
    else:
        return 'No Match'

define_match_state_udf = udf(define_match_state, StringType())

# Process data in chunks
chunk_size = 10000

# Read the job summary and job skills data once as they are usually smaller
job_summary_df = spark.read.csv(local_job_summary_path, header=True)
job_skills_df = spark.read.csv(local_job_skills_path, header=True)

# Merge job summaries with job skills
jobs_df = job_summary_df.join(job_skills_df, on='job_link', how='left')

# Group jobs by job_link to aggregate skills
jobs_grouped_df = jobs_df.groupBy('job_link', 'job_summary') \
    .agg(collect_list('job_skills').alias('job_skills'))

# Read profiles in chunks and process
profiles_df = spark.read.csv(local_profiles_path, header=True)

# Split profiles_df into chunks
profiles_df_chunks = profiles_df.randomSplit([chunk_size/profiles_df.count()] * (profiles_df.count() // chunk_size + 1))

for i, chunk in enumerate(profiles_df_chunks):
    logging.info(f"Processing chunk {i+1} out of {len(profiles_df_chunks)}")

    # Merge profiles with jobs
    profiles_jobs_df = chunk.crossJoin(jobs_grouped_df)

    # Apply UDF to calculate match metrics
    profiles_jobs_df = profiles_jobs_df.withColumn("metrics", calculate_match(col("about"), col("job_summary"), col("job_skills")))

    # Split the struct into separate columns
    profiles_jobs_df = profiles_jobs_df.select(
        "id", "name", "about", "job_summary", "job_skills",
        col("metrics.lev_ratio").alias("lev_ratio"),
        col("metrics.skill_match_ratio").alias("skill_match_ratio"),
        col("metrics.missing_skills").alias("missing_skills"),
        col("metrics.extra_skills").alias("extra_skills")
    )

    # Convert array columns to delimited strings
    profiles_jobs_df = profiles_jobs_df.withColumn("job_skills", concat_ws(",", col("job_skills")))
    profiles_jobs_df = profiles_jobs_df.withColumn("missing_skills", concat_ws(",", col("missing_skills")))
    profiles_jobs_df = profiles_jobs_df.withColumn("extra_skills", concat_ws(",", col("extra_skills")))

    # Calculate final match score (you can adjust the weights)
    profiles_jobs_df = profiles_jobs_df.withColumn('match_score', 0.5 * col('lev_ratio') + 0.5 * col('skill_match_ratio'))

    # Define matching states based on the match score
    profiles_jobs_df = profiles_jobs_df.withColumn('match_state', define_match_state_udf(col('match_score')))

    # Write the results to Google Cloud Storage
    output_path = 'gs://{}/data/cleaned/job_matching_results_{}.csv'.format(bucket_name, chunk.rdd.id())
    profiles_jobs_df.write.csv(output_path, header=True, mode='append')
    logging.info(f"Chunk {i+1} processed and saved to {output_path}")

logging.info("Data processing and saving completed.")
