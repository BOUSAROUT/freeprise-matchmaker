import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variable
API_KEY = os.getenv('API_KEY')

def clean_jobs_df(job_df):
    if job_df.empty:
        return job_df

    job_df = job_df[['name', 'publication_date', 'id', 'locations', 'categories', 'levels', 'tags', 'company', 'refs', 'contents']]
    job_df.columns = ['job_title', 'publication_date', 'job_id', 'locations', 'categories', 'levels', 'tags', 'company', 'refs', 'job_description']

    job_df['locations'] = job_df['locations'].apply(lambda x: ';'.join(loc['name'] for loc in eval(x)) if pd.notnull(x) else None)
    job_df['categories'] = job_df['categories'].apply(lambda x: ';'.join(cat['name'] for cat in eval(x)) if pd.notnull(x) else None)
    job_df['levels'] = job_df['levels'].apply(lambda x: eval(x)[0]['name'] if pd.notnull(x) and eval(x) else None)
    job_df['tags'] = job_df['tags'].apply(lambda x: ';'.join(tag['name'] for tag in eval(x)) if pd.notnull(x) else None)
    job_df['company_id'] = job_df['company'].apply(lambda x: eval(x)['id'] if pd.notnull(x) else None)
    job_df['company_name'] = job_df['company'].apply(lambda x: eval(x)['short_name'] if pd.notnull(x) else None)
    job_df['link'] = job_df['refs'].apply(lambda x: eval(x)['landing_page'] if pd.notnull(x) else None)

    job_df.drop(columns=['company', 'refs'], inplace=True)

    return job_df

def clean_companies_df(company_df):
    if company_df.empty:
        return company_df

    company_df = company_df[['name', 'id', 'locations', 'industries', 'size', 'description', 'tags', 'twitter', 'refs']]
    company_df.columns = ['company_name', 'company_id', 'locations', 'industries', 'size', 'description', 'tags', 'twitter', 'refs']

    company_df['locations'] = company_df['locations'].apply(lambda x: ';'.join(loc['name'] for loc in eval(x)) if pd.notnull(x) else None)
    company_df['industries'] = company_df['industries'].apply(lambda x: ';'.join(ind['name'] for ind in eval(x)) if pd.notnull(x) else None)
    company_df['tags'] = company_df['tags'].apply(lambda x: ';'.join(tag['name'] for tag in eval(x)) if pd.notnull(x) else None)
    company_df['company_page'] = company_df['refs'].apply(lambda x: eval(x)['landing_page'] if pd.notnull(x) else None)
    company_df['job_page'] = company_df['refs'].apply(lambda x: eval(x)['jobs_page'] if pd.notnull(x) else None)
    company_df['logo_link'] = company_df['refs'].apply(lambda x: eval(x)['logo_image'] if pd.notnull(x) else None)

    company_df.drop(columns=['refs'], inplace=True)

    return company_df

# Load the data from CSV files
jobs_file_path = 'data/source/themuse_jobs_part_1.csv'
companies_file_path = 'data/source/themuse_companies_part_1.csv'
# Add the path to your coaches CSV file if it exists
# coaches_file_path = 'data/themuse_coaches_part_1.csv'

jobs_df = pd.read_csv(jobs_file_path)
companies_df = pd.read_csv(companies_file_path)
# coaches_df = pd.read_csv(coaches_file_path)

# Clean the data
cleaned_jobs_df = clean_jobs_df(jobs_df)
cleaned_companies_df = clean_companies_df(companies_df)
# cleaned_coaches_df = clean_coaches_df(coaches_df)

# Save the cleaned data back to CSV files
cleaned_jobs_file_path = 'data/cleaned/cleaned_themuse_jobs_part_1.csv'
cleaned_companies_file_path = 'data/cleaned/cleaned_themuse_companies_part_1.csv'
# cleaned_coaches_file_path = 'data/cleaned_themuse_coaches_part_1.csv'

cleaned_jobs_df.to_csv(cleaned_jobs_file_path, index=False)
cleaned_companies_df.to_csv(cleaned_companies_file_path, index=False)
# cleaned_coaches_df.to_csv(cleaned_coaches_file_path, index=False)

print(f"Cleaned jobs data saved to '{cleaned_jobs_file_path}'")
print(f"Cleaned companies data saved to '{cleaned_companies_file_path}'")
# print(f"Cleaned coaches data saved to '{cleaned_coaches_file_path}'")
