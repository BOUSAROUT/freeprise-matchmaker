import os
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variable
API_KEY = os.getenv('API_KEY')

def clean_themuseapi_data():
    def clean_jobs_df(job_df):
        if job_df.empty:
            return job_df

        job_df = job_df[['name', 'publication_date', 'id', 'locations', 'categories', 'levels', 'tags', 'company', 'refs', 'contents']].copy()
        job_df.columns = ['job_title', 'publication_date', 'job_id', 'locations', 'categories', 'levels', 'tags', 'company', 'refs', 'job_description']

        job_df.loc[:, 'locations'] = job_df['locations'].apply(lambda x: ';'.join(loc['name'] for loc in eval(x)) if pd.notnull(x) else None)
        job_df.loc[:, 'categories'] = job_df['categories'].apply(lambda x: ';'.join(cat['name'] for cat in eval(x)) if pd.notnull(x) else None)
        job_df.loc[:, 'levels'] = job_df['levels'].apply(lambda x: eval(x)[0]['name'] if pd.notnull(x) and eval(x) else None)
        job_df.loc[:, 'tags'] = job_df['tags'].apply(lambda x: ';'.join(tag['name'] for tag in eval(x)) if pd.notnull(x) else None)
        job_df.loc[:, 'company_id'] = job_df['company'].apply(lambda x: eval(x)['id'] if pd.notnull(x) else None)
        job_df.loc[:, 'company_name'] = job_df['company'].apply(lambda x: eval(x)['short_name'] if pd.notnull(x) else None)
        job_df.loc[:, 'link'] = job_df['refs'].apply(lambda x: eval(x)['landing_page'] if pd.notnull(x) else None)

        job_df.drop(columns=['company', 'refs'], inplace=True)

        return job_df

    def clean_companies_df(company_df):
        if company_df.empty:
            return company_df

        company_df = company_df[['name', 'id', 'locations', 'industries', 'size', 'description', 'tags', 'twitter', 'refs']].copy()
        company_df.columns = ['company_name', 'company_id', 'locations', 'industries', 'size', 'description', 'tags', 'twitter', 'refs']

        company_df.loc[:, 'locations'] = company_df['locations'].apply(lambda x: ';'.join(loc['name'] for loc in eval(x)) if pd.notnull(x) else None)
        company_df.loc[:, 'industries'] = company_df['industries'].apply(lambda x: ';'.join(ind['name'] for ind in eval(x)) if pd.notnull(x) else None)
        company_df.loc[:, 'tags'] = company_df['tags'].apply(lambda x: ';'.join(tag['name'] for tag in eval(x)) if pd.notnull(x) else None)
        company_df.loc[:, 'company_page'] = company_df['refs'].apply(lambda x: eval(x)['landing_page'] if pd.notnull(x) else None)
        company_df.loc[:, 'job_page'] = company_df['refs'].apply(lambda x: eval(x)['jobs_page'] if pd.notnull(x) else None)
        company_df.loc[:, 'logo_link'] = company_df['refs'].apply(lambda x: eval(x)['logo_image'] if pd.notnull(x) else None)

        company_df.drop(columns=['refs'], inplace=True)

        return company_df

    source_dir = 'data/raw'
    cleaned_dir = 'data/cleaned'

    os.makedirs(cleaned_dir, exist_ok=True)

    all_jobs_df = []
    all_companies_df = []

    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        if 'jobs' in filename:
            jobs_df = pd.read_csv(filepath)
            cleaned_jobs_df = clean_jobs_df(jobs_df)
            all_jobs_df.append(cleaned_jobs_df)
        elif 'companies' in filename:
            companies_df = pd.read_csv(filepath)
            cleaned_companies_df = clean_companies_df(companies_df)
            all_companies_df.append(cleaned_companies_df)

    # Combine all cleaned jobs files into one
    if all_jobs_df:
        combined_jobs_df = pd.concat(all_jobs_df, ignore_index=True)
        combined_jobs_df.to_csv(os.path.join(cleaned_dir, 'themuse_jobs.csv'), index=False)
        print(f"Combined cleaned jobs data saved to 'data/cleaned/themuse_jobs.csv'")

    # Combine all cleaned companies files into one
    if all_companies_df:
        combined_companies_df = pd.concat(all_companies_df, ignore_index=True)
        combined_companies_df.to_csv(os.path.join(cleaned_dir, 'themuse_companies.csv'), index=False)
        print(f"Combined cleaned companies data saved to 'data/cleaned/themuse_companies.csv'")
