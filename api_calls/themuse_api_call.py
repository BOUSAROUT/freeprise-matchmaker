import os
import requests
import time
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variable
api_key = os.getenv('API_KEY')

# Define the base URL and your API key
jobs_base_url = 'https://www.themuse.com/api/public/jobs'
companies_base_url = 'https://www.themuse.com/api/public/companies'

def themuse_api_call():
    # Function to fetch job listings
    def fetch_jobs(page=0, descending=False, company=None, category=None, level=None, location=None):
        # Define the query parameters
        params = {
            'api_key': api_key,
            'page': page,
            'descending': descending,
            'company': company,
            'category': category,
            'level': level,
            'location': location,
        }

        # Remove any None values from params
        params = {k: v for k, v in params.items() if v is not None}

        # Make the GET request
        response = requests.get(jobs_base_url, params=params)

        # Check the status code
        if response.status_code == 200:
            # Return the JSON response and headers
            return response.json(), response.headers
        else:
            # Handle errors
            error_data = response.json()
            return {
                'error': f"Error {error_data.get('code')}: {error_data.get('error')}",
                'status_code': response.status_code
            }, response.headers

    # Function to fetch company listings
    def fetch_companies(page=0, descending=False, industry=None, size=None, location=None):
        # Define the query parameters
        params = {
            'api_key': api_key,
            'page': page,
            'descending': descending,
            'industry': industry,
            'size': size,
            'location': location,
        }

        # Remove any None values from params
        params = {k: v for k, v in params.items() if v is not None}

        # Make the GET request
        response = requests.get(companies_base_url, params=params)

        # Check the status code
        if response.status_code == 200:
            # Return the JSON response and headers
            return response.json(), response.headers
        else:
            # Handle errors
            error_data = response.json()
            return {
                'error': f"Error {error_data.get('code')}: {error_data.get('error')}",
                'status_code': response.status_code
            }, response.headers

    # Enhanced function to fetch all jobs and handle rate limits
    def fetch_all_jobs(descending=False, company=None, category=None, level=None, location=None):
        all_jobs = []
        page = 0
        file_index = 0

        while True:
            jobs_data, headers = fetch_jobs(page=page, descending=descending, company=company, category=category, level=level, location=location)
            if 'results' in jobs_data:
                all_jobs.extend(jobs_data['results'])
                page += 1

                # Save data for every 10 pages
                if page % 10 == 0 or page >= jobs_data['page_count']:
                    file_index += 1
                    df_jobs = pd.DataFrame(all_jobs)
                    jobs_csv_path = os.path.join('data/raw', f'themuse_jobs_part_{file_index}.csv')
                    os.makedirs(os.path.dirname(jobs_csv_path), exist_ok=True)
                    df_jobs.to_csv(jobs_csv_path, index=False)
                    print(f"Saved to '{jobs_csv_path}': {len(all_jobs)} jobs")
                    all_jobs = []

                if page >= jobs_data['page_count']:
                    break
            else:
                print(jobs_data['error'])
                break

            # Check rate limit headers
            rate_limit_remaining = int(headers.get('X-RateLimit-Remaining', 1))
            rate_limit_reset = int(headers.get('X-RateLimit-Reset', 0))

            if rate_limit_remaining <= 0:
                print(f"Rate limit reached. Waiting for {rate_limit_reset} seconds.")
                time.sleep(rate_limit_reset)

        return all_jobs

    # Enhanced function to fetch all companies and handle rate limits
    def fetch_all_companies(descending=False, industry=None, size=None, location=None):
        all_companies = []
        page = 0
        file_index = 0

        while True:
            companies_data, headers = fetch_companies(page=page, descending=descending, industry=industry, size=size, location=location)
            if 'results' in companies_data:
                all_companies.extend(companies_data['results'])
                page += 1

                # Save data for every 10 pages
                if page % 10 == 0 or page >= companies_data['page_count']:
                    file_index += 1
                    df_companies = pd.DataFrame(all_companies)
                    companies_csv_path = os.path.join('data/raw', f'themuse_companies_part_{file_index}.csv')
                    os.makedirs(os.path.dirname(companies_csv_path), exist_ok=True)
                    df_companies.to_csv(companies_csv_path, index=False)
                    print(f"Saved to '{companies_csv_path}': {len(all_companies)} companies")
                    all_companies = []

                if page >= companies_data['page_count']:
                    break
            else:
                print(companies_data['error'])
                break

            # Check rate limit headers
            rate_limit_remaining = int(headers.get('X-RateLimit-Remaining', 1))
            rate_limit_reset = int(headers.get('X-RateLimit-Reset', 0))

            if rate_limit_remaining <= 0:
                print(f"Rate limit reached. Waiting for {rate_limit_reset} seconds.")
                time.sleep(rate_limit_reset)

        return all_companies

    # Fetch all jobs
    fetch_all_jobs()

    # Fetch all companies
    fetch_all_companies()

if __name__=="__main__":
    themuse_api_call()
