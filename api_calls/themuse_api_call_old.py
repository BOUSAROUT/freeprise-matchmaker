import os
import requests
import time
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get the API key from environment variable
API_KEY = os.getenv('API_KEY')

# Define the base URL
jobs_base_url = 'https://www.themuse.com/api/public/jobs'
companies_base_url = 'https://www.themuse.com/api/public/companies'
coaches_base_url = 'https://www.themuse.com/api/public/coaches'

# Function to fetch job listings
def fetch_jobs(page=0, descending=False, company=None, category=None, level=None, location=None):
    params = {
        'api_key': API_KEY,
        'page': page,
        'descending': descending,
        'company': company,
        'category': category,
        'level': level,
        'location': location,
    }
    params = {k: v for k, v in params.items() if v is not None}
    response = requests.get(jobs_base_url, params=params)
    if response.status_code == 200:
        return response.json(), response.headers
    else:
        error_data = response.json()
        return {'error': f"Error {error_data.get('code')}: {error_data.get('error')}", 'status_code': response.status_code}, response.headers

# Function to fetch company listings
def fetch_companies(page=0, descending=False, industry=None, size=None, location=None):
    params = {
        'api_key': API_KEY,
        'page': page,
        'descending': descending,
        'industry': industry,
        'size': size,
        'location': location,
    }
    params = {k: v for k, v in params.items() if v is not None}
    response = requests.get(companies_base_url, params=params)
    if response.status_code == 200:
        return response.json(), response.headers
    else:
        error_data = response.json()
        return {'error': f"Error {error_data.get('code')}: {error_data.get('error')}", 'status_code': response.status_code}, response.headers

# Function to fetch coach listings
def fetch_coaches(page=0, descending=False, offering=None, level=None, specialization=None):
    params = {
        'api_key': API_KEY,
        'page': page,
        'descending': descending,
        'offering': offering,
        'level': level,
        'specialization': specialization,
    }
    params = {k: v for k, v in params.items() if v is not None}
    response = requests.get(coaches_base_url, params=params)
    if response.status_code == 200:
        return response.json(), response.headers
    else:
        error_data = response.json()
        return {'error': f"Error {error_data.get('code')}: {error_data.get('error')}", 'status_code': response.status_code}, response.headers

# Enhanced function to fetch all jobs and handle rate limits
def fetch_all_jobs(descending=False, company=None, category=None, level=None, location=None):
    all_jobs = []
    page = 0
    file_index = 0

    while page < 10:  # Limit to 10 pages
        jobs_data, headers = fetch_jobs(page=page, descending=descending, company=company, category=category, level=level, location=location)
        if 'results' in jobs_data:
            all_jobs.extend(jobs_data['results'])
            page += 1

            if page % 10 == 0 or page >= jobs_data['page_count']:
                file_index += 1
                df_jobs = pd.DataFrame(all_jobs)
                jobs_csv_path = os.path.join('data', f'themuse_jobs_part_{file_index}.csv')
                os.makedirs(os.path.dirname(jobs_csv_path), exist_ok=True)
                df_jobs.to_csv(jobs_csv_path, index=False)
                print(f"Saved to '{jobs_csv_path}': {len(all_jobs)} jobs")
                all_jobs = []

            if page >= jobs_data['page_count']:
                break
        else:
            print(jobs_data['error'])
            break

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

    while page < 10:  # Limit to 10 pages
        companies_data, headers = fetch_companies(page=page, descending=descending, industry=industry, size=size, location=location)
        if 'results' in companies_data:
            all_companies.extend(companies_data['results'])
            page += 1

            if page % 10 == 0 or page >= companies_data['page_count']:
                file_index += 1
                df_companies = pd.DataFrame(all_companies)
                companies_csv_path = os.path.join('data', f'themuse_companies_part_{file_index}.csv')
                os.makedirs(os.path.dirname(companies_csv_path), exist_ok=True)
                df_companies.to_csv(companies_csv_path, index=False)
                print(f"Saved to '{companies_csv_path}': {len(all_companies)} companies")
                all_companies = []

            if page >= companies_data['page_count']:
                break
        else:
            print(companies_data['error'])
            break

        rate_limit_remaining = int(headers.get('X-RateLimit-Remaining', 1))
        rate_limit_reset = int(headers.get('X-RateLimit-Reset', 0))

        if rate_limit_remaining <= 0:
            print(f"Rate limit reached. Waiting for {rate_limit_reset} seconds.")
            time.sleep(rate_limit_reset)

    return all_companies

# Enhanced function to fetch all coaches and handle rate limits
def fetch_all_coaches(descending=False, offering=None, level=None, specialization=None):
    all_coaches = []
    page = 0
    file_index = 0

    while page < 10:  # Limit to 10 pages
        coaches_data, headers = fetch_coaches(page=page, descending=descending, offering=offering, level=level, specialization=specialization)
        if 'results' in coaches_data:
            all_coaches.extend(coaches_data['results'])
            page += 1

            if page % 10 == 0 or page >= coaches_data['page_count']:
                file_index += 1
                df_coaches = pd.DataFrame(all_coaches)
                coaches_csv_path = os.path.join('data', f'themuse_coaches_part_{file_index}.csv')
                os.makedirs(os.path.dirname(coaches_csv_path), exist_ok=True)
                df_coaches.to_csv(coaches_csv_path, index=False)
                print(f"Saved to '{coaches_csv_path}': {len(all_coaches)} coaches")
                all_coaches = []

            if page >= coaches_data['page_count']:
                break
        else:
            print(coaches_data['error'])
            break

        rate_limit_remaining = int(headers.get('X-RateLimit-Remaining', 1))
        rate_limit_reset = int(headers.get('X-RateLimit-Reset', 0))

        if rate_limit_remaining <= 0:
            print(f"Rate limit reached. Waiting for {rate_limit_reset} seconds.")
            time.sleep(rate_limit_reset)

    return all_coaches

# Fetch all jobs
fetch_all_jobs()

# Fetch all companies
fetch_all_companies()

# Fetch all coaches
fetch_all_coaches()
