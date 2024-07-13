#!/usr/bin/env python3

import json
import sys
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Ensure the module path is set correctly
sys.path.append(str(Path(__file__).resolve().parent.parent))

from linkedin_api.linkedin import Linkedin

# Load environment variables from .env file
load_dotenv()

# Load cookies from a JSON file
cookies_file_path = os.path.join('linkedin_api', 'linkedin_cookies.json')
with open(cookies_file_path, 'r') as file:
    cookies = json.load(file)

# Authenticate LinkedIn API using cookies
api = Linkedin('', '', cookies=cookies)

# Read profile names from CSV file
profile_names_file_path = 'data/source/profile_names.csv'
profile_names_df = pd.read_csv(profile_names_file_path)

# List to store profile data
profile_data_list = []

# Specify the columns to keep
columns_to_keep = [
    'firstName', 'lastName', 'headline', 'summary', 'industryName', 'address',
    'locationName', 'geoLocationName', 'profile_id', 'public_id', 'experience',
    'education', 'languages', 'certifications', 'honors', 'projects', 'skills'
]

# Output file path
output_file_path = os.path.join('data', 'source', 'profiles.csv')
os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

# Function to save profile data to CSV file
def save_profiles_to_csv(profiles, path):
    df = pd.DataFrame(profiles)
    if not df.empty:
        if os.path.exists(path):
            df.to_csv(path, mode='a', header=False, index=False)
        else:
            df.to_csv(path, index=False)

# Loop through each profile name and fetch profile data
for index, row in profile_names_df.iterrows():
    profile_name = row['profile_name']
    print(f"Fetching data for profile: {profile_name}")
    profile_data = api.get_profile(profile_name)

    # Filter profile data to keep only specified columns
    filtered_profile_data = {key: profile_data.get(key, None) for key in columns_to_keep}
    profile_data_list.append(filtered_profile_data)

    # Save data every 10 records
    if (index + 1) % 10 == 0:
        save_profiles_to_csv(profile_data_list, output_file_path)
        profile_data_list = []  # Reset the list

# Save any remaining data
if profile_data_list:
    save_profiles_to_csv(profile_data_list, output_file_path)

print(f"Profile data saved to '{output_file_path}'")
