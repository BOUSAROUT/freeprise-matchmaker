import json
import pandas as pd
import os
import re

# Define the input and output file paths
input_csv = 'data/source/profiles.csv'
experience_output_csv = 'data/cleaned/experiences_data.csv'
certification_output_csv = 'data/cleaned/certifications_data.csv'
skills_output_csv = 'data/cleaned/skills_data.csv'
projects_output_csv = 'data/cleaned/projects_data.csv'

# Read the profiles CSV file
df = pd.read_csv(input_csv)

# Enhanced function to clean and parse the JSON string
def clean_and_parse_json(json_str):
    if isinstance(json_str, str) and json_str.strip():
        try:
            # Apply common fixes and attempt to parse again
            json_str = json_str.replace("'", "\"")  # Replace single quotes with double quotes
            #json_str = re.sub(r'(\w+):', r'"\1":', json_str)  # Add quotes around keys
            json_str = re.sub(r'\\(?!["\\/bfnrtu])', r'\\\\', json_str)  # Escape backslashes not followed by a valid escape character
            json_str = json_str.replace('\n', ' ').replace('\r', ' ')  # Replace newlines and carriage returns with spaces
            json_str = re.sub(r'}\s*{', '},{', json_str)  # Ensure proper comma placement between objects
            # Fix specific issues with URN formatting
            json_str = json_str.replace('""urn""', '"urn"')

            # Attempt to parse the cleaned JSON string
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            # Log the error and return an empty list to ignore complex cases
            print(f"Error decoding JSON: {e} in string: {json_str[:1000]}...")  # Print only the first 1000 chars
            return []
    return []

# Extract and parse the columns
df['parsed_experience'] = df['experience'].apply(clean_and_parse_json)
df['parsed_certifications'] = df['certifications'].apply(clean_and_parse_json)
df['parsed_skills'] = df['skills'].apply(clean_and_parse_json)
df['parsed_projects'] = df['projects'].apply(clean_and_parse_json)

# Function to flatten and filter data
def extract_data(parsed_data, keys, profile_id, last_name):
    extracted_data = []
    for item in parsed_data:
        filtered_item = {key: item.get(key) for key in keys}
        filtered_item['profile_id'] = profile_id
        filtered_item['last_name'] = last_name
        extracted_data.append(filtered_item)
    return extracted_data

# Flatten the experience data
experience_data = []
for _, row in df.iterrows():
    experience_data.extend(extract_data(row['parsed_experience'], ['locationName', 'geoLocationName', 'companyName', 'timePeriod', 'description', 'title'], row['profile_id'], row['lastName']))

# Convert the flattened experience data to a DataFrame
experience_df = pd.DataFrame(experience_data)

# Write the experience data to a CSV file
os.makedirs(os.path.dirname(experience_output_csv), exist_ok=True)
experience_df.to_csv(experience_output_csv, index=False)

# Flatten the certification data
certification_data = []
for _, row in df.iterrows():
    certification_data.extend(extract_data(row['parsed_certifications'], ['authority', 'name', 'timePeriod'], row['profile_id'], row['lastName']))

# Convert the flattened certification data to a DataFrame
certification_df = pd.DataFrame(certification_data)

# Write the certification data to a CSV file
os.makedirs(os.path.dirname(certification_output_csv), exist_ok=True)
certification_df.to_csv(certification_output_csv, index=False)

# Flatten the skills data
skills_data = []
for _, row in df.iterrows():
    skills_data.extend(extract_data(row['parsed_skills'], ['name'], row['profile_id'], row['lastName']))

# Convert the flattened skills data to a DataFrame
skills_df = pd.DataFrame(skills_data)

# Write the skills data to a CSV file
os.makedirs(os.path.dirname(skills_output_csv), exist_ok=True)
skills_df.to_csv(skills_output_csv, index=False)

# Flatten the projects data
projects_data = []
for _, row in df.iterrows():
    projects_data.extend(extract_data(row['parsed_projects'], ['title', 'description', 'timePeriod'], row['profile_id'], row['lastName']))

# Convert the flattened projects data to a DataFrame
projects_df = pd.DataFrame(projects_data)

# Write the projects data to a CSV file
os.makedirs(os.path.dirname(projects_output_csv), exist_ok=True)
projects_df.to_csv(projects_output_csv, index=False)

print(f"Experience data has been successfully written to {experience_output_csv}")
print(f"Certification data has been successfully written to {certification_output_csv}")
print(f"Skills data has been successfully written to {skills_output_csv}")
print(f"Projects data has been successfully written to {projects_output_csv}")
