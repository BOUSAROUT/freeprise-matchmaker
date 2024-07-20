import pandas as pd
import os
import data_processing_functions

def extract_from_raw_data():
    # Define the input and output file paths
    input_csv = 'data/raw/raw_profiles.csv'
    input_csv_job_company = 'data/raw/raw_company.csv'
    experience_output_csv = 'data/cleaned/experiences_data.csv'
    education_output_csv = 'data/cleaned/education_data.csv'
    certifications_output_csv = 'data/cleaned/certifications_data.csv'
    output_dir_with_hash = 'data/cleaned'

    # Read the profiles CSV file
    df = pd.read_csv(input_csv)

    # Extract and parse the columns
    df['parsed_experience'] = df['experience'].apply(data_processing_functions.clean_and_parse_json)
    df['parsed_education'] = df['education'].apply(data_processing_functions.clean_and_parse_json)
    df['parsed_certifications'] = df['certifications'].apply(data_processing_functions.clean_and_parse_json)

    # Flatten the experience data
    experience_data = []
    for _, row in df.iterrows():
        profile_id = row.get('id', None)
        experience_data.extend(data_processing_functions.extract_experience_data(
            row['parsed_experience'],
            profile_id  # Profile ID
        ))

    # Convert the flattened experience data to a DataFrame
    experience_df = pd.DataFrame(experience_data)

    # Write the experience data to a CSV file
    os.makedirs(os.path.dirname(experience_output_csv), exist_ok=True)
    experience_df.to_csv(experience_output_csv, index=False)

    # Flatten the education data
    education_data = []
    for _, row in df.iterrows():
        profile_id = row.get('id', None)
        education_data.extend(data_processing_functions.extract_education_data(
            row['parsed_education'],
            profile_id  # Profile ID
        ))

    # Convert the flattened education data to a DataFrame
    education_df = pd.DataFrame(education_data)

    # Write the education data to a CSV file
    os.makedirs(os.path.dirname(education_output_csv), exist_ok=True)
    education_df.to_csv(education_output_csv, index=False)

    # Flatten the certifications data
    certifications_data = []
    for _, row in df.iterrows():
        profile_id = row.get('id', None)
        certifications_data.extend(data_processing_functions.extract_certifications_data(
            row['parsed_certifications'],
            profile_id
        ))

    # Convert the flattened certifications data to a DataFrame
    certifications_df = pd.DataFrame(certifications_data)

    # Write the certifications data to a CSV file
    os.makedirs(os.path.dirname(certifications_output_csv), exist_ok=True)
    certifications_df.to_csv(certifications_output_csv, index=False)

    print(f"Experience data has been successfully written to {experience_output_csv}")
    print(f"Education data has been successfully written to {education_output_csv}")
    print(f"Certifications data has been successfully written to {certifications_output_csv}")
