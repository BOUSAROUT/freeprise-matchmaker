import json
import re

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

# Function to extract the column needed from profiles
def extract_profile_data(profile_df, destination_folder):
    if profile_df.empty:
        return profile_df

    required_columns = ["id", "name", "current_company:company_id", "position", "about", "url",
                        "recommandations", "recommandation_url", "city", "country_code", "region"]

    # Ensure only existing columns are selected
    existing_columns = [col for col in required_columns if col in profile_df.columns]

    if not existing_columns:
        raise ValueError("None of the required columns are present in the DataFrame")

    profile_df = profile_df[existing_columns]

    try:
        profile_df.to_csv(destination_folder, index=False)
    except Exception as e:
        print(f"Error saving the file: {e}")
        raise
    return profile_df
