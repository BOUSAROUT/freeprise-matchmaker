import requests
import pandas as pd

# Fetch data from the API
url = "https://www.arbeitnow.com/api/job-board-api"
response = requests.get(url)
data = response.json()

# Extract the relevant part of the data if needed
jobs = data.get('data', [])

# Convert the job data to a DataFrame
df = pd.DataFrame(jobs)

# Convert list fields to strings for CSV storage
df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
df['job_types'] = df['job_types'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

# Define the CSV file path
csv_file_path = 'data/arbeitnow_api.csv'

# Write DataFrame to CSV
df.to_csv(csv_file_path, index=False, encoding='utf-8')

print(f"Data has been written to {csv_file_path}")
