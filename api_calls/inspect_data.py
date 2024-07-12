import pandas as pd

# Load the data from CSV files
jobs_file_path = 'data/themuse_jobs_part_1.csv'
companies_file_path = 'data/themuse_companies_part_1.csv'

jobs_df = pd.read_csv(jobs_file_path)
companies_df = pd.read_csv(companies_file_path)

# Print the column names
print("Jobs DataFrame columns:")
print(jobs_df.columns)

print("\nCompanies DataFrame columns:")
print(companies_df.columns)
