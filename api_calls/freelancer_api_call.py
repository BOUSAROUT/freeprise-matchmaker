from freelancersdk.session import Session
from freelancersdk.resources.users.users import search_freelancers, get_portfolios
from freelancersdk.resources.users.exceptions import \
    UsersNotFoundException, PortfoliosNotFoundException
from freelancersdk.resources.users.helpers import (
    create_get_users_details_object
)
from dotenv import load_dotenv
import os
import pandas as pd
from typing import Dict, Union, List
from pandera.typing import DataFrame
from data_utils.upload import upload_to_lake, get_data_files

# Load environment variables from .env file
load_dotenv()

# User data columns that are ignored and not uploaded to bucket
exclude_user_cols = [
    'limited_account',
    'pool_ids',
    'enterprise_ids',
    'escrowcom_account_linked',
    'escrowcom_interaction_required',
    'enterprises',
    'oauth_password_credentials_allowed',
    'registration_completed',
    'is_profile_visible',
    'registration_date',
    'chosen_role',
    'role',
    'status',
    'timezone'
]

# Portfolio data columns that are ignored and not uploaded to bucket
exclude_portfolio_cols = [
    'content_type',
    'featured',
    'files',
    'articles',
    'jobs',
    'categories',
    'last_modify_date',
    'position'
]

# User data columns containing dicts that request further processing in ETL
dict_user_cols = [
    'id',
    'reputation',
    'location',
    'primary_currency',
    'corporate',
]

# User data columns containing list of dicts that request further processing in ETL
list_user_cols = [
    # list columns
    'id',
    'jobs',
    'qualifications'
]


# Get a freelancersdk Session object to use the api
def get_fln_session() -> Session:
    """
    Creates a freelancer.com session
    Args:
        - None
    Returns:
        - Session object
    """
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_API_TOKEN')
    return Session(oauth_token=oauth_token, url=url)


def get_search_terms(infile: str) -> List[str]:
    """
    Get search terms to call the freelancer api with from an input file
    Args:
        - infile: the input file name as a string
    Returns:
        - A list of keywords
    """
    with open(infile, 'r') as file:
        search_terms = file.read().replace('\n', ' ')

    results = search_terms.split()
    print(f"{len(results)} keywords to search for in the freelancer api")
    return results


def get_freelance_users(session: Session, query: Union[str, List[str]]) -> Dict:
    """
    Get the data for users who are freelancers on freelancer.com
    Args:
        - session: The freelancersdk Session object
        - query: The query as a string or a list of strings
    Returns:
        A dictionary with the following keys:
            - users - <to do>
            - total_count - <to do>
    """
    user_details = create_get_users_details_object(
        country=True,
        status=True,
        display_info=True,
        profile_description=True,
        qualifications=True,
        portfolio=True,
        jobs=True,
        reputation=True,
        corporate_accounts=True
    )
    try:
        result = search_freelancers(
            session=session,
            query=query,
            user_details=user_details,
            limit=100 # maximum results = 100
        )
    except UsersNotFoundException as e:
        print('Error message: {}'.format(e.message))
        print('Server response: {}'.format(e.error_code))
        return None
    else:
        return result


def get_user_portfolios(session: Session, user_ids: Union[str, List[str]])  -> Dict:
    """
    Get the data for the portfolios associated with the user based on their user id
    Args:
        session: The freelancersdk Session object
        user_ids: The user ids as a string or a list of strings
    Returns:
        A dictionary with the following keys:
            - users - <to do>
            - total_count - <to do>
    """
    user_ids = user_ids
    try:
        portfolios = get_portfolios(session, user_ids=user_ids)
    except PortfoliosNotFoundException as e:
        print('Error message: {}'.format(e.message))
        print('Server: response: {}'.format(e.error_code))
        return None
    else:
        return portfolios


def create_user_df(session: Session, search_keywords: List[str], exclude_cols: List[str], ofile_path: str) -> DataFrame:
    """
    Create a dataframe of user information by calling the freelancer.com api with search keys
    Args:
        - session: The freelancersdk Session object
        - search_keywords: A list of keywords as a string or list of strings
        - exclude_cols: Columns to exclude from the final output
        - ofile_path: The name of the outfile saved as a parquet file
    Returns:
        - Dataframe of user information
    """
    df_list = [pd.DataFrame.from_dict(get_freelance_users(session,keyword)['users']) for keyword in search_keywords]
    # Concatenate list into one consolidated dataframe
    df = pd.concat(df_list, ignore_index=True)
    print(df.shape)
    # Drop all excluded columns
    df.drop(exclude_cols, axis=1, inplace=True)
    print(f"User dataframe shape: {df.shape}")
    # Save as parquet file
    df.to_parquet(ofile_path, engine='pyarrow', index=False)
    return df

def get_unique_user_ids(df: DataFrame) -> List[int]:
    """
    Get a list of user ids from the user dataframe extracted from the freelancer api
    Args:
        - df: The user dataframe
    Returns:
        - A list of unique user ids
    """
    return df['id'].unique().tolist()


def create_portfolio_df(session: Session, user_ids: List[int], exclude_cols: List[str], ofile_path: str) -> None:
    """
    Get the portfolio data from a list of user ids using the portfio api and saves the results as parquet
    Args:
        - session: The freelancersdk Session object
        - user_ids: A list of unique user ids
        - exclude_cols: Columns to exclude from the final output
        - ofile_path: The name of the outfile saved as a parquet file
    Returns:
        - None
    """
    # Need to split the user ids into a batch size of 100 because that is the maximum api limit
    batch_size = 100
    # Instantiate a list to store the portfolio batches as dataframes
    df_list = []

    # Iterate through batches
    for i in range(0, len(user_ids), batch_size):
        # Get current batch of ids
        batch_ids = user_ids[i:i+batch_size]
        # Call portfolio api
        batch_result = get_user_portfolios(session, batch_ids)

        # Rearrange the api results dict such that each row is a portfolio id with user id as a foreign key
        if batch_result:
            batch_portfolios = []
            users = batch_result['users'].keys()
            for k in users:
                batch_portfolios+=(batch_result['portfolios'][k])

            # Add the portfolio dataframe from the batch to the list with excluded columns dropped
            df_list.append(pd.DataFrame.from_records(batch_portfolios))

    # Concatenate list into one consolidated dataframe
    df = pd.concat(df_list, ignore_index=True)
    # Drop excluded columns
    df.drop(exclude_cols, axis=1, inplace=True)

    print('Got {} portfolios from {} users'.format(len(df), len(user_ids)))
    # Save as parquet file
    df.to_parquet(ofile_path, engine='pyarrow', index=False)


# Extract columns so they can be subjected to cleaning and transformation during ETL process - for raw
def extract_cols_for_cleaning(df: DataFrame, cols: List[str], ofile_path: str) -> None:
    """
    Extract columns which need to be unpacked for cleaning
    Args:
        - df: Input user dataframe
        - cols: List of columns in the dataframe to be extracted
        - ofile_path: The name of the out file
    Returns:
        - None
    """
    df = df[cols]
    df.to_parquet(ofile_path, engine='pyarrow', index=False)
    print(df.shape)
    print(df.columns)
    return df # return not used


# Perform light cleaning on user df - for raw
def user_df_light_cleanse(df: DataFrame, cols: List[str], ofile_path: str) -> None:
    """
    Performances deduplication on the user dataframe
    Args:
        - df: Input user dataframe
        - cols: List of columns in the dataframe to be extracted
        - ofile_path: The name of the out file
    Returns:
        - None
    """
    _df = user_df.drop(cols, axis=1)
    df = _df.drop_duplicates(ignore_index=True)
    df.to_parquet(ofile_path, engine='pyarrow', index=False)
    print(f"{_df.shape[0] - df.shape[0]} duplicate rows dropped")
    print(f"User dataframe shape: {df.shape}")




if __name__ == '__main__':
    # The following steps describe the data extraction process for the freelancer.com user and portfolio data
    # This is for testing locally and the script below needs to be transformed to Airflow to perform these actions as a dag
    session = get_fln_session()
    search_terms = get_search_terms('freelancer_unique_search_terms.txt')
    # For testing, search using the first 20 keywords only
    test_keywords = search_terms[0:20]
    # Create the user dataframe
    user_df = create_user_df(session, test_keywords, exclude_user_cols, 'data/raw/fln_users_all.parquet')
    # Get the list of unique users
    user_ids = get_unique_user_ids(user_df)
    # Extract user dict columns that require ETL to cleanse (e.g. deduplication)
    dict_user_df = extract_cols_for_cleaning(user_df, dict_user_cols, 'data/raw/fln_users_dict_cols.parquet')
    # Extract user list of dict columns that require ETL to cleanse (e.g. deduplication)
    list_user_df = extract_cols_for_cleaning(user_df, list_user_cols, 'data/raw/fln_users_list_dict_cols.parquet')
    # Extract other user columns that can be deduplicated
    cleanse_cols_exc = dict_user_cols + list_user_cols
    # There's a bug here [Errno 2] No such file or directory: 'data/silver/fln_users.parquet': check for dir before proceeding
    user_clean_df = user_df_light_cleanse(user_df,cleanse_cols_exc, 'data/raw/fln_users.parquet')
    # create the portfolio dataframe
    portfolio_df = create_portfolio_df(session, user_ids, exclude_portfolio_cols, 'data/raw/fln_portfolio.parquet')

    # upload files to raw dir in bucket
    raw_files = get_data_files()

    for file in raw_files:
        upload_to_lake('freelancer.com', file)
