from freelancersdk.session import Session
from freelancersdk.resources.users.users import search_freelancers, get_users, get_user_by_id, get_portfolios
from freelancersdk.resources.users.exceptions import \
    UsersNotFoundException, PortfoliosNotFoundException
from freelancersdk.resources.users.helpers import (
    create_get_users_details_object, create_get_users_object
)
from freelancersdk.resources.projects.projects import get_jobs
from freelancersdk.resources.projects.exceptions import \
    JobsNotFoundException
from dotenv import load_dotenv
import os
import pandas as pd
import json
from typing import Dict, Union, List

# Load environment variables from .env file
load_dotenv()

search_keywords = ['engineer', 'data', 'design', 'web']

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

# Get a freelancersdk Session object to use the api
def get_fln_session() -> Session:
    """
    To do: <describe function>
    Args: None
    Returns:
        Session object
    """
    url = os.environ.get('FLN_URL')
    oauth_token = os.environ.get('FLN_API_TOKEN')
    return Session(oauth_token=oauth_token, url=url)

# Search for freelancer_users
def search_freelance_users(session: Session, query: Union[str, List[str]]) -> Dict:
    """
    To do: <describe function>
    Args:
        session: The freelancersdk Session object
        query: The query as a string or a list of strings
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

# Search for portfolios based on user ds
def sample_get_portfolios(session, user_ids):
    user_ids = user_ids
    try:
        portfolios = get_portfolios(session, user_ids=user_ids)
    except PortfoliosNotFoundException as e:
        print('Error message: {}'.format(e.message))
        print('Server: response: {}'.format(e.error_code))
        return None
    else:
        return portfolios


# Transform the data such that it can be converted into a data
def create_portfolio_df(result, exclude_cols):
    # Rearrange the api results dict such that each row is a portfolio id with user id as a foreign key
    if result:
        portfolios = []
        users = result['users'].keys()
        for k in users:
            portfolios+=(result['portfolios'][k])
    print('Got {} portfolios from {} users.'.format(len(portfolios), len(users)))

    df = pd.DataFrame.from_records(portfolios)
    return df.drop(exclude_cols, axis=1)


if __name__ == '__main__':
    session = get_fln_session()
    # Get a list of dataframes by searching keywords one by one
    #!!!!To do refactor the following!!!!#
    df_list = [pd.DataFrame.from_dict(search_freelance_users(session,keyword)['users']) for keyword in search_keywords]
    print("Returning results")
    # Concatenate list into one consolidated dataframe
    user_df = pd.concat(df_list, ignore_index=True)
    # Drop all excluded columns
    final_user_df = user_df.drop(exclude_user_cols, axis=1)
    print(f"User dataframe shape: {final_user_df.shape}")
    # Export csv
    #!!!!To do refactor the following!!!!#
    # Fix the dict bug
    # df.to_parquet('myfile.parquet', engine='fastparquet')
    final_user_df.to_csv('data/raw/fln_users.csv', index=False)
    # !!!!!!!TO DO deduplicate rows in the final_user_df!!!!!!!
    # !!!!!!!Get a unique list of user ids to call other apis!!!!!!!
