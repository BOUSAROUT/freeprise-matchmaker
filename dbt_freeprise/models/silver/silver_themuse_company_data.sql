{{ config(materialized='table') }}


SELECT
company_id as id,
company_page as url,
company_name as name,
industries,
description as about,
locations,
job_page,
size
from
  {{ source('Bronze', 'raw_themuse_companies_data') }}
