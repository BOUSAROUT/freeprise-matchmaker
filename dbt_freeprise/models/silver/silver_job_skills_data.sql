{{ config(materialized='table') }}

SELECT
job_link,
job_skills
from   {{ source('Bronze', 'raw_job_skills_data') }}
