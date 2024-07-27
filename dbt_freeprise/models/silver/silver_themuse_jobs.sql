{{ config(materialized='table') }}

select
link as job_link,
job_id,
levels as job_level,
job_title,
locations as job_location,
categories,
company_name as company,
job_description

from   {{ source('Bronze', 'raw_themuse_jobs_data') }}
