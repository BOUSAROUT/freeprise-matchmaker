{{ config(materialized='table') }}

SELECT
job_link,
job_summary
from   {{ source('Bronze', 'raw_job_summary_data') }}
