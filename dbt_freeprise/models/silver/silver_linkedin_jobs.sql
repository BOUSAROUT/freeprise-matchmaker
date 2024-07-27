{{ config(materialized='table') }}

select
job_link,
job_title,
search_position,
job_level,
job_type,
got_summary,
got_ner,
is_being_worked,
job_location,
search_city,
search_country,
company

from   {{ source('Bronze', 'raw_job_posting') }}
