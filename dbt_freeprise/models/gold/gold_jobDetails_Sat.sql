
{{ config(materialized='table') }}

select * from (
SELECT
  MD5(job_link) AS job_id,
  CURRENT_TIMESTAMP AS LoadDate,
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
  company,
  cast(first_seen as TIMESTAMP) as publication_date,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_linkedin_jobs') }}

Union all

SELECT
  MD5(job_link) AS job_id,
  CURRENT_TIMESTAMP AS LoadDate,
  job_title,
  null as search_position,
  job_level,
  null as job_type,
  null as got_summary,
  null as got_ner,
  null as is_being_worked,
  job_location,
  null as search_city,
  null as search_country,
  company,
  cast(publication_date as TIMESTAMP) as publication_date,
  'Themuse_dataset' AS RecordSource
FROM
  {{ ref('silver_themuse_jobs') }})
Limit 10000
