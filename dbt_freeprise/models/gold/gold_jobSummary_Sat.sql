
{{ config(materialized='table') }}

select * from (
SELECT
  MD5(job_link) AS job_id,
  job_summary,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_job_summary_data') }}
order by job_link asc
  )
Limit 10000
