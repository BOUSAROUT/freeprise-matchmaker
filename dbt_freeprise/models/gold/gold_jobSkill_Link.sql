
{{ config(materialized='table') }}

SELECT
  MD5(job_link) AS job_id,
  MD5(job_link) AS Skill_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_linkedin_jobs') }}

order by job_link asc
