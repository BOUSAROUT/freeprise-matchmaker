
{{ config(materialized='table') }}


SELECT
  MD5(job_link) AS job_id,
  job_link AS BusinessKey_job_link,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_linkedin_jobs') }}


Union all

SELECT
  MD5(job_link) AS job_id,
  job_link AS BusinessKey_job_link,
  CURRENT_TIMESTAMP AS LoadDate,
  'Themuse_dataset' AS RecordSource
FROM
  {{ ref('silver_themuse_jobs') }}
