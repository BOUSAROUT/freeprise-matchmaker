{{ config(materialized='table') }}


SELECT
  MD5(job_link) AS Skill_id,
  job_link AS BusinessKey_job_link,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_job_skills_data') }}
