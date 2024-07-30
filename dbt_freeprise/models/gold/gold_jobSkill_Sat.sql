{{ config(materialized='table') }}

SELECT
  MD5(job_link) AS Skill_id,
  CURRENT_TIMESTAMP AS LoadDate,
  job_skills,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_job_skills_data') }}
