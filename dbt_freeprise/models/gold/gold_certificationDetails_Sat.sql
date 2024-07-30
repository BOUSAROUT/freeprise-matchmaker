{{ config(materialized='table', schema='Gold') }}




SELECT
  MD5(title || subtitle ) AS certification_id,
  CURRENT_TIMESTAMP AS LoadDate,
  title,
  subtitle,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_certifications_data') }}
Where ifnull(title, '') <> '' and ifnull(subtitle, '') <> ''
