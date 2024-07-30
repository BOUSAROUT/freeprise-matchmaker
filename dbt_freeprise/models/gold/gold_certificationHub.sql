{{ config(materialized='table', schema='Gold') }}

SELECT Distinct
  MD5(title||subtitle) AS certification_id,
  concat(title||subtitle) AS BusinessKey_certification_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_certifications_data') }}
where ifnull(title,'') <> '' and ifnull(subtitle, '') <> ''
