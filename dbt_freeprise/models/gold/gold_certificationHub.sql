{{ config(materialized='table', schema='Gold') }}

SELECT
  MD5(ifnull(CAST(title AS STRING), '0') || ifnull(CAST(subtitle AS STRING), '0')) AS certification_id,
  concat(ifnull(CAST(title AS STRING), '0'), ifnull(CAST(subtitle AS STRING), '0')) AS BusinessKey_certification_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_certifications_data') }}
