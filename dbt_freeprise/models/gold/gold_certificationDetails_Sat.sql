{{ config(materialized='table', schema='Gold') }}

SELECT
  MD5(ifnull(CAST(title AS STRING), '0') || ifnull(CAST(subtitle AS STRING), '0')) AS certification_id,
  CURRENT_TIMESTAMP AS LoadDate,
  title,
  subtitle,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_certifications_data') }}
