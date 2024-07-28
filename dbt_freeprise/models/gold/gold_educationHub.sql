{{ config(materialized='table', schema='Gold') }}

SELECT
  MD5(ifnull(CAST(title AS STRING), '0') || ifnull(CAST(degree AS STRING), '0')) AS education_id,
  concat(ifnull(CAST(title AS STRING), '0'), ifnull(CAST(degree AS STRING), '0')) AS BusinessKey_education_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_education_data') }}
