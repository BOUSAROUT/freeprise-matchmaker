{{ config(materialized='table', schema='Gold') }}

SELECT
  MD5(ifnull(CAST(title AS STRING), '0') || ifnull(CAST(degree AS STRING), '0')) AS education_id,
  CURRENT_TIMESTAMP AS LoadDate,
  title,
  degree,
  start_year,
  end_year,
  field,
  meta,
  url as linkedin_url,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_education_data') }}
