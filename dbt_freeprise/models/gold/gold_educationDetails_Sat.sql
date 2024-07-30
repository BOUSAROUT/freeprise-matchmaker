{{ config(materialized='table', schema='Gold') }}


SELECT
  MD5(title || degree) AS education_id,
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
  where ifnull(title, '') <> '' and ifnull(degree, '') <> ''
