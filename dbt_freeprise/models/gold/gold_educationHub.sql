{{ config(materialized='table', schema='Gold') }}

SELECT Distinct
 MD5(title || degree) AS education_id,
 concat(title || degree) AS BusinessKey_education_id,
 CURRENT_TIMESTAMP AS LoadDate,
'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_education_data') }}
where ifnull(title,'') <> '' and ifnull(degree,'') <> ''
