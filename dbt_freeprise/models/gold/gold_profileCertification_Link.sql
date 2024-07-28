-- models/gold/gold_profileHub.sql

{{ config( materialized='table', schema='Gold' ) }}

SELECT
  MD5(silver_profile_data.id) AS Profile_id,
  MD5(ifnull(CAST(title AS STRING), '0') || ifnull(CAST(subtitle AS STRING), '0')) AS certification_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_profile_data') }} as silver_profile_data
  left join  {{ ref('silver_certifications_data') }} as silver_education_data on silver_education_data.Profile_id = silver_profile_data.id
