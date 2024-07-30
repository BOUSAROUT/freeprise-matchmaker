
{{ config(materialized='table') }}

SELECT
  MD5(id) AS Profile_id,
  CURRENT_TIMESTAMP AS LoadDate,
  name,
  about,
  region,
  position,
  recommendations,
  recommendations_count,
  country_code,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_profile_data') }}
