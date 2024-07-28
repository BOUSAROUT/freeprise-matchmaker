-- models/gold/gold_profileHub.sql

{{ config( materialized='table', schema='Gold' ) }}

SELECT
  MD5(id) AS Profile_id,
  id AS BusinessKey_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_profile_data') }}
