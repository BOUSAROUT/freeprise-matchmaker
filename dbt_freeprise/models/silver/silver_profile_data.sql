-- models/silver/silver_profile_data.sql

{{ config(
    materialized='table',
    schema='Silver'
) }}

SELECT
  id,
  url,
  city,
  name,
  about,
  region,
  position,
  country_code
FROM
  {{ source('Bronze', 'raw_profiles_data') }}
