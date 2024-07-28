-- models/silver/silver_profile_data.sql

{{ config(materialized='table') }}

SELECT
  id,
  url,
  city,
  name,
  about,
  region,
  position,
  recommendations,
  recommendations_count,
  current_company as company,
  country_code,
  current_company_company_id as company_id
FROM
  {{ source('Bronze', 'raw_profiles') }}
