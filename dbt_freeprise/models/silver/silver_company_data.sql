-- models/silver/silver_company_data.sql

{{ config(materialized='table') }}

SELECT
  id,
  url,
  name,
  about,
  sphere,
  founded,
  similar,
  website,
  formatted_locations AS locations,
  company_id,
  industries,
  specialties,
  company_size,
  country_code,
  organization_type
FROM
  {{ source('Bronze', 'raw_company') }}
