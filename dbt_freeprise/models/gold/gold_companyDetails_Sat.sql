-- models/gold/gold_companyHub_temp.sql

{{ config(materialized='table') }}




SELECT
  MD5(id) AS company_id,
  CURRENT_TIMESTAMP AS LoadDate,
  name,
  about,
  sphere,
  similar,
  website,
  locations,
  industries,
  specialties,
  company_size as size,
  country_code,
  organization_type,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_company_data') }}

Union all

SELECT
  MD5(id) AS company_id,
  CURRENT_TIMESTAMP AS LoadDate,
  name,
  about,
  null as sphere,
  null as similar,
  url as website,
  locations,
  industries,
  null as specialties,
  size,
  null as country_code,
  null as organization_type,
  'Themuse_dataset' AS RecordSource
FROM
  {{ ref('silver_themuse_company_data') }}
