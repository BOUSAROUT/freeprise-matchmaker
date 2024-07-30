-- models/gold/gold_companyHub_temp.sql

{{ config(materialized='table') }}

SELECT
  MD5(silver_company_data.id) AS company_id,
  id AS BusinessKey_company_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_company_data') }}

Union all
SELECT
  MD5(id) AS company_id,
  id AS BusinessKey_company_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Themuse_dataset' AS RecordSource
FROM
  {{ ref('silver_themuse_company_data') }}
