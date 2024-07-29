-- models/gold/gold_companyHub_temp.sql

{{ config(materialized='table') }}

SELECT
  MD5(ifnull(CAST(silver_company_data.id AS STRING), '0')) AS company_id,
  id AS BusinessKey_company_id,
  CURRENT_TIMESTAMP AS LoadDate,
  'Linkedin_dataset' AS RecordSource
FROM
  {{ ref('silver_company_data') }}
order by company_id asc

--Union all

--SELECT
--  MD5(id) AS company_id,
--  id AS BusinessKey_company_id,
--  CURRENT_TIMESTAMP AS LoadDate,
--  'Themuse_dataset' AS RecordSource
--FROM
--  {{ ref('silver_themuse_company_data') }}
