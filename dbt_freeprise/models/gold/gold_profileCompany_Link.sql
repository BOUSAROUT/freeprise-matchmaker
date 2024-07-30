{{ config(materialized='table') }}

SELECT
    MD5(silver_profile_data.id || ifnull(CAST(silver_company_data.id AS STRING), '0')) AS link_ProfileCompany_id,
    MD5(silver_profile_data.id) AS Profile_id,
    MD5(ifnull(CAST(silver_company_data.id AS STRING), '0')) AS company_id,
    CURRENT_TIMESTAMP AS LoadDate,
    'Linkedin_Dataset' AS RecordSource
FROM
    {{ ref('silver_profile_data') }} AS silver_profile_data
    LEFT JOIN {{ ref('silver_company_data') }} AS silver_company_data
        ON silver_company_data.id = silver_profile_data.company_id

UNION ALL

SELECT
    MD5(silver_profile_data.id || ifnull(CAST(silver_themuse_company_data.id AS STRING), '0')) AS link_ProfileCompany_id,
    MD5(silver_profile_data.id) AS Profile_id,
    MD5(ifnull(CAST(silver_themuse_company_data.id AS STRING), '0')) AS company_id,
    CURRENT_TIMESTAMP AS LoadDate,
    'Themuse_Dataset' AS RecordSource
FROM
    {{ ref('silver_profile_data') }} AS silver_profile_data
    LEFT JOIN {{ ref('silver_themuse_company_data') }} AS silver_themuse_company_data
        ON silver_themuse_company_data.id = silver_profile_data.company_id
