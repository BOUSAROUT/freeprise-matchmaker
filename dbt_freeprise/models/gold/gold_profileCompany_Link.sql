{{ config(materialized='table') }}

SELECT
    MD5(silver_profile_data.id || silver_company_data.id) AS link_ProfileCompany_id,
    MD5(silver_profile_data.id) AS Profile_id,
    MD5(silver_company_data.id) AS company_id,
    CURRENT_TIMESTAMP AS LoadDate,
    'Linkedin_Dataset' AS RecordSource
FROM
    {{ ref('silver_profile_data') }} AS silver_profile_data
     JOIN {{ ref('silver_company_data') }} AS silver_company_data
        ON silver_company_data.id = silver_profile_data.company_id or trim(replace(upper(silver_company_data.name),' ','')) = trim(replace(upper(silver_profile_data.company),' ',''))
where ifnull(silver_company_data.id, '') <> '' and ifnull(silver_profile_data.company, '') <> ''

UNION ALL

SELECT
    MD5(silver_profile_data.id || silver_themuse_company_data.id) AS link_ProfileCompany_id,
    MD5(silver_profile_data.id) AS Profile_id,
    MD5(silver_themuse_company_data.id) AS company_id,
    CURRENT_TIMESTAMP AS LoadDate,
    'Themuse_Dataset' AS RecordSource
FROM
    {{ ref('silver_profile_data') }} AS silver_profile_data
     JOIN {{ ref('silver_themuse_company_data') }} AS silver_themuse_company_data
        ON silver_themuse_company_data.id = silver_profile_data.company_id
