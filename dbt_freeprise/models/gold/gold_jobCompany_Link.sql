{{ config(materialized='table') }}


select * from (
SELECT
    MD5(silver_linkedin_jobs.job_link || ifnull(CAST(silver_company_data.id AS STRING), 'null')) AS link_jobCompany_id,
    MD5(silver_linkedin_jobs.job_link) AS job_id,
    MD5(ifnull(CAST(silver_company_data.id AS STRING), '0')) AS company_id,
    CURRENT_TIMESTAMP AS LoadDate,
    'Linkedin_Dataset' AS RecordSource
FROM
    {{ ref('silver_linkedin_jobs') }} AS silver_linkedin_jobs
    LEFT JOIN {{ ref('silver_company_data') }} AS silver_company_data
        ON trim(replace(upper(silver_company_data.name),' ','')) = trim(replace(upper(silver_linkedin_jobs.company),' ',''))
--where ifnull(silver_company_data.id, '') <> ''
order by silver_linkedin_jobs.job_link, silver_company_data.id asc

--UNION ALL

--SELECT
--    MD5(silver_themuse_jobs.job_link|| ifnull(CAST(silver_themuse_company_data.id AS STRING), 'null')) AS link_ProfileCompany_id,
--    MD5(silver_themuse_jobs.job_link) AS job_id,
--    MD5(ifnull(CAST(silver_themuse_company_data.id AS STRING), '0')) AS company_id,
--    CURRENT_TIMESTAMP AS LoadDate,
--    'Themuse_Dataset' AS RecordSource
--FROM
--    {{ ref('silver_themuse_jobs') }} AS silver_themuse_jobs
--    LEFT JOIN {{ ref('silver_themuse_company_data') }} AS silver_themuse_company_data
--        ON trim(replace(upper(silver_themuse_company_data.name), '','')) = trim(replace(upper(silver_themuse_jobs.company),' ',''))
--where ifnull(silver_themuse_company_data.id, '') <> ''
--order by silver_themuse_jobs.job_link asc
)
limit 10000
