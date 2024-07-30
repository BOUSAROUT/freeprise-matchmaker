{{ config(materialized='table') }}

    select
        md5(silver_linkedin_jobs.job_link || silver_company_data.id) as link_jobcompany_id,
        md5(silver_linkedin_jobs.job_link) as job_id,
        md5(silver_company_data.id) as company_id,
        current_timestamp as loaddate,
        'linkedin_dataset' as recordsource
    from
        {{ ref('silver_linkedin_jobs') }} as silver_linkedin_jobs
        join {{ ref('silver_company_data') }} as silver_company_data
            on trim(replace(upper(silver_company_data.name), ' ', '')) = trim(replace(upper(silver_linkedin_jobs.company), ' ', ''))
    where ifnull(silver_company_data.id, '') <> ''

   union all
    select
        md5(silver_themuse_jobs.job_link || silver_themuse_company_data.id) as link_profilecompany_id,
        md5(silver_themuse_jobs.job_link) as job_id,
        md5(silver_themuse_company_data.id) as company_id,
        current_timestamp as loaddate,
        'themuse_dataset' as recordsource
    from
        {{ ref('silver_themuse_jobs') }} as silver_themuse_jobs
        join {{ ref('silver_themuse_company_data') }} as silver_themuse_company_data
            on trim(regexp_replace(upper(silver_themuse_company_data.name), ' ', '')) = trim(replace(upper(silver_themuse_jobs.company), ' ', ''))
    where ifnull(silver_themuse_company_data.id, '') <> ''
