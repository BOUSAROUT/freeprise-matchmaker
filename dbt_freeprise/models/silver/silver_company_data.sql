-- models/silver/silver_company_data.sql

{{ config(materialized='table') }}

select distinct
    id,
    url,
    name,
    about,
    sphere,
    founded,
    similar,
    website,
    formatted_locations as locations,
    company_id,
    industries,
    specialties,
    company_size,
    country_code,
    organization_type
from {{ source('Bronze', 'raw_company') }}

union distinct

select
    cast(company as string) as id,
    cast(null as string) as url,
    cast(company as string) as name,
    cast(null as string) as about,
    cast(null as string) as sphere,
    cast(null as string) as founded,
    cast(null as string) as similar,
    cast(null as string) as website,
    max(job_location) as locations,
    cast(null as string) as company_id,
    cast(null as string) as industries,
    cast(null as string) as specialties,
    cast(null as string) as company_size,
    cast(null as string) as country_code,
    cast(null as string) as organization_type
from {{ source('Bronze', 'raw_job_posting') }}
where company not in ('.', '..', ' ')
group by company
