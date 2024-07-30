{{ config(materialized='view') }}

SELECT
sphere,
count(*) as count
FROM {{ ref('gold_companyDetails_Sat') }}
where sphere is not null
group by sphere
order by count(*) desc
limit 10
