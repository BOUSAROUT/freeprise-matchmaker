{{ config(materialized='materialized_view') }}

SELECT
case when job_type is null then 'Not Defined' else job_type end as job_type ,
Count(*) as count
FROM  {{ ref('gold_jobDetails_Sat') }}
Group by  job_type
