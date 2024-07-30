{{ config(materialized='view') }}

SELECT
  j.Job_id,
  s.Job_title,
  s.Job_location,
  s.Search_city,
  s.Search_country,
  COUNT(j.Job_id) AS Jobs_Count,
  s.LoadDate
FROM
  {{ ref('gold_jobHub') }} j
JOIN
  {{ ref('gold_jobDetails_Sat') }} s
ON
  j.Job_id = s.Job_id
GROUP BY
  j.Job_id,
  s.Job_title,
  s.Job_location,
  s.Search_city,
  s.Search_country,
  s.LoadDate
ORDER BY
  s.LoadDate
