{{ config(materialized='materialized_view') }}

SELECT
s.Industries,
COUNT(c.Company_id) AS Total_Companies,
s.Size,
s.Country_code
FROM {{ref('gold_companyHub')}} c
JOIN  {{ref('gold_companyDetails_Sat')}} s ON c.Company_id = s.Company_id
GROUP BY s.Industries, s.Size, s.Country_code;
