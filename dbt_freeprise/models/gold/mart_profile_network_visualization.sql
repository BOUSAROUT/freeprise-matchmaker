{{ config(materialized='materialized_view') }}

SELECT
  p.Profile_id,
  p.BusinessKey_id AS Profile_key,
  c.Company_id,
  c.BusinessKey_company_id AS Company_key,
  s.Name,
  s.Position,
  s.region,
  s.`recommendations Count`
FROM {{ ref('gold_profileHub') }} p
JOIN {{ ref('gold_profileCompany_Link') }} lpc ON p.Profile_id = lpc.Profile_id
JOIN {{ ref('gold_companyHub') }} c ON lpc.Company_id = c.Company_id
JOIN {{ ref('gold_profileDetails_Sat') }} s ON p.Profile_id = s.Profile_id;
