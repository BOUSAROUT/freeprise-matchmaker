{{ config(materialized='materialized_view') }}

SELECT
  p.Profile_id,
  c.Certification_id,
  cd.Title,
  cd.Subtitle,
  pd.Position,
  pd.company_id
FROM {{ ref('gold_profileHub') }} p
JOIN {{ ref('gold_profileCertification_Link') }} lpc ON p.Profile_id = lpc.Profile_id
JOIN {{ ref('gold_certificationHub') }} c ON lpc.Certification_id = c.Certification_id
JOIN {{ ref('gold_certificationDetails_Sat') }} cd ON c.Certification_id = cd.Certification_id
JOIN {{ ref('gold_profileDetails_Sat') }} pd ON p.Profile_id = pd.Profile_id;
