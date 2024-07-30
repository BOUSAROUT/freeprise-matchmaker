{{ config(materialized='materialized_view') }}

SELECT
  p.Profile_id,
  e.Education_id,
  ed.Degree,
  ed.Title,
  ed.linkedin_url,
  pd.Name,
  pd.Position
FROM {{ ref('gold_profileHub') }} p
JOIN {{ ref('gold_profileEducation_Link') }} lpe ON p.Profile_id = lpe.Profile_id
JOIN {{ ref('gold_educationHub') }} e ON lpe.Education_id = e.Education_id
JOIN {{ ref('gold_educationDetails_Sat') }} ed ON e.Education_id = ed.Education_id
JOIN {{ ref('gold_profileDetails_Sat') }} pd ON p.Profile_id = pd.Profile_id
