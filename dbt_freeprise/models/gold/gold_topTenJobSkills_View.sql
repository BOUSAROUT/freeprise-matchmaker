{{ config(materialized='view') }}

WITH SkillsExpanded AS (
  SELECT
    TRIM(skill) AS skill
  FROM
    {{ ref('gold_jobSkill_Sat') }},
    UNNEST(SPLIT(job_skills, ',')) AS skill
  WHERE
    IFNULL(job_skills, '') <> ''
)

SELECT
  skill,
  COUNT(*) AS count
FROM
  SkillsExpanded
GROUP BY
  skill
HAVING
  COUNT(*) > 1
ORDER BY
  count DESC
Limit 10
