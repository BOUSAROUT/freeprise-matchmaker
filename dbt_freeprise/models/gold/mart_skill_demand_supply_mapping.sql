{{ config(materialized='materialized_view') }}

SELECT
  sk.Skill_id,
  jss.Job_skills,
  COUNT(j.Job_id) AS Demand_Count
FROM
  {{ ref('gold_skillHub') }} sk
JOIN
  {{ ref('gold_jobSkill_Link') }} ljs ON sk.Skill_id = ljs.Skill_id
JOIN
  {{ ref('gold_jobSkill_Sat') }} jss ON jss.Skill_id = ljs.Skill_id
JOIN
  {{ ref('gold_jobHub') }} j ON ljs.Job_id = j.Job_id
GROUP BY
  sk.Skill_id,
  jss.Job_skills;
