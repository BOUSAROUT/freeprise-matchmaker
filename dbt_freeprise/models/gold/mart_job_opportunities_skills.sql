{{ config(materialized='materialized_view') }}

SELECT
j.Job_id,
j.BusinessKey_job_link,
c.Company_id,
s.Job_title,
sk.Job_skills
FROM {{ ref('gold_jobHub') }} j
JOIN {{ref('gold_jobCompany_Link') }} ljc ON j.Job_id = ljc.Job_id
JOIN {{ ref('gold_jobDetails_Sat') }} s ON j.Job_id = s.Job_id
JOIN {{ ref('gold_jobSkill_Link') }} ljs ON j.Job_id = ljs.Job_id
JOIN {{ ref('gold_jobSkill_Sat') }} sk ON ljs.Skill_id = sk.Skill_id
JOIN {{ ref('gold_companyHub') }} c ON ljc.Company_id = c.Company_id;
