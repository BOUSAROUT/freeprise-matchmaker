{{ config(materialized='view') }}

select
        jobsum.job_id,
        profDetailsSat.name,
        jobsum.job_summary,
        profDetailsSat.about as profile_description,
        jobSkillSat.job_skills
    from {{ ref('gold_jobSummary_Sat') }} as jobsum
    join {{ ref('gold_jobHub') }} jobHub on jobHub.job_id = jobsum.job_id
    join {{ ref('gold_jobSkill_Link') }} jobSkillLink on jobSkillLink.job_id = jobHub.job_id
    join {{ ref('gold_jobSkill_Sat') }} as jobSkillSat on jobSkillSat.Skill_id = jobSkillLink.Skill_id
    join {{ ref('gold_jobCompany_Link') }} jobComLink on jobComLink.job_id = jobHub.job_id
    join {{ ref('gold_companyHub') }} compHub on compHub.company_id = jobComLink.company_id
    join {{ ref('gold_profileCompany_Link') }} profCompLink on profCompLink.company_id = compHub.company_id
    join {{ ref('gold_profileHub') }} profileHub on profileHub.Profile_id = profCompLink.Profile_id
    join {{ ref('gold_profileDetails_Sat') }} profDetailsSat on profDetailsSat.Profile_id = profileHub.Profile_id
