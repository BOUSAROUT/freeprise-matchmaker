{{ config(materialized='table') }}

WITH cleaned_peopleperhour AS (
    SELECT
        Type,
        Title,
        CAST(Budget AS FLOAT64) AS budget,
        Currency,
        Duration,
        Location,
        Experience,
        Client_City,
        DATE(Date_Posted) AS date_posted,
        Description,
        Category_Name AS category,
        Client_Country,
        Client_Currency,
        Client_Job_Title,
        Sub_Category_Name AS sub_category,
        DATE(Client_Registration_Date) AS client_registration_date,
        Freelancer_Preferred_From
    FROM
        {{ source('your_dataset', 'raw_peopleperhour') }}
),

-- Summarizing the PeoplePerHour projects data
peopleperhour_summary AS (
    SELECT
        category,
        COUNT(*) AS total_projects,
        AVG(budget) AS avg_budget,
        AVG(TIMESTAMP_DIFF(TIMESTAMP(NOW()), TIMESTAMP(date_posted), DAY)) AS avg_days_since_posted,
        MIN(budget) AS min_budget,
        MAX(budget) AS max_budget,
        COUNT(DISTINCT Client_Country) AS distinct_client_countries,
        COUNT(DISTINCT Freelancer_Preferred_From) AS distinct_freelancer_locations
    FROM
        cleaned_peopleperhour
    GROUP BY
        category
),

-- Additional insights per sub-category
sub_category_summary AS (
    SELECT
        sub_category,
        COUNT(*) AS total_projects,
        AVG(budget) AS avg_budget,
        AVG(TIMESTAMP_DIFF(TIMESTAMP(NOW()), TIMESTAMP(date_posted), DAY)) AS avg_days_since_posted
    FROM
        cleaned_peopleperhour
    GROUP BY
        sub_category
)

-- Final output
SELECT
    pps.category,
    pps.total_projects,
    pps.avg_budget,
    pps.min_budget,
    pps.max_budget,
    pps.avg_days_since_posted,
    pps.distinct_client_countries,
    pps.distinct_freelancer_locations,
    scs.sub_category,
    scs.total_projects AS sub_category_total_projects,
    scs.avg_budget AS sub_category_avg_budget,
    scs.avg_days_since_posted AS sub_category_avg_days_since_posted
FROM
    peopleperhour_summary pps
LEFT JOIN
    sub_category_summary scs
ON
    pps.category = scs.sub_category
