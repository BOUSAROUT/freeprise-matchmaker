{{ config(materialized='table') }}

  SELECT
    MD5(id) AS Profile_id,
    CURRENT_TIMESTAMP AS LoadDate,
    name,
    case when about = 'null' then 'Whoops! Looks like you forgot to tell us about yourself!' else about end as about,
    CASE WHEN (region = 'null' or region = 'NA') THEN 'Not Specified' ELSE region END AS region,
    position,
    CASE WHEN (recommendations = 'null' OR recommendations = '') THEN 'Not yet!' ELSE recommendations END AS recommendations,
    CASE WHEN recommendations_count = 'null' THEN '0' ELSE recommendations_count END AS `recommendations Count`,
    country_code as `country Code`,
    CASE
    WHEN country_code = 'US' THEN 'United States'
    WHEN country_code = 'IQ' THEN 'Iraq'
    WHEN country_code = 'IR' THEN 'Iran'
    WHEN country_code = 'IT' THEN 'Italy'
    WHEN country_code = 'JP' THEN 'Japan'
    WHEN country_code = 'KR' THEN 'South Korea'
    WHEN country_code = 'KW' THEN 'Kuwait'
    WHEN country_code = 'LK' THEN 'Sri Lanka'
    WHEN country_code = 'LV' THEN 'Latvia'
    WHEN country_code = 'MA' THEN 'Morocco'
    WHEN country_code = 'MK' THEN 'North Macedonia'
    WHEN country_code = 'MO' THEN 'Macau'
    WHEN country_code = 'MX' THEN 'Mexico'
    WHEN country_code = 'MY' THEN 'Malaysia'
    WHEN country_code = 'NG' THEN 'Nigeria'
    ELSE 'Not Specified' END AS country,
    company_id,
    'Linkedin_dataset' AS RecordSource
  FROM
    {{ ref('silver_profile_data') }}
  where ifnull(name,'') <> '' and name <> ''
