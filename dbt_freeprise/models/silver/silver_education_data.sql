{{ config(materialized='table') }}

SELECT
profile_id,
title,
degree,
start_year,
end_year,
field,
meta,
url
from   {{ source('Bronze', 'raw_education_data') }}
