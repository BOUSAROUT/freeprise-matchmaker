{{ config(materialized='table') }}

SELECT
profile_id,
title,
subtitle,
meta
from   {{ source('Bronze', 'raw_certifications_data') }}
