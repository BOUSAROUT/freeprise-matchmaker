{{ config(materialized='table') }}

WITH cleaned_data AS (
    SELECT
        name,
        CAST(REGEXP_EXTRACT(price, r'\d+(\.\d+)?') AS FLOAT64) AS price,
        CAST(REGEXP_EXTRACT(stars, r'\d+(\.\d+)?') AS FLOAT64) AS rating,
        Subcat AS sub_category,
        _Category AS category
    FROM
        {{ source('your_dataset', 'raw_fiverr') }}
),
summary AS (
    SELECT
        category,
        COUNT(*) AS total_jobs,
        AVG(price) AS avg_price,
        AVG(rating) AS avg_rating
    FROM
        cleaned_data
    GROUP BY
        category
)
SELECT * FROM summary
