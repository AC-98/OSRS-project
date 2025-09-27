-- Data test: Ensure bronze → silver preserves row counts
WITH bronze_count AS (
    SELECT COUNT(*) as bronze_rows
    FROM {{ ref('bronze_timeseries') }}
),

silver_count AS (
    SELECT COUNT(*) as silver_rows  
    FROM {{ ref('silver_prices_daily') }}
)

SELECT 
    bronze_rows,
    silver_rows,
    bronze_rows - silver_rows as row_difference
FROM bronze_count 
CROSS JOIN silver_count
WHERE bronze_rows != silver_rows
