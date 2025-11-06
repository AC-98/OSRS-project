{{
  config(
    materialized='incremental',
    unique_key=['item_id', 'date'],
    on_schema_change='fail',
    description='Daily OSRS item features with price statistics and outlier detection'
  )
}}

WITH daily_aggregates AS (
  SELECT 
    item_id,
    CAST(ts AS DATE) AS date,
    
    -- Daily aggregates
    AVG(price_avg_high) AS avg_price_high,
    AVG(price_avg_low) AS avg_price_low,
    MIN(price_avg_low) AS min_price,
    MAX(price_avg_high) AS max_price,
    SUM(volume) AS total_volume,
    COUNT(*) AS observation_count
    
  FROM {{ ref('silver_timeseries') }}
  
  {% if is_incremental() %}
    -- Only process new data in incremental mode
    WHERE CAST(ts AS DATE) > (SELECT MAX(date) FROM {{ this }})
  {% endif %}
  
  GROUP BY item_id, CAST(ts AS DATE)
),

daily_latest AS (
  SELECT 
    item_id,
    CAST(ts AS DATE) AS date,
    price_avg_high,
    price_avg_low,
    ROW_NUMBER() OVER (
      PARTITION BY item_id, CAST(ts AS DATE) 
      ORDER BY ts DESC
    ) AS rn
  FROM {{ ref('silver_timeseries') }}
  
  {% if is_incremental() %}
    WHERE CAST(ts AS DATE) > (SELECT MAX(date) FROM {{ this }})
  {% endif %}
),

daily_prices AS (
  SELECT 
    a.item_id,
    a.date,
    l.price_avg_high AS latest_price_high,
    l.price_avg_low AS latest_price_low,
    a.avg_price_high,
    a.avg_price_low,
    a.min_price,
    a.max_price,
    a.total_volume,
    a.observation_count
  FROM daily_aggregates a
  LEFT JOIN daily_latest l ON a.item_id = l.item_id AND a.date = l.date AND l.rn = 1
),

with_features AS (
  SELECT 
    item_id,
    date,
    latest_price_high,
    latest_price_low,
    
    -- Use midpoint of high/low as representative price (cast to DOUBLE to prevent INT32 overflow)
    (CAST(latest_price_high AS DOUBLE) + CAST(latest_price_low AS DOUBLE)) / 2.0 AS latest_price,
    
    avg_price_high,
    avg_price_low,
    min_price,
    max_price,
    total_volume,
    observation_count,
    
    -- 7-day moving average of latest price
    AVG((CAST(latest_price_high AS DOUBLE) + CAST(latest_price_low AS DOUBLE)) / 2.0) OVER (
      PARTITION BY item_id 
      ORDER BY date 
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS price_7d_ma,
    
    -- Daily return (percentage change from previous day)
    ((CAST(latest_price_high AS DOUBLE) + CAST(latest_price_low AS DOUBLE)) / 2.0) / 
    LAG((CAST(latest_price_high AS DOUBLE) + CAST(latest_price_low AS DOUBLE)) / 2.0) OVER (
      PARTITION BY item_id ORDER BY date
    ) - 1.0 AS daily_return,
    
    -- Simple volatility proxy: (high - low) / midpoint
    CASE 
      WHEN (CAST(latest_price_high AS DOUBLE) + CAST(latest_price_low AS DOUBLE)) > 0
      THEN (CAST(latest_price_high AS DOUBLE) - CAST(latest_price_low AS DOUBLE)) / ((CAST(latest_price_high AS DOUBLE) + CAST(latest_price_low AS DOUBLE)) / 2.0)
      ELSE NULL
    END AS volatility_proxy
    
  FROM daily_prices
),

with_outlier_detection AS (
  SELECT *,
    
    -- Z-score based outlier detection for daily returns
    CASE 
      WHEN daily_return IS NOT NULL
      THEN ABS(
        (daily_return - AVG(daily_return) OVER (PARTITION BY item_id)) /
        NULLIF(STDDEV(daily_return) OVER (PARTITION BY item_id), 0)
      )
      ELSE NULL
    END AS return_zscore,
    
    -- Outlier flag: |z-score| > 2.5 or volatility > 50%
    CASE 
      WHEN (
        ABS(
          (daily_return - AVG(daily_return) OVER (PARTITION BY item_id)) /
          NULLIF(STDDEV(daily_return) OVER (PARTITION BY item_id), 0)
        ) > 2.5
      ) OR (volatility_proxy > 0.5)
      THEN TRUE
      ELSE FALSE
    END AS is_outlier
    
  FROM with_features
)

SELECT 
  item_id,
  date,
  latest_price,
  latest_price_high,
  latest_price_low,
  price_7d_ma,
  daily_return,
  volatility_proxy,
  is_outlier,
  total_volume,
  observation_count,
  min_price,
  max_price,
  CURRENT_TIMESTAMP AS dbt_updated_at
FROM with_outlier_detection
ORDER BY item_id, date
