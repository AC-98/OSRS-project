{{
  config(
    materialized='table',
    description='Daily aggregated price data with technical indicators'
  )
}}

-- Silver layer: Daily price aggregations with technical indicators
WITH daily_aggregates AS (
    SELECT 
        CAST(timestamp AS DATE) as price_date,
        item_id,
        
        -- Aggregate prices
        MAX(avgHighPrice) as high_price,
        MIN(avgLowPrice) as low_price,
        AVG(avgHighPrice) as avg_price,
        
        -- Volume metrics
        SUM(COALESCE(highPriceVolume, 0) + COALESCE(lowPriceVolume, 0)) as total_volume,
        
        -- Data quality
        COUNT(*) as data_points,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp
        
    FROM {{ source('bronze', 'timeseries_raw') }}
    WHERE avgHighPrice IS NOT NULL 
      AND avgLowPrice IS NOT NULL
    GROUP BY CAST(timestamp AS DATE), item_id
),

daily_ohlc AS (
    SELECT 
        item_id,
        CAST(timestamp AS DATE) as price_date,
        avgHighPrice,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, CAST(timestamp AS DATE) 
            ORDER BY timestamp ASC
        ) as rn_first,
        ROW_NUMBER() OVER (
            PARTITION BY item_id, CAST(timestamp AS DATE) 
            ORDER BY timestamp DESC
        ) as rn_last
    FROM {{ source('bronze', 'timeseries_raw') }}
    WHERE avgHighPrice IS NOT NULL
),

daily_prices AS (
    SELECT 
        a.*,
        o_first.avgHighPrice as open_price,
        o_last.avgHighPrice as close_price
    FROM daily_aggregates a
    LEFT JOIN daily_ohlc o_first ON a.item_id = o_first.item_id 
        AND a.price_date = o_first.price_date 
        AND o_first.rn_first = 1
    LEFT JOIN daily_ohlc o_last ON a.item_id = o_last.item_id 
        AND a.price_date = o_last.price_date 
        AND o_last.rn_last = 1
),

with_technical_indicators AS (
    SELECT 
        *,
        
        -- Price changes
        close_price - LAG(close_price, 1) OVER (
            PARTITION BY item_id 
            ORDER BY price_date
        ) as price_change,
        
        -- Moving averages (7-day)
        AVG(close_price) OVER (
            PARTITION BY item_id 
            ORDER BY price_date 
            ROWS 6 PRECEDING
        ) as ma_7d,
        
        -- Volatility (7-day rolling standard deviation)
        STDDEV(close_price) OVER (
            PARTITION BY item_id 
            ORDER BY price_date 
            ROWS 6 PRECEDING
        ) as volatility_7d,
        
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM daily_prices
)

SELECT * FROM with_technical_indicators
ORDER BY item_id, price_date
