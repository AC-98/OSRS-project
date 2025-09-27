{{
  config(
    materialized='table',
    description='Item-level trading metrics and statistics for ML features'
  )
}}

-- Gold layer: Item-level metrics for forecasting and API
WITH price_aggregates AS (
    SELECT 
        p.item_id,
        i.name,
        i.item_category,
        i.membership_type,
        
        -- Price statistics (last 30 days)
        COUNT(*) as trading_days_30d,
        AVG(p.close_price) as avg_price_30d,
        STDDEV(p.close_price) as price_volatility_30d,
        MIN(p.close_price) as min_price_30d,
        MAX(p.close_price) as max_price_30d,
        
        -- Volume statistics  
        AVG(p.total_volume) as avg_volume_30d,
        SUM(p.total_volume) as total_volume_30d
        
    FROM {{ ref('silver_prices_daily') }} p
    JOIN {{ ref('silver_items') }} i ON p.item_id = i.item_id
    WHERE p.price_date >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY p.item_id, i.name, i.item_category, i.membership_type
),

latest_prices AS (
    SELECT 
        p.item_id,
        p.close_price as latest_price,
        p.price_date as latest_date,
        ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.price_date DESC) as rn_latest
    FROM {{ ref('silver_prices_daily') }} p
    WHERE p.price_date >= CURRENT_DATE - INTERVAL 30 DAY
),

first_prices AS (
    SELECT 
        p.item_id,
        p.close_price as first_price,
        ROW_NUMBER() OVER (PARTITION BY p.item_id ORDER BY p.price_date ASC) as rn_first
    FROM {{ ref('silver_prices_daily') }} p
    WHERE p.price_date >= CURRENT_DATE - INTERVAL 30 DAY
),

item_stats AS (
    SELECT 
        a.*,
        l.latest_price,
        l.latest_date,
        
        -- Price trends
        (l.latest_price - f.first_price) / NULLIF(f.first_price, 0) * 100 as price_change_pct_30d,
        
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM price_aggregates a
    LEFT JOIN latest_prices l ON a.item_id = l.item_id AND l.rn_latest = 1
    LEFT JOIN first_prices f ON a.item_id = f.item_id AND f.rn_first = 1
),

final AS (
    SELECT 
        *,
        
        -- Data quality flags
        CASE 
            WHEN trading_days_30d >= 20 THEN 'High'
            WHEN trading_days_30d >= 10 THEN 'Medium' 
            ELSE 'Low'
        END as data_quality,
        
        -- Forecasting suitability
        CASE
            WHEN trading_days_30d >= 15 AND avg_volume_30d > 100 THEN true
            ELSE false
        END as suitable_for_forecasting
        
    FROM item_stats
)

SELECT * FROM final
ORDER BY total_volume_30d DESC
