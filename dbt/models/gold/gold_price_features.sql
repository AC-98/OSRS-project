{{
  config(
    materialized='table',
    description='Feature engineering table for ML model training and prediction'
  )
}}

-- Gold layer: ML-ready features for price forecasting
WITH feature_base AS (
    SELECT 
        price_date,
        item_id,
        close_price,
        total_volume,
        
        -- Lag features (previous days)
        LAG(close_price, 1) OVER w as price_lag_1d,
        LAG(close_price, 2) OVER w as price_lag_2d,
        LAG(close_price, 3) OVER w as price_lag_3d,
        LAG(close_price, 7) OVER w as price_lag_7d,
        
        -- Moving averages
        ma_7d,
        AVG(close_price) OVER (PARTITION BY item_id ORDER BY price_date ROWS 13 PRECEDING) as ma_14d,
        
        -- Price ratios
        close_price / NULLIF(ma_7d, 0) as price_to_ma7_ratio,
        
        -- Volume features
        LAG(total_volume, 1) OVER w as volume_lag_1d,
        AVG(total_volume) OVER (PARTITION BY item_id ORDER BY price_date ROWS 6 PRECEDING) as volume_ma_7d,
        
        -- Volatility features
        volatility_7d,
        volatility_7d / NULLIF(close_price, 0) * 100 as volatility_pct,
        
        -- Day of week (cyclical features)
        EXTRACT(DOW FROM price_date) as day_of_week,
        SIN(2 * PI() * EXTRACT(DOW FROM price_date) / 7) as day_of_week_sin,
        COS(2 * PI() * EXTRACT(DOW FROM price_date) / 7) as day_of_week_cos,
        
        -- Price momentum
        (close_price - LAG(close_price, 3) OVER w) / NULLIF(LAG(close_price, 3) OVER w, 0) as momentum_3d,
        
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM {{ ref('silver_prices_daily') }}
    WINDOW w AS (PARTITION BY item_id ORDER BY price_date)
),

with_derived_features AS (
    SELECT 
        *,
        
        -- Target variable (next day price for supervised learning)
        LEAD(close_price, 1) OVER (PARTITION BY item_id ORDER BY price_date) as target_price_1d,
        
        -- Feature interactions
        price_lag_1d * volume_lag_1d as price_volume_interaction,
        
        -- Relative strength vs moving average
        CASE 
            WHEN price_to_ma7_ratio > 1.05 THEN 'Strong_Up'
            WHEN price_to_ma7_ratio > 1.02 THEN 'Mild_Up'
            WHEN price_to_ma7_ratio < 0.95 THEN 'Strong_Down'
            WHEN price_to_ma7_ratio < 0.98 THEN 'Mild_Down'
            ELSE 'Neutral'
        END as trend_signal
        
    FROM feature_base
),

final AS (
    SELECT 
        price_date,
        item_id,
        close_price,
        target_price_1d,
        
        -- Core features for ML
        price_lag_1d,
        price_lag_2d, 
        price_lag_3d,
        price_lag_7d,
        ma_7d,
        ma_14d,
        price_to_ma7_ratio,
        total_volume,
        volume_lag_1d,
        volume_ma_7d,
        volatility_7d,
        volatility_pct,
        momentum_3d,
        day_of_week_sin,
        day_of_week_cos,
        price_volume_interaction,
        trend_signal,
        
        -- Metadata
        dbt_updated_at
        
    FROM with_derived_features
    WHERE price_lag_1d IS NOT NULL  -- Ensure we have at least 1 lag
      AND target_price_1d IS NOT NULL  -- Ensure we have target for training
)

SELECT * FROM final
ORDER BY item_id, price_date
