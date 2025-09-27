{{
  config(
    materialized='table',
    description='Raw OSRS timeseries price data from the Wiki API'
  )
}}

-- Bronze layer: Raw timeseries data with basic cleaning
SELECT 
    timestamp,
    item_id,
    avgHighPrice as avg_high_price,
    avgLowPrice as avg_low_price,
    highPriceVolume as high_price_volume,
    lowPriceVolume as low_price_volume,
    ingested_at,
    CURRENT_TIMESTAMP as dbt_updated_at
FROM {{ source('bronze', 'timeseries_raw') }}
WHERE timestamp IS NOT NULL
  AND item_id IS NOT NULL
