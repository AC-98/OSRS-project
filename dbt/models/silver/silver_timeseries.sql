{{
  config(
    materialized='incremental',
    unique_key=['item_id', 'ts'],
    on_schema_change='fail',
    description='Normalized OSRS timeseries data with canonical column names'
  )
}}

WITH bronze_data AS (
  SELECT 
    item_id,
    timestamp AS ts,
    avgHighPrice,
    avgLowPrice,
    highPriceVolume,
    lowPriceVolume,
    ingested_at AS load_ts
  FROM {{ source('bronze', 'timeseries_raw') }}
  
  {% if is_incremental() %}
    -- Only process new data in incremental mode
    WHERE ingested_at > (SELECT MAX(load_ts) FROM {{ this }})
  {% endif %}
),

normalized AS (
  SELECT 
    item_id,
    ts,
    
    -- Price fields: use avgHighPrice/avgLowPrice as primary, fallback to high/low
    CASE 
      WHEN avgHighPrice IS NOT NULL AND avgHighPrice >= 0 THEN avgHighPrice
      ELSE NULL
    END AS price_avg_high,
    
    CASE 
      WHEN avgLowPrice IS NOT NULL AND avgLowPrice >= 0 THEN avgLowPrice  
      ELSE NULL
    END AS price_avg_low,
    
    -- For now, high/low prices are same as avg (API only returns avg prices)
    CASE 
      WHEN avgHighPrice IS NOT NULL AND avgHighPrice >= 0 THEN avgHighPrice
      ELSE NULL
    END AS price_high,
    
    CASE 
      WHEN avgLowPrice IS NOT NULL AND avgLowPrice >= 0 THEN avgLowPrice
      ELSE NULL
    END AS price_low,
    
    -- Volume: sum of high and low price volumes
    CASE 
      WHEN (highPriceVolume IS NOT NULL AND highPriceVolume >= 0) 
        OR (lowPriceVolume IS NOT NULL AND lowPriceVolume >= 0)
      THEN COALESCE(highPriceVolume, 0) + COALESCE(lowPriceVolume, 0)
      ELSE NULL
    END AS volume,
    
    'osrs_wiki_api' AS source,
    load_ts
    
  FROM bronze_data
),

-- Deduplicate on (item_id, ts) - keep most recent load_ts
deduplicated AS (
  SELECT *
  FROM normalized
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY item_id, ts 
    ORDER BY load_ts DESC
  ) = 1
)

SELECT 
  item_id,
  ts,
  price_high,
  price_low, 
  price_avg_high,
  price_avg_low,
  volume,
  source,
  load_ts
FROM deduplicated
ORDER BY item_id, ts
