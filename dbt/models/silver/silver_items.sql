{{
  config(
    materialized='table',
    description='Cleaned and enriched item metadata'
  )
}}

-- Silver layer: Enhanced item data with additional attributes
SELECT 
    id as item_id,
    name,
    examine,
    members,
    lowalch as low_alch_value,
    highalch as high_alch_value,
    "limit" as trade_limit,
    
    -- Derived attributes
    CASE 
        WHEN members = true THEN 'Members'
        ELSE 'Free-to-Play'
    END as membership_type,
    
    CASE
        WHEN highalch > 0 THEN highalch - lowalch
        ELSE NULL
    END as alch_profit_potential,
    
    -- Item category inference (basic)
    CASE
        WHEN name ILIKE '%rune%' THEN 'Runes'
        WHEN name ILIKE '%log%' THEN 'Logs'
        WHEN name ILIKE '%ore%' THEN 'Ores'
        WHEN name ILIKE '%bar%' THEN 'Bars'
        WHEN name ILIKE '%dragon%' OR name ILIKE '%whip%' THEN 'Weapons'
        WHEN name ILIKE '%helm%' OR name ILIKE '%tassets%' THEN 'Armor'
        ELSE 'Other'
    END as item_category,
    
    dbt_updated_at
    
FROM {{ ref('bronze_item_mapping') }}
