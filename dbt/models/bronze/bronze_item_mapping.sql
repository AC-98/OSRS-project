{{
  config(
    materialized='table',
    description='Raw OSRS item mapping data from the Wiki API'
  )
}}

-- Bronze layer: Raw item mapping data
SELECT 
    id,
    name,
    examine,
    members,
    lowalch,
    highalch,
    "limit",
    CURRENT_TIMESTAMP as dbt_updated_at
FROM {{ source('bronze', 'item_mapping') }}
