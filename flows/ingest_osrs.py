"""
OSRS Grand Exchange data ingestion flow.

Fetches item mapping and timeseries data from OSRS Wiki API,
validates with Pandera schemas, and loads into DuckDB bronze tables.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import requests
import pandera as pa
import pandas as pd
from pandera.typing import DataFrame
from prefect import flow, task
from prefect.logging import get_run_logger
from pydantic import BaseModel, Field

# Import config utilities
import sys
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))
try:
    from config_utils import (
        load_selected_items,
        load_ingestion_config,
        resolve_names_to_ids_from_mapping,
        compute_auto_discover_candidates
    )
except ImportError:
    # Fallback if config_utils not available
    def load_selected_items(config_path=None, fallback_items=None):
        return fallback_items or [4151, 561, 5616]
    def load_ingestion_config(config_path=None):
        return {
            'ingest_targets': {'include_ids': [], 'include_names': [], 'exclude_ids': [], 'exclude_names': [], 'auto_discover': {'enabled': False, 'top_k': 10, 'min_limit': 1000, 'members_only': None}},
            'runtime_caps': {'max_items_per_run': 25, 'request_sleep_ms': 450}
        }
    def resolve_names_to_ids_from_mapping(names, mapping_df):
        return []
    def compute_auto_discover_candidates(mapping_df, min_limit, members_only, top_k):
        return []

# Environment setup
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass  # Continue without .env file

# Configuration
OSRS_USER_AGENT = os.getenv("OSRS_USER_AGENT", "AC-OSRS-Signals (mailto:vmanahmet@live.dk)")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "warehouse/osrs.duckdb")
TIMESTEP = os.getenv("TIMESTEP", "1h")
RAW_DATA_PATH = Path("warehouse/raw")
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

# Emergency seed (used only if all config sources are empty)
# Minimal set to ensure ingestion never completely fails
EMERGENCY_SEED_ITEMS = [
    561,    # Nature rune (high volume)
    536,    # Dragon bones (stable prices)
    12934,  # Zulrah's scales (endgame content)
]


class ItemMappingSchema(pa.DataFrameModel):
    """Schema for OSRS item mapping data."""
    
    id: pa.typing.Series[int] = pa.Field(ge=1)
    name: pa.typing.Series[str]
    examine: pa.typing.Series[str] = pa.Field(nullable=True)
    members: pa.typing.Series[bool] = pa.Field(nullable=True)
    lowalch: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    highalch: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    limit: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    
    class Config:
        strict = False  # Allow extra columns
        coerce = True


class TimeseriesSchema(pa.DataFrameModel):
    """Schema for OSRS timeseries price data (flexible for unknown response format)."""
    
    timestamp: pa.typing.Series[int] = pa.Field(ge=0)  # Unix timestamp as integer
    item_id: pa.typing.Series[int] = pa.Field(ge=1)
    
    # Price fields - at least one should be present, all nullable
    avgHighPrice: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    avgLowPrice: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    high: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    low: pa.typing.Series[pd.Int64Dtype] = pa.Field(ge=0, nullable=True)
    
    class Config:
        strict = False  # Allow extra columns for unknown response format
        coerce = True


@task(retries=3, retry_delay_seconds=5)
def fetch_with_backoff(
    url: str, 
    headers: Dict[str, str], 
    timeout: int = 20
) -> Dict[str, Any]:
    """Fetch URL with exponential backoff and caching."""
    logger = get_run_logger()
    
    # Simple file-based cache
    cache_key = url.replace("/", "_").replace(":", "").replace("?", "_").replace("&", "_").replace("=", "_")
    cache_file = RAW_DATA_PATH / f"cache_{cache_key}.json"
    
    # Check cache (1 hour TTL)
    if cache_file.exists():
        cache_age = time.time() - cache_file.stat().st_mtime
        if cache_age < 3600:  # 1 hour
            logger.info(f"Using cached response for {url}")
            with open(cache_file, "r") as f:
                return json.load(f)
    
    # Make request with backoff
    for attempt in range(3):
        try:
            logger.info(f"Fetching {url} (attempt {attempt + 1}/3)")
            response = requests.get(url, headers=headers, timeout=timeout)
            
            # Detailed logging for non-200 responses
            if response.status_code != 200:
                logger.error(f"HTTP {response.status_code} for URL: {response.url}")
                logger.error(f"Response body: {response.text[:500]}")
                response.raise_for_status()
                
            data = response.json()
            
            # Cache successful response
            with open(cache_file, "w") as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Successfully fetched {url}")
            return data
                
        except requests.RequestException as e:
            wait_time = 2 ** attempt
            logger.warning(f"Request failed (attempt {attempt + 1}): {e}. Waiting {wait_time}s")
            if attempt < 2:  # Don't sleep on last attempt
                time.sleep(wait_time)
    
    raise Exception(f"Failed to fetch {url} after 3 attempts")


@task
def fetch_item_mapping() -> DataFrame[ItemMappingSchema]:
    """Fetch and validate OSRS item mapping data."""
    logger = get_run_logger()
    
    url = "https://prices.runescape.wiki/api/v1/osrs/mapping"
    headers = {"User-Agent": OSRS_USER_AGENT}
    
    raw_data = fetch_with_backoff(url, headers)
    
    # Convert to DataFrame
    df = pd.DataFrame(raw_data)
    
    # Validate schema
    try:
        validated_df = ItemMappingSchema.validate(df)
        logger.info(f"Validated {len(validated_df)} items in mapping")
        return validated_df
    except pa.errors.SchemaError as e:
        logger.error(f"Schema validation failed: {e}")
        raise


@task
def fetch_timeseries_for_items(item_ids: List[int], sleep_ms: int = 450) -> DataFrame[TimeseriesSchema]:
    """Fetch timeseries data for multiple items, one request per item."""
    logger = get_run_logger()
    
    all_records = []
    base_url = "https://prices.runescape.wiki/api/v1/osrs"
    path = "/timeseries"
    
    for i, item_id in enumerate(item_ids):
        logger.info(f"Fetching timeseries for item {item_id} ({i+1}/{len(item_ids)})")
        
        # Build URL with proper parameters
        params = {"timestep": TIMESTEP, "id": item_id}
        param_string = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{base_url}{path}?{param_string}"
        
        headers = {"User-Agent": OSRS_USER_AGENT}
        
        try:
            raw_data = fetch_with_backoff(url, headers)
            
            # Save raw response for inspection
            raw_file = RAW_DATA_PATH / f"timeseries_{item_id}_{TIMESTEP}.json"
            with open(raw_file, 'w') as f:
                json.dump(raw_data, f, indent=2)
            logger.info(f"Saved raw response to {raw_file}")
            
            # Process response data (flexible parsing)
            item_records = []
            if isinstance(raw_data, dict) and "data" in raw_data:
                data_content = raw_data["data"]
                
                if isinstance(data_content, list):
                    # Format: {"data": [{"timestamp": ..., "price": ...}, ...]}
                    for entry in data_content:
                        try:
                            record = {
                                "timestamp": entry.get("timestamp", 0),
                                "item_id": item_id,
                                "avgHighPrice": entry.get("avgHighPrice"),
                                "avgLowPrice": entry.get("avgLowPrice"),
                                "high": entry.get("high"),
                                "low": entry.get("low"),
                                "highPriceVolume": entry.get("highPriceVolume"),
                                "lowPriceVolume": entry.get("lowPriceVolume"),
                            }
                            item_records.append(record)
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Skipping invalid entry for item {item_id}: {e}")
                            
                elif isinstance(data_content, dict):
                    # Format: {"data": {timestamp: {price_fields}}}
                    for timestamp_str, price_data in data_content.items():
                        try:
                            timestamp = int(timestamp_str)
                            record = {
                                "timestamp": timestamp,
                                "item_id": item_id,
                                "avgHighPrice": price_data.get("avgHighPrice"),
                                "avgLowPrice": price_data.get("avgLowPrice"),
                                "high": price_data.get("high"),
                                "low": price_data.get("low"),
                            }
                            item_records.append(record)
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Skipping invalid timestamp {timestamp_str} for item {item_id}: {e}")
                            
            all_records.extend(item_records)
            logger.info(f"Processed {len(item_records)} records for item {item_id}")
            
        except Exception as e:
            logger.error(f"Failed to fetch timeseries for item {item_id}: {e}")
            continue  # Continue with other items
        
        # Rate limiting: sleep between requests
        if i < len(item_ids) - 1:  # Don't sleep after last request
            sleep_time = sleep_ms / 1000.0  # Convert ms to seconds
            logger.debug(f"Sleeping {sleep_time}s before next request")
            time.sleep(sleep_time)
    
    if not all_records:
        logger.warning("No timeseries data received for any items")
        return pd.DataFrame(columns=["timestamp", "item_id", "avgHighPrice", "avgLowPrice", "high", "low"])
    
    df = pd.DataFrame(all_records)
    logger.info(f"Collected {len(df)} total timeseries records for {len(item_ids)} items")
    
    # Convert Unix timestamp to datetime for DuckDB compatibility
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    logger.info("Converted timestamps to datetime format")
    
    # Try validation, but continue if it fails (skip timestamp validation for now)
    try:
        # Create a copy for validation without timestamp conversion
        validation_df = df.copy()
        validation_df['timestamp'] = df['timestamp'].astype('int64') // 10**9  # Convert back to Unix for validation
        validated_df = TimeseriesSchema.validate(validation_df)
        logger.info(f"Schema validation passed")
        return df  # Return the datetime version
    except pa.errors.SchemaError as e:
        logger.warning(f"Schema validation failed: {e}")
        logger.info("Continuing with unvalidated data for now")
        return df


@task
def save_to_bronze_tables(
    mapping_df: DataFrame[ItemMappingSchema],
    timeseries_df: DataFrame[TimeseriesSchema]
) -> None:
    """Save validated data to DuckDB bronze tables."""
    logger = get_run_logger()
    
    # Ensure warehouse directory exists
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    with duckdb.connect(DUCKDB_PATH) as conn:
        # Create bronze schema if not exists
        conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        
        # Create or replace item mapping table
        conn.execute("""
            CREATE OR REPLACE TABLE bronze.item_mapping AS 
            SELECT * FROM mapping_df
        """)
        
        # Create timeseries table with append-only behavior
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze.timeseries_raw (
                timestamp TIMESTAMP,
                avgHighPrice INTEGER,
                avgLowPrice INTEGER,
                highPriceVolume INTEGER,
                lowPriceVolume INTEGER,
                item_id INTEGER,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert new timeseries data (avoiding duplicates)
        if not timeseries_df.empty:
            conn.execute("""
                INSERT INTO bronze.timeseries_raw 
                (timestamp, avgHighPrice, avgLowPrice, highPriceVolume, lowPriceVolume, item_id)
                SELECT timestamp, avgHighPrice, avgLowPrice, highPriceVolume, lowPriceVolume, item_id
                FROM timeseries_df
                WHERE NOT EXISTS (
                    SELECT 1 FROM bronze.timeseries_raw existing
                    WHERE existing.timestamp = timeseries_df.timestamp 
                    AND existing.item_id = timeseries_df.item_id
                )
            """)
        
        # Log results
        mapping_count = conn.execute("SELECT COUNT(*) FROM bronze.item_mapping").fetchone()[0]
        timeseries_count = conn.execute("SELECT COUNT(*) FROM bronze.timeseries_raw").fetchone()[0]
        
        logger.info(f"Bronze tables updated: {mapping_count} items, {timeseries_count} timeseries records")


@flow(name="ingest-osrs-data")
def ingest_osrs_data(item_ids: Optional[List[int]] = None) -> None:
    """
    Main flow to ingest OSRS Grand Exchange data.
    
    Uses config-driven approach:
    1. Fetch item mapping from API
    2. Load curated items from items_selected.json (if exists)
    3. Load ingestion targets from items.yaml
    4. Resolve names to IDs
    5. Auto-discover liquid items (if enabled)
    6. Build final target list with exclusions
    7. Apply per-run cap
    8. Fetch timeseries data
    """
    logger = get_run_logger()
    
    # Step 1: Fetch mapping (needed for name resolution and auto-discovery)
    logger.info("Step 1: Fetching item mapping from OSRS Wiki API")
    mapping_df = fetch_item_mapping()
    
    # If item_ids explicitly provided, use them directly
    if item_ids is not None:
        logger.info(f"Using explicitly provided item IDs: {item_ids}")
        target_ids = item_ids
        sleep_ms = 450  # Default sleep
    else:
        # Step 2: Load config-driven targets
        logger.info("Step 2: Loading config-driven ingestion targets")
        
        # Load selected items (curated by selection pipeline)
        selected_ids = []
        try:
            selected_ids = load_selected_items("config/items_selected.json", fallback_items=[])
            logger.info(f"  • Selected items: {len(selected_ids)} items")
        except Exception as e:
            logger.warning(f"Could not load selected items: {e}")
        
        # Load ingestion config
        try:
            config = load_ingestion_config("config/items.yaml")
            ingest_targets = config['ingest_targets']
            runtime_caps = config['runtime_caps']
            sleep_ms = runtime_caps.get('request_sleep_ms', 450)
            max_items = runtime_caps.get('max_items_per_run', 25)
        except Exception as e:
            logger.error(f"Failed to load ingestion config: {e}")
            ingest_targets = {
                'include_ids': [],
                'include_names': [],
                'exclude_ids': [],
                'exclude_names': [],
                'auto_discover': {'enabled': False}
            }
            sleep_ms = 450
            max_items = 25
        
        # Step 3: Resolve include/exclude names to IDs
        logger.info("Step 3: Resolving item names to IDs")
        include_ids_from_names = resolve_names_to_ids_from_mapping(
            ingest_targets.get('include_names', []),
            mapping_df
        )
        exclude_ids_from_names = resolve_names_to_ids_from_mapping(
            ingest_targets.get('exclude_names', []),
            mapping_df
        )
        
        # Step 4: Auto-discover liquid items (if enabled)
        discover_ids = []
        auto_discover = ingest_targets.get('auto_discover', {})
        if auto_discover.get('enabled', False):
            logger.info("Step 4: Auto-discovering liquid items")
            discover_ids = compute_auto_discover_candidates(
                mapping_df,
                min_limit=auto_discover.get('min_limit', 1000),
                members_only=auto_discover.get('members_only'),
                top_k=auto_discover.get('top_k', 10)
            )
            logger.info(f"  • Auto-discovered: {len(discover_ids)} items")
        else:
            logger.info("Step 4: Auto-discovery disabled")
        
        # Step 5: Build final target list (union of sources, then subtract excludes)
        logger.info("Step 5: Building final target list")
        
        # Union all include sources
        include_sources = []
        if selected_ids:
            include_sources.append(('selected', selected_ids))
        if ingest_targets.get('include_ids'):
            include_sources.append(('include_ids', ingest_targets['include_ids']))
        if include_ids_from_names:
            include_sources.append(('include_names', include_ids_from_names))
        if discover_ids:
            include_sources.append(('auto_discover', discover_ids))
        
        # Combine with priority: selected > include_ids > include_names > discover
        # This ensures stable ordering when applying the cap
        all_include_ids = []
        for source_name, source_ids in include_sources:
            all_include_ids.extend(source_ids)
            logger.info(f"  • {source_name}: {len(source_ids)} items")
        
        # Remove duplicates while preserving order (selected items come first)
        seen = set()
        target_ids = []
        for item_id in all_include_ids:
            if item_id not in seen:
                seen.add(item_id)
                target_ids.append(item_id)
        
        # Apply excludes
        exclude_ids = set(ingest_targets.get('exclude_ids', []) + exclude_ids_from_names)
        if exclude_ids:
            logger.info(f"  • Excluding {len(exclude_ids)} items: {list(exclude_ids)[:5]}...")
            target_ids = [item_id for item_id in target_ids if item_id not in exclude_ids]
        
        # Step 6: Apply per-run cap
        logger.info(f"Step 6: Applying per-run cap (max={max_items})")
        if len(target_ids) > max_items:
            logger.info(f"  • Capping from {len(target_ids)} to {max_items} items")
            target_ids = target_ids[:max_items]
        
        # Step 7: Emergency seed if target list is empty
        if not target_ids:
            logger.warning(
                "=" * 80 + "\n"
                "WARNING: No items to ingest from any config source!\n"
                "Using emergency seed items to prevent complete failure.\n"
                "Please configure items in config/items.yaml or run item selection.\n"
                + "=" * 80
            )
            target_ids = EMERGENCY_SEED_ITEMS
        
        # Log summary
        logger.info("\n" + "=" * 80)
        logger.info("INGESTION TARGET SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total items to fetch: {len(target_ids)}")
        logger.info(f"First 10 IDs: {target_ids[:10]}")
        if len(target_ids) > 10:
            logger.info(f"... and {len(target_ids) - 10} more")
        logger.info("=" * 80 + "\n")
    
    # Step 8: Fetch timeseries data
    logger.info(f"Step 8: Fetching timeseries data for {len(target_ids)} items")
    timeseries_df = fetch_timeseries_for_items(target_ids, sleep_ms=sleep_ms)
    
    # Step 9: Save to bronze tables
    logger.info("Step 9: Saving to bronze tables")
    save_to_bronze_tables(mapping_df, timeseries_df)
    
    logger.info("✓ OSRS data ingestion completed successfully")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run the flow
    ingest_osrs_data()
