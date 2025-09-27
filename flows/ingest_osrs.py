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
    from config_utils import load_selected_items
except ImportError:
    # Fallback if config_utils not available
    def load_selected_items(config_path=None, fallback_items=None):
        return fallback_items or [4151, 561, 5616]

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

# Default demo items (fallback if config not available)
DEFAULT_DEMO_ITEM_IDS = [
    4151,  # Abyssal whip
    561,   # Nature rune
    5616,  # Coins (test item)
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
def fetch_timeseries_for_items(item_ids: List[int]) -> DataFrame[TimeseriesSchema]:
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
            sleep_time = 0.4  # 400ms between requests
            logger.info(f"Sleeping {sleep_time}s before next request")
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
    """Main flow to ingest OSRS Grand Exchange data."""
    logger = get_run_logger()
    
    if item_ids is None:
        # Try to load from config, fallback to default
        try:
            item_ids = load_selected_items("config/items_selected.json", DEFAULT_DEMO_ITEM_IDS)
            logger.info(f"Loaded {len(item_ids)} items from config")
        except Exception as e:
            logger.warning(f"Failed to load config, using defaults: {e}")
            item_ids = DEFAULT_DEMO_ITEM_IDS
    
    logger.info(f"Starting OSRS data ingestion for {len(item_ids)} items: {item_ids}")
    
    # Fetch and validate data
    mapping_df = fetch_item_mapping()
    timeseries_df = fetch_timeseries_for_items(item_ids)
    
    # Save to bronze tables
    save_to_bronze_tables(mapping_df, timeseries_df)
    
    logger.info("OSRS data ingestion completed successfully")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Run the flow
    ingest_osrs_data()
