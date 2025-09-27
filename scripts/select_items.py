#!/usr/bin/env python3
"""
OSRS Signals - Item Selection Script

Automatically selects items for forecasting based on trading volume and data continuity.
Reads criteria from config/items.yaml and outputs to config/items_selected.json.

Usage:
    python scripts/select_items.py
    python scripts/select_items.py --config config/items.yaml --output config/items_selected.json
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import yaml

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "warehouse/osrs.duckdb")
DEFAULT_CONFIG_PATH = "config/items.yaml"
DEFAULT_OUTPUT_PATH = "config/items_selected.json"


def load_config(config_path: str) -> Dict:
    """Load item selection configuration from YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
    
    except FileNotFoundError:
        logger.warning(f"Config file {config_path} not found, using defaults")
        return {
            "auto_select": {
                "top_n": 10,
                "min_days": 180,
                "min_volume": 1000000,
                "lookback_days": 365
            },
            "manual_include": [],
            "manual_exclude": []
        }
    
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise


def get_item_statistics(lookback_days: int = 365) -> pd.DataFrame:
    """
    Get trading statistics for all items from bronze/silver tables.
    
    Args:
        lookback_days: Number of days to look back for analysis
        
    Returns:
        DataFrame with item statistics
    """
    logger.info(f"Analyzing item statistics over last {lookback_days} days")
    
    try:
        with duckdb.connect(DUCKDB_PATH) as conn:
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=lookback_days)
            
            # Query to get comprehensive item statistics
            query = """
            WITH item_daily_stats AS (
                -- Get daily statistics per item from silver timeseries
                SELECT 
                    st.item_id,
                    CAST(st.ts AS DATE) as trade_date,
                    SUM(st.volume) as daily_volume,
                    COUNT(*) as daily_observations,
                    AVG((st.price_avg_high + st.price_avg_low) / 2) as avg_price
                FROM main_silver.silver_timeseries st
                WHERE st.ts >= ?
                  AND st.volume > 0
                GROUP BY st.item_id, CAST(st.ts AS DATE)
            ),
            
            item_aggregates AS (
                -- Aggregate statistics per item
                SELECT 
                    ids.item_id,
                    im.name,
                    
                    -- Volume metrics (in units, not GP)
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ids.daily_volume) as median_units_per_day,
                    AVG(ids.daily_volume) as avg_units_per_day,
                    SUM(ids.daily_volume) as total_units,
                    
                    -- Coverage metrics
                    COUNT(DISTINCT ids.trade_date) as days_with_data,
                    ? as total_possible_days,
                    COUNT(DISTINCT ids.trade_date) * 100.0 / ? as coverage_pct,
                    
                    -- Price metrics
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ids.avg_price) as median_price,
                    AVG(ids.avg_price) as avg_price,
                    STDDEV(ids.avg_price) as price_volatility,
                    
                    -- Data quality
                    AVG(ids.daily_observations) as avg_obs_per_day,
                    
                    -- Latest data
                    MAX(ids.trade_date) as last_trade_date
                    
                FROM item_daily_stats ids
                JOIN main_bronze.bronze_item_mapping im ON ids.item_id = im.id
                GROUP BY ids.item_id, im.name
            )
            
            SELECT 
                item_id,
                name,
                median_units_per_day,
                avg_units_per_day,
                total_units,
                median_price,
                days_with_data,
                total_possible_days,
                coverage_pct,
                avg_price,
                price_volatility,
                avg_obs_per_day,
                last_trade_date,
                
                -- Calculate turnover (units × price)
                (median_units_per_day * median_price) as median_turnover_gp,
                
                -- Ranking score: combine volume and coverage
                (LOG(median_units_per_day + 1) * coverage_pct / 100.0) as ranking_score
                
            FROM item_aggregates
            WHERE days_with_data >= 5  -- Minimum data requirement for demo
            ORDER BY ranking_score DESC
            """
            
            df = conn.execute(query, [cutoff_date, lookback_days, lookback_days]).df()
            
            logger.info(f"Found {len(df)} items with trading data")
            return df
            
    except Exception as e:
        logger.error(f"Failed to get item statistics: {e}")
        raise


def select_items(
    stats_df: pd.DataFrame,
    config: Dict,
    manual_include: List[int] = None,
    manual_exclude: List[int] = None
) -> List[Dict]:
    """
    Select items based on configuration criteria and manual overrides.
    
    Args:
        stats_df: DataFrame with item statistics
        config: Configuration dictionary
        manual_include: List of item IDs to always include
        manual_exclude: List of item IDs to always exclude
        
    Returns:
        List of selected items with metadata
    """
    auto_config = config.get("auto_select", {})
    top_n = auto_config.get("top_n", 10)
    min_days = auto_config.get("min_days", 180)
    min_volume = auto_config.get("min_volume", 1000000)
    
    manual_include = manual_include or config.get("manual_include", [])
    manual_exclude = manual_exclude or config.get("manual_exclude", [])
    
    logger.info(f"Selection criteria: top_n={top_n}, min_days={min_days}, min_volume={min_volume}")
    logger.info(f"Manual overrides: include={manual_include}, exclude={manual_exclude}")
    
    # Filter by minimum criteria
    filtered_df = stats_df[
        (stats_df['days_with_data'] >= min_days) &
        (stats_df['median_units_per_day'] >= min_volume)
    ].copy()
    
    logger.info(f"Items meeting minimum criteria: {len(filtered_df)}")
    
    # Apply manual exclusions
    if manual_exclude:
        filtered_df = filtered_df[~filtered_df['item_id'].isin(manual_exclude)]
        logger.info(f"After exclusions: {len(filtered_df)} items")
    
    # Get top N by ranking score
    auto_selected = filtered_df.head(top_n)
    
    # Add manual inclusions (if they meet minimum days requirement)
    manual_items = pd.DataFrame()
    if manual_include:
        manual_items = stats_df[
            (stats_df['item_id'].isin(manual_include)) &
            (stats_df['days_with_data'] >= min_days)
        ]
        logger.info(f"Manual inclusions meeting criteria: {len(manual_items)}")
    
    # Combine and deduplicate
    selected_df = pd.concat([auto_selected, manual_items]).drop_duplicates(subset=['item_id'])
    
    # Convert to list of dictionaries
    selected_items = []
    for _, row in selected_df.iterrows():
        item = {
            "id": int(row['item_id']),
            "name": str(row['name']),
            "median_units_per_day": float(row['median_units_per_day']),
            "median_price": float(row['median_price']) if pd.notna(row['median_price']) else None,
            "median_turnover_gp": float(row['median_turnover_gp']) if pd.notna(row['median_turnover_gp']) else None,
            "coverage": float(row['coverage_pct']),
            "days_with_data": int(row['days_with_data']),
            "avg_price": float(row['avg_price']) if pd.notna(row['avg_price']) else None,
            "ranking_score": float(row['ranking_score']),
            "last_trade_date": str(row['last_trade_date']),
            "selection_reason": "manual_include" if int(row['item_id']) in manual_include else "auto_selected"
        }
        selected_items.append(item)
    
    # Sort by ranking score
    selected_items.sort(key=lambda x: x['ranking_score'], reverse=True)
    
    logger.info(f"Final selection: {len(selected_items)} items")
    return selected_items


def save_selected_items(items: List[Dict], output_path: str) -> None:
    """Save selected items to JSON file."""
    try:
        # Create output directory if needed
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Add metadata
        output_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "total_items": len(items),
                "selection_criteria": "median_daily_volume + coverage_pct",
                "version": "1.0"
            },
            "items": items
        }
        
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2, default=str)
        
        logger.info(f"Saved {len(items)} selected items to {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to save selected items: {e}")
        raise


def print_selection_summary(items: List[Dict]) -> None:
    """Print a summary of the selected items."""
    print("\n" + "="*80)
    print("ITEM SELECTION SUMMARY")
    print("="*80)
    
    if not items:
        print("No items selected!")
        return
    
    print(f"Total selected items: {len(items)}")
    print()
    
    # Summary statistics
    units = [item['median_units_per_day'] for item in items]
    turnovers = [item['median_turnover_gp'] for item in items if item['median_turnover_gp'] is not None]
    coverages = [item['coverage'] for item in items]
    
    print(f"Units/day range: {min(units):,.0f} - {max(units):,.0f} units")
    if turnovers:
        print(f"Turnover range: {min(turnovers):,.0f} - {max(turnovers):,.0f} gp/day")
    print(f"Coverage range: {min(coverages):.1f}% - {max(coverages):.1f}%")
    print()
    
    # Top items
    print("Selected Items:")
    print("-" * 80)
    print(f"{'Rank':<4} {'ID':<6} {'Name':<25} {'Units/Day':<12} {'Turnover/Day':<13} {'Coverage':<10} {'Reason':<12}")
    print("-" * 80)
    
    for i, item in enumerate(items[:15], 1):  # Show top 15
        turnover_str = f"{item['median_turnover_gp']:>12,.0f}" if item['median_turnover_gp'] else "N/A".rjust(12)
        print(f"{i:<4} {item['id']:<6} {item['name'][:24]:<25} "
              f"{item['median_units_per_day']:>11,.0f} {turnover_str} "
              f"{item['coverage']:>8.1f}% {item['selection_reason']:<12}")
    
    if len(items) > 15:
        print(f"... and {len(items) - 15} more items")
    
    print()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Select OSRS items for forecasting based on volume and continuity",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/select_items.py
  python scripts/select_items.py --config config/items.yaml
  python scripts/select_items.py --output config/custom_items.json
        """
    )
    
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Configuration file path (default: {DEFAULT_CONFIG_PATH})"
    )
    
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output JSON file path (default: {DEFAULT_OUTPUT_PATH})"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show selection results without saving"
    )
    
    args = parser.parse_args()
    
    logger.info("Starting OSRS item selection process")
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Get item statistics
        stats_df = get_item_statistics(
            lookback_days=config["auto_select"].get("lookback_days", 365)
        )
        
        if stats_df.empty:
            logger.error("No item statistics found. Please run data ingestion first.")
            sys.exit(1)
        
        # Select items
        selected_items = select_items(stats_df, config)
        
        if len(selected_items) < 5:
            logger.warning(f"Only {len(selected_items)} items selected. Consider relaxing criteria.")
        
        # Print summary
        print_selection_summary(selected_items)
        
        # Save results
        if not args.dry_run:
            save_selected_items(selected_items, args.output)
            print(f"\n✓ Results saved to: {args.output}")
        else:
            print(f"\n[DRY RUN] Would save to: {args.output}")
        
        logger.info("Item selection completed successfully")
        
    except Exception as e:
        logger.error(f"Item selection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
