#!/usr/bin/env python3
"""
OSRS Signals - Backtesting CLI

Run rolling backtests for OSRS price forecasting models.
Logs results to MLflow and persists summary metrics to DuckDB.

Usage:
    python scripts/run_backtest.py --items 4151 561 5616 --horizon 1 --window 28
    python scripts/run_backtest.py --help
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import mlflow
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from models_ml.backtest import BaselineModel, ETSModel, smape
from scripts.config_utils import load_selected_items, resolve_items_argument

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "warehouse/osrs.duckdb")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "file:./mlruns")

# Set MLflow tracking URI
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


def create_rolling_splits(
    df: pd.DataFrame,
    item_id: int,
    window_size: int = 28,
    horizon: int = 1,
    max_splits: int = 50
) -> List[Tuple[pd.DataFrame, pd.DataFrame, str]]:
    """
    Create rolling walk-forward validation splits.
    
    Args:
        df: DataFrame with price data
        item_id: Item ID to filter for
        window_size: Training window size (days)
        horizon: Forecast horizon (days)
        max_splits: Maximum number of splits to create
        
    Returns:
        List of (train, test, forecast_date) tuples
    """
    item_data = df[df['item_id'] == item_id].sort_values('date').reset_index(drop=True)
    
    if len(item_data) < window_size + horizon:
        logger.warning(f"Insufficient data for item {item_id}: {len(item_data)} points")
        return []
    
    splits = []
    
    # Create rolling splits
    for i in range(window_size, len(item_data) - horizon + 1):
        train_data = item_data.iloc[i-window_size:i]
        test_data = item_data.iloc[i:i + horizon]
        forecast_date = test_data.iloc[0]['date']
        
        splits.append((train_data, test_data, str(forecast_date)))
        
        if len(splits) >= max_splits:
            break
    
    logger.info(f"Created {len(splits)} rolling splits for item {item_id} (window={window_size}, horizon={horizon})")
    return splits


def run_method_backtest(
    item_id: int,
    method: str,
    window_size: int = 28,
    horizon: int = 1,
    experiment_name: str = "osrs_rolling_backtest"
) -> List[Dict[str, Any]]:
    """
    Run backtest for a specific method and item.
    
    Args:
        item_id: OSRS item ID
        method: Method name ('last_value', 'moving_average', 'ets')
        window_size: Training window size
        horizon: Forecast horizon
        experiment_name: MLflow experiment name
        
    Returns:
        List of results for each forecast date
    """
    logger.info(f"Running {method} backtest for item {item_id}")
    
    # Load data from DuckDB gold layer
    try:
        with duckdb.connect(DUCKDB_PATH) as conn:
            # Try gold_daily_features first, fallback to other tables
            queries = [
                """
                SELECT date, item_id, latest_price as price
                FROM main_gold.gold_daily_features 
                WHERE item_id = ?
                ORDER BY date
                """,
                """
                SELECT price_date as date, item_id, close_price as price
                FROM main_silver.silver_prices_daily 
                WHERE item_id = ?
                ORDER BY price_date
                """
            ]
            
            df = None
            for query in queries:
                try:
                    df = conn.execute(query, [item_id]).df()
                    if not df.empty:
                        break
                except Exception as e:
                    logger.debug(f"Query failed: {e}")
                    continue
                    
            if df is None or df.empty:
                logger.warning(f"No data found for item {item_id}")
                return []
                
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return []
    
    # Create rolling splits
    splits = create_rolling_splits(df, item_id, window_size, horizon)
    if not splits:
        return []
    
    # Initialize model
    if method == "last_value":
        model = BaselineModel("last_value")
    elif method == "moving_average":
        model = BaselineModel("moving_average")
    elif method == "ets":
        model = ETSModel(trend="add")
    else:
        raise ValueError(f"Unknown method: {method}")
    
    # Run backtest for each split
    results = []
    
    for train_data, test_data, forecast_date in splits:
        try:
            # Prepare data
            y_train = train_data['price']
            y_test = test_data['price'].values
            
            # Make prediction
            y_pred = model.fit_predict(y_train, steps=len(test_data))
            
            # Calculate metrics
            mae = mean_absolute_error(y_test, y_pred)
            smape_val = smape(y_test, y_pred)
            
            # Store result
            result = {
                'item_id': item_id,
                'date': forecast_date,
                'method': method,
                'mae': mae,
                'smape': smape_val,
                'actual': y_test[0] if len(y_test) > 0 else np.nan,
                'predicted': y_pred[0] if len(y_pred) > 0 else np.nan,
                'horizon': horizon,
                'window_size': window_size,
                'timestamp': datetime.now().isoformat()
            }
            
            results.append(result)
            
        except Exception as e:
            logger.warning(f"Prediction failed for {forecast_date}: {e}")
            continue
    
    logger.info(f"Completed {len(results)} forecasts for item {item_id} using {method}")
    return results


def log_results_to_mlflow(
    results: List[Dict[str, Any]], 
    experiment_name: str = "osrs_rolling_backtest"
) -> None:
    """Log backtest results to MLflow."""
    if not results:
        return
        
    try:
        mlflow.set_experiment(experiment_name)
        
        # Group results by method and item
        grouped = {}
        for result in results:
            key = (result['item_id'], result['method'])
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(result)
        
        # Log each method/item combination as a separate run
        for (item_id, method), method_results in grouped.items():
            run_name = f"{method}_item_{item_id}_{datetime.now().strftime('%Y%m%d_%H%M')}"
            
            with mlflow.start_run(run_name=run_name):
                # Log parameters
                mlflow.log_param("item_id", item_id)
                mlflow.log_param("method", method)
                mlflow.log_param("n_forecasts", len(method_results))
                mlflow.log_param("window_size", method_results[0]['window_size'])
                mlflow.log_param("horizon", method_results[0]['horizon'])
                
                # Calculate summary metrics
                mae_values = [r['mae'] for r in method_results if not np.isnan(r['mae'])]
                smape_values = [r['smape'] for r in method_results if not np.isnan(r['smape'])]
                
                if mae_values:
                    mlflow.log_metric("mean_mae", np.mean(mae_values))
                    mlflow.log_metric("median_mae", np.median(mae_values))
                    mlflow.log_metric("std_mae", np.std(mae_values))
                
                if smape_values:
                    mlflow.log_metric("mean_smape", np.mean(smape_values))
                    mlflow.log_metric("median_smape", np.median(smape_values))
                    mlflow.log_metric("std_smape", np.std(smape_values))
                
                # Save detailed results as artifact
                results_df = pd.DataFrame(method_results)
                results_df.to_csv("detailed_results.csv", index=False)
                mlflow.log_artifact("detailed_results.csv")
                
                # Clean up
                os.remove("detailed_results.csv")
                
        logger.info(f"Logged {len(grouped)} method/item combinations to MLflow")
        
    except Exception as e:
        logger.warning(f"Failed to log to MLflow: {e}")


def save_results_to_duckdb(results: List[Dict[str, Any]]) -> None:
    """Save backtest results to DuckDB gold layer."""
    if not results:
        return
        
    try:
        results_df = pd.DataFrame(results)
        
        with duckdb.connect(DUCKDB_PATH) as conn:
            # Create gold_backtest_metrics table if it doesn't exist
            conn.execute("""
                CREATE SCHEMA IF NOT EXISTS main_gold
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS main_gold.gold_backtest_metrics (
                    item_id INTEGER,
                    date DATE,
                    method VARCHAR,
                    mae DOUBLE,
                    smape DOUBLE,
                    actual DOUBLE,
                    predicted DOUBLE,
                    horizon INTEGER,
                    window_size INTEGER,
                    timestamp TIMESTAMP,
                    PRIMARY KEY (item_id, date, method)
                )
            """)
            
            # Insert results using REPLACE to handle duplicates
            conn.execute("""
                INSERT OR REPLACE INTO main_gold.gold_backtest_metrics 
                SELECT * FROM results_df
            """)
            
        logger.info(f"Saved {len(results_df)} backtest results to DuckDB")
        
    except Exception as e:
        logger.error(f"Failed to save results to DuckDB: {e}")


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="Run OSRS price forecasting backtests",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_backtest.py --items 4151 561 5616
  python scripts/run_backtest.py --items 4151 --horizon 3 --window 14
  python scripts/run_backtest.py --items 4151 --methods last_value moving_average
        """
    )
    
    parser.add_argument(
        "--items",
        type=str,
        default="@config/items_selected.json",
        help="Item IDs to backtest. Can be: @config/items_selected.json, '4151,561,5616', or space-separated integers"
    )
    
    parser.add_argument(
        "--horizon",
        type=int,
        default=1,
        help="Forecast horizon in days (default: 1)"
    )
    
    parser.add_argument(
        "--window",
        type=int,
        default=28,
        help="Training window size in days (default: 28)"
    )
    
    parser.add_argument(
        "--methods",
        nargs="+",
        default=["last_value", "moving_average", "ets"],
        choices=["last_value", "moving_average", "ets"],
        help="Methods to test (default: all)"
    )
    
    parser.add_argument(
        "--experiment",
        default="osrs_rolling_backtest",
        help="MLflow experiment name"
    )
    
    parser.add_argument(
        "--max-splits",
        type=int,
        default=50,
        help="Maximum number of rolling splits per item (default: 50)"
    )
    
    args = parser.parse_args()
    
    # Resolve items argument
    if isinstance(args.items, str):
        if args.items.startswith('@'):
            # Config file reference
            item_ids = load_selected_items(args.items[1:])
        elif ',' in args.items:
            # Comma-separated
            item_ids = [int(x.strip()) for x in args.items.split(',')]
        else:
            # Single item
            item_ids = [int(args.items)]
    else:
        # List of integers (shouldn't happen with current setup, but for safety)
        item_ids = args.items
    
    logger.info("=" * 60)
    logger.info("OSRS Signals - Backtesting CLI")
    logger.info("=" * 60)
    logger.info(f"Items: {item_ids}")
    logger.info(f"Methods: {args.methods}")
    logger.info(f"Window: {args.window} days")
    logger.info(f"Horizon: {args.horizon} days")
    logger.info(f"Experiment: {args.experiment}")
    logger.info("=" * 60)
    
    # Run backtests
    all_results = []
    
    for item_id in item_ids:
        for method in args.methods:
            logger.info(f"\nRunning {method} backtest for item {item_id}...")
            
            try:
                method_results = run_method_backtest(
                    item_id=item_id,
                    method=method,
                    window_size=args.window,
                    horizon=args.horizon,
                    experiment_name=args.experiment
                )
                
                all_results.extend(method_results)
                
                if method_results:
                    mae_values = [r['mae'] for r in method_results if not np.isnan(r['mae'])]
                    smape_values = [r['smape'] for r in method_results if not np.isnan(r['smape'])]
                    
                    if mae_values:
                        logger.info(f"  ✓ {len(method_results)} forecasts completed")
                        logger.info(f"    Mean MAE: {np.mean(mae_values):.2f}")
                        logger.info(f"    Mean sMAPE: {np.mean(smape_values):.2f}%")
                    else:
                        logger.warning(f"  ⚠ No valid forecasts for item {item_id} with {method}")
                else:
                    logger.warning(f"  ✗ No results for item {item_id} with {method}")
                    
            except Exception as e:
                logger.error(f"  ✗ Failed: {e}")
    
    # Log and save results
    if all_results:
        logger.info(f"\n{'='*60}")
        logger.info(f"BACKTEST SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total forecasts: {len(all_results)}")
        
        # Summary by method
        method_summary = {}
        for result in all_results:
            method = result['method']
            if method not in method_summary:
                method_summary[method] = {'mae': [], 'smape': []}
            
            if not np.isnan(result['mae']):
                method_summary[method]['mae'].append(result['mae'])
            if not np.isnan(result['smape']):
                method_summary[method]['smape'].append(result['smape'])
        
        for method, metrics in method_summary.items():
            if metrics['mae']:
                logger.info(f"{method:>15}: MAE={np.mean(metrics['mae']):>6.2f} ± {np.std(metrics['mae']):>5.2f}, "
                           f"sMAPE={np.mean(metrics['smape']):>6.2f}% ± {np.std(metrics['smape']):>5.2f}%")
        
        # Log to MLflow
        logger.info(f"\nLogging results to MLflow...")
        log_results_to_mlflow(all_results, args.experiment)
        
        # Save to DuckDB
        logger.info(f"Saving results to DuckDB...")
        save_results_to_duckdb(all_results)
        
        logger.info(f"\n{'='*60}")
        logger.info("NEXT STEPS")
        logger.info(f"{'='*60}")
        logger.info("1. View MLflow UI:")
        logger.info("   mlflow ui --port 5000")
        logger.info("   Then visit: http://localhost:5000")
        logger.info("")
        logger.info("2. Query results in DuckDB:")
        logger.info("   SELECT * FROM gold.gold_backtest_metrics ORDER BY date DESC LIMIT 10;")
        logger.info("")
        logger.info("3. API endpoints will now serve backtest metrics:")
        logger.info("   GET /metrics?item_id=4151&last=30d")
        
    else:
        logger.error("No results generated!")
        sys.exit(1)


if __name__ == "__main__":
    main()
