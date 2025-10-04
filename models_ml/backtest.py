"""
Backtesting utilities for OSRS price forecasting models.

Implements rolling walk-forward validation with baseline and statistical models.
Logs metrics to MLflow for experiment tracking.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union

import duckdb
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from dotenv import load_dotenv

# Load environment with error handling
try:
    load_dotenv()
except UnicodeDecodeError:
    # Skip .env if it has encoding issues - use defaults instead
    pass

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


class BaselineModel:
    """Simple baseline forecasting models."""
    
    def __init__(self, method: str = "last_value"):
        """
        Initialize baseline model.
        
        Args:
            method: Baseline method ('last_value', 'moving_average')
        """
        self.method = method
        self.window_size = 7  # For moving average
        
    def fit(self, y: pd.Series) -> "BaselineModel":
        """Fit the baseline model (no-op for most baselines)."""
        return self
        
    def predict(self, steps: int = 1) -> np.ndarray:
        """Generate predictions."""
        if not hasattr(self, '_last_values'):
            raise ValueError("Model must be fitted before prediction")
            
        if self.method == "last_value":
            return np.full(steps, self._last_values.iloc[-1])
        elif self.method == "moving_average":
            ma_value = self._last_values.tail(self.window_size).mean()
            return np.full(steps, ma_value)
        else:
            raise ValueError(f"Unknown baseline method: {self.method}")
    
    def fit_predict(self, y: pd.Series, steps: int = 1) -> np.ndarray:
        """Fit and predict in one step."""
        self._last_values = y
        return self.predict(steps)


class ETSModel:
    """Exponential Smoothing (ETS) model wrapper."""
    
    def __init__(self, trend: Optional[str] = None, seasonal: Optional[str] = None):
        """
        Initialize ETS model.
        
        Args:
            trend: Trend component ('add', 'mul', None)
            seasonal: Seasonal component ('add', 'mul', None)  
        """
        self.trend = trend
        self.seasonal = seasonal
        self.model = None
        self.fitted_model = None
        
    def fit(self, y: pd.Series) -> "ETSModel":
        """Fit the ETS model."""
        try:
            # Skip if insufficient data
            if len(y) < 10:
                logger.warning(f"Insufficient data for ETS: {len(y)} points")
                return self
                
            self.model = ExponentialSmoothing(
                y.values,
                trend=self.trend,
                seasonal=self.seasonal,
                seasonal_periods=7 if self.seasonal else None
            )
            self.fitted_model = self.model.fit(optimized=True)
            return self
            
        except Exception as e:
            logger.warning(f"ETS fit failed: {e}")
            self.fitted_model = None
            return self
    
    def predict(self, steps: int = 1) -> np.ndarray:
        """Generate predictions."""
        if self.fitted_model is None:
            # Fallback to last value if ETS failed
            logger.warning("ETS model not fitted, using last value fallback")
            return np.full(steps, np.nan)
            
        try:
            forecast = self.fitted_model.forecast(steps)
            return np.array(forecast)
        except Exception as e:
            logger.warning(f"ETS prediction failed: {e}")
            return np.full(steps, np.nan)
    
    def fit_predict(self, y: pd.Series, steps: int = 1) -> np.ndarray:
        """Fit and predict in one step."""
        self.fit(y)
        return self.predict(steps)


def smape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Calculate Symmetric Mean Absolute Percentage Error.
    
    Args:
        y_true: True values
        y_pred: Predicted values
        
    Returns:
        sMAPE value (0-100 scale)
    """
    # Handle NaN predictions
    mask = ~(np.isnan(y_true) | np.isnan(y_pred))
    if not mask.any():
        return np.nan
        
    y_true_masked = y_true[mask]
    y_pred_masked = y_pred[mask]
    
    denominator = (np.abs(y_true_masked) + np.abs(y_pred_masked)) / 2
    # Avoid division by zero
    denominator = np.where(denominator == 0, 1e-8, denominator)
    
    return np.mean(np.abs(y_true_masked - y_pred_masked) / denominator) * 100


def create_train_test_splits(
    df: pd.DataFrame,
    item_id: int,
    min_train_size: int = 14,
    test_size: int = 1,
    max_splits: int = 30
) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Create rolling walk-forward validation splits.
    
    Args:
        df: DataFrame with price data
        item_id: Item ID to filter for
        min_train_size: Minimum training set size
        test_size: Test set size (days)
        max_splits: Maximum number of splits to create
        
    Returns:
        List of (train, test) DataFrame tuples
    """
    item_data = df[df['item_id'] == item_id].sort_values('price_date').reset_index(drop=True)
    
    if len(item_data) < min_train_size + test_size:
        logger.warning(f"Insufficient data for item {item_id}: {len(item_data)} points")
        return []
    
    splits = []
    
    # Create rolling splits
    for i in range(min_train_size, len(item_data) - test_size + 1):
        train_data = item_data.iloc[:i]
        test_data = item_data.iloc[i:i + test_size]
        
        splits.append((train_data, test_data))
        
        if len(splits) >= max_splits:
            break
    
    logger.info(f"Created {len(splits)} train/test splits for item {item_id}")
    return splits


def run_backtest(
    item_id: int,
    model_name: str = "baseline_last_value",
    experiment_name: str = "osrs_price_forecasting"
) -> Dict[str, Any]:
    """
    Run backtesting for a specific item and model.
    
    Args:
        item_id: OSRS item ID
        model_name: Model to test ('baseline_last_value', 'baseline_ma', 'ets_simple')
        experiment_name: MLflow experiment name
        
    Returns:
        Dictionary with backtest results
    """
    logger.info(f"Running backtest for item {item_id} with model {model_name}")
    
    # Load data from DuckDB
    try:
        with duckdb.connect(DUCKDB_PATH) as conn:
            query = """
                SELECT price_date, item_id, close_price
                FROM gold.gold_price_features 
                WHERE item_id = ?
                ORDER BY price_date
            """
            df = conn.execute(query, [item_id]).df()
            
        if df.empty:
            logger.warning(f"No data found for item {item_id}")
            return {"error": f"No data found for item {item_id}"}
            
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return {"error": f"Data loading failed: {e}"}
    
    # Create train/test splits
    splits = create_train_test_splits(df, item_id)
    if not splits:
        return {"error": "Insufficient data for backtesting"}
    
    # Initialize model
    if model_name == "baseline_last_value":
        model = BaselineModel("last_value")
    elif model_name == "baseline_ma":
        model = BaselineModel("moving_average")
    elif model_name == "ets_simple":
        model = ETSModel(trend="add")
    else:
        raise ValueError(f"Unknown model: {model_name}")
    
    # Run backtest
    predictions = []
    actuals = []
    mae_scores = []
    smape_scores = []
    
    for train_data, test_data in splits:
        # Prepare training data
        y_train = train_data['close_price']
        y_test = test_data['close_price'].values
        
        # Make prediction
        try:
            y_pred = model.fit_predict(y_train, steps=len(test_data))
            
            # Store results
            predictions.extend(y_pred)
            actuals.extend(y_test)
            
            # Calculate metrics for this split
            mae = mean_absolute_error(y_test, y_pred)
            smape_val = smape(y_test, y_pred)
            
            mae_scores.append(mae)
            smape_scores.append(smape_val)
            
        except Exception as e:
            logger.warning(f"Prediction failed for split: {e}")
            continue
    
    if not predictions:
        return {"error": "All predictions failed"}
    
    # Calculate overall metrics
    overall_mae = mean_absolute_error(actuals, predictions)
    overall_smape = smape(np.array(actuals), np.array(predictions))
    
    # Prepare results
    results = {
        "item_id": item_id,
        "model_name": model_name,
        "n_splits": len(splits),
        "n_predictions": len(predictions),
        "mae": overall_mae,
        "smape": overall_smape,
        "mae_std": np.std(mae_scores),
        "smape_std": np.std(smape_scores),
        "predictions": predictions,
        "actuals": actuals,
        "timestamp": datetime.now().isoformat()
    }
    
    # Log to MLflow
    try:
        mlflow.set_experiment(experiment_name)
        
        with mlflow.start_run(run_name=f"{model_name}_item_{item_id}"):
            # Log parameters
            mlflow.log_param("item_id", item_id)
            mlflow.log_param("model_name", model_name)
            mlflow.log_param("n_splits", len(splits))
            
            # Log metrics
            mlflow.log_metric("mae", overall_mae)
            mlflow.log_metric("smape", overall_smape)
            mlflow.log_metric("mae_std", np.std(mae_scores))
            mlflow.log_metric("smape_std", np.std(smape_scores))
            
            # Log predictions as artifact
            pred_df = pd.DataFrame({
                'actual': actuals,
                'predicted': predictions
            })
            pred_df.to_csv("predictions.csv", index=False)
            mlflow.log_artifact("predictions.csv")
            
            # Clean up temp file
            os.remove("predictions.csv")
            
        logger.info(f"Logged results to MLflow for item {item_id}")
        
    except Exception as e:
        logger.warning(f"Failed to log to MLflow: {e}")
    
    return results


def run_benchmark_suite(
    item_ids: Optional[List[int]] = None,
    models: Optional[List[str]] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Run backtesting benchmark suite across multiple items and models.
    
    Args:
        item_ids: List of item IDs to test (None for all available)
        models: List of model names to test
        
    Returns:
        Dictionary with results for each model
    """
    if models is None:
        models = ["baseline_last_value", "baseline_ma", "ets_simple"]
    
    if item_ids is None:
        # Get available items from database
        try:
            with duckdb.connect(DUCKDB_PATH) as conn:
                query = """
                    SELECT DISTINCT item_id 
                    FROM gold.gold_item_metrics 
                    WHERE suitable_for_forecasting = true
                    LIMIT 10
                """
                item_ids = conn.execute(query).df()['item_id'].tolist()
        except Exception as e:
            logger.error(f"Failed to get item list: {e}")
            return {"error": "Failed to get item list"}
    
    logger.info(f"Running benchmark suite: {len(item_ids)} items, {len(models)} models")
    
    results = {}
    
    for model_name in models:
        model_results = []
        
        for item_id in item_ids:
            result = run_backtest(item_id, model_name)
            model_results.append(result)
            
        results[model_name] = model_results
    
    # Log summary metrics
    try:
        mlflow.set_experiment("osrs_benchmark_suite")
        
        with mlflow.start_run(run_name=f"benchmark_{datetime.now().strftime('%Y%m%d_%H%M')}"):
            for model_name, model_results in results.items():
                valid_results = [r for r in model_results if 'error' not in r]
                
                if valid_results:
                    avg_mae = np.mean([r['mae'] for r in valid_results])
                    avg_smape = np.mean([r['smape'] for r in valid_results])
                    
                    mlflow.log_metric(f"{model_name}_avg_mae", avg_mae)
                    mlflow.log_metric(f"{model_name}_avg_smape", avg_smape)
                    mlflow.log_metric(f"{model_name}_success_rate", 
                                    len(valid_results) / len(model_results))
                    
        logger.info("Logged benchmark summary to MLflow")
        
    except Exception as e:
        logger.warning(f"Failed to log benchmark summary: {e}")
    
    return results


if __name__ == "__main__":
    # Example usage
    logger.info("Running OSRS price forecasting backtest")
    
    # Run single item backtest
    result = run_backtest(4151, "baseline_last_value")  # Abyssal whip
    print(f"Backtest result: MAE={result.get('mae', 'N/A'):.2f}, sMAPE={result.get('smape', 'N/A'):.2f}%")
    
    # Run benchmark suite
    # benchmark_results = run_benchmark_suite()
    # print(f"Benchmark completed for {len(benchmark_results)} models")
