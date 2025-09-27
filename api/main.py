"""
OSRS Signals FastAPI application.

Provides REST API endpoints for item data, price predictions, and model metrics.
All endpoints are powered by real data from DuckDB gold tables.
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
from contextlib import contextmanager

import duckdb
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, validator
from dotenv import load_dotenv
import json

# Import our ML utilities
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'models_ml'))
from backtest import BaselineModel, ETSModel

# Import config utilities
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))
from config_utils import load_selected_items

# Load environment
load_dotenv()

# Initialize Jinja2 templates
templates = Jinja2Templates(directory="api/templates")

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration  
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "warehouse/osrs.duckdb")
API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Initialize FastAPI app
app = FastAPI(
    title="OSRS Signals API",
    description="OSRS Grand Exchange price forecasting API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Database connection helper
@contextmanager
def get_db_connection():
    """Context manager for database connections with proper error handling."""
    conn = None
    try:
        conn = duckdb.connect(DUCKDB_PATH)
        # Test connection
        conn.execute("SELECT 1").fetchone()
        logger.debug("Database connection established")
        yield conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(
            status_code=503,
            detail="Database connection failed. Please ensure data pipeline has run."
        )
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")


# Pydantic models
class HealthResponse(BaseModel):
    """Health check response model."""
    status: str = Field(..., description="Service status")
    timestamp: datetime = Field(..., description="Response timestamp")
    database_connected: bool = Field(..., description="Database connection status")
    version: str = Field(..., description="API version")


class ItemInfo(BaseModel):
    """Item information model."""
    item_id: int = Field(..., description="OSRS item ID")
    name: str = Field(..., description="Item name")

    class Config:
        from_attributes = True


class ItemsResponse(BaseModel):
    """Items list response model."""
    items: List[ItemInfo] = Field(..., description="List of available items")
    total_count: int = Field(..., description="Total number of items")
    page: int = Field(..., description="Current page number")
    page_size: int = Field(..., description="Items per page")
    total_pages: int = Field(..., description="Total number of pages")


class PredictionResponse(BaseModel):
    """Prediction response model."""
    item_id: int = Field(..., description="OSRS item ID")
    item_name: str = Field(..., description="Item name")
    current_price: float = Field(..., description="Current price (gp)")
    forecast: List[float] = Field(..., description="Predicted prices for each day")
    lower: List[Optional[float]] = Field(..., description="Lower confidence bounds (NA for now)")
    upper: List[Optional[float]] = Field(..., description="Upper confidence bounds (NA for now)")
    horizon: int = Field(..., description="Prediction horizon in days")
    model_used: str = Field(..., description="Model used for prediction")
    timestamp: datetime = Field(..., description="Prediction timestamp")


class MetricsResponse(BaseModel):
    """Model metrics response model."""
    item_id: int = Field(..., description="OSRS item ID")
    item_name: str = Field(..., description="Item name")
    model_name: str = Field(..., description="Model name")
    mae: float = Field(..., description="Mean Absolute Error")
    smape: float = Field(..., description="Symmetric Mean Absolute Percentage Error (%)")
    evaluation_period: str = Field(..., description="Evaluation time period")
    n_predictions: int = Field(..., description="Number of predictions evaluated")
    last_updated: datetime = Field(..., description="Metrics last updated")


class SelectionItem(BaseModel):
    """Item in the selection response."""
    id: int = Field(..., description="Item ID")
    name: str = Field(..., description="Item name")
    median_units_per_day: float = Field(..., description="Median daily trading volume (units)")
    median_turnover_gp: Optional[float] = Field(..., description="Median daily turnover (GP)")
    coverage: float = Field(..., description="Data coverage percentage")
    days_with_data: int = Field(..., description="Days with trading data")
    selection_reason: str = Field(..., description="Reason for selection")


class SelectionResponse(BaseModel):
    """Response model for selection endpoint."""
    total_items: int = Field(..., description="Total selected items")
    generated_at: str = Field(..., description="Selection timestamp")
    items: List[SelectionItem] = Field(..., description="Selected items")


class LeaderboardEntry(BaseModel):
    """Entry in the leaderboard response."""
    item_id: int = Field(..., description="Item ID")
    name: str = Field(..., description="Item name")
    method: str = Field(..., description="Best performing method")
    smape: float = Field(..., description="sMAPE score (%)")
    mae: float = Field(..., description="MAE score")
    n: int = Field(..., description="Number of predictions")


class LeaderboardResponse(BaseModel):
    """Response model for leaderboard endpoint."""
    total_items: int = Field(..., description="Total items in leaderboard")
    period_days: int = Field(..., description="Evaluation period in days")
    entries: List[LeaderboardEntry] = Field(..., description="Leaderboard entries")


# API Endpoints
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    logger.info("Health check requested")
    try:
        with get_db_connection():
            db_connected = True
        logger.info("Health check passed")
    except Exception as e:
        logger.warning(f"Health check failed: {e}")
        db_connected = False
    
    return HealthResponse(
        status="healthy" if db_connected else "degraded",
        timestamp=datetime.now(),
        database_connected=db_connected,
        version="1.0.0"
    )


@app.get("/items", response_model=ItemsResponse)
async def get_items(
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(50, ge=1, le=1000, description="Items per page"),
):
    """
    Get paginated list of available items ordered by name.
    
    Returns item_id and name for all items with available data.
    Pagination is applied if total items > 1000.
    """
    logger.info(f"Items requested: page={page}, page_size={page_size}")
    
    with get_db_connection() as conn:
        try:
            # Get curated item IDs
            try:
                curated_item_ids = load_selected_items("config/items_selected.json")
                logger.info(f"Using {len(curated_item_ids)} curated items")
            except Exception as e:
                logger.warning(f"Failed to load curated items, using all available: {e}")
                curated_item_ids = None
            
            # Build query for total count with curated items filter
            if curated_item_ids:
                placeholders = ','.join(['?'] * len(curated_item_ids))
                count_query = f"""
                    SELECT COUNT(*) as total_count
                    FROM main_gold.gold_item_metrics
                    WHERE item_id IN ({placeholders})
                """
                total_count = conn.execute(count_query, curated_item_ids).fetchone()[0]
            else:
                count_query = """
                    SELECT COUNT(*) as total_count
                    FROM main_gold.gold_item_metrics
                """
                total_count = conn.execute(count_query).fetchone()[0]
            logger.info(f"Total items available: {total_count}")
            
            # Calculate pagination
            offset = (page - 1) * page_size
            total_pages = (total_count + page_size - 1) // page_size
            
            # Validate page number
            if page > total_pages and total_count > 0:
                logger.warning(f"Invalid page {page}, max pages: {total_pages}")
                raise HTTPException(
                    status_code=400, 
                    detail=f"Page {page} does not exist. Maximum page is {total_pages}."
                )
            
            # Get items with pagination, ordered by name, filtered by curated items
            if curated_item_ids:
                placeholders = ','.join(['?'] * len(curated_item_ids))
                items_query = f"""
                    SELECT 
                        item_id,
                        name
                    FROM main_gold.gold_item_metrics
                    WHERE item_id IN ({placeholders})
                    ORDER BY name ASC
                    LIMIT ? OFFSET ?
                """
                params = curated_item_ids + [page_size, offset]
            else:
                items_query = """
                    SELECT 
                        item_id,
                        name
                    FROM main_gold.gold_item_metrics
                    ORDER BY name ASC
                    LIMIT ? OFFSET ?
                """
                params = [page_size, offset]
            
            df = conn.execute(items_query, params).df()
            
            # Convert to response model
            items = []
            for _, row in df.iterrows():
                items.append(ItemInfo(
                    item_id=int(row['item_id']),
                    name=str(row['name'])
                ))
            
            logger.info(f"Returning {len(items)} items for page {page}")
            
            return ItemsResponse(
                items=items,
                total_count=total_count,
                page=page,
                page_size=page_size,
                total_pages=total_pages
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error fetching items: {e}")
            raise HTTPException(status_code=500, detail="Failed to fetch items")


@app.get("/predict", response_model=PredictionResponse)
async def predict_price(
    item_id: int = Query(..., description="OSRS item ID"),
    horizon: int = Query(1, ge=1, le=7, description="Prediction horizon in days")
):
    """
    Generate price predictions using 7-day moving average baseline model.
    
    Uses recent gold features data to forecast future prices.
    Returns 503 if gold tables are missing or insufficient data.
    """
    logger.info(f"Prediction requested: item_id={item_id}, horizon={horizon}")
    
    with get_db_connection() as conn:
        try:
            # Check if gold tables exist and get item info
            item_query = """
                SELECT name, latest_price
                FROM main_gold.gold_item_metrics
                WHERE item_id = ?
            """
            item_result = conn.execute(item_query, [item_id]).fetchone()
            
            if not item_result:
                logger.warning(f"Item {item_id} not found in gold_item_metrics")
                raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
            
            item_name, current_price = item_result
            logger.info(f"Found item: {item_name}, current_price: {current_price}")
            
            # Get recent price data from gold_daily_features (last 14 days for 7-day MA + buffer)
            price_query = """
                SELECT date, latest_price
                FROM main_gold.gold_daily_features
                WHERE item_id = ?
                ORDER BY date DESC
                LIMIT 14
            """
            
            df = conn.execute(price_query, [item_id]).df()
            
            if df.empty or len(df) < 7:
                logger.error(f"Insufficient price data for item {item_id}: {len(df) if not df.empty else 0} days")
                raise HTTPException(
                    status_code=503,
                    detail="Insufficient price data for prediction. Need at least 7 days of data. Please run data pipeline."
                )
            
            # Use 7-day moving average baseline model
            logger.info(f"Using {len(df)} days of price data for prediction")
            
            # Reverse to chronological order for model training
            prices = df['latest_price'].iloc[::-1]
            
            # Initialize 7-day moving average model
            predictor = BaselineModel("moving_average")
            predictor.window_size = 7
            
            # Generate predictions
            predictions = predictor.fit_predict(prices, steps=horizon)
            
            logger.info(f"Generated {len(predictions)} predictions for item {item_id}")
            
            return PredictionResponse(
                item_id=item_id,
                item_name=item_name,
                current_price=float(current_price) if current_price else 0.0,
                forecast=[float(p) for p in predictions],
                lower=[None] * horizon,  # NA for now as requested
                upper=[None] * horizon,  # NA for now as requested
                horizon=horizon,
                model_used="7-day moving average",
                timestamp=datetime.now()
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Prediction error for item {item_id}: {e}")
            if "does not exist" in str(e).lower() or "no such table" in str(e).lower():
                raise HTTPException(
                    status_code=503,
                    detail="Gold tables missing. Please run the data pipeline first."
                )
            raise HTTPException(status_code=500, detail="Prediction failed")


@app.get("/metrics", response_model=MetricsResponse)
async def get_model_metrics(
    item_id: int = Query(..., description="OSRS item ID"),
    last: str = Query("30d", description="Time period for metrics (e.g., '30d', '7d')")
):
    """
    Get model performance metrics for an item from backtest results.
    
    Reads from gold_backtest_metrics table for the last N days.
    Returns 404 if no backtest data exists for the item.
    """
    logger.info(f"Metrics requested: item_id={item_id}, last={last}")
    
    # Validate and parse time period
    try:
        if last.endswith('d'):
            days = int(last[:-1])
            if days <= 0:
                raise ValueError("Days must be positive")
        else:
            raise ValueError("Time period must end with 'd' (e.g., '30d')")
    except ValueError as e:
        logger.warning(f"Invalid time period '{last}': {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid time period '{last}'. Use format like '30d', '7d'."
        )
    
    with get_db_connection() as conn:
        try:
            # Get item name first
            item_query = "SELECT name FROM main_gold.gold_item_metrics WHERE item_id = ?"
            item_result = conn.execute(item_query, [item_id]).fetchone()
            
            if not item_result:
                logger.warning(f"Item {item_id} not found in gold_item_metrics")
                raise HTTPException(status_code=404, detail=f"Item {item_id} not found")
            
            item_name = item_result[0]
            logger.info(f"Found item: {item_name}")
            
            # Query backtest metrics from gold layer
            # Use the most common method or aggregate across methods
            metrics_query = f"""
                SELECT 
                    method,
                    AVG(mae) as mean_mae,
                    AVG(smape) as mean_smape,
                    COUNT(*) as n_predictions,
                    MAX(timestamp) as last_updated
                FROM main_gold.gold_backtest_metrics
                WHERE item_id = ? 
                  AND date >= (CURRENT_DATE - INTERVAL {days} DAY)
                GROUP BY method
                ORDER BY COUNT(*) DESC
                LIMIT 1
            """
            
            metrics_result = conn.execute(metrics_query, [item_id]).fetchone()
            
            if not metrics_result or metrics_result[1] is None:
                logger.warning(f"No backtest metrics found for item {item_id} in last {days} days")
                raise HTTPException(
                    status_code=404,
                    detail=f"No backtest metrics found for item {item_id} in the last {last}. "
                           f"Run: python scripts/run_backtest.py --items {item_id}"
                )
            
            method, mean_mae, mean_smape, n_predictions, last_updated = metrics_result
            
            logger.info(f"Found metrics for {item_name}: method={method}, n_predictions={n_predictions}")
            
            return MetricsResponse(
                item_id=item_id,
                item_name=item_name,
                model_name=method,
                mae=float(mean_mae),
                smape=float(mean_smape),
                evaluation_period=last,
                n_predictions=int(n_predictions),
                last_updated=last_updated if last_updated else datetime.now()
            )
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Metrics error for item {item_id}: {e}")
            if "does not exist" in str(e).lower() or "no such table" in str(e).lower():
                raise HTTPException(
                    status_code=404,
                    detail=f"No backtest metrics available. Run: python scripts/run_backtest.py --items {item_id}"
                )
            raise HTTPException(status_code=500, detail="Failed to fetch metrics")


@app.get("/selection", response_model=SelectionResponse)
async def get_selection():
    """Get the current item selection from config/items_selected.json."""
    logger.info("Selection requested")
    
    try:
        # Load the selection file
        selection_file = Path("config/items_selected.json")
        
        if not selection_file.exists():
            raise HTTPException(
                status_code=404,
                detail="No item selection available. Run: python scripts/select_items.py"
            )
        
        with open(selection_file, 'r') as f:
            data = json.load(f)
        
        # Convert to response format
        items = []
        for item in data.get("items", []):
            items.append(SelectionItem(
                id=item["id"],
                name=item["name"],
                median_units_per_day=item["median_units_per_day"],
                median_turnover_gp=item.get("median_turnover_gp"),
                coverage=item["coverage"],
                days_with_data=item["days_with_data"],
                selection_reason=item["selection_reason"]
            ))
        
        logger.info(f"Returning {len(items)} selected items")
        return SelectionResponse(
            total_items=len(items),
            generated_at=data.get("metadata", {}).get("generated_at", ""),
            items=items
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Selection error: {e}")
        raise HTTPException(status_code=500, detail="Failed to load item selection")


@app.get("/leaderboard", response_model=LeaderboardResponse)
async def get_leaderboard(last: str = Query("30d", description="Time period (e.g., 30d, 7d)")):
    """Get the model performance leaderboard for best method per item."""
    logger.info(f"Leaderboard requested: period={last}")
    
    # Parse the time period
    try:
        if last.endswith('d'):
            period_days = int(last[:-1])
        else:
            period_days = 30  # Default fallback
    except ValueError:
        period_days = 30
    
    cutoff_date = datetime.now() - timedelta(days=period_days)
    
    with get_db_connection() as conn:
        try:
            # Query to get best method per item (lowest sMAPE, tie-break by MAE)
            query = """
            WITH item_method_stats AS (
                SELECT 
                    gbm.item_id,
                    im.name,
                    gbm.method,
                    AVG(gbm.smape) as avg_smape,
                    AVG(gbm.mae) as avg_mae,
                    COUNT(*) as n_predictions
                FROM main_gold.gold_backtest_metrics gbm
                JOIN main_bronze.bronze_item_mapping im ON gbm.item_id = im.id
                WHERE gbm.date >= ?
                GROUP BY gbm.item_id, im.name, gbm.method
            ),
            
            best_methods AS (
                SELECT 
                    item_id,
                    name,
                    method,
                    avg_smape,
                    avg_mae,
                    n_predictions,
                    ROW_NUMBER() OVER (
                        PARTITION BY item_id 
                        ORDER BY avg_smape ASC, avg_mae ASC
                    ) as rank
                FROM item_method_stats
            )
            
            SELECT 
                item_id,
                name,
                method,
                avg_smape as smape,
                avg_mae as mae,
                n_predictions as n
            FROM best_methods
            WHERE rank = 1
            ORDER BY smape ASC, mae ASC
            """
            
            df = conn.execute(query, [cutoff_date]).df()
            
            if df.empty:
                logger.warning("No backtest metrics found for leaderboard")
                return LeaderboardResponse(
                    total_items=0,
                    period_days=period_days,
                    entries=[]
                )
            
            # Convert to response format
            entries = []
            for _, row in df.iterrows():
                entries.append(LeaderboardEntry(
                    item_id=int(row['item_id']),
                    name=str(row['name']),
                    method=str(row['method']),
                    smape=float(row['smape']),
                    mae=float(row['mae']),
                    n=int(row['n'])
                ))
            
            logger.info(f"Returning leaderboard with {len(entries)} items")
            return LeaderboardResponse(
                total_items=len(entries),
                period_days=period_days,
                entries=entries
            )
            
        except Exception as e:
            logger.error(f"Leaderboard error: {e}")
            if "does not exist" in str(e).lower() or "no such table" in str(e).lower():
                raise HTTPException(
                    status_code=404,
                    detail="No backtest metrics available. Run: python scripts/run_backtest.py"
                )
            raise HTTPException(status_code=500, detail="Failed to fetch leaderboard")


@app.get("/ui", response_class=HTMLResponse)
async def get_ui():
    """Serve the static UI dashboard."""
    logger.info("UI requested")
    
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OSRS Project Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #f5f5f5;
            color: #333;
            line-height: 1.6;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .header h1 {
            color: #2c3e50;
            margin-bottom: 10px;
        }
        
        .header p {
            color: #7f8c8d;
        }
        
        .grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .panel {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .panel h2 {
            margin-bottom: 15px;
            color: #2c3e50;
            font-size: 1.2em;
            border-bottom: 2px solid #3498db;
            padding-bottom: 5px;
        }
        
        .chart-panel {
            grid-column: 1 / -1;
        }
        
        .table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }
        
        .table th,
        .table td {
            padding: 8px 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        
        .table th {
            background-color: #f8f9fa;
            font-weight: 600;
            color: #2c3e50;
        }
        
        .table tr:hover {
            background-color: #f8f9fa;
        }
        
        .dropdown {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            margin-bottom: 15px;
        }
        
        .dropdown:focus {
            outline: none;
            border-color: #3498db;
        }
        
        .metrics-chips {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .chip {
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 600;
        }
        
        .chip-mae {
            background-color: #e8f4f8;
            color: #2980b9;
        }
        
        .chip-smape {
            background-color: #f0f8e8;
            color: #27ae60;
        }
        
        .loading {
            text-align: center;
            color: #7f8c8d;
            padding: 20px;
        }
        
        .error {
            background-color: #fdf2f2;
            color: #e74c3c;
            padding: 15px;
            border-radius: 4px;
            border-left: 4px solid #e74c3c;
            margin: 10px 0;
        }
        
        .empty-state {
            text-align: center;
            color: #7f8c8d;
            padding: 40px 20px;
        }
        
        .chart-container {
            position: relative;
            height: 400px;
            margin-top: 20px;
        }
        
        @media (max-width: 768px) {
            .grid {
                grid-template-columns: 1fr;
            }
            
            .container {
                padding: 10px;
            }
        }
        
        /* Accessibility improvements */
        .sr-only {
            position: absolute;
            width: 1px;
            height: 1px;
            padding: 0;
            margin: -1px;
            overflow: hidden;
            clip: rect(0, 0, 0, 0);
            white-space: nowrap;
            border: 0;
        }
        
        button:focus,
        select:focus {
            outline: 2px solid #3498db;
            outline-offset: 2px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>OSRS Project Dashboard</h1>
            <p>Real-time item selection, model performance, and price forecasting</p>
        </div>
        
        <div class="grid">
            <div class="panel">
                <h2>Selected Items</h2>
                <div id="selection-content">
                    <div class="loading">Loading selected items...</div>
                </div>
            </div>
            
            <div class="panel">
                <h2>Performance Leaderboard</h2>
                <div id="leaderboard-content">
                    <div class="loading">Loading leaderboard...</div>
                </div>
            </div>
        </div>
        
        <div class="panel chart-panel">
            <h2>Item Analysis</h2>
            <select id="item-dropdown" class="dropdown" aria-label="Select item for analysis">
                <option value="">Select an item...</option>
            </select>
            
            <div id="metrics-chips" class="metrics-chips" style="display: none;">
                <div class="chip chip-mae" id="mae-chip" aria-label="Mean Absolute Error">
                    MAE: <span id="mae-value">-</span>
                </div>
                <div class="chip chip-smape" id="smape-chip" aria-label="Symmetric Mean Absolute Percentage Error">
                    sMAPE: <span id="smape-value">-</span>%
                </div>
            </div>
            
            <div id="chart-content">
                <div class="empty-state">
                    Select an item to view price history and forecasts
                </div>
            </div>
        </div>
    </div>

    <script>
        // Global state
        let selectedItems = [];
        let chart = null;
        
        // API helpers
        async function fetchAPI(endpoint) {
            try {
                const response = await fetch(endpoint);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                return await response.json();
            } catch (error) {
                console.error(`API Error (${endpoint}):`, error);
                throw error;
            }
        }
        
        // Load initial data
        async function loadInitialData() {
            try {
                // Load selection and leaderboard in parallel
                const [selectionData, leaderboardData] = await Promise.all([
                    fetchAPI('/selection'),
                    fetchAPI('/leaderboard?last=30d')
                ]);
                
                selectedItems = selectionData.items;
                renderSelection(selectionData);
                renderLeaderboard(leaderboardData);
                populateItemDropdown(selectedItems);
                
            } catch (error) {
                renderError('selection-content', 'Failed to load data. Please ensure the pipeline has run.');
                renderError('leaderboard-content', 'Failed to load leaderboard data.');
            }
        }
        
        // Render functions
        function renderSelection(data) {
            const content = document.getElementById('selection-content');
            
            if (!data.items || data.items.length === 0) {
                content.innerHTML = '<div class="empty-state">No items selected. Run item selection first.</div>';
                return;
            }
            
            const table = `
                <table class="table" role="table">
                    <thead>
                        <tr>
                            <th scope="col">Item</th>
                            <th scope="col">Units/Day</th>
                            <th scope="col">Coverage</th>
                            <th scope="col">Reason</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.items.map(item => `
                            <tr>
                                <td><strong>${item.name}</strong></td>
                                <td>${formatNumber(item.median_units_per_day)}</td>
                                <td>${item.coverage.toFixed(1)}%</td>
                                <td>${item.selection_reason.replace('_', ' ')}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
                <p style="margin-top: 10px; font-size: 0.9em; color: #7f8c8d;">
                    ${data.total_items} items selected on ${new Date(data.generated_at).toLocaleDateString()}
                </p>
            `;
            
            content.innerHTML = table;
        }
        
        function renderLeaderboard(data) {
            const content = document.getElementById('leaderboard-content');
            
            if (!data.entries || data.entries.length === 0) {
                content.innerHTML = '<div class="empty-state">No metrics yet—run backtests first.</div>';
                return;
            }
            
            const table = `
                <table class="table" role="table">
                    <thead>
                        <tr>
                            <th scope="col">Item</th>
                            <th scope="col">Method</th>
                            <th scope="col">sMAPE</th>
                            <th scope="col">MAE</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.entries.slice(0, 10).map(entry => `
                            <tr>
                                <td><strong>${entry.name}</strong></td>
                                <td>${entry.method.replace('_', ' ')}</td>
                                <td>${entry.smape.toFixed(2)}%</td>
                                <td>${formatNumber(entry.mae)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
                <p style="margin-top: 10px; font-size: 0.9em; color: #7f8c8d;">
                    Top performers over last ${data.period_days} days
                </p>
            `;
            
            content.innerHTML = table;
        }
        
        function renderError(containerId, message) {
            const container = document.getElementById(containerId);
            container.innerHTML = `<div class="error" role="alert">${message}</div>`;
        }
        
        function populateItemDropdown(items) {
            const dropdown = document.getElementById('item-dropdown');
            dropdown.innerHTML = '<option value="">Select an item...</option>';
            
            items.forEach(item => {
                const option = document.createElement('option');
                option.value = item.id;
                option.textContent = item.name;
                dropdown.appendChild(option);
            });
        }
        
        // Chart functions
        async function loadItemData(itemId) {
            if (!itemId) {
                hideChart();
                return;
            }
            
            try {
                const [metricsData, predictionData] = await Promise.all([
                    fetchAPI(`/metrics?item_id=${itemId}&last=30d`),
                    fetchAPI(`/predict?item_id=${itemId}&horizon=1`)
                ]);
                
                updateMetricsChips(metricsData);
                renderChart(metricsData, predictionData);
                
            } catch (error) {
                renderError('chart-content', 'Failed to load item data. Ensure backtests have been run for this item.');
                hideMetricsChips();
            }
        }
        
        function updateMetricsChips(metricsData) {
            const maeValue = document.getElementById('mae-value');
            const smapeValue = document.getElementById('smape-value');
            const chipsContainer = document.getElementById('metrics-chips');
            
            maeValue.textContent = formatNumber(metricsData.mae);
            smapeValue.textContent = metricsData.smape.toFixed(2);
            chipsContainer.style.display = 'flex';
        }
        
        function hideMetricsChips() {
            document.getElementById('metrics-chips').style.display = 'none';
        }
        
        function renderChart(metricsData, predictionData) {
            const chartContent = document.getElementById('chart-content');
            chartContent.innerHTML = '<div class="chart-container"><canvas id="price-chart" role="img" aria-label="Price history and forecast chart"></canvas></div>';
            
            // For now, create a simple chart with the current price and prediction
            // In a real implementation, you'd fetch historical data from metrics
            const ctx = document.getElementById('price-chart').getContext('2d');
            
            if (chart) {
                chart.destroy();
            }
            
            const today = new Date();
            const tomorrow = new Date(today);
            tomorrow.setDate(tomorrow.getDate() + 1);
            
            chart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: [today.toLocaleDateString(), tomorrow.toLocaleDateString()],
                    datasets: [
                        {
                            label: 'Current Price',
                            data: [predictionData.current_price, null],
                            borderColor: '#3498db',
                            backgroundColor: '#3498db',
                            pointRadius: 6,
                            pointHoverRadius: 8,
                            tension: 0.1
                        },
                        {
                            label: 'Forecast',
                            data: [null, predictionData.forecast[0]],
                            borderColor: '#e74c3c',
                            backgroundColor: '#e74c3c',
                            pointRadius: 6,
                            pointHoverRadius: 8,
                            borderDash: [5, 5]
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        title: {
                            display: true,
                            text: `${predictionData.item_name} - Price Forecast`
                        },
                        legend: {
                            display: true
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: false,
                            title: {
                                display: true,
                                text: 'Price (GP)'
                            }
                        },
                        x: {
                            title: {
                                display: true,
                                text: 'Date'
                            }
                        }
                    }
                }
            });
        }
        
        function hideChart() {
            const chartContent = document.getElementById('chart-content');
            chartContent.innerHTML = '<div class="empty-state">Select an item to view price history and forecasts</div>';
            hideMetricsChips();
            
            if (chart) {
                chart.destroy();
                chart = null;
            }
        }
        
        // Utility functions
        function formatNumber(num) {
            if (num >= 1000000) {
                return (num / 1000000).toFixed(1) + 'M';
            } else if (num >= 1000) {
                return (num / 1000).toFixed(1) + 'K';
            }
            return Math.round(num).toString();
        }
        
        // Event listeners
        document.getElementById('item-dropdown').addEventListener('change', (e) => {
            loadItemData(e.target.value);
        });
        
        // Keyboard navigation
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                document.getElementById('item-dropdown').value = '';
                hideChart();
            }
        });
        
        // Initialize app
        document.addEventListener('DOMContentLoaded', () => {
            loadInitialData();
        });
    </script>
</body>
</html>
    """
    
    return HTMLResponse(content=html_content)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler with structured logging."""
    logger.error(f"Unhandled exception on {request.method} {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting OSRS Signals API on {API_HOST}:{API_PORT}")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info("API documentation available at /docs")
    
    uvicorn.run(
        "main:app",
        host=API_HOST,
        port=API_PORT,
        reload=True,
        log_level="info"
    )
