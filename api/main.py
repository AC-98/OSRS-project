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
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator
from dotenv import load_dotenv

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
