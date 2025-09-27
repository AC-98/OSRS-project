"""
Unit tests for backtesting utilities.
"""

import unittest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'models_ml'))

from backtest import (
    BaselineModel, 
    ETSModel, 
    smape, 
    create_train_test_splits
)


class TestBacktestUtilities(unittest.TestCase):
    """Test cases for backtesting utilities."""
    
    def setUp(self):
        """Set up test data."""
        # Create sample price data
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        prices = 100 + np.cumsum(np.random.randn(30) * 2)  # Random walk around 100
        
        self.sample_data = pd.DataFrame({
            'price_date': dates,
            'item_id': [1001] * 30,
            'close_price': prices
        })
        
        self.price_series = pd.Series(prices, index=dates)
    
    def test_baseline_last_value_model(self):
        """Test baseline last value model."""
        model = BaselineModel("last_value")
        
        # Test single prediction
        predictions = model.fit_predict(self.price_series, steps=1)
        self.assertEqual(len(predictions), 1)
        self.assertEqual(predictions[0], self.price_series.iloc[-1])
        
        # Test multiple predictions
        predictions = model.fit_predict(self.price_series, steps=3)
        self.assertEqual(len(predictions), 3)
        self.assertTrue(all(p == self.price_series.iloc[-1] for p in predictions))
    
    def test_baseline_moving_average_model(self):
        """Test baseline moving average model."""
        model = BaselineModel("moving_average")
        
        predictions = model.fit_predict(self.price_series, steps=2)
        self.assertEqual(len(predictions), 2)
        
        # Should be close to the 7-day moving average
        expected_ma = self.price_series.tail(7).mean()
        self.assertAlmostEqual(predictions[0], expected_ma, places=2)
    
    def test_ets_model_basic(self):
        """Test ETS model basic functionality."""
        model = ETSModel(trend="add")
        
        # Should not raise exception
        predictions = model.fit_predict(self.price_series, steps=1)
        self.assertEqual(len(predictions), 1)
        
        # With insufficient data, should handle gracefully
        short_series = self.price_series.head(5)
        predictions = model.fit_predict(short_series, steps=1)
        self.assertEqual(len(predictions), 1)
    
    def test_smape_calculation(self):
        """Test sMAPE metric calculation."""
        y_true = np.array([100, 200, 300])
        y_pred = np.array([110, 190, 310])
        
        smape_val = smape(y_true, y_pred)
        self.assertGreater(smape_val, 0)
        self.assertLess(smape_val, 100)
        
        # Perfect prediction should give 0
        perfect_smape = smape(y_true, y_true)
        self.assertAlmostEqual(perfect_smape, 0, places=2)
        
        # Test with NaN values
        y_pred_nan = np.array([110, np.nan, 310])
        smape_nan = smape(y_true, y_pred_nan)
        self.assertFalse(np.isnan(smape_nan))
    
    def test_train_test_splits(self):
        """Test train/test split creation."""
        splits = create_train_test_splits(
            self.sample_data, 
            item_id=1001,
            min_train_size=10,
            test_size=1,
            max_splits=5
        )
        
        self.assertGreater(len(splits), 0)
        self.assertLessEqual(len(splits), 5)
        
        # Check split structure
        for train, test in splits:
            self.assertGreater(len(train), 0)
            self.assertEqual(len(test), 1)
            self.assertTrue(all(train['item_id'] == 1001))
            self.assertTrue(all(test['item_id'] == 1001))
            
            # Train should come before test chronologically
            self.assertLess(train['price_date'].max(), test['price_date'].min())
    
    def test_train_test_splits_insufficient_data(self):
        """Test train/test splits with insufficient data."""
        # Create very small dataset
        small_data = self.sample_data.head(5)
        
        splits = create_train_test_splits(
            small_data,
            item_id=1001,
            min_train_size=10,  # More than available data
            test_size=1
        )
        
        self.assertEqual(len(splits), 0)
    
    def test_train_test_splits_nonexistent_item(self):
        """Test train/test splits for nonexistent item."""
        splits = create_train_test_splits(
            self.sample_data,
            item_id=9999,  # Doesn't exist
            min_train_size=5,
            test_size=1
        )
        
        self.assertEqual(len(splits), 0)


class TestDataValidation(unittest.TestCase):
    """Test cases for data validation."""
    
    def test_price_data_structure(self):
        """Test that sample data has expected structure."""
        # This would typically test against real data from bronze tables
        expected_columns = ['price_date', 'item_id', 'close_price']
        
        # Create mock data structure
        test_data = pd.DataFrame({
            'price_date': pd.date_range('2024-01-01', periods=5),
            'item_id': [1001] * 5,
            'close_price': [100, 105, 103, 108, 110]
        })
        
        for col in expected_columns:
            self.assertIn(col, test_data.columns)
        
        # Test data types
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(test_data['price_date']))
        self.assertTrue(pd.api.types.is_integer_dtype(test_data['item_id']))
        self.assertTrue(pd.api.types.is_numeric_dtype(test_data['close_price']))
    
    def test_bronze_to_silver_row_preservation_logic(self):
        """Test the logic that would be used in dbt test."""
        # Simulate bronze data
        bronze_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=10, freq='H'),
            'item_id': [1001] * 10,
            'avg_high_price': range(100, 110)
        })
        
        # Simulate silver aggregation (daily)
        silver_data = bronze_data.groupby([
            bronze_data['timestamp'].dt.date,
            'item_id'
        ]).agg({
            'avg_high_price': 'mean'
        }).reset_index()
        
        # In this case, 10 hourly records become 1 daily record
        self.assertEqual(len(bronze_data), 10)
        self.assertEqual(len(silver_data), 1)
        
        # This is expected behavior - bronze has more granular data
        # The actual dbt test would check for unexpected data loss


if __name__ == '__main__':
    unittest.main()
