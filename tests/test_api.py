"""
Unit tests for FastAPI endpoints.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'api'))

from fastapi.testclient import TestClient
from main import app


class TestAPIEndpoints(unittest.TestCase):
    """Test cases for API endpoints."""
    
    def setUp(self):
        """Set up test client."""
        self.client = TestClient(app)
    
    def test_health_endpoint_success(self):
        """Test health endpoint with successful database connection."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            response = self.client.get("/health")
            
            self.assertEqual(response.status_code, 200)
            data = response.json()
            
            self.assertEqual(data["status"], "healthy")
            self.assertTrue(data["database_connected"])
            self.assertEqual(data["version"], "1.0.0")
            self.assertIn("timestamp", data)
    
    def test_health_endpoint_db_failure(self):
        """Test health endpoint with database connection failure."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_db.side_effect = Exception("DB connection failed")
            
            response = self.client.get("/health")
            
            self.assertEqual(response.status_code, 200)
            data = response.json()
            
            self.assertEqual(data["status"], "degraded")
            self.assertFalse(data["database_connected"])
    
    def test_items_endpoint_success(self):
        """Test items endpoint with successful data retrieval."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock database result
            mock_df = MagicMock()
            mock_df.empty = False
            mock_df.iterrows.return_value = [
                (0, {
                    'item_id': 4151,
                    'name': 'Abyssal whip',
                    'category': 'Weapons',
                    'membership_type': 'Members',
                    'latest_price': 2500000,
                    'data_quality': 'High',
                    'suitable_for_forecasting': True
                })
            ]
            
            mock_conn.execute.return_value.df.return_value = mock_df
            
            response = self.client.get("/items?limit=10")
            
            self.assertEqual(response.status_code, 200)
            data = response.json()
            
            self.assertIn("items", data)
            self.assertIn("total_count", data)
            self.assertEqual(len(data["items"]), 1)
            
            item = data["items"][0]
            self.assertEqual(item["item_id"], 4151)
            self.assertEqual(item["name"], "Abyssal whip")
    
    def test_items_endpoint_empty_result(self):
        """Test items endpoint with no results."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock empty database result
            mock_df = MagicMock()
            mock_df.empty = True
            mock_conn.execute.return_value.df.return_value = mock_df
            
            response = self.client.get("/items")
            
            self.assertEqual(response.status_code, 200)
            data = response.json()
            
            self.assertEqual(data["items"], [])
            self.assertEqual(data["total_count"], 0)
    
    def test_predict_endpoint_success(self):
        """Test predict endpoint with successful prediction."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock item lookup
            mock_conn.execute.return_value.fetchone.return_value = (
                "Abyssal whip", 2500000, True
            )
            
            # Mock price data
            mock_df = MagicMock()
            mock_df.empty = False
            mock_df.__getitem__.return_value.iloc.__getitem__.return_value = [
                2500000, 2480000, 2520000, 2510000, 2490000
            ]
            mock_conn.execute.return_value.df.return_value = mock_df
            
            with patch('api.main.BaselineModel') as mock_model:
                mock_predictor = MagicMock()
                mock_predictor.fit_predict.return_value = [2505000]
                mock_model.return_value = mock_predictor
                
                response = self.client.get("/predict?item_id=4151&horizon=1")
                
                self.assertEqual(response.status_code, 200)
                data = response.json()
                
                self.assertEqual(data["item_id"], 4151)
                self.assertEqual(data["item_name"], "Abyssal whip")
                self.assertEqual(data["horizon"], 1)
                self.assertIn("predictions", data)
                self.assertIn("timestamp", data)
    
    def test_predict_endpoint_item_not_found(self):
        """Test predict endpoint with nonexistent item."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock item not found
            mock_conn.execute.return_value.fetchone.return_value = None
            
            response = self.client.get("/predict?item_id=99999")
            
            self.assertEqual(response.status_code, 404)
            data = response.json()
            self.assertIn("Item 99999 not found", data["detail"])
    
    def test_predict_endpoint_unsuitable_item(self):
        """Test predict endpoint with item unsuitable for forecasting."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock item found but unsuitable
            mock_conn.execute.return_value.fetchone.return_value = (
                "Test item", 1000, False  # suitable_for_forecasting = False
            )
            
            response = self.client.get("/predict?item_id=1001")
            
            self.assertEqual(response.status_code, 400)
            data = response.json()
            self.assertIn("not suitable for forecasting", data["detail"])
    
    def test_metrics_endpoint_success(self):
        """Test metrics endpoint with successful data retrieval."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock item lookup
            mock_conn.execute.return_value.fetchone.return_value = ("Abyssal whip",)
            
            response = self.client.get("/metrics?item_id=4151")
            
            self.assertEqual(response.status_code, 200)
            data = response.json()
            
            self.assertEqual(data["item_id"], 4151)
            self.assertEqual(data["item_name"], "Abyssal whip")
            self.assertIn("mae", data)
            self.assertIn("smape", data)
            self.assertIn("n_predictions", data)
    
    def test_metrics_endpoint_item_not_found(self):
        """Test metrics endpoint with nonexistent item."""
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            
            # Mock item not found
            mock_conn.execute.return_value.fetchone.return_value = None
            
            response = self.client.get("/metrics?item_id=99999")
            
            self.assertEqual(response.status_code, 404)
    
    def test_predict_endpoint_validation(self):
        """Test predict endpoint parameter validation."""
        # Test invalid horizon
        response = self.client.get("/predict?item_id=4151&horizon=10")
        self.assertEqual(response.status_code, 422)  # Validation error
        
        # Test invalid model
        with patch('api.main.get_db_connection') as mock_db:
            mock_conn = MagicMock()
            mock_db.return_value = mock_conn
            mock_conn.execute.return_value.fetchone.return_value = (
                "Test item", 1000, True
            )
            mock_df = MagicMock()
            mock_df.empty = False
            mock_conn.execute.return_value.df.return_value = mock_df
            
            response = self.client.get("/predict?item_id=4151&model=invalid_model")
            self.assertEqual(response.status_code, 400)


if __name__ == '__main__':
    unittest.main()
