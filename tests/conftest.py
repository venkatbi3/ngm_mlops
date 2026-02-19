"""
Shared pytest fixtures and configuration for all tests.

This file is automatically discovered by pytest and makes fixtures
available to all test files without needing to import them.
"""

import pytest
import os
import sys
from unittest.mock import Mock, MagicMock
import pandas as pd
import numpy as np


# Add src to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@pytest.fixture(scope="session")
def test_env():
    """Test environment configuration."""
    return {
        'env': 'test',
        'catalog': 'test_catalog',
        'schema': 'ngm_mlops_test',
        'mlflow_experiment': '/Shared/ngm_mlops/test'
    }


@pytest.fixture(scope="session")
def mock_databricks_env(monkeypatch):
    """Mock Databricks environment variables."""
    monkeypatch.setenv('DATABRICKS_HOST', 'https://test.databricks.com')
    monkeypatch.setenv('DATABRICKS_TOKEN', 'test_token')
    monkeypatch.setenv('MLFLOW_TRACKING_URI', 'databricks')


@pytest.fixture
def sample_churn_data():
    """Generate realistic churn prediction dataset."""
    np.random.seed(42)
    n_samples = 1000
    
    return pd.DataFrame({
        'customer_id': range(n_samples),
        'age': np.random.randint(18, 80, n_samples),
        'tenure_months': np.random.randint(1, 120, n_samples),
        'monthly_charges': np.random.uniform(20, 200, n_samples),
        'total_charges': np.random.uniform(100, 10000, n_samples),
        'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], n_samples),
        'payment_method': np.random.choice(['Electronic check', 'Credit card', 'Bank transfer'], n_samples),
        'internet_service': np.random.choice(['DSL', 'Fiber optic', 'No'], n_samples),
        'online_security': np.random.choice(['Yes', 'No', 'No internet service'], n_samples),
        'tech_support': np.random.choice(['Yes', 'No', 'No internet service'], n_samples),
        'churn': np.random.choice([0, 1], n_samples, p=[0.73, 0.27])
    })


@pytest.fixture
def sample_fraud_data():
    """Generate realistic fraud detection dataset."""
    np.random.seed(42)
    n_samples = 1000
    
    return pd.DataFrame({
        'transaction_id': range(n_samples),
        'customer_id': np.random.randint(1, 500, n_samples),
        'amount': np.random.lognormal(4, 1.5, n_samples),
        'merchant_category': np.random.choice(['retail', 'food', 'travel', 'online', 'gas'], n_samples),
        'merchant_id': np.random.randint(1, 200, n_samples),
        'time_since_last_transaction': np.random.exponential(2, n_samples),
        'num_transactions_24h': np.random.poisson(3, n_samples),
        'num_transactions_7d': np.random.poisson(15, n_samples),
        'is_international': np.random.choice([0, 1], n_samples, p=[0.92, 0.08]),
        'device_type': np.random.choice(['mobile', 'desktop', 'tablet'], n_samples),
        'transaction_hour': np.random.randint(0, 24, n_samples),
        'is_fraud': np.random.choice([0, 1], n_samples, p=[0.985, 0.015])
    })


@pytest.fixture
def mock_spark_session():
    """Mock Spark session for unit tests."""
    spark = MagicMock()
    spark.table.return_value = Mock()
    spark.createDataFrame.return_value = Mock()
    return spark


@pytest.fixture
def mock_mlflow():
    """Mock MLflow for tests that don't need actual tracking."""
    with pytest.MonkeyPatch.context() as m:
        # Mock MLflow functions
        m.setattr('mlflow.start_run', Mock())
        m.setattr('mlflow.end_run', Mock())
        m.setattr('mlflow.log_param', Mock())
        m.setattr('mlflow.log_params', Mock())
        m.setattr('mlflow.log_metric', Mock())
        m.setattr('mlflow.log_metrics', Mock())
        m.setattr('mlflow.log_artifact', Mock())
        m.setattr('mlflow.sklearn.log_model', Mock())
        
        yield


@pytest.fixture
def trained_sklearn_model():
    """Fixture providing a trained scikit-learn model."""
    from sklearn.ensemble import RandomForestClassifier
    
    X = np.random.rand(100, 5)
    y = np.random.randint(0, 2, 100)
    
    model = RandomForestClassifier(n_estimators=10, random_state=42, max_depth=3)
    model.fit(X, y)
    
    return model


@pytest.fixture
def temp_data_dir(tmp_path):
    """Create temporary directory for test data."""
    data_dir = tmp_path / "test_data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture
def mock_config():
    """Mock configuration object."""
    return {
        'model': {
            'name': 'test_model',
            'version': '1.0.0',
            'target_column': 'target'
        },
        'training': {
            'test_size': 0.2,
            'random_state': 42,
            'n_estimators': 100
        },
        'data': {
            'input_table': 'test_catalog.test_schema.input_data',
            'output_table': 'test_catalog.test_schema.predictions'
        },
        'validation': {
            'min_auc': 0.70,
            'max_null_percentage': 0.05
        }
    }


# Pytest hooks for test organization
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests that run fast without dependencies"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests requiring Databricks"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take more than 1 minute"
    )


def pytest_collection_modifyitems(config, items):
    """Automatically mark tests based on their location."""
    for item in items:
        # Auto-mark unit tests
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        
        # Auto-mark integration tests
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
        
        # Skip integration tests if no Databricks connection
        if "integration" in str(item.fspath) and not os.getenv('DATABRICKS_HOST'):
            item.add_marker(pytest.mark.skip(
                reason="Integration tests require DATABRICKS_HOST environment variable"
            ))


# Session-level fixtures for expensive setup
@pytest.fixture(scope="session")
def spark_session():
    """Create a real Spark session for integration tests (session-scoped)."""
    if not os.getenv('DATABRICKS_HOST'):
        pytest.skip("Spark session requires Databricks environment")
    
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("NGM MLOps Tests") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="session")
def mlflow_test_client():
    """Create MLflow client for integration tests (session-scoped)."""
    if not os.getenv('DATABRICKS_HOST'):
        pytest.skip("MLflow client requires Databricks environment")
    
    import mlflow
    
    mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI', 'databricks'))
    mlflow.set_experiment("/Shared/ngm_mlops/integration_tests")
    
    client = mlflow.tracking.MlflowClient()
    
    yield client
    
    # Cleanup: delete test experiment runs
    experiment = mlflow.get_experiment_by_name("/Shared/ngm_mlops/integration_tests")
    if experiment:
        runs = client.search_runs(experiment.experiment_id)
        for run in runs:
            client.delete_run(run.info.run_id)
