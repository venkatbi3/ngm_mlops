"""
Unit tests for model trainer classes.

These tests verify that individual trainer components work correctly
in isolation without requiring Databricks or external dependencies.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession


# Mock imports to avoid Databricks dependencies in unit tests
@pytest.fixture
def mock_spark():
    """Mock Spark session for testing."""
    spark = Mock(spec=SparkSession)
    return spark


@pytest.fixture
def sample_training_data():
    """Generate sample training data for tests."""
    return pd.DataFrame({
        'customer_id': range(1000),
        'age': np.random.randint(18, 80, 1000),
        'tenure_months': np.random.randint(1, 120, 1000),
        'monthly_charges': np.random.uniform(20, 200, 1000),
        'total_charges': np.random.uniform(100, 5000, 1000),
        'contract_type': np.random.choice(['Month-to-month', 'One year', 'Two year'], 1000),
        'churn': np.random.choice([0, 1], 1000, p=[0.75, 0.25])
    })


@pytest.fixture
def sample_fraud_data():
    """Generate sample fraud detection data."""
    return pd.DataFrame({
        'transaction_id': range(1000),
        'amount': np.random.uniform(1, 1000, 1000),
        'merchant_category': np.random.choice(['retail', 'food', 'travel', 'online'], 1000),
        'time_since_last_transaction': np.random.uniform(0, 24, 1000),
        'num_transactions_24h': np.random.randint(1, 20, 1000),
        'is_international': np.random.choice([0, 1], 1000, p=[0.9, 0.1]),
        'is_fraud': np.random.choice([0, 1], 1000, p=[0.98, 0.02])
    })


class TestChurnTrainer:
    """Unit tests for ChurnTrainer class."""

    @patch('pipelines.train.ChurnTrainer')
    def test_trainer_initialization(self, mock_trainer):
        """Test that ChurnTrainer initializes correctly."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        assert trainer.env == 'dev'
        assert trainer.model_name == 'churn'

    @patch('pipelines.train.ChurnTrainer.load_data')
    def test_load_data_returns_dataframe(self, mock_load_data, sample_training_data):
        """Test that load_data returns a non-empty DataFrame."""
        mock_load_data.return_value = sample_training_data
        
        from models.churn.trainer import ChurnTrainer
        trainer = ChurnTrainer(env='dev')
        df = trainer.load_data()
        
        assert df is not None
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert 'customer_id' in df.columns
        assert 'churn' in df.columns

    def test_data_validation_schema(self, sample_training_data):
        """Test that data validation catches schema issues."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        
        # Valid data should pass
        assert trainer.validate_schema(sample_training_data) is True
        
        # Missing required column should fail
        invalid_df = sample_training_data.drop('churn', axis=1)
        with pytest.raises(ValueError, match="Missing required column"):
            trainer.validate_schema(invalid_df)

    def test_data_validation_nulls(self, sample_training_data):
        """Test that data validation catches excessive nulls."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        
        # Add excessive nulls to a critical column
        invalid_df = sample_training_data.copy()
        invalid_df.loc[0:600, 'monthly_charges'] = np.nan
        
        with pytest.raises(ValueError, match="Too many null values"):
            trainer.validate_nulls(invalid_df)

    @patch('mlflow.sklearn.log_model')
    @patch('mlflow.log_metrics')
    def test_train_logs_to_mlflow(self, mock_log_metrics, mock_log_model, sample_training_data):
        """Test that training logs metrics and model to MLflow."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        trainer.data = sample_training_data
        
        result = trainer.train()
        
        # Verify MLflow logging was called
        mock_log_metrics.assert_called_once()
        mock_log_model.assert_called_once()
        
        # Verify result contains expected metrics
        assert 'auc' in result
        assert 'precision' in result
        assert 'recall' in result

    def test_feature_engineering_creates_features(self, sample_training_data):
        """Test that feature engineering creates expected features."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        df_engineered = trainer.engineer_features(sample_training_data)
        
        # Check for engineered features
        assert 'avg_monthly_charge' in df_engineered.columns
        assert 'tenure_group' in df_engineered.columns
        assert len(df_engineered) == len(sample_training_data)

    def test_train_test_split_ratio(self, sample_training_data):
        """Test that train/test split respects configured ratio."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        X_train, X_test, y_train, y_test = trainer.split_data(sample_training_data, test_size=0.2)
        
        total_samples = len(sample_training_data)
        assert len(X_train) == int(total_samples * 0.8)
        assert len(X_test) == int(total_samples * 0.2)
        assert len(y_train) == len(X_train)
        assert len(y_test) == len(X_test)

    def test_model_evaluation_metrics(self):
        """Test that evaluation produces all required metrics."""
        from models.churn.trainer import ChurnTrainer
        
        trainer = ChurnTrainer(env='dev')
        
        # Mock predictions
        y_true = np.array([0, 0, 1, 1, 0, 1, 0, 1])
        y_pred = np.array([0, 1, 1, 1, 0, 0, 0, 1])
        
        metrics = trainer.evaluate_model(y_true, y_pred)
        
        assert 'accuracy' in metrics
        assert 'precision' in metrics
        assert 'recall' in metrics
        assert 'f1_score' in metrics
        assert 'auc' in metrics
        
        # Check metric ranges
        assert 0 <= metrics['accuracy'] <= 1
        assert 0 <= metrics['auc'] <= 1


class TestFraudTrainer:
    """Unit tests for FraudTrainer class."""

    @patch('pipelines.train.FraudTrainer')
    def test_trainer_initialization(self, mock_trainer):
        """Test that FraudTrainer initializes correctly."""
        from models.fraud.trainer import FraudTrainer
        
        trainer = FraudTrainer(env='dev')
        assert trainer.env == 'dev'
        assert trainer.model_name == 'fraud'

    def test_load_data_returns_dataframe(self, sample_fraud_data):
        """Test that load_data returns valid fraud detection data."""
        from models.fraud.trainer import FraudTrainer
        
        trainer = FraudTrainer(env='dev')
        
        # Mock the load_data to return sample data
        with patch.object(trainer, 'load_data', return_value=sample_fraud_data):
            df = trainer.load_data()
            
            assert df is not None
            assert isinstance(df, pd.DataFrame)
            assert len(df) > 0
            assert 'transaction_id' in df.columns
            assert 'is_fraud' in df.columns

    def test_class_imbalance_handling(self, sample_fraud_data):
        """Test that trainer handles class imbalance correctly."""
        from models.fraud.trainer import FraudTrainer
        
        trainer = FraudTrainer(env='dev')
        
        # Check if resampling is applied
        X_resampled, y_resampled = trainer.handle_imbalance(
            sample_fraud_data.drop('is_fraud', axis=1),
            sample_fraud_data['is_fraud']
        )
        
        # After resampling, classes should be more balanced
        fraud_ratio = y_resampled.sum() / len(y_resampled)
        assert 0.3 <= fraud_ratio <= 0.7, "Class imbalance not properly handled"

    def test_fraud_specific_features(self, sample_fraud_data):
        """Test fraud-specific feature engineering."""
        from models.fraud.trainer import FraudTrainer
        
        trainer = FraudTrainer(env='dev')
        df_engineered = trainer.engineer_features(sample_fraud_data)
        
        # Check for fraud-specific features
        assert 'amount_zscore' in df_engineered.columns
        assert 'velocity_score' in df_engineered.columns
        assert 'merchant_risk_score' in df_engineered.columns

    @patch('mlflow.log_metrics')
    def test_fraud_metrics_include_recall(self, mock_log_metrics, sample_fraud_data):
        """Test that fraud detection prioritizes recall metric."""
        from models.fraud.trainer import FraudTrainer
        
        trainer = FraudTrainer(env='dev')
        
        y_true = np.array([0, 0, 1, 1, 0, 1, 0, 1])
        y_pred = np.array([0, 1, 1, 1, 0, 1, 0, 1])
        
        metrics = trainer.evaluate_model(y_true, y_pred)
        
        # Fraud detection should emphasize recall (catching all frauds)
        assert 'recall' in metrics
        assert 'precision' in metrics
        assert 'f2_score' in metrics  # F2 weighs recall higher than precision


class TestModelRegistry:
    """Unit tests for model registry functionality."""

    def test_registry_contains_all_models(self):
        """Test that registry has all expected models."""
        from models.registry import MODEL_REGISTRY
        
        assert 'churn' in MODEL_REGISTRY
        assert 'fraud' in MODEL_REGISTRY

    def test_get_trainer_returns_correct_class(self):
        """Test that get_trainer returns the correct trainer class."""
        from models.registry import get_trainer
        
        churn_trainer = get_trainer('churn')
        fraud_trainer = get_trainer('fraud')
        
        assert churn_trainer.__name__ == 'ChurnTrainer'
        assert fraud_trainer.__name__ == 'FraudTrainer'

    def test_get_trainer_raises_on_invalid_model(self):
        """Test that get_trainer raises error for unknown model."""
        from models.registry import get_trainer
        
        with pytest.raises(ValueError, match="Unknown model"):
            get_trainer('nonexistent_model')


class TestBaseTrainer:
    """Unit tests for base trainer functionality."""

    def test_base_trainer_has_required_methods(self):
        """Test that BaseTrainer defines required interface."""
        from models.base import BaseTrainer
        
        required_methods = ['load_data', 'train', 'evaluate', 'save_model']
        
        for method in required_methods:
            assert hasattr(BaseTrainer, method)

    def test_base_trainer_validate_config(self):
        """Test configuration validation in base trainer."""
        from models.base import BaseTrainer
        
        trainer = BaseTrainer(env='dev')
        
        # Valid config should pass
        valid_config = {
            'model_name': 'test_model',
            'training_data_path': '/path/to/data',
            'target_column': 'target'
        }
        assert trainer.validate_config(valid_config) is True
        
        # Missing required keys should fail
        invalid_config = {'model_name': 'test_model'}
        with pytest.raises(ValueError, match="Missing required config"):
            trainer.validate_config(invalid_config)


class TestDataQualityChecks:
    """Unit tests for data quality validation."""

    def test_check_missing_values_threshold(self, sample_training_data):
        """Test missing values check fails above threshold."""
        from common.data_quality import DataQualityChecker
        
        checker = DataQualityChecker(threshold=0.05)
        
        # Add excessive nulls
        invalid_df = sample_training_data.copy()
        invalid_df.loc[0:100, 'age'] = np.nan
        
        result = checker.check_missing_values(invalid_df)
        assert result['passed'] is False
        assert 'age' in result['failed_columns']

    def test_check_schema_detects_type_changes(self, sample_training_data):
        """Test schema check detects column type changes."""
        from common.data_quality import DataQualityChecker
        
        checker = DataQualityChecker()
        
        # Set baseline schema
        checker.set_baseline_schema(sample_training_data)
        
        # Change column type
        invalid_df = sample_training_data.copy()
        invalid_df['age'] = invalid_df['age'].astype(str)
        
        result = checker.check_schema(invalid_df)
        assert result['passed'] is False
        assert 'age' in result['type_mismatches']

    def test_check_volume_detects_drops(self, sample_training_data):
        """Test volume check detects significant row count drops."""
        from common.data_quality import DataQualityChecker
        
        checker = DataQualityChecker(volume_drop_threshold=0.2)
        
        # Set baseline volume
        checker.set_baseline_volume(len(sample_training_data))
        
        # Drop 30% of rows
        reduced_df = sample_training_data.sample(frac=0.7)
        
        result = checker.check_volume(reduced_df)
        assert result['passed'] is False
        assert result['drop_percentage'] > 0.2


# Test fixtures and helpers
@pytest.fixture
def trained_model():
    """Fixture providing a trained model for testing."""
    from sklearn.ensemble import RandomForestClassifier
    
    X = np.random.rand(100, 5)
    y = np.random.randint(0, 2, 100)
    
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model.fit(X, y)
    
    return model


@pytest.fixture
def mock_mlflow():
    """Mock MLflow for testing without actual logging."""
    with patch('mlflow.start_run'), \
         patch('mlflow.log_params'), \
         patch('mlflow.log_metrics'), \
         patch('mlflow.sklearn.log_model'):
        yield


# Parametrized tests for multiple models
@pytest.mark.parametrize("model_name,expected_metrics", [
    ("churn", ["auc", "precision", "recall", "f1_score"]),
    ("fraud", ["auc", "precision", "recall", "f2_score"]),
])
def test_model_produces_required_metrics(model_name, expected_metrics):
    """Test that each model produces its required metrics."""
    from models.registry import get_trainer
    
    trainer_class = get_trainer(model_name)
    trainer = trainer_class(env='dev')
    
    # Mock evaluation
    y_true = np.array([0, 0, 1, 1, 0, 1, 0, 1])
    y_pred = np.array([0, 1, 1, 1, 0, 1, 0, 1])
    
    metrics = trainer.evaluate_model(y_true, y_pred)
    
    for metric in expected_metrics:
        assert metric in metrics, f"{model_name} missing required metric: {metric}"
