from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from src.models.churn.trainer import ChurnTrainer

def test_trainer_stores_xy():
    """Test that trainer loads and stores X, y."""
    config = Mock()
    config.data.features_table = "test_table"
    config.data.label_col = "label"
    
    # Create mock data
    mock_data = pd.DataFrame({
        "feature1": [1, 2, 3],
        "feature2": [4, 5, 6],
        "label": [0, 1, 0]
    })
    
    trainer = ChurnTrainer(config)
    
    # Mock spark.table() to return our test dataframe
    with patch('src.models.churn.trainer.SparkSession') as mock_spark_class:
        mock_spark_instance = MagicMock()
        mock_spark_class.builder.getOrCreate.return_value = mock_spark_instance
        mock_spark_instance.table.return_value.toPandas.return_value = mock_data
        
        # Simulate load_data
        X, y = trainer.load_data()
        
        # Verify X and y are stored
        assert trainer.X is not None
        assert trainer.y is not None
        assert len(trainer.X) == 3
        assert len(trainer.y) == 3

def test_trainer_evaluate_uses_stored_data():
    """Test that evaluate method uses stored X, y."""
    config = Mock()
    config.data.features_table = "test"
    config.data.label_col = "label"
    
    trainer = ChurnTrainer(config)
    
    # Mock X and y
    trainer.X = np.array([[1, 2, 3], [4, 5, 6]])
    trainer.y = np.array([0, 1])
    
    # Mock model - predict_proba must return numpy array
    model = Mock()
    model.predict_proba.return_value = np.array([[0.4, 0.6], [0.2, 0.8]])
    
    metrics = trainer.evaluate(model)
    assert "auc" in metrics