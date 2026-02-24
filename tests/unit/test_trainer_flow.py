from unittest.mock import Mock, patch, MagicMock
from src.models.churn.trainer import ChurnTrainer

def test_trainer_stores_xy():
    """Test that trainer loads and stores X, y."""
    config = Mock()
    config.data.features_table = "test_table"
    config.data.label_col = "label"
    
    trainer = ChurnTrainer(config)
    
    with patch('pyspark.sql.SparkSession') as mock_spark:
        mock_df = MagicMock()
        mock_df.drop.return_value = MagicMock()  # X
        
        # Simulate load_data
        X, y = trainer.load_data()
        
        # Verify X and y are stored
        assert trainer.X is not None
        assert trainer.y is not None

def test_trainer_evaluate_uses_stored_data():
    """Test that evaluate method uses stored X, y."""
    config = Mock()
    config.data.features_table = "test"
    config.data.label_col = "label"
    
    trainer = ChurnTrainer(config)
    
    # Mock X and y
    trainer.X = [[1, 2, 3], [4, 5, 6]]
    trainer.y = [0, 1]
    
    # Mock model
    model = Mock()
    model.predict_proba.return_value = [[0.4, 0.6], [0.2, 0.8]]
    
    metrics = trainer.evaluate(model)
    assert "auc" in metrics