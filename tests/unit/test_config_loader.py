import pytest
from src.common.config import load_model_config

def test_load_churn_config():
    config = load_model_config("churn")
    assert config.model_key == "churn"
    assert config.registered_model_name == "churn_model"
    assert config.data.features_table
    assert config.metrics.auc_threshold == 0.75

def test_invalid_config():
    with pytest.raises(FileNotFoundError):
        load_model_config("nonexistent_model")

def test_config_validation():
    """Test that invalid configs raise FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_model_config("bad_config")