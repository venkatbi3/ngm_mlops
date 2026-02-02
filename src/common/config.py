import os
import yaml
from pathlib import Path
from typing import Any, Dict
from pydantic import BaseModel, Field, validator

class DataConfig(BaseModel):
    features_table: str
    label_col: str

class MetricsConfig(BaseModel):
    auc_threshold: float = Field(ge=0, le=1)

class HyperparametersConfig(BaseModel):
    n_estimators: int = Field(gt=0)
    max_depth: int = Field(gt=0)

class OutputConfig(BaseModel):
    catalog: str
    schema: str
    table: str

class ModelConfig(BaseModel):
    model_key: str
    registered_model_name: str
    trainer_class: str
    validator_class: str
    inference_class: str
    data: DataConfig
    metrics: MetricsConfig
    hyperparameters: HyperparametersConfig
    output: OutputConfig

def load_model_config(model_key: str) -> ModelConfig:
    """Load and validate model configuration."""
    config_path = Path(f"src/models/{model_key}/config.yml")
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    
    with open(config_path) as f:
        config_dict = yaml.safe_load(f)
    
    try:
        config = ModelConfig(**config_dict)
        return config
    except Exception as e:
        raise ValueError(f"Invalid config for model {model_key}: {e}") from e