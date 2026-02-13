"""
MLflow Model Registry utilities.
Handles model registration, versioning, and promotion.
"""
import logging
from typing import Optional, Dict, List
from mlflow.tracking import MlflowClient
from mlflow.entities.model_registry import ModelVersion

logger = logging.getLogger(__name__)


class ModelRegistry:
    """Wrapper around MLflow Model Registry."""
    
    def __init__(self):
        self.client = MlflowClient()
    
    def register_model(self, run_id: str, artifact_path: str, model_name: str) -> str:
        """
        Register a model from a run.
        
        Args:
            run_id: MLflow run ID
            artifact_path: Path to model artifact (e.g., 'model')
            model_name: Name for registered model
            
        Returns:
            Model URI
        """
        model_uri = f"runs:/{run_id}/{artifact_path}"
        
        try:
            mv = self.client.create_model_version(
                name=model_name,
                source=model_uri,
                run_id=run_id
            )
            logger.info(f"Registered {model_name} version {mv.version}")
            return model_uri
        except Exception as e:
            logger.error(f"Failed to register model: {e}")
            raise
    
    def promote_model(self, model_name: str, version: str, alias: str) -> None:
        """
        Promote model to an alias (Champion, Challenger, Archived).
        
        Args:
            model_name: Registered model name
            version: Model version number
            alias: Alias to assign (e.g., 'Champion')
        """
        try:
            self.client.set_registered_model_alias(model_name, alias, version)
            logger.info(f"Promoted {model_name} v{version} to {alias}")
        except Exception as e:
            logger.error(f"Failed to promote model: {e}")
            raise
    
    def get_champion(self, model_name: str) -> Optional[ModelVersion]:
        """Get current Champion version."""
        try:
            return self.client.get_model_version_by_alias(model_name, "Champion")
        except:
            return None
    
    def get_challenger(self, model_name: str) -> Optional[ModelVersion]:
        """Get current Challenger version."""
        try:
            return self.client.get_model_version_by_alias(model_name, "Challenger")
        except:
            return None
    
    def list_versions(self, model_name: str) -> List[ModelVersion]:
        """List all versions of a model."""
        try:
            return self.client.search_model_versions(f"name='{model_name}'")
        except Exception as e:
            logger.error(f"Failed to list versions for {model_name}: {e}")
            return []
    
    def archive_old_champion(self, model_name: str, new_champion_version: str) -> None:
        """
        Archive the current Champion and promote new one.
        
        Args:
            model_name: Registered model name
            new_champion_version: Version to promote to Champion
        """
        current_champion = self.get_champion(model_name)
        
        if current_champion:
            # Archive old champion
            self.promote_model(model_name, current_champion.version, "Archived")
            logger.info(f"Archived {model_name} v{current_champion.version}")
        
        # Promote new champion
        self.promote_model(model_name, new_champion_version, "Champion")
        logger.info(f"Promoted {model_name} v{new_champion_version} to Champion")
    
    def compare_versions(self, model_name: str, version1: str, version2: str) -> Dict:
        """
        Compare two model versions.
        
        Args:
            model_name: Registered model name
            version1: First version
            version2: Second version
            
        Returns:
            Dictionary with comparison results
        """
        try:
            mv1 = self.client.get_model_version(model_name, version1)
            mv2 = self.client.get_model_version(model_name, version2)
            
            return {
                "version1": {
                    "version": version1,
                    "stage": mv1.current_stage,
                    "status": mv1.status,
                    "created_at": mv1.creation_timestamp
                },
                "version2": {
                    "version": version2,
                    "stage": mv2.current_stage,
                    "status": mv2.status,
                    "created_at": mv2.creation_timestamp
                }
            }
        except Exception as e:
            logger.error(f"Failed to compare versions: {e}")
            return {}