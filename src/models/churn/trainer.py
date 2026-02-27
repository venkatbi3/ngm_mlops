import logging
import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from src.models.base import BaseTrainer
from src.common.logger import get_logger, log_with_context
from src.common.exceptions import DataLoadError, ModelTrainingError
from pyspark.sql import SparkSession

logger = get_logger(__name__)


class ChurnTrainer(BaseTrainer):
    """Churn prediction model trainer."""

    def load_data(self):
        """Load features and labels from Unity Catalog."""
        logger.info(f"Loading data from {self.config.data.features_table}")
        try:
            spark = SparkSession.builder.getOrCreate()
            df = spark.table(self.config.data.features_table).toPandas()
            
            if df.empty:
                raise DataLoadError("DataFrame is empty", table=self.config.data.features_table)
            
            self.X = df.drop(self.config.data.label_col, axis=1)
            self.y = df[self.config.data.label_col]
            
            log_with_context(
                logger, logging.INFO,
                "Data loaded successfully",
                {"samples": len(self.X), "features": self.X.shape[1], "table": self.config.data.features_table}
            )
            
            return self.X, self.y
        except DataLoadError:
            raise
        except Exception as e:
            logger.error(f"Failed to load data: {e}", exc_info=True)
            raise DataLoadError(f"Failed to load from {self.config.data.features_table}", table=self.config.data.features_table, context={"error": str(e)}) from e

    def train(self):
        """Train Random Forest model for churn prediction."""
        logger.info("Training Random Forest model")
        try:
            hp = self.config.hyperparameters

            model = RandomForestClassifier(
                n_estimators=hp.n_estimators,
                max_depth=hp.max_depth,
                min_samples_split=getattr(hp, 'min_samples_split', 2),
                min_samples_leaf=getattr(hp, 'min_samples_leaf', 1),
                random_state=getattr(hp, 'random_state', 42),
                n_jobs=-1
            )
            model.fit(self.X, self.y)
            
            log_with_context(
                logger, logging.INFO,
                "Model training completed",
                {"model_type": "RandomForest", "n_estimators": hp.n_estimators, "max_depth": hp.max_depth}
            )
            
            return model
        except Exception as e:
            logger.error(f"Training failed: {e}", exc_info=True)
            raise ModelTrainingError(f"Training failed", context={"error": str(e)}) from e

    def evaluate(self, model):
        """Evaluate model using test set."""
        logger.info("Evaluating model")
        try:
            preds = model.predict_proba(self.X)[:, 1]
            auc = roc_auc_score(self.y, preds)
            
            metrics = {"auc": auc}
            
            log_with_context(
                logger, logging.INFO,
                "Model evaluation completed",
                {"auc": round(auc, 4), "threshold": self.config.metrics.auc_threshold}
            )
            
            return metrics
        except Exception as e:
            logger.error(f"Evaluation failed: {e}", exc_info=True)
            raise ModelTrainingError(f"Evaluation failed", context={"error": str(e)}) from e
