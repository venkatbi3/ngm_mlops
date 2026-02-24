import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from src.models.base import BaseTrainer
from pyspark.sql import SparkSession

class ChurnTrainer(BaseTrainer):
    """Churn prediction model trainer."""

    def load_data(self):
        """Load features and labels from Unity Catalog."""
        spark = SparkSession.builder.getOrCreate()

        df = spark.table(self.config.data.features_table).toPandas()

        self.X = df.drop(self.config.data.label_col, axis=1)
        self.y = df[self.config.data.label_col]

        return self.X, self.y

    def train(self):
        """Train Random Forest model for churn prediction."""
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
        return model

    def evaluate(self, model):
        """Evaluate model using test set."""
        preds = model.predict_proba(self.X)[:, 1]
        auc = roc_auc_score(self.y, preds)
        return {"auc": auc}
