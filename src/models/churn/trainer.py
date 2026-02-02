import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from common.base_trainer import BaseTrainer
from pyspark.sql import SparkSession

class ChurnTrainer(BaseTrainer):

    def load_data(self):
        spark = SparkSession.builder.getOrCreate()

        df = spark.table(self.config["data"]["features_table"]).toPandas()

        X = df.drop(self.config["data"]["label_col"], axis=1)
        y = df[self.config["data"]["label_col"]]

        return X, y

    def train(self):
        hp = self.config["hyperparameters"]

        model = RandomForestClassifier(
            n_estimators=hp["n_estimators"],
            max_depth=hp["max_depth"],
            random_state=42
        )
        model.fit(self.X, self.y)
        return model

    def evaluate(self, model):
        preds = model.predict_proba(self.X)[:, 1]
        auc = roc_auc_score(self.y, preds)
        return {"auc": auc}
