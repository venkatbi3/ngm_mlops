import pandas as pd
from pyspark.sql import SparkSession
from common.base_inference import BaseInference
from datetime import datetime
from pyspark.sql.functions import col, lit

class ChurnInference(BaseInference):

    def load_features(self):
        spark = SparkSession.builder.getOrCreate()

        df = spark.table(
            self.config["data"]["features_table"]
        )

        return df

    def score(self, model, features_df):
        pdf = features_df.toPandas()

        preds = model.predict_proba(pdf)[:, 1]
        pdf["churn_score"] = preds

        # 🔐 Model lineage stamping
        pdf["model_name"] = self.metadata["model_name"]
        pdf["model_version"] = self.metadata["model_version"]
        pdf["run_id"] = self.metadata["run_id"]
        pdf["scored_at"] = self.metadata["scored_at"]

        return pdf

    def write_output(self, predictions_pdf):
        spark = SparkSession.builder.getOrCreate()
        
        # Add deduplication key
        predictions_pdf["score_batch_id"] = f"{datetime.utcnow().isoformat()}"
        
        spark_df = spark.createDataFrame(predictions_pdf)
        output_table = f"{self.config['output']['catalog']}.{self.config['output']['schema']}.{self.config['output']['table']}"
        
        # Create table if not exists
        if not spark.catalog.tableExists(output_table):
            spark_df.write.mode("overwrite").format("delta").partitionBy("scored_at").saveAsTable(output_table)
        else:
            # Upsert: delete existing batch, then insert new
            spark.sql(f"""
                DELETE FROM {output_table} 
                WHERE score_batch_id = '{predictions_pdf['score_batch_id'].iloc[0]}'
            """)
            spark_df.write.mode("append").insertInto(output_table)
