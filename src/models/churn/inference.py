import logging
import pandas as pd
from pyspark.sql import SparkSession
from src.models.base import BaseInference
from src.common.logger import get_logger, log_with_context
from src.common.exceptions import DataLoadError
from datetime import datetime
from pyspark.sql.functions import col, lit

logger = get_logger(__name__)


class ChurnInference(BaseInference):

    def load_features(self):
        logger.info(f"Loading features from {self.config.data.features_table}")
        try:
            spark = SparkSession.builder.getOrCreate()
            df = spark.table(self.config.data.features_table)
            
            log_with_context(
                logger, logging.INFO,
                "Features loaded",
                {"table": self.config.data.features_table, "rows": df.count()}
            )
            
            return df
        except Exception as e:
            logger.error(f"Failed to load features: {e}", exc_info=True)
            raise DataLoadError(f"Failed to load features", table=self.config.data.features_table, context={"error": str(e)}) from e

    def score(self, model, features_df):
        logger.info("Generating predictions")
        try:
            pdf = features_df.toPandas()
            preds = model.predict_proba(pdf)[:, 1]
            pdf["churn_score"] = preds

            # Model lineage stamping
            pdf["model_name"] = self.metadata["model_name"]
            pdf["model_version"] = self.metadata["model_version"]
            pdf["run_id"] = self.metadata["run_id"]
            pdf["scored_at"] = self.metadata["scored_at"]
            
            log_with_context(
                logger, logging.INFO,
                "Predictions generated",
                {"row_count": len(pdf), "model_version": self.metadata["model_version"]}
            )

            return pdf
        except Exception as e:
            logger.error(f"Scoring failed: {e}", exc_info=True)
            raise DataLoadError(f"Scoring failed", context={"error": str(e)}) from e

    def write_output(self, predictions_pdf):
        logger.info(f"Writing predictions to {self.config.output.schema}.{self.config.output.table}")
        try:
            spark = SparkSession.builder.getOrCreate()
            
            # Add deduplication key
            predictions_pdf["score_batch_id"] = f"{datetime.utcnow().isoformat()}"
            
            spark_df = spark.createDataFrame(predictions_pdf)
            output_table = f"{self.config.output.catalog}.{self.config.output.schema}.{self.config.output.table}"
            
            # Create table if not exists
            if not spark.catalog.tableExists(output_table):
                spark_df.write.mode("overwrite").format("delta").partitionBy("scored_at").saveAsTable(output_table)
                logger.info(f"Created table: {output_table}")
            else:
                # Upsert: delete existing batch, then insert new
                spark.sql(f"""
                    DELETE FROM {output_table} 
                    WHERE score_batch_id = '{predictions_pdf['score_batch_id'].iloc[0]}'
                """)
                spark_df.write.mode("append").insertInto(output_table)
                logger.info(f"Updated table: {output_table}")
            
            log_with_context(
                logger, logging.INFO,
                "Predictions written",
                {"table": output_table, "rows": len(predictions_pdf)}
            )
        except Exception as e:
            logger.error(f"Failed to write output: {e}", exc_info=True)
            raise DataLoadError(f"Failed to write predictions", context={"table": self.config.output.table, "error": str(e)}) from e
