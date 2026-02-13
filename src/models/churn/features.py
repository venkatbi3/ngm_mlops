"""
Feature engineering for churn model.
Uses SQL-based templates and configurations for portability across environments.
"""

from pyspark.sql import SparkSession
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class FeatureBuilder:
    """
    Build features from Databricks UC tables using SQL templates.
    All source and target tables are in Unity Catalog.
    """
    
    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        source_catalog: str,
        source_schema: str,
        start_date: str,
        end_date: str,
        sql_dir: str = "src/models/churn/sql"
    ):
        """
        Initialize feature builder.
        
        Args:
            spark: SparkSession
            catalog: Target catalog (UC) for features
            schema: Target schema for features
            source_catalog: Source catalog containing raw data
            source_schema: Source schema containing raw data
            start_date: Start date for feature window
            end_date: End date for feature window
            sql_dir: Directory containing SQL templates
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.start_date = start_date
        self.end_date = end_date
        self.sql_dir = Path(sql_dir)
    
    def execute_sql_file(self, sql_file: str, table_name: Optional[str] = None) -> str:
        """
        Execute a SQL template file with parameter substitution.
        
        Args:
            sql_file: Name of SQL file in sql_dir
            table_name: Optional override for output table name
            
        Returns:
            Full UC table name (catalog.schema.table)
        """
        sql_path = self.sql_dir / sql_file
        
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL template not found: {sql_path}")
        
        logger.info(f"Executing SQL template: {sql_path}")
        
        # Read SQL template
        with open(sql_path, 'r') as f:
            sql_template = f.read()
        
        # Infer table name from filename if not provided
        if not table_name:
            table_name = sql_file.replace(".sql", "")
        
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        # Substitute parameters
        sql = sql_template.format(
            start_date=self.start_date,
            end_date=self.end_date,
            source_catalog=self.source_catalog,
            source_schema=self.source_schema,
            output_table=full_table_name
        )
        
        # Execute SQL
        logger.info(f"Creating feature table: {full_table_name}")
        self.spark.sql(sql)
        
        # Verify
        count = self.spark.table(full_table_name).count()
        logger.info(f"✓ Created {count:,} rows in {full_table_name}")
        
        return full_table_name
    
    def build_account_features(self) -> str:
        """Build account-level features."""
        return self.execute_sql_file("account_features.sql")
    
    def build_transaction_features(self) -> str:
        """Build transaction-level features."""
        return self.execute_sql_file("transaction_features.sql")
    
    def build_combined_features(self) -> str:
        """Build combined feature set."""
        return self.execute_sql_file("combined_features.sql")
    
    def build_all_features(self) -> str:
        """
        Build all features in sequence.
        
        Returns:
            Final combined feature table name
        """
        logger.info(f"Building features: {self.start_date} to {self.end_date}")
        logger.info(f"Source: {self.source_catalog}.{self.source_schema}")
        logger.info(f"Target: {self.catalog}.{self.schema}")
        
        # Execute SQL files in order
        self.build_account_features()
        self.build_transaction_features()
        combined = self.build_combined_features()
        
        logger.info(f"✅ All features created: {combined}")
        
        return combined


def main():
    """Main entry point for feature engineering."""
    
    from src.common.logger import setup_logging
    from src.common.config import load_model_config
    import sys
    
    setup_logging()
    
    model_key = sys.argv[1] if len(sys.argv) > 1 else "churn"
    config = load_model_config(model_key)
    
    spark = SparkSession.builder.appName(f"{model_key}-FeatureBuilder").getOrCreate()
    
    # Build features using config-driven approach
    builder = FeatureBuilder(
        spark=spark,
        catalog=config.output.catalog,
        schema=config.output.schema,
        source_catalog=config.data.source_catalog,
        source_schema=config.data.source_schema,
        start_date=config.data.start_date,
        end_date=config.data.end_date
    )
    
    feature_table = builder.build_all_features()
    print(f"\n✅ Features available at: {feature_table}")


if __name__ == "__main__":
    main()