import mlflow

with mlflow.start_run(run_name="feature_engineering"):
    # Log parameters
    mlflow.log_param("source", "Databricks Unity Catalog")
    mlflow.log_param("target", "Databricks Delta")
    mlflow.log_param("start_date", start_date)
    mlflow.log_param("end_date", end_date)
    
    # Build features
    builder = HybridFeatureBuilder(...)
    table = builder.build_all_features()
    
    # Log metrics
    count = spark.table(table).count()
    mlflow.log_metric("feature_count", count)
    
    # Log table location
    mlflow.log_param("feature_table", table)
    
    # Log data quality
    null_counts = spark.sql(f"""
        SELECT 
            SUM(CASE WHEN BalanceSlope IS NULL THEN 1 ELSE 0 END) as null_balance_slope
        FROM {table}
    """).first()
    
    mlflow.log_metric("null_balance_slope", null_counts['null_balance_slope'])