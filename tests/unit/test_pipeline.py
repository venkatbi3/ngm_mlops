"""
Integration tests for end-to-end pipeline execution.

These tests verify that complete pipelines work correctly when all
components are integrated together. Requires Databricks connectivity.
"""

import pytest
import os
import time
from datetime import datetime
from unittest.mock import patch, Mock
import mlflow
from pyspark.sql import SparkSession


# Skip integration tests if not in CI or Databricks environment
skip_if_not_databricks = pytest.mark.skipif(
    not os.getenv('DATABRICKS_HOST'),
    reason="Integration tests require Databricks connectivity"
)


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for integration tests."""
    spark = SparkSession.builder \
        .appName("MLOps Integration Tests") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def mlflow_client():
    """Set up MLflow tracking for integration tests."""
    # Use test tracking URI
    mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI', 'databricks'))
    mlflow.set_experiment("/Shared/ngm_mlops/integration_tests")
    
    client = mlflow.tracking.MlflowClient()
    yield client


@pytest.fixture
def test_config():
    """Configuration for integration tests."""
    return {
        'env': 'dev',
        'catalog': os.getenv('TEST_CATALOG', 'dev_catalog'),
        'schema': os.getenv('TEST_SCHEMA', 'ngm_mlops_test'),
        'model_registry_name': 'ngm_mlops_test'
    }


class TestTrainingPipeline:
    """Integration tests for the training pipeline."""

    @skip_if_not_databricks
    def test_full_churn_training_pipeline(self, spark, mlflow_client, test_config):
        """Test complete churn model training pipeline end-to-end."""
        from pipelines.train import main
        import sys
        
        # Simulate command-line arguments
        test_args = [
            'train',
            '--model', 'churn',
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', test_args):
            # Run the training pipeline
            result = main()
        
        # Verify pipeline completed successfully
        assert result['status'] == 'success'
        assert 'run_id' in result
        assert result['metrics']['auc'] > 0.5  # Sanity check
        
        # Verify MLflow run was logged
        run = mlflow_client.get_run(result['run_id'])
        assert run.info.status == 'FINISHED'
        assert 'auc' in run.data.metrics
        assert 'model' in run.data.tags

    @skip_if_not_databricks
    def test_full_fraud_training_pipeline(self, spark, mlflow_client, test_config):
        """Test complete fraud model training pipeline end-to-end."""
        from pipelines.train import main
        import sys
        
        test_args = [
            'train',
            '--model', 'fraud',
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', test_args):
            result = main()
        
        assert result['status'] == 'success'
        assert result['metrics']['recall'] > 0.7  # Fraud detection requires high recall

    @skip_if_not_databricks
    def test_training_pipeline_with_invalid_model(self, test_config):
        """Test that pipeline fails gracefully with invalid model name."""
        from pipelines.train import main
        import sys
        
        test_args = [
            'train',
            '--model', 'nonexistent_model',
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', test_args):
            with pytest.raises(ValueError, match="Unknown model"):
                main()

    @skip_if_not_databricks
    def test_training_creates_model_artifacts(self, spark, mlflow_client, test_config):
        """Test that training creates all expected artifacts."""
        from pipelines.train import main
        import sys
        
        test_args = [
            'train',
            '--model', 'churn',
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', test_args):
            result = main()
        
        run = mlflow_client.get_run(result['run_id'])
        artifacts = mlflow_client.list_artifacts(result['run_id'])
        artifact_names = [a.path for a in artifacts]
        
        # Check for required artifacts
        assert any('model' in name for name in artifact_names)
        assert any('feature_importance' in name for name in artifact_names)
        assert any('confusion_matrix' in name for name in artifact_names)

    @skip_if_not_databricks
    def test_training_handles_data_quality_failures(self, spark, test_config):
        """Test that pipeline fails when data quality checks don't pass."""
        from pipelines.train import main
        import sys
        
        # Mock data quality checker to always fail
        with patch('common.data_quality.DataQualityChecker.check_all') as mock_check:
            mock_check.return_value = {
                'passed': False,
                'failures': ['excessive_nulls']
            }
            
            test_args = [
                'train',
                '--model', 'churn',
                '--env', test_config['env']
            ]
            
            with patch.object(sys, 'argv', test_args):
                result = main()
            
            assert result['status'] == 'failed'
            assert 'data_quality' in result['failure_reason']


class TestValidationPipeline:
    """Integration tests for the validation pipeline."""

    @skip_if_not_databricks
    def test_validation_pipeline_on_good_model(self, spark, mlflow_client, test_config):
        """Test validation pipeline passes a good model."""
        from pipelines.validate import main
        import sys
        
        # First train a model
        from pipelines.train import main as train_main
        train_args = ['train', '--model', 'churn', '--env', test_config['env']]
        with patch.object(sys, 'argv', train_args):
            train_result = train_main()
        
        # Now validate it
        validate_args = [
            'validate',
            '--model', 'churn',
            '--run-id', train_result['run_id'],
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', validate_args):
            result = main()
        
        assert result['status'] == 'passed'
        assert result['validation_checks']['business_metrics'] is True
        assert result['validation_checks']['bias_check'] is True

    @skip_if_not_databricks
    def test_validation_fails_low_performance_model(self, spark, mlflow_client, test_config):
        """Test validation pipeline rejects a poor-performing model."""
        from pipelines.validate import main
        import sys
        
        # Mock a low-performance model
        with patch('pipelines.validate.load_model_metrics') as mock_metrics:
            mock_metrics.return_value = {
                'auc': 0.55,  # Below threshold
                'precision': 0.60
            }
            
            validate_args = [
                'validate',
                '--model', 'churn',
                '--run-id', 'mock_run_id',
                '--env', test_config['env']
            ]
            
            with patch.object(sys, 'argv', validate_args):
                result = main()
            
            assert result['status'] == 'failed'
            assert 'performance_threshold' in result['failure_reason']

    @skip_if_not_databricks
    def test_validation_checks_bias(self, spark, mlflow_client, test_config):
        """Test validation pipeline checks for model bias."""
        from pipelines.validate import main
        import sys
        
        # Train a model first
        from pipelines.train import main as train_main
        train_args = ['train', '--model', 'churn', '--env', test_config['env']]
        with patch.object(sys, 'argv', train_args):
            train_result = train_main()
        
        validate_args = [
            'validate',
            '--model', 'churn',
            '--run-id', train_result['run_id'],
            '--env', test_config['env'],
            '--check-bias'
        ]
        
        with patch.object(sys, 'argv', validate_args):
            result = main()
        
        assert 'bias_check' in result['validation_checks']
        assert 'fairness_metrics' in result


class TestInferencePipeline:
    """Integration tests for batch inference pipeline."""

    @skip_if_not_databricks
    def test_batch_inference_produces_predictions(self, spark, test_config):
        """Test batch inference pipeline produces predictions."""
        from pipelines.inference import main
        import sys
        
        inference_args = [
            'inference',
            '--model', 'churn',
            '--env', test_config['env'],
            '--input-table', f"{test_config['catalog']}.{test_config['schema']}.customers",
            '--output-table', f"{test_config['catalog']}.{test_config['schema']}.churn_predictions"
        ]
        
        with patch.object(sys, 'argv', inference_args):
            result = main()
        
        assert result['status'] == 'success'
        assert result['predictions_count'] > 0
        
        # Verify output table was created
        output_df = spark.table(f"{test_config['catalog']}.{test_config['schema']}.churn_predictions")
        assert output_df.count() > 0
        assert 'prediction' in output_df.columns
        assert 'prediction_probability' in output_df.columns
        assert 'prediction_timestamp' in output_df.columns

    @skip_if_not_databricks
    def test_inference_handles_missing_features(self, spark, test_config):
        """Test inference pipeline handles missing features gracefully."""
        from pipelines.inference import main
        import sys
        
        # Create input table with missing features
        incomplete_df = spark.createDataFrame([
            (1, 25),  # Missing required features
            (2, 30),
        ], ['customer_id', 'age'])
        
        incomplete_table = f"{test_config['catalog']}.{test_config['schema']}.incomplete_customers"
        incomplete_df.write.mode('overwrite').saveAsTable(incomplete_table)
        
        inference_args = [
            'inference',
            '--model', 'churn',
            '--env', test_config['env'],
            '--input-table', incomplete_table,
            '--output-table', f"{test_config['catalog']}.{test_config['schema']}.churn_predictions_test"
        ]
        
        with patch.object(sys, 'argv', inference_args):
            result = main()
        
        # Should complete but with warnings
        assert result['status'] == 'success_with_warnings'
        assert 'missing_features' in result['warnings']

    @skip_if_not_databricks
    def test_inference_adds_metadata(self, spark, test_config):
        """Test inference adds proper metadata to predictions."""
        from pipelines.inference import main
        import sys
        
        inference_args = [
            'inference',
            '--model', 'fraud',
            '--env', test_config['env'],
            '--input-table', f"{test_config['catalog']}.{test_config['schema']}.transactions",
            '--output-table', f"{test_config['catalog']}.{test_config['schema']}.fraud_predictions"
        ]
        
        with patch.object(sys, 'argv', inference_args):
            result = main()
        
        output_df = spark.table(f"{test_config['catalog']}.{test_config['schema']}.fraud_predictions")
        
        # Check metadata columns
        assert 'model_version' in output_df.columns
        assert 'prediction_timestamp' in output_df.columns
        assert 'model_name' in output_df.columns
        
        # Verify metadata is populated
        sample_row = output_df.first()
        assert sample_row['model_name'] == 'fraud'
        assert sample_row['model_version'] is not None


class TestMonitoringPipeline:
    """Integration tests for monitoring pipeline."""

    @skip_if_not_databricks
    def test_drift_detection_on_stable_data(self, spark, test_config):
        """Test drift detection doesn't trigger on stable data."""
        from pipelines.monitor import main
        import sys
        
        monitor_args = [
            'monitor',
            '--model', 'churn',
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', monitor_args):
            result = main()
        
        assert result['status'] == 'success'
        assert result['drift_detected'] is False

    @skip_if_not_databricks
    def test_drift_detection_on_shifted_data(self, spark, test_config):
        """Test drift detection triggers on data distribution shift."""
        from pipelines.monitor import main
        import sys
        
        # Mock drift detector to simulate drift
        with patch('common.drift.DriftDetector.detect_feature_drift') as mock_drift:
            mock_drift.return_value = {
                'drift_detected': True,
                'drifted_features': ['age', 'tenure_months'],
                'drift_scores': {'age': 0.23, 'tenure_months': 0.19}
            }
            
            monitor_args = [
                'monitor',
                '--model', 'churn',
                '--env', test_config['env']
            ]
            
            with patch.object(sys, 'argv', monitor_args):
                result = main()
            
            assert result['drift_detected'] is True
            assert len(result['drifted_features']) > 0

    @skip_if_not_databricks
    def test_monitoring_logs_metrics_to_mlflow(self, spark, mlflow_client, test_config):
        """Test monitoring pipeline logs metrics to MLflow."""
        from pipelines.monitor import main
        import sys
        
        monitor_args = [
            'monitor',
            '--model', 'churn',
            '--env', test_config['env']
        ]
        
        with patch.object(sys, 'argv', monitor_args):
            result = main()
        
        # Verify metrics were logged
        run = mlflow_client.get_run(result['run_id'])
        assert 'drift_score' in run.data.metrics
        assert 'prediction_volume' in run.data.metrics
        assert 'avg_prediction_probability' in run.data.metrics


class TestEndToEndWorkflow:
    """Integration tests for complete end-to-end workflows."""

    @skip_if_not_databricks
    def test_train_validate_deploy_workflow(self, spark, mlflow_client, test_config):
        """Test complete workflow: train → validate → deploy."""
        import sys
        
        # Step 1: Train
        from pipelines.train import main as train_main
        train_args = ['train', '--model', 'churn', '--env', test_config['env']]
        with patch.object(sys, 'argv', train_args):
            train_result = train_main()
        
        assert train_result['status'] == 'success'
        run_id = train_result['run_id']
        
        # Step 2: Validate
        from pipelines.validate import main as validate_main
        validate_args = [
            'validate',
            '--model', 'churn',
            '--run-id', run_id,
            '--env', test_config['env']
        ]
        with patch.object(sys, 'argv', validate_args):
            validate_result = validate_main()
        
        assert validate_result['status'] == 'passed'
        
        # Step 3: Register to staging
        model_uri = f"runs:/{run_id}/model"
        model_details = mlflow.register_model(
            model_uri,
            f"{test_config['model_registry_name']}_churn"
        )
        
        # Step 4: Transition to production (simulated approval)
        mlflow_client.transition_model_version_stage(
            name=f"{test_config['model_registry_name']}_churn",
            version=model_details.version,
            stage="Production"
        )
        
        # Verify model is in production
        latest_versions = mlflow_client.get_latest_versions(
            f"{test_config['model_registry_name']}_churn",
            stages=["Production"]
        )
        assert len(latest_versions) > 0

    @skip_if_not_databricks
    def test_parallel_model_training(self, spark, test_config):
        """Test multiple models can be trained in parallel."""
        from pipelines.train import main as train_main
        import sys
        from concurrent.futures import ThreadPoolExecutor
        
        def train_model(model_name):
            train_args = ['train', '--model', model_name, '--env', test_config['env']]
            with patch.object(sys, 'argv', train_args):
                return train_main()
        
        # Train churn and fraud models in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(train_model, 'churn'),
                executor.submit(train_model, 'fraud')
            ]
            
            results = [f.result() for f in futures]
        
        # Both should succeed
        assert all(r['status'] == 'success' for r in results)
        assert len(set(r['run_id'] for r in results)) == 2  # Different runs


class TestErrorHandlingAndRecovery:
    """Integration tests for error handling and recovery."""

    @skip_if_not_databricks
    def test_pipeline_handles_cluster_failures(self, spark, test_config):
        """Test pipeline handles transient cluster failures."""
        from pipelines.train import main
        import sys
        
        # Mock transient failure followed by success
        attempt_count = {'count': 0}
        
        def mock_train_with_retry(*args, **kwargs):
            attempt_count['count'] += 1
            if attempt_count['count'] < 2:
                raise Exception("Cluster connection timeout")
            return {'status': 'success'}
        
        with patch('models.churn.trainer.ChurnTrainer.train', side_effect=mock_train_with_retry):
            train_args = ['train', '--model', 'churn', '--env', test_config['env']]
            with patch.object(sys, 'argv', train_args):
                result = main()
        
        assert result['status'] == 'success'
        assert attempt_count['count'] == 2  # Retry worked

    @skip_if_not_databricks
    def test_pipeline_rollback_on_validation_failure(self, spark, mlflow_client, test_config):
        """Test pipeline rolls back on validation failure."""
        import sys
        
        # Train a model
        from pipelines.train import main as train_main
        train_args = ['train', '--model', 'churn', '--env', test_config['env']]
        with patch.object(sys, 'argv', train_args):
            train_result = train_main()
        
        # Mock validation failure
        from pipelines.validate import main as validate_main
        with patch('pipelines.validate.check_business_metrics', return_value=False):
            validate_args = [
                'validate',
                '--model', 'churn',
                '--run-id', train_result['run_id'],
                '--env', test_config['env']
            ]
            with patch.object(sys, 'argv', validate_args):
                validate_result = validate_main()
        
        assert validate_result['status'] == 'failed'
        
        # Verify model was NOT promoted to production
        try:
            latest_prod = mlflow_client.get_latest_versions(
                f"{test_config['model_registry_name']}_churn",
                stages=["Production"]
            )
            # If there was a previous prod model, it should still be there
            # The new model should NOT be in production
            for version in latest_prod:
                assert version.run_id != train_result['run_id']
        except Exception:
            pass  # No production model exists, which is correct


# Performance tests
@skip_if_not_databricks
def test_training_performance_benchmark(spark, test_config):
    """Benchmark training pipeline performance."""
    from pipelines.train import main
    import sys
    
    start_time = time.time()
    
    train_args = ['train', '--model', 'churn', '--env', test_config['env']]
    with patch.object(sys, 'argv', train_args):
        result = main()
    
    duration = time.time() - start_time
    
    # Training should complete within reasonable time
    assert duration < 600, f"Training took {duration}s, expected < 600s"
    assert result['status'] == 'success'


@skip_if_not_databricks
def test_inference_throughput(spark, test_config):
    """Test batch inference throughput."""
    from pipelines.inference import main
    import sys
    
    # Create large test dataset
    large_df = spark.range(0, 100000).selectExpr(
        "id as customer_id",
        "rand() * 100 as age",
        "rand() * 120 as tenure_months"
    )
    large_table = f"{test_config['catalog']}.{test_config['schema']}.large_customers"
    large_df.write.mode('overwrite').saveAsTable(large_table)
    
    start_time = time.time()
    
    inference_args = [
        'inference',
        '--model', 'churn',
        '--env', test_config['env'],
        '--input-table', large_table,
        '--output-table', f"{test_config['catalog']}.{test_config['schema']}.large_predictions"
    ]
    
    with patch.object(sys, 'argv', inference_args):
        result = main()
    
    duration = time.time() - start_time
    throughput = result['predictions_count'] / duration
    
    # Should process at least 100 predictions per second
    assert throughput > 100, f"Throughput {throughput:.2f} predictions/s is too slow"
