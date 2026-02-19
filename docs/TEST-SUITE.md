# NGM MLOps Testing Suite

This directory contains the complete testing suite for the NGM MLOps platform.

## Test Structure

```
tests/
├── unit/                   # Fast, isolated unit tests
│   └── test_trainer.py     # Tests for model trainer classes
├── integration/            # End-to-end integration tests
│   ├── test_smoke.py       # Smoke test
│   └── test_pipeline.py    # Tests for complete pipeline workflows
├── conftest.py             # Shared fixtures and configuration
└── pytest.ini              # Pytest configuration
```

## Test Categories

### Unit Tests (`tests/unit/`)
- **Purpose**: Test individual components in isolation
- **Speed**: Fast (< 1 second per test)
- **Dependencies**: None (mocked)
- **When to run**: Every commit, local development

```bash
# Run only unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=html
```

### Integration Tests (`tests/integration/`)
- **Purpose**: Test complete workflows end-to-end
- **Speed**: Slow (1-10 minutes per test)
- **Dependencies**: Requires Databricks connectivity
- **When to run**: Before deployment, in CI pipeline

```bash
# Run only integration tests (requires DATABRICKS_HOST)
pytest tests/integration/ -v

# Skip integration tests
pytest -m "not integration"
```

## Running Tests

### Local Development

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_trainer.py -v

# Run specific test function
pytest tests/unit/test_trainer.py::TestChurnTrainer::test_trainer_initialization -v

# Run tests matching a pattern
pytest -k "test_training" -v

# Run with markers
pytest -m unit  # only unit tests
pytest -m integration  # only integration tests
pytest -m "not slow"  # skip slow tests
```

### CI Pipeline

```bash
# Fast smoke test suite
pytest -m smoke --tb=short

# Full test suite with coverage
pytest --cov=src --cov-report=xml --cov-report=html

# Parallel execution (faster)
pytest -n auto
```

### Databricks Environment

Integration tests require Databricks connectivity. Set these environment variables:

```bash
export DATABRICKS_HOST="https://your-workspace.databricks.com"
export DATABRICKS_TOKEN="your-token"
export MLFLOW_TRACKING_URI="databricks"
```

## Test Markers

Tests are organized using pytest markers:

- `@pytest.mark.unit` - Fast unit tests (auto-applied to tests/unit/)
- `@pytest.mark.integration` - Integration tests requiring Databricks (auto-applied to tests/integration/)
- `@pytest.mark.slow` - Tests taking > 1 minute
- `@pytest.mark.smoke` - Quick smoke tests for CI

## Writing New Tests

### Unit Test Template

```python
# tests/unit/test_new_feature.py
import pytest
from unittest.mock import Mock, patch

class TestNewFeature:
    """Unit tests for new feature."""
    
    def test_feature_basic_functionality(self):
        """Test that feature works correctly."""
        from models.new_feature import NewFeature
        
        feature = NewFeature()
        result = feature.do_something()
        
        assert result is not None
        assert result == expected_value
    
    @patch('models.new_feature.external_dependency')
    def test_feature_with_mocked_dependency(self, mock_dep):
        """Test feature with mocked external dependency."""
        mock_dep.return_value = "mocked_value"
        
        from models.new_feature import NewFeature
        feature = NewFeature()
        result = feature.do_something()
        
        mock_dep.assert_called_once()
        assert result == "expected_value"
```

### Integration Test Template

```python
# tests/integration/test_new_pipeline.py
import pytest

@pytest.mark.integration
class TestNewPipeline:
    """Integration tests for new pipeline."""
    
    @pytest.mark.skip_if_not_databricks
    def test_pipeline_end_to_end(self, spark, mlflow_client, test_config):
        """Test complete pipeline execution."""
        from pipelines.new_pipeline import main
        import sys
        
        test_args = ['pipeline', '--model', 'test', '--env', 'dev']
        with patch.object(sys, 'argv', test_args):
            result = main()
        
        assert result['status'] == 'success'
        # Additional assertions...
```

## Fixtures

Common fixtures are defined in `conftest.py`:

- `test_env` - Test environment configuration
- `sample_churn_data` - Realistic churn dataset
- `sample_fraud_data` - Realistic fraud dataset
- `mock_spark_session` - Mocked Spark session
- `mock_mlflow` - Mocked MLflow tracking
- `trained_sklearn_model` - Pre-trained model for testing
- `temp_data_dir` - Temporary directory for test files

## Test Coverage

View coverage reports:

```bash
# Generate coverage report
pytest --cov=src --cov-report=html

# Open report in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

Coverage targets:
- **Overall**: > 80%
- **Critical paths**: > 90% (training, validation, inference pipelines)
- **Utilities**: > 70% (logging, config, helpers)

## Debugging Tests

```bash
# Run with verbose output and stop on first failure
pytest -vsx

# Run with Python debugger (pdb)
pytest --pdb

# Show print statements
pytest -s

# Run with detailed traceback
pytest --tb=long
```

## Performance Testing

```bash
# Show slowest 10 tests
pytest --durations=10

# Profile test execution
pytest --profile

# Benchmark specific test
pytest tests/integration/test_pipeline.py::test_training_performance_benchmark -v
```

## Continuous Integration

Tests are automatically run in CI on:
- Every pull request
- Every push to main branch
- Nightly full test suite

CI pipeline stages:
1. **Lint & Format** - Code quality checks
2. **Unit Tests** - Fast smoke tests (< 5 minutes)
3. **Integration Tests** - Full pipeline tests (DEV environment)
4. **Coverage Report** - Upload to coverage service

## Troubleshooting

### Common Issues

**Import errors:**
```bash
# Ensure src is in Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

**Databricks connection failures:**
```bash
# Verify environment variables
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN

# Test connection
databricks clusters list
```

**Fixture not found:**
- Check that `conftest.py` is in the tests directory
- Ensure pytest is discovering the file

**Tests hanging:**
- Set timeout: `pytest --timeout=300`
- Check for infinite loops or blocking operations

## Best Practices

1. **Test Organization**
   - One test file per source file
   - Group related tests in classes
   - Use descriptive test names

2. **Test Independence**
   - Tests should not depend on each other
   - Use fixtures for setup/teardown
   - Clean up resources after tests

3. **Assertions**
   - One logical assertion per test
   - Use descriptive assertion messages
   - Test both success and failure cases

4. **Mocking**
   - Mock external dependencies
   - Don't mock the code under test
   - Verify mock calls when relevant

5. **Performance**
   - Keep unit tests fast (< 1s)
   - Use session-scoped fixtures for expensive setup
   - Run integration tests only when needed

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [MLOps Testing Best Practices](https://ml-ops.org/content/testing-ml-systems)
- [Databricks Testing Guide](https://docs.databricks.com/dev-tools/testing.html)
