# Model Lifecycle

This document explains how models flow through training, validation, and deployment.

---

## Model Lifecycle Stages

```
Created (New Version)
    ↓
Trained (in MLflow)
    ↓
Validated (metrics checked)
    ↓
Champion (promoted for batch inference)
    ↓
Archived (replaced by new champion)
```

---

## 1. Training Stage

### Trigger

```bash
# Via job schedule
databricks jobs run -j ngm-train-model

# Or manually
python src/pipelines/train.py churn
```

### Pipeline Steps

```
1. Load features from UC table
2. Split into train/test
3. Train model
4. Evaluate on test set
5. Log metrics to MLflow
6. Register model in MLflow Model Registry
7. Create new version (unalased)
```

### Training Configuration

```yaml
# resources/jobs/train.yml
tasks:
  - task_key: train
    spark_python_task:
      python_file: ../../src/pipelines/train.py
      parameters:
        - churn              # Model key
        - --git_sha
        - ${var.git_sha}     # For lineage

    schedule:
      quartz_cron_expression: "0 0 6 * * ?"  # Daily 6 AM
      timezone_id: "Australia/Sydney"
```

### Output

- MLflow run with metrics logged
- Model artifact stored in MLflow
- New model version registered (status: READY)

---

## 2. Validation Stage

### Trigger

```bash
python src/pipelines/validate.py churn
```

### Validation Checks

```python
# src/models/churn/validator.py
def validate(self, run_id: str) -> bool:
    run = mlflow.get_run(run_id)
    auc = run.data.metrics["auc"]
    
    # Minimum AUC threshold
    return auc >= self.config.metrics.auc_threshold  # e.g., 0.75
```

### Success Criteria

Model passes if:
1. ✓ AUC ≥ threshold
2. ✓ Feature engineering stable
3. ✓ No data drift detected

### Promotion on Success

If validation passes:
1. Get Champion alias (current production model)
2. Current Champion → Archived
3. Validated version → Champion

---

## 3. Batch Inference Stage

### Trigger

```yaml
# resources/jobs/batch_inference.yml
schedule:
  quartz_cron_expression: "0 0 10 * * ?"  # Daily 10 AM
```

### Pipeline Steps

```python
# src/pipelines/inference.py
1. Load Champion model (latest promoted)
2. Load new features
3. Score all records
4. Stamp with model_version, run_id, scored_at
5. Write to predictions table
6. Track metrics (batch size, score distribution)
```

### Prediction Output

```
predictions.churn_scores table
├── account_id
├── churn_probability
├── churn_predicted
├── model_version         # MLflow version
├── run_id               # MLflow run
├── scored_at            # Timestamp
└── score_batch_id       # Batch identifier
```

---

## 4. Online Inference (Future)

### Model Serving Endpoint

```yaml
# resources/serving/churn_endpoint.yml (commented out)
resources:
  serving_endpoints:
    - name: ngm-churn-scoring
      config:
        served_models:
          - model_name: ngm-churn-model
            model_version: 5  # Champion
            scale_to_zero_enabled: true
```

### Usage

```python
import mlflow.pyfunc

model = mlflow.pyfunc.load_model("models:/ngm-churn-model@Champion")
predictions = model.predict(features_df)
```

---

## 5. Monitoring & Drift Detection

### Drift Monitoring

```python
# Built into pipelines
from src.common.drift import DriftDetector

detector = DriftDetector(p_threshold=0.05)
drift_results = detector.detect_drift(baseline_df, current_df)

if drift_results["overall_drift"]:
    # Trigger retraining
    logger.warning("Data drift detected - retraining recommended")
```

### Metrics Tracked

```
- Feature statistics (distribution, mean, std)
- Model performance (AUC, precision, recall)
- Prediction distribution
- Batch size and latency
```

---

## Model Configuration

### Per-Model Settings

```yaml
# src/models/churn/config.yml
model_key: churn
registered_model_name: ngm-churn-model

trainer_class: ChurnTrainer        # Python class
validator_class: ChurnValidator
inference_class: ChurnInference

data:
  features_table: uat.features.churn_features
  label_col: churn

hyperparameters:
  n_estimators: 100
  max_depth: 10

metrics:
  auc_threshold: 0.75          # Min for validation
  
output:
  catalog: uat
  schema: predictions
  table: churn_scores
```

---

## MLflow Integration

### Run Lifecycle

```
MLflow Run (e.g., churn-20250313-143000)
├── Parameters (hyperparameters logged)
├── Metrics (auc, precision, recall)
├── Artifacts (model, feature importance)
└── Tags (environment, git_sha, status)
```

### Model Registry

```
MLflow Model Registry
└── ngm-churn-model (registered model)
    ├── Version 5 (Champion) ← Latest promoted
    ├── Version 4 (Archived)  ← Previous champion
    ├── Version 3 (Archived)
    └── Version 2             ← Available for comparison
```

### Querying MLflow

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Get champion version
champion = client.get_model_version_by_alias("ngm-churn-model", "Champion")
print(f"Champion: v{champion.version}, run_id: {champion.run_id}")

# Compare versions
versions = client.search_model_versions("name='ngm-churn-model'")
for v in versions:
    print(f"v{v.version}: {v.status}, created: {v.creation_timestamp}")
```

---

## Automated Retraining

### Trigger Conditions

Retraining triggers when:

1. **Scheduled**: Daily/weekly job runs
2. **On Drift**: Data drift detected above threshold
3. **On Performance Drop**: Prediction accuracy below baseline
4. **Manual**: Operator triggers
5. **On New Data**: New features available

### Retraining Pipeline

```
Detect Trigger
    ↓
Schedule Training Job
    ↓
Train New Model (Version N)
    ↓
Validate (pass/fail)
    ↓
If Pass: Promote to Champion
If Fail: Keep current Champion, alert ops
    ↓
Resume Batch Inference with Champion
```

---

## Model Governance

### Lineage Tracking

Every prediction includes:

```python
{
  "model_name": "ngm-churn-model",
  "model_version": 5,
  "run_id": "abc123def456",
  "trained_at": "2025-03-10T14:30:00Z",
  "scored_at": "2025-03-13T10:00:00Z",
  "git_sha": "a1b2c3d4e5f6"
}
```

### Audit Trail

Track all deployments:

```sql
SELECT 
  model_name,
  version,
  promotion_timestamp,
  promoted_by,
  from_stage,
  to_stage
FROM mlflow.model.promotions
ORDER BY promotion_timestamp DESC
```

---

## Rollback Procedure

### If Model Performs Poorly in Production

```python
from src.models.registry import ModelRegistry

registry = ModelRegistry()

# Revert to previous champion
previous_champion = registry.get_archived_versions("ngm-churn-model")[0]

# Promote previous back to champion
registry.promote_model(
    "ngm-churn-model",
    previous_champion.version,
    "Champion"
)

print(f"Rolled back to v{previous_champion.version}")
```

### Manual Rollback via Databricks

```sql
-- In Databricks Model Registry UI
-- Or via MLflow API
UPDATE model_versions
SET alias = 'Archived'
WHERE model_name = 'ngm-churn-model' AND version = 5;

UPDATE model_versions
SET alias = 'Champion'
WHERE model_name = 'ngm-churn-model' AND version = 4;
```

---

## Multi-Model Orchestration

Train multiple models in sequence:

```bash
# Training job that handles all models
for model in churn fraud; do
  echo "Training $model..."
  python src/pipelines/train.py $model
  
  echo "Validating $model..."
  python src/pipelines/validate.py $model
  
  echo "Running inference for $model..."
  python src/pipelines/inference.py $model
done
```

---

## Performance Optimization

### Feature Caching

Features are computed once, reused for training and inference:

```sql
-- Feature table (computed daily)
CREATE TABLE uat.features.churn_features AS
SELECT ... FROM uat.raw_data.accounts
WHERE compute_date = CURRENT_DATE()

-- Used by training (full history)
SELECT * FROM uat.features.churn_features
WHERE compute_date >= '2024-01-01'

-- Used by batch inference (recent only)
SELECT * FROM uat.features.churn_features
WHERE compute_date = CURRENT_DATE()
```

### Job Parallelization

Train multiple models in parallel:

```yaml
resources:
  jobs:
    train_all:
      tasks:
        - task_key: train_churn
          depends_on: []
        - task_key: train_fraud
          depends_on: []        # No dependency = parallel
        - task_key: validate_churn
          depends_on:
            - task_key: train_churn
        - task_key: validate_fraud
          depends_on:
            - task_key: train_fraud
```

---

## Next Steps

- [Data Quality Monitoring](./DATA-QUALITY.md)
- [Troubleshooting Guide](./TROUBLESHOOTING.md)
- [Performance Tuning](./PERFORMANCE.md)
