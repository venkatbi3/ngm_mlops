# Configuration Guide

This guide explains how to configure the MLOps framework for your models and environments.

---

## Environment Setup

### 1. GitHub Secrets (Required)

Set these in your GitHub repository settings: **Settings → Secrets and variables → Actions**

```
DATABRICKS_TOKEN              # Personal access token for Databricks API
DATABRICKS_HOST_RND           # RND workspace: https://adb-xxxx.xx.azuredatabricks.net
DATABRICKS_HOST_DEV           # DEV workspace: https://adb-xxxx.xx.azuredatabricks.net
DATABRICKS_HOST_UAT           # UAT workspace: https://adb-xxxx.xx.azuredatabricks.net
DATABRICKS_HOST_PREPROD       # Preprod workspace: https://adb-xxxx.xx.azuredatabricks.net
DATABRICKS_HOST_PROD          # Prod workspace: https://adb-xxxx.xx.azuredatabricks.net
```

**Important**: Each workspace can be in the same or different Databricks accounts.

### 2. Databricks Bundle Configuration

Edit `databricks.yml` to pass workspace host via CLI:

```bash
# Development/Testing
databricks bundle deploy -t rnd \
  --var="databricks_host=https://adb-xxxx.xx.azuredatabricks.net" \
  --var="git_sha=abc123"

# Production
databricks bundle deploy -t prod \
  --var="databricks_host=https://adb-xxxx.xx.azuredatabricks.net" \
  --var="git_sha=abc123"
```

---

## Model Configuration

Each model has a YAML configuration file: `src/models/{model_key}/config.yml`

### Configuration Structure

```yaml
# Example: src/models/churn/config.yml
model_key: churn
registered_model_name: ngm-churn-model
trainer_class: ChurnTrainer
validator_class: ChurnValidator
inference_class: ChurnInference

data:
  source_catalog: uat                    # Source UC catalog
  source_schema: raw_data               # Source schema
  features_table: uat.features.churn_features
  label_col: churn
  start_date: "2024-01-01"
  end_date: "2025-03-01"

hyperparameters:
  n_estimators: 100
  max_depth: 10

metrics:
  auc_threshold: 0.75                   # Min AUC to pass validation
  min_recall: 0.80                      # For fraud detection

output:
  catalog: uat
  schema: predictions
  table: churn_scores
```

### Creating a New Model Configuration

1. Create directory: `src/models/{model_key}/`
2. Create config: `src/models/{model_key}/config.yml`
3. Implement trainers:
   - `trainer.py` - extends `BaseTrainer`
   - `inference.py` - extends `BaseInference`
   - `validator.py` - extends `BaseValidator`
4. Create SQL templates in `src/models/{model_key}/sql/`

---

## Catalog & Schema Requirements

### Unity Catalog Structure

```
dev/                          # Dev environment catalog
├── raw_data/                # Raw source tables
├── features/                # Feature tables
└── predictions/             # Model predictions

uat/                          # UAT environment catalog
├── raw_data/
├── features/
└── predictions/

prod/                         # Production catalog
├── raw_data/
├── features/
└── predictions/
```

### Creating Catalogs

In Databricks SQL:

```sql
-- Create catalogs for each environment
CREATE CATALOG IF NOT EXISTS dev;
CREATE CATALOG IF NOT EXISTS uat;
CREATE CATALOG IF NOT EXISTS prod;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS dev.raw_data;
CREATE SCHEMA IF NOT EXISTS dev.features;
CREATE SCHEMA IF NOT EXISTS dev.predictions;

-- Similar for uat, prod
```

---

## Local Development (.env)

Create `.env` file for local runs:

```
MLFLOW_TRACKING_URI=https://adb-xxxx.xx.azuredatabricks.net
DATABRICKS_HOST=https://adb-xxxx.xx.azuredatabricks.net
DATABRICKS_CLIENT_ID=your-client-id
DATABRICKS_CLIENT_SECRET=your-client-secret
DATABRICKS_TENANT_ID=your-tenant-id
DEFAULT_MODEL_KEY=churn
```

**Note**: Never commit `.env` file.

---

## Multi-Model Support

The framework supports multiple models in a single repository:

```
src/models/
├── churn/
│   ├── config.yml
│   ├── trainer.py
│   ├── inference.py
│   ├── validator.py
│   └── sql/
│
├── fraud/
│   ├── config.yml
│   ├── trainer.py
│   ├── inference.py
│   ├── validator.py
│   └── sql/
```

**Deploy all models:**

```bash
python src/pipelines/train.py churn
python src/pipelines/train.py fraud
```

---

## Feature Engineering Configuration

Features are built from SQL templates:

```
src/models/{model_key}/sql/
├── account_features.sql
├── transaction_features.sql
└── combined_features.sql
```

### SQL Template Variables

```sql
-- Use these placeholders in SQL templates
SELECT 
  account_id,
  SUM(amount) as total_amount
FROM {source_catalog}.{source_schema}.transactions
WHERE date BETWEEN '{start_date}' AND '{end_date}'
GROUP BY account_id
INTO {output_table}
```

---

## Environment-Specific Variables

### Bundle Variables (databricks.yml)

```yaml
variables:
  databricks_host:
    description: "Workspace host URL"
    default: ""
  
  catalog_name:
    description: "UC catalog for model artifacts"
    default: "dev"
  
  git_sha:
    description: "Git commit SHA"
    default: "local"
```

### Usage

```bash
# Pass via CLI
databricks bundle deploy -t prod \
  --var="databricks_host=https://adb-xxxx.xx.azuredatabricks.net" \
  --var="catalog_name=prod" \
  --var="git_sha=$GIT_SHA"
```

---

## Secrets Management

### Databricks Secrets (Recommended)

```bash
# In Databricks CLI
databricks secrets put --scope mlops --key db-token "your-token"
databricks secrets put --scope mlops --key api-key "your-key"
```

### Access in Python

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
token = spark.sql("SELECT get_secret('mlops', 'db-token')").collect()[0][0]
```

### GitHub Secrets

Use for CI/CD only:

```yaml
env:
  API_KEY: ${{ secrets.API_KEY }}
```

---

## Troubleshooting

### Issue: "Config not found"
- **Cause**: Model config path incorrect
- **Fix**: Ensure `src/models/{model_key}/config.yml` exists

### Issue: "Invalid workspace path"
- **Cause**: `databricks_host` not provided or empty
- **Fix**: Pass `--var="databricks_host=..."` to bundle deploy

### Issue: "Catalog does not exist"
- **Cause**: UC catalogs not created
- **Fix**: Create catalogs in Databricks workspace using SQL above

---

## Next Steps

- [Deployment Guide](./DEPLOYMENT-GUIDE.md)
- [CI/CD Workflows](./CI-CD-GUIDE.md)
- [Model Lifecycle](./MODEL-LIFECYCLE.md)
