# Architecture Overview

This document describes the enterprise MLOps architecture for Azure Databricks.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          GitHub Repository                          │
│  ├── .github/workflows/          (CI/CD pipelines)                  │
│  ├── src/                         (Model code)                      │
│  ├── resources/                   (Job definitions)                 │
│  └── tests/                       (Unit & integration tests)         │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
                    (GitHub Actions Triggering)
                                ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    5 Databricks Environments                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  RND (Research)  → DEV (Test) → UAT (QA) → PREPROD → PROD (Live)  │
│  (dev mode)        (dev)        (prod)      (prod)      (prod)     │
│                                                                     │
│  Catalogs:     Catalogs:      Catalogs:   Catalogs:   Catalogs:   │
│  ├── rnd       ├── dev        ├── uat     ├── preprod ├── prod    │
│  └── features  └── features   └── features└── features└── features│
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
                                ↓
                        (Databricks Bundles)
                                ↓
┌──────────────────────────────────────────────────────────────────────┐
│                    Databricks Workspace Assets                       │
├──────────────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────────────┐ │
│ │  MLflow Tracking Server                                         │ │
│ │  ├── Runs (training telemetry)                                 │ │
│ │  ├── Registered Models (versioned artifacts)                  │ │
│ │  └── Experiments (organized by model)                         │ │
│ └─────────────────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────────────────┐ │
│ │  Unity Catalog                                                  │ │
│ │  ├── {env}.raw_data (source tables)                            │ │
│ │  ├── {env}.features (engineered features)                      │ │
│ │  └── {env}.predictions (model outputs)                         │ │
│ └─────────────────────────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────────────────────────┐ │
│ │  Workflows                                                      │ │
│ │  ├── Training Job (ngm-train-model)                           │ │
│ │  ├── Validation Job (ngm-validate-model)                      │ │
│ │  ├── Batch Inference Job (ngm-batch-inference)                │ │
│ │  ├── Feature Engineering Job (ngm-feature-engineering)        │ │
│ │  └── Monitoring Job (ngm-monitor)                             │ │
│ └─────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
                                ↓
                (Delta Lake & Unity Catalog)
                                ↓
                    External Applications
                    (Dashboards, APIs, etc.)
```

---

## Repository Structure

```
ngm_mlops/
├── .github/workflows/          # CI/CD automation
│   ├── ci.yml                  # Unit tests + RND deploy
│   ├── cd-dev.yml              # DEV deployment
│   ├── cd-uat.yml              # UAT deployment
│   ├── cd-preprod.yml          # PREPROD deployment
│   └── cd-prod.yml             # PROD deployment + gates
│
├── src/                         # Python source code
│   ├── common/                 # Shared utilities
│   │   ├── config.py           # Configuration loader
│   │   ├── logger.py           # Structured logging
│   │   ├── exceptions.py       # Custom exceptions
│   │   ├── drift.py            # Drift detection
│   │   ├── data_quality.py     # Quality checks
│   │   └── mlflow_utils.py     # MLflow helpers
│   │
│   ├── models/                 # Per-model implementations
│   │   ├── base.py             # Base classes
│   │   ├── registry.py         # Model registry management
│   │   ├── churn/              # Churn model
│   │   │   ├── trainer.py
│   │   │   ├── inference.py
│   │   │   └── validator.py
│   │   │
│   │   └── fraud/              # Fraud model
│   │       ├── trainer.py
│   │       ├── inference.py
│   │       └── validator.py
│   │
│   └── pipelines/              # Orchestration entry points
│       ├── train.py            # Training pipeline
│       ├── validate.py         # Validation pipeline
│       ├── inference.py        # Batch inference pipeline
│       └── monitor.py          # Monitoring pipeline
│
├── resources/                  # IaC - Databricks resources
│   └── jobs/                   # Job definitions
│       ├── train.yml
│       ├── validate.yml
│       ├── batch_inference.yml
│       └── feature_engineering.yml
│
├── tests/                      # Test suite
│   └── unit/
│       └── test_smoke.py
│
├── docs/                       # Documentation
│   ├── ARCHITECTURE.md         # This file
│   ├── CONFIGURATION-GUIDE.md  # Setup guide
│   ├── CI-CD-GUIDE.md         # Workflow guide
│   └── MODEL-LIFECYCLE.md     # Model flow
│
├── databricks.yml             # Bundle configuration (environment-driven)
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## Environment Strategy

```
RND (Research & Development)
├── Auto-deploy: Yes (on main branch push)
├── Catalog: ngm_ml_rnd
├── Data: Sample/test data
└── Purpose: Rapid experimentation

DEV (Development)
├── Auto-deploy: Yes (on dev branch push)
├── Catalog: dev
├── Data: Full dataset
└── Purpose: Unit testing

UAT (User Acceptance Testing)
├── Auto-deploy: Yes (on uat branch push)
├── Catalog: uat
├── Data: Production replica
└── Purpose: Integration testing & stakeholder validation

PREPROD (Pre-Production)
├── Auto-deploy: Yes (on preprod branch push)
├── Catalog: preprod
├── Data: Production replica
└── Purpose: Final validation before prod

PROD (Production)
├── Auto-deploy: Yes (on prod branch push) + manual approval gate
├── Catalog: prod
├── Data: Live business data
└── Purpose: Production serving
```

---

## Key Improvements

### 1. Configuration Management ✓
- **Fixed**: Hardcoded DATABRICKS_HOST removed
- **Solution**: Environment-driven via GitHub Secrets + bundle variables
- **Benefit**: Different workspaces per environment, no code changes needed

### 2. GCP Migration Cleanup ✓
- **Fixed**: BigQuery references removed
- **Removed**: GCP service account Spark config
- **Updated**: Features builder to use UC catalogs only
- **Benefit**: Pure Azure Databricks, no cloud vendor lock-in

### 3. Import Path Standardization ✓
- **Fixed**: All imports use `src.` prefix
- **Updated**: trainer.py, inference.py, validator.py
- **Benefit**: No module resolution issues

### 4. Model Lifecycle ✓
- **Training**: Auto-run on schedule, log to MLflow
- **Validation**: Check metrics, promote to Champion on pass
- **Inference**: Always uses Champion version
- **Benefit**: Automated model promotion

### 5. Multi-Model Support ✓
- **Fraud Model**: Scaffold created with separate trainer/inference
- **Extensible**: Add more models by copying pattern
- **Benefit**: Reusable training/validation pipelines

### 6. Data Quality & Monitoring ✓
- **DataQualityChecker**: Null, duplicate, range checks
- **DriftDetector**: KS test, Chi-square, population stability
- **Registry Manager**: Model promotion, archival, comparison
- **Benefit**: Production-grade MLOps practices

---

## Data Flow

### Training → Validation → Inference

```
Features (UC Table)
├── Load
│
Train Job
├── Train model
├── Evaluate metrics
└── Log to MLflow v1, v2, v3...
│
Validate Job
├── Check metrics >= threshold
├── If pass: mark as Champion
└── If fail: keep as archived
│
Infer Job (Batch)
├── Load Champion v3
├── Score records
├── Add lineage (run_id, version, timestamp)
└── Write to predictions table
│
Applications
└── Use predictions for business logic
```

---

## Security & Governance

- **Authentication**: Service Principal + PAT
- **Secrets**: GitHub Secrets + Databricks Secrets scope
- **Audit**: Git commit SHA + MLflow run tracking
- **Lineage**: model_version + run_id + timestamp

---

## Next Steps

→ [Configuration Guide](./CONFIGURATION-GUIDE.md) - Set up environments
→ [CI/CD Guide](./CI-CD-GUIDE.md) - Understand workflows
→ [Model Lifecycle](./MODEL-LIFECYCLE.md) - Model flow
