# CI/CD Guide

This guide explains the GitHub Actions workflows and deployment process.

---

## Gitbhub Workflow Overview
The full promotion flow:
```
Developer pushes code to GitHub
  ↓
CI runs (GitHub Actions) (`.github/workflows/cd-rnd.yml`)
  ├── Linting (code quality)
  ├── Unit tests  (`pytest tests/unit/`)
  ├── Builds wheel artifact
  └── Auto-deploys to RND environment
  
  ↓ (manual approval)
  
CD to DEV (`.github/workflows/cd-dev.yml`)
  ├── Integration tests  (`pytest tests/integration/`)
  ├── Deploys to DEV Databricks workspace
  └── Jobs remain PAUSED (manual trigger only)
  
  ↓ (manual approval)
  
CD to UAT (`.github/workflows/cd-uat.yml`)
  ├── Model validation checks
  ├── Business metric thresholds
  └── Smoke tests with real data
  
  ↓ (manual approval + sign-off)
  
CD to PREPROD (`.github/workflows/cd-preprod.yml`)
  ├── Full production workload simulation
  ├── Performance benchmarks
  └── Drift detection enabled
  
  ↓ (manager approval required)
  
CD to PROD (`.github/workflows/cd-prod.yml`)
  ├── Blue-green deployment
  ├── Jobs set to UNPAUSED (live schedule)
  ├── Monitoring alerts activated
  └── Automatic rollback on failure
```

---

## Deployment Process

### 1. Development Deployment (RND)

Automatically triggered on every push to `main`:

```bash
git switch -c my-feature
# Make changes
git commit -m "Add new feature"
git push origin my-feature
# Create PR → merge to main
# CI tests run automatically
# CD-RND deploys to RND automatically
```

**Monitor in GitHub**: Actions tab → CI workflow

### 2. Testing Deployment (DEV)

Manually triggered by merging to `dev`:

```bash
# After RND validation
git switch main
git pull
git switch -c dev
git merge main
git push origin dev
# CD-DEV deploys to DEV automatically
```

### 3. UAT Deployment

Manually triggered:

```bash
git switch uat
git merge dev
git push origin uat
# CD-UAT deploys to UAT automatically
```

### 4. Pre Production Deployment (PREPROD)

**Requires approval gate**:

```bash
git switch preprod
git merge uat
git push origin preprod
# GitHub environment protection blocks automatic deployment
# Review deployment in GitHub UI
# Click "Approve and deploy"
# CD-PREPROD deploys to PREPROD
```

### 4. Production Deployment (PROD)

**Requires approval gate**:

```bash
git switch prod
git merge preprod
git push origin prod
# GitHub environment protection blocks automatic deployment
# Review deployment in GitHub UI
# Click "Approve and deploy"
# CD-PROD deploys to PROD
```

---

## Manual Deployments

### Redeploy a specific commit

Use workflow dispatch:

```bash
gh workflow run cd-prod.yml \
  -f git_sha=abc123def456
```

Or in GitHub UI:
1. Go to Actions
2. Select workflow (e.g., "CD - Prod")
3. Click "Run workflow"
4. Enter Git SHA (optional)

---

## Bundle Deployment Details

### What Gets Deployed

The `databricks.yml` bundle includes:

```
resources/
├── jobs/
│   ├── train.yml            # Training job
│   ├── validate.yml         # Validation job
│   ├── batch_inference.yml  # Batch scoring
│   ├── feature_engineering.yml  # Feature computation
│   └── monitor.yml          # Monitoring job
├── models/
│   └── serving.yml          # Serving endpoints (commented out)
```

Each environment gets its own:
- Catalog (dev, uat, prod)
- Workspace path (`/Workspace/Repos/ngm_mlops-{env}`)
- Job schedules

---

## Troubleshooting

### Deployment Failed: "Invalid databricks_host"

**Cause**: Secret not set or empty

**Fix**:
```bash
# Add secret
gh secret set DATABRICKS_HOST_DEV -b "https://adb-xxxx.12.azuredatabricks.net"

# Verify
gh secret list | grep DATABRICKS_HOST
```

### Deployment Failed: "Bundle validation failed"

**Cause**: YAML syntax error or missing catalog

**Fix**:
1. Check databricks.yml syntax
2. Verify catalogs exist in Databricks
3. Run locally: `databricks bundle validate -t dev`

### Models not deploying

**Cause**: Job configuration errors

**Check**:
1. Verify model config exists: `src/models/{model_key}/config.yml`
2. Check trainer/inference classes exist
3. Validate SQL templates exist

---

## Best Practices

### 1. Create Feature Branches

```bash
git switch main
git pull
git switch -c feature/my-change
# Make changes
git commit -m "descriptive message"
git push origin feature/my-change
# Create PR for code review
```

### 2. Test in Lower Environments First

```
main → RND (auto)
 ↓
dev (manual PR)
 ↓
uat (manual PR)
 ↓
prod (manual + approval)
```

### 3. Use Meaningful Commit Messages

```
[FEATURE] Add new fraud detection feature
[FIX] Correct missing value handling
[CHORE] Update dependencies
[PROD-HOTFIX] Fix production issue
```

### 4. Monitor Deployments

```bash
# Watch workflow status
gh run list --workflow=cd-rnd.yml --limit 5

# Get detailed logs
gh run view {run_id} -v
```

### 5. Rollback Strategy

```bash
# If prod deployment fails:
git revert {bad_commit}
git push origin prod
# CD-PROD auto-redeploys with previous version
```

---

## Environment Promotion Checklist

### RND → DEV
- [ ] Tests pass in RND
- [ ] Code reviewed
- [ ] No breaking changes

### DEV → UAT
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] All features working

### UAT → PREPROD
- [ ] UAT sign-off
- [ ] Load testing complete
- [ ] Performance validated

### PREPROD → PROD
- [ ] Preprod validation complete
- [ ] Runbook documented
- [ ] Rollback plan ready
- [ ] Stakeholder approval

---

The Execution chain : pyproject.toml 
```
python_wheel_task
  └── package_name: ngm_mlops     →  finds the installed wheel
        └── entry_point: ngm-train  →  looks up [project.scripts] in pyproject.toml
                └── "ngm-train = pipelines.train:main"  →  calls main() in src/pipelines/train.py
```

## Deployment Monitoring

### In Databricks

After deployment, verify in workspace:

```
/Workspace/Repos/ngm_mlops-{env}/
├── src/
├── resources/
├── .databricks/
└── bundle.yml
```

### Check Job Settings

```sql
-- List all jobs in environment
SELECT job_id, settings FROM system.jobs.jobinfo
WHERE name LIKE 'ngm-%'
```

### Monitor Execution

```bash
# View job runs
databricks jobs get-run --run-id {run_id}

# Tail logs
databricks runs get-output --run-id {run_id}
```

---

## Next Steps

- [Model Lifecycle](./MODEL-LIFECYCLE.md)
