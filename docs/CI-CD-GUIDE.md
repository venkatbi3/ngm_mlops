# CI/CD Guide

This guide explains the GitHub Actions workflows and deployment process.

---

## Workflow Overview

```
Commit to main
    ↓
[CI] Run unit tests & validate
    ↓
[CD-RND] Auto-deploy to RND (Research & Development)
    ↓
Manual: Create PR and merge to dev
    ↓
[CD-DEV] Auto-deploy to DEV (Development)
    ↓
Manual: Create PR and merge to uat
    ↓
[CD-UAT] Auto-deploy to UAT (User Acceptance Testing)
    ↓
Manual: Create PR and merge to preprod
    ↓
[CD-PREPROD] Auto-deploy to PREPROD (Pre-Production)
    ↓
Manual: Create PR and merge to prod
    ↓
[CD-PROD] Auto-deploy to PROD (Production)
```

---

## GitHub Workflows

### 1. CI (`.github/workflows/ci.yml`)

**Triggers**: Push to `main` | Pull requests to `main`

**Steps**:
1. Checkout code
2. Run unit tests (`pytest tests/unit/`)
3. Validate Python imports
4. (If main branch): Deploy to RND auto-deploy

**Success Criteria**:
- All tests pass
- No import errors
- Bundle validates


### 2. CD - RND (`.github/workflows/cd-dev.yml` for other envs)

**Triggers**: Push to branch `dev` | `uat` | `preprod` | `prod`

**Each environment runs**:
1. Checkout code
2. Install dependencies
3. Validate bundle configuration
4. Deploy using Databricks CLI bundles
5. Log deployment info

**Environment-Specific**:
- **dev**: Use `DATABRICKS_HOST_DEV` secret
- **uat**: Use `DATABRICKS_HOST_UAT` secret
- **prod**: Use `DATABRICKS_HOST_PROD` secret + manual approval gate

---

## Setting Up Secrets

### In GitHub Repository

Go to: **Settings → Secrets and variables → Actions**

Add these secrets:

```
DATABRICKS_TOKEN              # Your Databricks PAT
DATABRICKS_HOST_RND           # https://adb-xxxx.12.azuredatabricks.net
DATABRICKS_HOST_DEV           # https://adb-yyyy.12.azuredatabricks.net
DATABRICKS_HOST_UAT           # https://adb-zzzz.12.azuredatabricks.net
DATABRICKS_HOST_PREPROD       # https://adb-aaaa.12.azuredatabricks.net
DATABRICKS_HOST_PROD          # https://adb-bbbb.12.azuredatabricks.net
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

### Environment Isolation

```yaml
# databricks.yml targets configuration
targets:
  dev:
    variables:
      catalog_name: dev      # All jobs use 'dev' catalog
      git_sha: "dev-build"
    workspace:
      host: ${var.databricks_host}
      root_path: /Workspace/Repos/ngm_mlops-dev
  
  prod:
    variables:
      catalog_name: prod     # All jobs use 'prod' catalog
      git_sha: "prod-build"
    workspace:
      host: ${var.databricks_host}
      root_path: /Workspace/Repos/ngm_mlops-prod
```

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
gh run list --workflow=ci.yml --limit 5

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
- [Deployment Troubleshooting](./TROUBLESHOOTING.md)
- [Advanced Configuration](./ADVANCED-CONFIG.md)
