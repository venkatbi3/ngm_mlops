# 5-Environment Restructuring Summary

## What Changed

### 1. **Updated `databricks.yml`**
- Replaced old 4-target setup (dev, staging, test, prod) with new 5-target structure
- New targets: **rnd**, **dev**, **uat**, **preprod**, **prod**
- Each target has own catalog and workspace path configured appropriately

| Target | Catalog | Mode | Path |
|--------|---------|------|------|
| rnd | rnd | development | ~/Repos/ngm_mlops-rnd |
| dev | dev | development | ~/Repos/ngm_mlops-dev |
| uat | uat | production | /Workspace/Repos/ngm_mlops-uat |
| preprod | preprod | production | /Workspace/Repos/ngm_mlops-preprod |
| prod | prod | production | /Workspace/Repos/ngm_mlops-prod |

### 2. **Updated CI Workflow (`ci.yml`)**
- Renamed deploy job from "deploy" to "deploy-rnd"
- Changed target from "dev" to "rnd"
- Auto-deploys to RND on every push to main branch after tests pass

### 3. **New GitHub Actions Workflows**

#### **cd-environments.yml** (Manual Dispatch)
- Allows manual deployment to any environment from GitHub UI
- Triggered: GitHub Actions → CD - Multi-Environment Deployment → Run workflow
- Select target environment from dropdown (dev, uat, preprod, or prod)
- Optional: Specify custom git_sha

#### **cd-dev.yml** (Branch-based)
- Auto-deploys to DEV when code is pushed to dev branch
- Validates and deploys with single command

#### **cd-uat-branch.yml** (Branch-based)
- Auto-deploys to UAT when code is pushed to uat branch
- Validates and deploys with single command

#### **cd-preprod.yml** (Branch-based)
- Auto-deploys to PREPROD when code is pushed to preprod branch
- Validates and deploys with single command

#### **cd-prod-branch.yml** (Branch-based)
- Auto-deploys to PROD when code is pushed to prod branch
- Validates and deploys with single command

### 4. **New Documentation**
- `docs/ENVIRONMENT-STRATEGY.md`: Detailed environment strategy, promotion flow, prerequisites
- `ENVIRONMENT-QUICK-START.md`: Quick reference for common commands and workflows

## Deployment Flow

```
main branch (auto-deploy)
    ↓
RND (Research & Development)
    ↓ (manual merge to dev branch)
dev branch (auto-deploy)
    ↓
DEV (Unit Testing)
    ↓ (manual merge to uat branch)
uat branch (auto-deploy)
    ↓
UAT (Integration Testing)
    ↓ (manual merge to preprod branch)
preprod branch (auto-deploy)
    ↓
PREPROD (Pre-Production)
    ↓ (manual merge to prod branch)
prod branch (auto-deploy)
    ↓
PROD (Production)
```

## How to Use

### Automatic Deployment (Branch-based)
```bash
# RND: Just push to main
git push origin main

# DEV: Create or push to dev branch
git push origin dev

# UAT: Push to uat branch
git push origin uat

# PREPROD: Push to preprod branch
git push origin preprod

# PROD: Push to prod branch
git push origin prod
```

### Manual Deployment (Any Environment)
1. Go to: https://github.com/venkatbi3/ngm_mlops/actions
2. Select: **CD - Multi-Environment Deployment**
3. Click: **Run workflow**
4. Choose target environment and click Run

## Benefits

✅ **Clear Development Path**: RND for active development  
✅ **Staged Testing**: DEV for unit tests, UAT for integration tests  
✅ **Safe Production**: PREPROD validates before PROD  
✅ **Automatic Promotion**: Branch-based workflows eliminate manual steps  
✅ **Manual Override**: Dispatch workflow for emergency hotfixes  
✅ **Environment Isolation**: Each environment has own catalog and workspace path  
✅ **Audit Trail**: Git history tracks all deployments  

## Next Steps

1. ✅ Create dev, uat, preprod, prod branches
2. Push code to main → RND deploys automatically
3. Merge to dev → DEV deploys automatically
4. Promote through pipeline as needed
5. For UAT/PREPROD/PROD: Use branch pushes or manual dispatch

## Troubleshooting

**CI workflow not running?**
- Ensure self-hosted runners are enabled
- Check GitHub Actions settings

**Deployment to environment failing?**
- Check target catalog exists in Databricks
- Verify DATABRICKS_TOKEN secret is valid
- Review workflow logs in GitHub Actions

**Tests failing?**
- Run `pytest -v` locally
- Check pytest.ini and requirements.txt

See `ENVIRONMENT-STRATEGY.md` for detailed guidance.
