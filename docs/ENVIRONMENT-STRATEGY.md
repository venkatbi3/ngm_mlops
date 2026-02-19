# Quick Start: 5-Environment Deployment

## Overview
This project now uses a 5-environment strategy for structured development, testing, and production deployments.

## Environments

| Environment | Catalog | Mode        | Purpose                              | Triggers                |
|-------------|---------|-------------|--------------------------------------|-------------------------|
| **rnd**     | rnd     | Development | Research & Development               | Auto-deploy on main     |
| **dev**     | dev     | Development | Unit Testing                         | Push to dev branch       |
| **uat**     | uat     | Production  | Integration Testing & UAT            | Push to uat branch       |
| **preprod** | preprod | Production  | Pre-Production / Staging             | Push to preprod branch   |
| **prod**    | prod    | Production  | Production / Live                    | Push to prod branch      |


## Deployment Flow

### Development & Testing
```
main branch
    ↓ (auto-deploy)
  RND Environment (Research & Development)
    ↓ (manual merge to dev)
dev branch
    ↓ (auto-deploy)
  DEV Environment (Unit Testing)
    ↓ (manual merge to uat)
uat branch
    ↓ (auto-deploy)
  UAT Environment (Integration Testing)
```

### Production Deployment
```
uat branch (validated)
    ↓ (manual merge to preprod)
preprod branch
    ↓ (auto-deploy)
  PREPROD Environment (Final Staging)
    ↓ (manual merge to prod)
prod branch
    ↓ (auto-deploy)
  PROD Environment (Live)
```

## Git Branching Strategy

- **main** → RND (active development, auto-deploys)
- **dev** → DEV (unit testing)
- **uat** → UAT (integration testing)
- **preprod** → PREPROD (pre-production validation)
- **prod** → PROD (production release)

## Workflow Usage

### Automatic Deployment (Branch-based)
Push to any branch to trigger auto-deployment:
```bash
git push origin dev     # Deploys to DEV environment
git push origin uat     # Deploys to UAT environment
git push origin preprod # Deploys to PREPROD environment
git push origin prod    # Deploys to PROD environment
```

### Manual Deployment (Workflow Dispatch)
Use the `CD - Multi-Environment Deployment` workflow to deploy to any environment manually:
1. Go to GitHub Actions → "CD - Multi-Environment Deployment"
2. Click "Run workflow"
3. Select target environment (dev, uat, preprod, or prod)
4. Optionally specify a custom git_sha
5. Click "Run workflow"

## Prerequisites

- `DATABRICKS_TOKEN` secret must be set in GitHub (Settings → Secrets → Actions)
- Ensure you have permissions to deploy to the target Databricks workspace

## Catalog Requirements

Ensure the following catalogs exist in your Databricks workspace:
- `rnd` (dev mode)
- `dev` (dev mode)
- `uat` (prod mode)
- `preprod` (prod mode)
- `prod` (prod mode)

Create them via SQL or Databricks UI if they don't exist:
```sql
CREATE CATALOG IF NOT EXISTS ngm_ml_rnd;
CREATE CATALOG IF NOT EXISTS dev;
CREATE CATALOG IF NOT EXISTS uat;
CREATE CATALOG IF NOT EXISTS preprod;
CREATE CATALOG IF NOT EXISTS prod;
```

## Quick Commands

### Push to RND (automatic)
```bash
git commit -am "your message"
git push origin main  # Triggers CI → test → auto-deploy to RND
```

### Deploy to DEV
```bash
git checkout dev
git merge main        # or make changes
git push origin dev   # Triggers cd-dev.yml → deploy to DEV
```

### Deploy to UAT
```bash
git checkout uat
git merge dev         # Promote from DEV
git push origin uat   # Triggers cd-uat-branch.yml → deploy to UAT
```

### Deploy to PREPROD
```bash
git checkout preprod
git merge uat         # Promote from UAT
git push origin preprod  # Triggers cd-preprod.yml → deploy to PREPROD
```

### Deploy to PROD
```bash
git checkout prod
git merge preprod     # Promote from PREPROD
git push origin prod  # Triggers cd-prod-branch.yml → deploy to PROD
```

### Manual Deployment (any environment)
1. Go to: https://github.com/venkatbi3/ngm_mlops/actions
2. Select: **CD - Multi-Environment Deployment**
3. Click: **Run workflow**
4. Choose environment from dropdown (dev, uat, preprod, or prod)
5. Click: **Run workflow**

## CI/CD Workflows

| Workflow File | Trigger | Deploys To | Status |
|---------------|---------|-----------|--------|
| cd-rnd.yml | Push to main | RND |   Auto |
| cd-dev.yml | Push to dev | DEV |   Manual |
| cd-uat-branch.yml | Push to uat | UAT |   Manual |
| cd-preprod.yml | Push to preprod | PREPROD |   Manual |
| cd-prod-branch.yml | Push to prod | PROD |   Manual |
| cd-environments.yml | Manual dispatch | Any |   Manual |

## Workspace Paths (Databricks)
- RND: `~/Repos/ngm_mlops-rnd`
- DEV: `~/Repos/ngm_mlops-dev`
- UAT: `/Workspace/Repos/ngm_mlops-uat`
- PREPROD: `/Workspace/Repos/ngm_mlops-preprod`
- PROD: `/Workspace/Repos/ngm_mlops-prod`


## Environment Promotion Checklist

### RND → DEV
- [ ] Tests pass in RND
- [ ] Code review completed
- [ ] Ready for unit testing

### DEV → UAT
- [ ] Unit tests pass in DEV
- [ ] All features integrated
- [ ] Ready for integration testing

### UAT → PREPROD
- [ ] UAT testing complete
- [ ] No critical issues
- [ ] Final validation environment

### PREPROD → PROD
- [ ] Preprod deployment verified
- [ ] Performance validated
- [ ] Ready for production release


## Troubleshooting

### Workflow not triggering?
- Check GitHub branch exists: `git branch -a`
- Verify branch protection rules allow workflow dispatch
- Check DATABRICKS_TOKEN secret is set in GitHub

### Deployment failed?
- Check workflow logs: https://github.com/venkatbi3/ngm_mlops/actions
- Verify target catalog exists in Databricks
- Check databricks-cli version (must be >=0.220.0)

### Tests failing before deploy?
- Run locally: `pytest -v`
- Check pytest.ini configuration
- Verify all dependencies in requirements.txt and requirements-dev.txt

## Related Documentation
- Full details: [ENVIRONMENT-STRATEGY.md](ENVIRONMENT-STRATEGY.md)
- Deployment issues: See workflow logs or check Databricks workspace
