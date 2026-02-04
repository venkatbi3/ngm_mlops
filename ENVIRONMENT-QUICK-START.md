# Quick Start: 5-Environment Deployment

## Overview
This project now uses a 5-environment strategy for structured development, testing, and production deployments.

## Environment Summary
- **RND**: Research & Development (auto-deploy on main push)
- **DEV**: Unit Testing (push to dev branch)
- **UAT**: Integration Testing (push to uat branch)
- **PREPROD**: Pre-Production (push to preprod branch)
- **PROD**: Production (push to prod branch)

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
| ci.yml | Push to main | RND | ✅ Auto |
| cd-dev.yml | Push to dev | DEV | ✅ Auto |
| cd-uat-branch.yml | Push to uat | UAT | ✅ Auto |
| cd-preprod.yml | Push to preprod | PREPROD | ✅ Auto |
| cd-prod-branch.yml | Push to prod | PROD | ✅ Auto |
| cd-environments.yml | Manual dispatch | Any | ✅ Manual |

## Workspace Paths (Databricks)
- RND: `~/Bundles/ngm_mlops-rnd`
- DEV: `~/Bundles/ngm_mlops-dev`
- UAT: `/Workspace/Bundles/ngm_mlops-uat`
- PREPROD: `/Workspace/Bundles/ngm_mlops-preprod`
- PROD: `/Workspace/Bundles/ngm_mlops-prod`

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
