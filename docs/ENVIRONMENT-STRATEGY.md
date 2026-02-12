# Environment Strategy & Promotion Flow

This project uses a 5-environment strategy for MLOps:

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
- Bundles will be deployed to workspace paths like `~/Repos/ngm_mlops-rnd`, `/Workspace/Repos/ngm_mlops-dev`, etc.

## Catalog Requirements

Ensure the following catalogs exist in your Databricks workspace:
- `rnd` (dev mode)
- `dev` (dev mode)
- `uat` (prod mode)
- `preprod` (prod mode)
- `prod` (prod mode)

Create them via SQL or Databricks UI if they don't exist:
```sql
CREATE CATALOG IF NOT EXISTS rnd;
CREATE CATALOG IF NOT EXISTS dev;
CREATE CATALOG IF NOT EXISTS uat;
CREATE CATALOG IF NOT EXISTS preprod;
CREATE CATALOG IF NOT EXISTS prod;
```

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

