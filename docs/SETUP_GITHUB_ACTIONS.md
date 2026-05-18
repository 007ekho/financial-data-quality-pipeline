# GitHub Actions Setup Guide

Complete step-by-step recreation of the two GitHub Actions workflows: the CI gate (Soda contracts on PR) and the auto-deploy (Astronomer on merge to main).

## What you'll build

- **CI gate workflow** that runs Soda contracts against `BRONZE_CI`/`GOLD_CI` whenever a PR touches `dbt/` or contracts
- **Auto-deploy workflow** that runs `astro deploy` whenever `astro_deploy/**` changes on main
- All the GitHub Secrets needed by both
- Branch protection making CI a required check

## Prerequisites

- Your repo exists at `github.com/<username>/financial-data-quality-pipeline`
- `BRONZE_CI` and `GOLD_CI` Snowflake schemas already populated with 100-row fixtures (from `SETUP_SNOWFLAKE.md`)
- An Astronomer deployment token (we'll generate one)
- Repo files in place:
  - `.github/workflows/data_quality_ci.yml`
  - `.github/workflows/deploy_astronomer.yml`
  - `ci/run_soda_scan.py`

---

## Step 1 — Generate an Astronomer API token for deploys

```bash
astro deployment token create \
  --deployment-id <YOUR_DEPLOYMENT_ID> \
  --name github-actions-deploy \
  --role DEPLOYMENT_ADMIN
```

**Copy the token immediately** — Astronomer only shows it once.

---

## Step 2 — Add GitHub Secrets

Go to **`https://github.com/<username>/financial-data-quality-pipeline/settings/secrets/actions`** and add each of these:

| Secret Name | Value | Where it comes from |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | `OEB79775-XYZ12345` | Snowflake → `SELECT CURRENT_ORGANIZATION_NAME() \|\| '-' \|\| CURRENT_ACCOUNT_NAME();` |
| `SNOWFLAKE_CI_USER` | `PIPELINE_USER` | Username from Snowflake setup |
| `SNOWFLAKE_CI_PASSWORD` | the PIPELINE_USER password | Snowflake setup |
| `SNOWFLAKE_DATABASE` | `FINANCIAL_DATA` | Snowflake setup |
| `SNOWFLAKE_CI_WAREHOUSE` | `PIPELINE_WH` | Snowflake setup |
| `ASTRO_API_TOKEN` | the deployment token from Step 1 | Astronomer CLI |

For each: click **New repository secret** → paste name → paste value → click **Add secret**.

---

## Step 3 — Create the CI gate workflow

Create `.github/workflows/data_quality_ci.yml`:

```yaml
# CI gate — runs on every PR touching contracts or dbt models.
# Blocks merge if Soda 4.x contracts fail against CI schemas.

name: Data quality CI gate

on:
  pull_request:
    paths:
      - "astro_deploy/include/contracts/**"
      - "dbt/**"

env:
  SNOWFLAKE_ACCOUNT:   ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER:      ${{ secrets.SNOWFLAKE_CI_USER }}
  SNOWFLAKE_PASSWORD:  ${{ secrets.SNOWFLAKE_CI_PASSWORD }}
  SNOWFLAKE_DATABASE:  ${{ secrets.SNOWFLAKE_DATABASE }}
  SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_CI_WAREHOUSE }}

jobs:
  data-quality-ci:
    name: Soda 4.x contracts (CI schemas)
    runs-on: ubuntu-latest

    steps:

      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip

      - name: Install Soda Core 4.x
        run: |
          python3 -m pip install --upgrade pip
          python3 -m pip install "soda-core==4.7.0" "soda-snowflake==4.7.0"
          python3 -c "from soda_core.contracts import verify_contract_locally; print('Soda 4.x ready')"

      - name: Generate Soda data source YAML files
        run: |
          mkdir -p .soda_runtime

          cat > .soda_runtime/data_source_bronze.yml << YAML
          type: snowflake
          name: snowflake_bronze
          connection:
            account:    ${SNOWFLAKE_ACCOUNT}
            user:       ${SNOWFLAKE_USER}
            password:   ${SNOWFLAKE_PASSWORD}
            database:   ${SNOWFLAKE_DATABASE}
            warehouse:  ${SNOWFLAKE_WAREHOUSE}
            schema:     BRONZE_CI
            role:       PIPELINE_ROLE
          YAML

          cat > .soda_runtime/data_source_gold.yml << YAML
          type: snowflake
          name: snowflake_gold
          connection:
            account:    ${SNOWFLAKE_ACCOUNT}
            user:       ${SNOWFLAKE_USER}
            password:   ${SNOWFLAKE_PASSWORD}
            database:   ${SNOWFLAKE_DATABASE}
            warehouse:  ${SNOWFLAKE_WAREHOUSE}
            schema:     GOLD_CI
            role:       PIPELINE_ROLE
          YAML

      - name: Patch contracts to point at CI schemas
        run: |
          sed -i 's|snowflake_bronze/BRONZE/|snowflake_bronze/BRONZE_CI/|' \
            astro_deploy/include/contracts/bronze/financial_transactions_bronze.yml

          sed -i 's|snowflake_gold/GOLD/|snowflake_gold/GOLD_CI/|' \
            astro_deploy/include/contracts/gold/fct_financial_transactions.yml

      - name: Soda BRONZE contract scan (CI)
        run: |
          python3 ci/run_soda_scan.py \
            .soda_runtime/data_source_bronze.yml \
            astro_deploy/include/contracts/bronze/financial_transactions_bronze.yml

      - name: Soda GOLD contract scan (CI)
        run: |
          python3 ci/run_soda_scan.py \
            .soda_runtime/data_source_gold.yml \
            astro_deploy/include/contracts/gold/fct_financial_transactions.yml

      - name: Post CI status to PR
        if: always()
        uses: actions/github-script@v7
        with:
          script: |
            const status = '${{ job.status }}';
            const body = status === 'success'
              ? '### Soda CI gate — all contracts pass ✅\nMerge is unblocked.'
              : '### Soda CI gate — contracts failed ❌\nCheck the Actions log for details. Merge is blocked.';
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner:        context.repo.owner,
              repo:         context.repo.repo,
              body
            });
```

---

## Step 4 — Create the auto-deploy workflow

Create `.github/workflows/deploy_astronomer.yml`:

```yaml
# Auto-deploy to Astronomer Astro when changes hit main.

name: Deploy to Astronomer

on:
  push:
    branches:
      - main
    paths:
      - "astro_deploy/**"

jobs:
  deploy:
    name: Push Astro project to cloud Airflow
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Deploy to Astronomer
        uses: astronomer/deploy-action@v0.9.0
        with:
          deployment-id: cmp93atnn6cez01oruyf4jv07
          root-folder: astro_deploy/
        env:
          ASTRO_API_TOKEN: ${{ secrets.ASTRO_API_TOKEN }}
```

Replace `cmp93atnn6cez01oruyf4jv07` with your actual deployment ID.

---

## Step 5 — Commit and push the workflows

```bash
git checkout dev
git pull origin dev
git add .github/workflows/data_quality_ci.yml .github/workflows/deploy_astronomer.yml
git commit -m "ci: add data-quality CI gate and Astronomer auto-deploy"
git push origin dev
```

---

## Step 6 — Test the CI gate

1. From `dev`, create a test branch:
   ```bash
   git checkout -b test/verify-ci-gate
   ```
2. Touch `dbt/dbt_project.yml` (any whitespace change) so the workflow triggers
3. Commit and push:
   ```bash
   git add dbt/dbt_project.yml
   git commit -m "test: trigger CI workflow"
   git push origin test/verify-ci-gate
   ```
4. Open a PR `test/verify-ci-gate → main` on GitHub
5. Watch the **Actions** tab — `Data quality CI gate` should run
6. Expected outcome: all 17 Soda checks pass on each contract; CI returns green
7. PR shows comment: "Soda CI gate — all contracts pass ✅"
8. Close the PR without merging (or merge if you want)

---

## Step 7 — Test the CI gate blocks a bad PR

1. From dev:
   ```bash
   git checkout dev
   git checkout -b test/break-ci-gate
   ```
2. Edit `astro_deploy/include/contracts/bronze/financial_transactions_bronze.yml`
3. Find the `AMOUNT` column block. Replace it with:
   ```yaml
     - name: AMOUNT
       data_type: float
       checks:
         - missing:
             threshold:
               must_be: 0
         - invalid:
             valid_min: 999999
             threshold:
               must_be: 0
   ```
4. Commit, push, open PR to main
5. CI gate runs and fails — `[FAILED]` on the invalid check, exit code 1
6. PR is blocked
7. Close the PR

This proves the gate actually catches broken contracts.

---

## Step 8 — Set up branch protection (optional but recommended)

1. Go to **Settings → Branches**
2. Click **Add rule** under "Branch protection rules"
3. **Branch name pattern:** `main`
4. Tick:
   - **Require a pull request before merging**
   - **Require status checks to pass before merging**
   - **Require branches to be up to date before merging**
5. Under "Status checks that are required", search for and add:
   - `data-quality-ci`
6. Click **Create**

Now no one can push directly to main — every change goes through a PR, and that PR must pass the Soda CI gate.

---

## Step 9 — Test the auto-deploy

1. Merge a real PR to main (anything touching `astro_deploy/`)
2. Watch the **Actions** tab — `Deploy to Astronomer` should fire automatically
3. Expected duration: ~90 seconds
4. Output ends with `Successfully uploaded DAGs with version ...`
5. Wait ~60 seconds for cloud Airflow to pick up the new bundle
6. In the Airflow UI, the DAG run path will show the new bundle timestamp

---

## What you've now got

- Two workflows: CI gate (`data_quality_ci.yml`) and auto-deploy (`deploy_astronomer.yml`)
- All required secrets configured
- Branch protection enforcing CI as a required check
- GitOps loop closed: edit → PR → CI gate → merge → auto-deploy → cloud Airflow

---

## Common gotchas

| Issue | Cause | Fix |
|---|---|---|
| CI workflow doesn't trigger | PR doesn't touch matched paths | Touch `dbt/` or `contracts/` to fire |
| `Snowflake authentication failed` | Wrong secret values | Double-check `SNOWFLAKE_ACCOUNT` format |
| `❌ Contract failed` with no detail | Old version of `run_soda_scan.py` | Use the verbose version that prints per-check results |
| Auto-deploy doesn't fire on merge | PR didn't touch `astro_deploy/` | Touch `astro_deploy/dags/` or `astro_deploy/include/` to fire |
| Auto-deploy fails with auth error | `ASTRO_API_TOKEN` expired | Regenerate with `astro deployment token create` |
| Deploy succeeds but DAG runs old code | Airflow picks up new bundle on ~1-min cadence | Wait 60-90 seconds after deploy |

---

## Teardown

Delete the workflow files from `.github/workflows/` and commit. Remove the GitHub Secrets from repo settings if you're done with the project. Revoke the Astro API token:

```bash
astro deployment token delete --deployment-id <ID> --name github-actions-deploy
```
