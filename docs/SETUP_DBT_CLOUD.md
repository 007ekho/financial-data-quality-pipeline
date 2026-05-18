# dbt Cloud Setup Guide

Complete step-by-step recreation of the dbt Cloud project, environment, and nightly job that powers the silver/gold transformations.

## What you'll build

- A dbt Cloud project connected to your GitHub repo
- A production environment pointing at `main` branch and writing to `FINANCIAL_DATA` schemas
- A nightly job that runs source freshness, the dbt models, and dbt tests
- An API token Astronomer uses to trigger the job from Airflow

## Prerequisites

- dbt Cloud account (Developer or Team plan)
- GitHub repository containing your `dbt/` directory
- Snowflake credentials for the `PIPELINE_USER` (created in Snowflake setup)
- The repo already contains:
  - `dbt/dbt_project.yml`
  - `dbt/packages.yml`
  - `dbt/macros/get_custom_schema.sql`
  - `dbt/models/silver/` and `dbt/models/gold/`

---

## Step 1 — Create dbt Cloud account

1. Go to https://www.getdbt.com/signup
2. Sign up with Google or GitHub
3. Choose **Snowflake** when asked which warehouse
4. Choose **GitHub** when asked which Git provider

You'll land on the dbt Cloud onboarding flow.

---

## Step 2 — Connect Snowflake

In dbt Cloud:

1. Click **Account settings** (gear icon, top right)
2. Click **Projects** in the left nav
3. Click **+ New Project**
4. Project name: `financial_data_quality_pipeline`
5. Click **Continue**
6. Choose **Snowflake** as the warehouse
7. Fill in the connection details:
   - **Name:** `Snowflake Production`
   - **Account:** `<org>-<account>` (e.g. `OEB79775-XYZ12345`)
   - **Database:** `FINANCIAL_DATA`
   - **Warehouse:** `PIPELINE_WH`
   - **Role:** `PIPELINE_ROLE`
   - **Auth method:** Username & Password
   - **Username:** `PIPELINE_USER`
   - **Password:** the password you set for PIPELINE_USER in Snowflake setup
   - **Schema:** `SILVER` (this becomes the user's default development schema)
   - **Threads:** `4`
8. Click **Test Connection** — should return green
9. Click **Save**

---

## Step 3 — Connect GitHub

1. In the same project setup wizard, choose **GitHub**
2. Click **Connect GitHub Account** (opens new tab)
3. Authorise dbt Cloud to access your repo
4. Pick the repository: `<your-username>/financial-data-quality-pipeline`
5. Click **Save**
6. Back in dbt Cloud, set:
   - **Repository:** the repo you just authorised
   - **Subdirectory:** `dbt/` (critical — dbt models live under this subfolder)
7. Click **Save**

---

## Step 4 — Initialise the project

1. Click **Develop** in the left nav
2. dbt Cloud will open the IDE
3. Click **Initialize dbt project** (only if it asks — should already be initialised if your repo has dbt_project.yml)
4. Run `dbt deps` from the dbt Cloud terminal at the bottom to install `dbt_utils`
5. Run `dbt debug` to verify the connection works

---

## Step 5 — Create the Production environment

1. Click **Orchestration** in the left nav
2. Click **Environments**
3. Click **+ Create environment**
4. **Environment name:** `Production`
5. **Environment type:** `Deployment`
6. **Deployment type:** `Production`
7. **dbt version:** `Versionless` (uses the latest stable dbt-fusion)
8. **Git branch:** `main`
9. **Schema:** `FINANCIAL_DATA` (root, custom schema macro will redirect to GOLD/SILVER)
10. Under **Deployment connection**:
    - **Username:** `PIPELINE_USER`
    - **Password:** (PIPELINE_USER password)
    - **Schema:** `FINANCIAL_DATA` (just the database root — macro handles the rest)
11. Click **Save**

---

## Step 6 — Create the nightly job

1. From the same Environments page, click into **Production**
2. Click **+ Create job**
3. Click **Deploy job**
4. **Job name:** `nightly-financial-transactions`
5. **Environment:** Production
6. Scroll to **Execution settings**:
   - **Job timeout (seconds):** `1800`
   - Tick **Run source freshness** at the top (this auto-runs `dbt source freshness` before commands)
   - Tick **Generate docs on run** if you want lineage docs
7. **Commands** — add these in order:
   ```
   dbt run --select fct_financial_transactions+
   dbt test
   ```
   (Do NOT add `dbt deps` — dbt Cloud runs it automatically before every job)
8. **Triggers:**
   - **Run on schedule:** OFF (Airflow triggers this via API instead of cron)
   - **Continuous integration:** ON if you want dbt Cloud's own CI; OFF for this project (GitHub Actions handles CI)
9. Click **Save**

---

## Step 7 — Get the job and account IDs

You need these for the Airflow `dbt_cloud_default` connection.

1. Look at the URL of the job page: `https://<region>.dbt.com/deploy/<ACCOUNT_ID>/projects/<PROJECT_ID>/jobs/<JOB_ID>`
2. Save these three numeric IDs:
   - **Account ID** (e.g. `270005`)
   - **Project ID** (e.g. `538776`)
   - **Job ID** (e.g. `1050326`)

Also identify your dbt Cloud region — the URL prefix tells you (`https://op686.us1.dbt.com` means region `us1`).

---

## Step 8 — Create an API token

1. Click your avatar (top right) → **Account settings**
2. Click **API tokens** in the left nav
3. Click **+ New token**
4. **Name:** `airflow-trigger`
5. **Permissions:** Account-level admin (simplest) OR scope to job-trigger-only if you want least-privilege
6. Click **Create**
7. **Copy the token now** — it's only shown once

Save it in your password manager temporarily. You'll paste it into the Airflow `dbt_cloud_default` connection in the Astronomer setup.

---

## Step 9 — Verify the job runs manually

1. From the job page, click **Run now**
2. Wait for the run to start (yellow → green)
3. Click into the run to see each step's logs:
   - Run source freshness
   - dbt deps
   - dbt run --select fct_financial_transactions+
   - dbt test

All should pass. If any fail, check:
- Snowflake credentials (Step 2)
- Schema permissions (PIPELINE_ROLE must have CREATE TABLE/VIEW on SILVER and GOLD)
- Bronze table has data (Snowpipe needs to have ingested at least one batch)

---

## Step 10 — Verify the tests actually fire

After the run completes:

1. Click on the job run
2. Find the `dbt test` step
3. Click into it
4. You should see a list of tests with their pass/fail status:

```
1 of 14 START test accepted_values_stg_financial_transactions_status .... [PASS]
2 of 14 START test not_null_stg_financial_transactions_account_id ........ [PASS]
3 of 14 START test relationships_fct_financial_transactions_transaction_id [PASS]
...
14 of 14 START test unique_fct_financial_transactions_transaction_id ..... [PASS]
PASS=14 WARN=0 ERROR=0 SKIP=0
```

If `PASS=14` (or however many tests you have) — everything works.

---

## What you've now got

- dbt Cloud project pointed at your repo's `dbt/` subdirectory
- Production environment writing to `FINANCIAL_DATA.SILVER` and `FINANCIAL_DATA.GOLD`
- Nightly job that:
  - Validates source data is fresh
  - Builds silver staging view
  - Builds gold fact table
  - Runs all dbt tests
- API token Airflow will use to trigger this job

Next: Astronomer setup wires Airflow to trigger this job (`docs/SETUP_ASTRONOMER.md`).

---

## Common gotchas

| Issue | Cause | Fix |
|---|---|---|
| `Could not find dbt_project.yml` | Subdirectory wrong | Set subdirectory to `dbt/` in project settings |
| `permission denied: SILVER` | PIPELINE_ROLE missing grants | Re-run Snowflake grants from setup |
| dbt test fails with no rows | Bronze table is empty | Run data_generator to seed BRONZE |
| Run hangs on `dbt deps` | Network issue to hub.getdbt.com | Retry — usually transient |
| `Deprecated test arguments` errors | dbt-fusion 2.0 stricter syntax | Use `arguments:` wrapper (see schema.yml files in the repo) |

---

## Teardown

In dbt Cloud:

1. Account settings → Projects → click your project → **Delete project**
2. Account settings → API tokens → revoke the airflow-trigger token
