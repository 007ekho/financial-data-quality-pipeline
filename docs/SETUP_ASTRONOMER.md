# Astronomer Astro Setup Guide

Complete step-by-step recreation of the Astronomer cloud deployment, Airflow connections, variables, and DAG deployment.

## What you'll build

- An Astronomer cloud workspace and deployment
- Airflow connections to Snowflake and dbt Cloud
- A deployment variable for the Slack webhook
- A deployed DAG running on the cloud Airflow

## Prerequisites

- Astronomer Astro account (free trial works — https://www.astronomer.io)
- The Astronomer CLI installed locally
- Your `astro_deploy/` directory contains:
  - `Dockerfile`
  - `requirements.txt`
  - `dags/financial_transactions_pipeline.py`
  - `include/contracts/bronze/financial_transactions_bronze.yml`
  - `include/contracts/gold/fct_financial_transactions.yml`
- dbt Cloud API token, account ID, and job ID (from `SETUP_DBT_CLOUD.md`)
- A Slack incoming webhook URL pointing at `#financial-dq-alerts`

---

## Step 1 — Install the Astro CLI

macOS:

```bash
brew install astro
```

Other OS:

```bash
curl -sSL https://install.astronomer.io | sudo bash -s
```

Verify:

```bash
astro version
```

---

## Step 2 — Create an Astronomer account and trial

1. Go to https://cloud.astronomer.io/signup
2. Sign up with Google or GitHub (whichever you'll have reliable access to)
3. When you see "Start a Free Astro Trial", click it
4. Enter a workspace name: `financial-data-quality`
5. Click **Create**

---

## Step 3 — Login from the CLI

```bash
astro login astronomer.io
```

A browser tab opens. Authenticate with the same account from Step 2. The terminal should say "Successfully logged in".

Verify:

```bash
astro workspace list
```

You should see the workspace you just created with an ID like `cmp937fdc694501mhnbhojvl0`.

---

## Step 4 — Create a deployment

```bash
astro deployment create \
  --name financial-data-quality-prod \
  --workspace-id cmp937fdc694501mhnbhojvl0
```

The CLI will prompt for a region. The four options are:

```
1     eastus2
2     australiaeast
3     westus2
4     westeurope
```

For UK users pick `4` (westeurope). For US users pick `1`.

After ~60 seconds the deployment is provisioned. Save the **Deployment ID** from the output (e.g. `cmp93atnn6cez01oruyf4jv07`).

Save the **Airflow UI URL** (e.g. `https://cmp93atnn6cez01oruyf4jv07.07.astronomer.run/dyf4jv07`).

---

## Step 5 — Deploy the project

From the project root:

```bash
cd astro_deploy
astro deploy <DEPLOYMENT_ID> -f
```

`-f` skips the "are you sure?" prompt. The deploy takes ~2 minutes. Output ends with:

```
Successfully pushed image to Astronomer registry.
Airflow UI: https://cmp93atnn6cez01oruyf4jv07.07.astronomer.run/dyf4jv07
```

---

## Step 6 — Open the cloud Airflow UI

In your browser open the Airflow UI URL from Step 4. Log in with the same Astronomer account.

You should see `financial_transactions_pipeline` listed but **paused** (the toggle next to its name is grey).

---

## Step 7 — Add Airflow connections

### 7a) Snowflake connection

1. In Airflow UI: **Admin → Connections**
2. Click **+ Add a new record**
3. Fill in:
   - **Connection Id:** `snowflake_pipeline`
   - **Connection Type:** `Snowflake`
   - **Login:** `PIPELINE_USER`
   - **Password:** (the PIPELINE_USER password)
   - **Schema:** `BRONZE`
   - **Extra:** (paste this JSON)
     ```json
     {
       "account": "OEB79775-XYZ12345",
       "database": "FINANCIAL_DATA",
       "warehouse": "PIPELINE_WH",
       "role": "PIPELINE_ROLE"
     }
     ```
     Replace `OEB79775-XYZ12345` with your actual Snowflake account identifier.
4. Click **Save**

### 7b) dbt Cloud connection

1. Click **+ Add a new record** again
2. Fill in:
   - **Connection Id:** `dbt_cloud_default`
   - **Connection Type:** `Generic`
   - **Password:** (paste your dbt Cloud API token from `SETUP_DBT_CLOUD.md`)
   - **Extra:** (paste this JSON)
     ```json
     {
       "account_id": "270005",
       "job_id": "1050326"
     }
     ```
     Use your actual account_id and job_id from dbt Cloud.
3. Click **Save**

---

## Step 8 — Add the Slack webhook deployment variable

The Airflow UI variables panel **does not work reliably** in this Airflow version. Use the CLI instead:

```bash
astro deployment variable create \
  --deployment-id <DEPLOYMENT_ID> \
  --key AIRFLOW_VAR_SLACK_WEBHOOK_URL \
  --value "https://hooks.slack.com/services/YOUR/WEBHOOK/HERE" \
  --secret
```

Replace the webhook URL with your real one (no trailing newline — paste directly).

Astronomer auto-translates `AIRFLOW_VAR_*` env vars into Airflow Variables. The DAG's `_get_slack_webhook()` helper reads it via env var.

Verify:

```bash
astro deployment variable list --deployment-id <DEPLOYMENT_ID>
```

You should see `AIRFLOW_VAR_SLACK_WEBHOOK_URL` with value `****` and secret `true`.

The deployment will restart automatically (~30 seconds) to pick up the env var.

---

## Step 9 — Unpause the DAG

In the cloud Airflow UI:

1. Click the **paused** toggle next to `financial_transactions_pipeline`
2. The DAG is now scheduled to run at 02:05 UTC nightly

---

## Step 10 — Verify with a manual trigger

1. Click on `financial_transactions_pipeline`
2. Click **▶ Trigger DAG**
3. The DAG runs

Expected task progression:

```
wait_for_snowpipe   ✅
soda_check_raw      ✅ (passed because clean data)
raw_quality_gate    ✅ (branches to run_dbt_transform)
run_dbt_transform   ✅ (triggers dbt Cloud, waits for completion)
soda_check_serving  ✅ (passed because dbt produced valid gold)
serving_quality_gate ✅ (branches to pipeline_complete)
pipeline_complete   ✅
end                 ✅
```

If `wait_for_snowpipe` hangs, run the data generator first (`python3 data_generator/generate_transactions.py`) to seed BRONZE.

---

## Step 11 — Verify the audit log was written

In Snowflake:

```sql
SELECT dataset_name, checkpoint, scan_status, dag_run_id, created_at
FROM FINANCIAL_DATA.AUDIT.DATA_QUALITY_LOG
ORDER BY created_at DESC
LIMIT 5;
```

You should see two rows for the latest run — bronze PASSED and gold PASSED — both carrying the same `dag_run_id` (a `manual__...` prefix for manual triggers, `scheduled__...` for scheduled).

---

## What you've now got

- Cloud Airflow with the DAG deployed and connections configured
- Scheduled to run nightly at 02:05 UTC
- Slack webhook wired up via env var
- Snowflake and dbt Cloud connections working
- Audit log capturing every run

Next: GitHub Actions setup wires up the CI gate and auto-deploy (`docs/SETUP_GITHUB_ACTIONS.md`).

---

## Common gotchas

| Issue | Cause | Fix |
|---|---|---|
| `organization with id ... is forbidden` | Old workspace permissions | Logout, login fresh account |
| Airflow Variable not found at runtime | UI Variable not propagating | Use `AIRFLOW_VAR_*` env var instead |
| Slack POST returns 404 | Trailing whitespace in webhook URL | Defensive `.strip()` in helper; re-paste URL |
| `NameError: name 'context' is not defined` | Task missing `**context` kwarg | Add `**context` to function signature |
| Deploy never finishes | Slow network | Wait — first deploy is ~2 min, subsequent are faster |

---

## Teardown

```bash
astro deployment delete <DEPLOYMENT_ID>
```

To delete the entire workspace, do it from the cloud UI.
