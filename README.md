# Financial Data Quality Pipeline

A nightly data quality pipeline that ingests financial transactions, enforces data contracts at the bronze and gold layers, quarantines bad rows, alerts on failure, and maintains a full audit trail.

Built with: **AWS S3 + SQS · Snowflake · Snowpipe · dbt Cloud · Astronomer Astro · Soda Core 4.x · GitHub Actions**

The pipeline runs end-to-end in production: clean data flows through to the gold table; bad data triggers the failure path with Slack alerts and quarantine — and dbt never runs on bad bronze data.

---

## Architecture

```
                    ┌─────────────────────────────────┐
                    │   AWS S3 (.parquet drops)       │
                    └─────────────┬───────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────────┐
                    │   SQS (file-arrival events)     │
                    └─────────────┬───────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────────┐
                    │   Snowpipe (auto-ingest)        │
                    └─────────────┬───────────────────┘
                                  │
                                  ▼
                    ┌─────────────────────────────────┐
                    │   BRONZE.FINANCIAL_TRANSACTIONS │
                    └─────────────┬───────────────────┘
                                  │
                                  ▼ ── Astronomer DAG starts (02:05 UTC)
                    ┌─────────────────────────────────┐
                    │   Soda BRONZE checkpoint        │
                    └─────────────┬───────────────────┘
                                  │
                       ┌──────────┴───────────┐
                       │                      │
                  ✅ pass                ❌ fail
                       │                      │
                       ▼                      ▼
            ┌─────────────────┐    ┌──────────────────────┐
            │ dbt source      │    │ Quarantine bad rows  │
            │ freshness check │    │ + Audit log FAILED   │
            └────────┬────────┘    │ + Slack alert        │
                     │             │ + STOP (no dbt run)  │
                     ▼             └──────────────────────┘
            ┌─────────────────┐
            │ dbt silver+gold │
            └────────┬────────┘
                     │
                     ▼
            ┌─────────────────┐
            │ Soda GOLD       │ ❌ fail → quarantine + slack
            │ checkpoint      │
            └────────┬────────┘
                     │ ✅ pass
                     ▼
            ┌─────────────────┐
            │ Audit log       │
            │ PASSED + run_id │
            └─────────────────┘

Side-channel monitoring:
- Snowflake ALERT runs hourly comparing BRONZE vs BRONZE_CI columns
  → emails me if schema drift between production and CI fixture
- GitHub Actions CI gate runs Soda contracts against BRONZE_CI/GOLD_CI on every PR
  → blocks merges that break contracts before they reach production
```

---

## Stack rationale

| Layer | Tool | Why |
|---|---|---|
| Object store | AWS S3 | Cheap, durable, native Snowpipe integration |
| Event channel | AWS SQS | Snowpipe's native ingest trigger — no Lambda glue |
| AWS IaC | AWS CloudFormation | Native AWS, no state file to manage, deploys S3 + SQS in one stack |
| Warehouse | Snowflake | Storage + compute + native primitives for pipes, alerts, RBAC |
| Auto-ingest | Snowpipe | Event-driven loading from S3 to BRONZE, no batch job needed |
| Transformation | dbt Cloud | Managed transformation with native CI, scheduling, source freshness |
| Orchestration | Astronomer Astro | Managed Airflow without the infrastructure burden |
| Data contracts | Soda Core 4.x | Contract-as-code, runs anywhere Python runs |
| CI gate | GitHub Actions | Native PR gate runs Soda contracts against test schemas |
| Alerting | Slack incoming webhook | Real-time failure notifications |
| Schema-drift alert | Snowflake `ALERT` + email | Hourly check that production schema matches CI fixture |

---

## Medallion architecture (bronze / silver / gold)

| Layer | Owner | Materialisation | Purpose |
|---|---|---|---|
| `BRONZE` | Snowpipe | Table | Raw data as it arrived from S3. No transformations. dbt never writes here. |
| `SILVER` | dbt (view) | View | Typed, cleansed, quarantine-excluded. Zero storage cost. |
| `GOLD` | dbt (table) | Table, clustered on `transaction_date` | Business-ready facts: FX-normalised amounts, derived columns, settlement timing. |

**Why these boundaries matter:**

- **Bronze isolation** — dbt never writes to bronze. If Snowpipe ingests bad data, dbt logic can't accidentally corrupt it. Bronze is the single source of truth for "what arrived."
- **Silver as a view** — no storage cost. The cleansing logic always evaluates against the latest bronze. Quarantine exclusion uses a `LEFT JOIN ANTI` pattern so flagged rows never bleed downstream.
- **Gold as a clustered table** — materialised for analytics performance. Clustered on `transaction_date` to optimise time-range queries.

---

## Data quality enforcement (four layers)

### 1. CI gate (PR-time)

Every PR touching `dbt/` or `astro_deploy/include/contracts/**` triggers a GitHub Actions workflow that:

1. Installs `soda-core==4.7.0`
2. Generates Soda data source YAML at runtime from GitHub Secrets
3. Patches the contracts to point at `BRONZE_CI` / `GOLD_CI` (isolated test schemas)
4. Runs Soda contract verification against the test fixtures
5. Posts pass/fail status to the PR

The CI schemas are static 100-row snapshots of production. Every PR sees the same fixture, so CI results are deterministic — a change passing today will still pass tomorrow.

### 2. Bronze Soda checkpoint (post-Snowpipe)

After Snowpipe finishes loading, the Airflow DAG runs `verify_contract_locally()` against `FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS`. Per-column checks include:

- No nulls on critical columns
- No duplicate `transaction_id`
- Valid enumerated values for `transaction_type`, `status`, `channel`, `source_system`
- Datatype enforcement

If any check fails, the DAG branches into the failure path. dbt does not run.

### 3. Gold Soda checkpoint (post-dbt)

After dbt produces `GOLD.FCT_FINANCIAL_TRANSACTIONS`, a second Soda scan validates the transformations. This catches scenarios where bronze data is fine but the dbt model introduced an issue — wrong derived value, broken FX conversion, invalid bucket assignment.

### 4. Schema drift email alert (proactive)

A Snowflake `ALERT` runs hourly comparing the columns of `BRONZE` against `BRONZE_CI`. If they diverge, an email fires telling me to refresh the CI fixture before the next PR runs against stale schema assumptions.

```sql
CREATE ALERT schema_drift_alert
  WAREHOUSE = PIPELINE_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT column_name FROM ... WHERE table_schema = 'BRONZE'
    EXCEPT
    SELECT column_name FROM ... WHERE table_schema = 'BRONZE_CI'
  ))
  THEN CALL SYSTEM$SEND_EMAIL(...);
```

---

## Failure path

The Airflow DAG uses a `@task.branch` to route the next task based on the Soda scan result:

```python
@task.branch(task_id="raw_quality_gate")
def raw_quality_gate(scan_result):
    if scan_result["failed"] > 0:
        return "quarantine_raw_failures"
    return "run_dbt_transform"
```

When bronze checks fail, the DAG does the following:

1. **Quarantine** — inserts the latest batch into `QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE` with metadata columns: `failed_check_name`, `quarantined_at`, `inserted_at`.
2. **Audit log** — writes a row to `AUDIT.DATA_QUALITY_LOG` with `dag_run_id`, scan timestamp, failed check IDs (as VARIANT JSON), quarantine row counts, `remediation_status='PENDING'`.
3. **Slack alert** — posts a formatted message to `#financial-dq-alerts` with failed check IDs and quarantined row counts.
4. **Pipeline stops** — dbt does NOT run. Bad bronze data can never produce bad gold data.

The same pattern protects the gold checkpoint: a serving-layer failure quarantines bad rows from gold and fires its own Slack alert.

---

## Idempotency

The `_write_audit_log` function uses Airflow's `dag_run_id` as a deduplication key:

```python
if dag_run_id:
    check_sql = f"""
        SELECT COUNT(*) FROM {AUDIT_TABLE}
        WHERE dag_run_id = '{dag_run_id}'
          AND checkpoint = '{scan_result["checkpoint"]}'
    """
    if cur.fetchone()[0] > 0:
        return  # already written for this run
```

If a task retries, the audit log only records the run once. dbt models are idempotent by construction — silver is a view, gold is `materialized='table'` (full replacement).

---

## Retry strategy

Per-task retries are configured for the failure mode each protects against:

```python
@task(
    task_id="wait_for_snowpipe",
    retries=3,
    retry_delay=timedelta(minutes=2),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
)
```

- `wait_for_snowpipe` — 3 retries, exponential backoff (2 → 4 → 8 min, capped at 10). Protects against transient Snowflake API hiccups.
- `run_dbt_transform` — 2 retries, 3-minute delay. dbt Cloud API can have brief 5xx responses.
- `soda_check_*` — 2 retries, 1-minute delay. Snowflake aggregation queries occasionally hit transient timeouts.

Retries are kept low so persistent failures surface quickly rather than being masked by repeated attempts.

---

## GitOps deployment workflow

```
local edit
  → git commit on `dev`
    → push to GitHub
      → open PR `dev → main`
        → CI gate fires (Soda contracts vs BRONZE_CI/GOLD_CI)
          → merge to main
            → astro deploy -f
              → cloud Airflow picks up the new bundle
```

All commits to `main` go via PR. The CI gate is a required check.

**Custom dbt schema macro** at `dbt/macros/get_custom_schema.sql`:

```jinja-sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
```

This overrides dbt's default concatenation behaviour, which by default produces `{target.schema}_{custom_schema_name}` — i.e. `GOLD_GOLD` and `SILVER_SILVER` if both are set to the same string. With this macro a model declaring `schema='GOLD'` writes directly to `GOLD`.

---

## Repository layout

```
financial-data-quality-pipeline/
├── astro_deploy/                  # Astronomer Astro project (source of truth for the orchestrator)
│   ├── Dockerfile
│   ├── requirements.txt           # soda-core==4.7.0, soda-snowflake==4.7.0
│   ├── dags/
│   │   └── financial_transactions_pipeline.py
│   └── include/
│       └── contracts/
│           ├── bronze/financial_transactions_bronze.yml
│           └── gold/fct_financial_transactions.yml
│
├── dbt/                           # dbt Cloud reads this directory via GitHub integration
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── get_custom_schema.sql
│   └── models/
│       ├── silver/
│       │   ├── sources.yml        # source freshness config
│       │   └── stg_financial_transactions.sql
│       └── gold/
│           └── fct_financial_transactions.sql
│
├── ci/
│   └── run_soda_scan.py           # Soda 4.x CI scan runner
│
├── cloudformation/
│   └── step1_s3_sqs.yml           # only AWS infra needed
│
├── data_generator/
│   └── generate_transactions.py   # synthetic data with --inject-errors flag
│
├── .github/workflows/
│   └── data_quality_ci.yml
│
├── JOURNEY.md                     # detailed build journey and decision log
└── README.md
```

---

## Setup (one-time)

### 1. AWS infrastructure

```bash
aws cloudformation deploy \
  --template-file cloudformation/step1_s3_sqs.yml \
  --stack-name financial-dq-s3-sqs-prod \
  --region eu-north-1 \
  --capabilities CAPABILITY_NAMED_IAM
```

This creates the S3 bucket, SQS queue, and the IAM role Snowflake uses to read from S3.

### 2. Snowflake objects

Set up bronze, silver, gold, quarantine, audit, and the CI fixture schemas. Provision the storage integration, external stage, and Snowpipe. The audit table has a `dag_run_id` column for idempotency.

### 3. dbt Cloud

- Connect repo with subdirectory `dbt/`
- Create Production environment pointing at `main` branch
- Create job `nightly-financial-transactions` with commands:
  ```
  dbt source freshness
  dbt run --select fct_financial_transactions+
  ```

### 4. Astronomer

```bash
astro login astronomer.io
astro deployment create --name financial-data-quality-prod
cd astro_deploy/
astro deploy
```

Then configure in the cloud Airflow UI:

- **Connections:** `snowflake_pipeline` (Snowflake type), `dbt_cloud_default` (Generic; password = dbt Cloud API token, extra = `{"account_id": "...", "job_id": "..."}`)
- **Deployment variable** (via CLI — the in-UI Variable did not resolve in this Airflow version, but env-var-style variables prefixed `AIRFLOW_VAR_*` work reliably):
  ```bash
  astro deployment variable create \
    --key AIRFLOW_VAR_SLACK_WEBHOOK_URL \
    --value "https://hooks.slack.com/..." \
    --secret
  ```

### 5. GitHub secrets

Set these in repo Settings → Secrets and variables → Actions:

| Secret | Value |
|---|---|
| `SNOWFLAKE_ACCOUNT` | `<org>-<account>` |
| `SNOWFLAKE_CI_USER` | `PIPELINE_USER` |
| `SNOWFLAKE_CI_PASSWORD` | (pipeline user password) |
| `SNOWFLAKE_DATABASE` | `FINANCIAL_DATA` |
| `SNOWFLAKE_CI_WAREHOUSE` | `PIPELINE_WH` |

---

## Running it

**Clean data:**

```bash
S3_BUCKET=financial-data-quality-pipeline-prod-raw-ingest \
python3 data_generator/generate_transactions.py
```

**Bad data (exercise the failure path):**

```bash
S3_BUCKET=financial-data-quality-pipeline-prod-raw-ingest \
python3 data_generator/generate_transactions.py --inject-errors
```

The `--inject-errors` flag injects 30 invalid rows (7 with `INVALID_TYPE`, plus duplicate IDs and missing values). The bronze Soda check fails, triggering quarantine + Slack alert.

---

## Observability

| Channel | Shows |
|---|---|
| Astronomer Airflow UI | Task graph, retry attempts, runtime per task |
| `AUDIT.DATA_QUALITY_LOG` | Every scan with `dag_run_id`, failed checks (JSON), quarantine stats |
| `QUARANTINE.*_QUARANTINE` | Every row that failed a check, with `failed_check_name` and timestamps |
| Slack `#financial-dq-alerts` | Real-time failure notifications |
| Email | Schema drift between BRONZE and BRONZE_CI |

Query the audit log:

```sql
SELECT 
    dataset_name,
    checkpoint,
    scan_status,
    failed_checks,
    quarantine_stats,
    dag_run_id,
    created_at
FROM FINANCIAL_DATA.AUDIT.DATA_QUALITY_LOG
WHERE scan_status = 'FAILED'
ORDER BY created_at DESC
LIMIT 20;
```

---

## See also

- **[JOURNEY.md](./JOURNEY.md)** — detailed build journey: decisions, dead ends, and the engineering trade-offs behind the final design. Worth reading if you want to understand why this codebase looks the way it does.

---

## What I'd add next

1. **dbt tests alongside Soda** — Soda enforces contracts, dbt tests enforce relationships
2. **Auto-deploy on merge** — GitHub Actions step that runs `astro deploy -f` after the CI gate passes
3. **Snowflake masking policies on PII columns** — pattern worth showing even though no PII exists today
4. **Lineage in dbt docs** — expose bronze → silver → gold via `dbt docs generate`, host on GitHub Pages
5. **Retention policy on QUARANTINE and AUDIT** — currently grows unbounded; needs a Snowflake task to archive >90-day rows

---

## License

MIT
