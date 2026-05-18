# Snowflake Setup Guide

Complete step-by-step recreation of all Snowflake objects: database, schemas, RBAC, Snowpipe, alerts, retention task, and CI fixtures.

## What you'll build

- One database (`FINANCIAL_DATA`) with seven schemas
- A pipeline user and role with least-privilege grants
- A storage integration and external stage pointing at S3
- A Snowpipe that auto-ingests from S3 on file arrival
- An hourly schema drift alert
- A daily retention task

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- AWS S3 bucket already created (see `SETUP_AWS.md`)
- SQS queue ARN from CloudFormation outputs
- IAM role ARN from CloudFormation outputs

---

## Step 1 — Create the database and warehouse

Log in to Snowflake as ACCOUNTADMIN. Open a worksheet.

```sql
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS FINANCIAL_DATA;
USE DATABASE FINANCIAL_DATA;

CREATE WAREHOUSE IF NOT EXISTS PIPELINE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
```

`X-SMALL` is fine for this workload. Auto-suspend at 60 seconds keeps costs low.

---

## Step 2 — Create the schemas

```sql
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.BRONZE;       -- Snowpipe lands here
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.SILVER;       -- dbt staging models
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.GOLD;         -- dbt fact tables
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.QUARANTINE;   -- failed quality rows
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.AUDIT;        -- quality run log
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.BRONZE_CI;    -- CI test fixture
CREATE SCHEMA IF NOT EXISTS FINANCIAL_DATA.GOLD_CI;      -- CI test fixture
```

Verify:

```sql
SHOW SCHEMAS IN DATABASE FINANCIAL_DATA;
```

Should return 7 user schemas plus `INFORMATION_SCHEMA` and `PUBLIC`.

---

## Step 3 — Create the role and user

```sql
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;

CREATE USER IF NOT EXISTS PIPELINE_USER
  PASSWORD = '<strong-password-here>'
  DEFAULT_ROLE = PIPELINE_ROLE
  DEFAULT_WAREHOUSE = PIPELINE_WH
  DEFAULT_NAMESPACE = FINANCIAL_DATA.BRONZE
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE PIPELINE_ROLE TO USER PIPELINE_USER;
GRANT ROLE PIPELINE_ROLE TO ROLE SYSADMIN;
```

**Save the password** — you'll need it for dbt Cloud, Airflow connection, and GitHub Secrets.

---

## Step 4 — Grant warehouse and database access

```sql
GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE PIPELINE_ROLE;
GRANT OPERATE ON WAREHOUSE PIPELINE_WH TO ROLE PIPELINE_ROLE;

GRANT USAGE ON DATABASE FINANCIAL_DATA TO ROLE PIPELINE_ROLE;
```

---

## Step 5 — Grant schema access

```sql
-- BRONZE
GRANT USAGE ON SCHEMA FINANCIAL_DATA.BRONZE TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE PIPE, CREATE STAGE
  ON SCHEMA FINANCIAL_DATA.BRONZE TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.BRONZE TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.BRONZE TO ROLE PIPELINE_ROLE;

-- SILVER (dbt creates views here)
GRANT USAGE ON SCHEMA FINANCIAL_DATA.SILVER TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA FINANCIAL_DATA.SILVER TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.SILVER TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.SILVER TO ROLE PIPELINE_ROLE;

-- GOLD (dbt creates tables here)
GRANT USAGE ON SCHEMA FINANCIAL_DATA.GOLD TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA FINANCIAL_DATA.GOLD TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.GOLD TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.GOLD TO ROLE PIPELINE_ROLE;

-- QUARANTINE
GRANT USAGE ON SCHEMA FINANCIAL_DATA.QUARANTINE TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE ON SCHEMA FINANCIAL_DATA.QUARANTINE TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.QUARANTINE TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.QUARANTINE TO ROLE PIPELINE_ROLE;

-- AUDIT
GRANT USAGE ON SCHEMA FINANCIAL_DATA.AUDIT TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE ON SCHEMA FINANCIAL_DATA.AUDIT TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.AUDIT TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.AUDIT TO ROLE PIPELINE_ROLE;

-- BRONZE_CI
GRANT USAGE ON SCHEMA FINANCIAL_DATA.BRONZE_CI TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.BRONZE_CI TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.BRONZE_CI TO ROLE PIPELINE_ROLE;

-- GOLD_CI
GRANT USAGE ON SCHEMA FINANCIAL_DATA.GOLD_CI TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FINANCIAL_DATA.GOLD_CI TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA FINANCIAL_DATA.GOLD_CI TO ROLE PIPELINE_ROLE;
```

---

## Step 6 — Create the BRONZE table

```sql
USE SCHEMA FINANCIAL_DATA.BRONZE;

CREATE OR REPLACE TABLE FINANCIAL_TRANSACTIONS (
  transaction_id      VARCHAR,
  account_id          VARCHAR,
  counterparty_id     VARCHAR,
  transaction_type    VARCHAR,
  amount              FLOAT,
  currency            VARCHAR,
  status              VARCHAR,
  channel             VARCHAR,
  merchant_category   VARCHAR,
  initiated_at        TIMESTAMP_NTZ,
  settled_at          TIMESTAMP_NTZ,
  is_flagged          BOOLEAN,
  flag_reason         VARCHAR,
  source_system       VARCHAR,
  ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## Step 7 — Create the storage integration

Use the IAM role ARN from CloudFormation:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION financial_dq_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::707258693559:role/financial-dq-snowflake-s3-read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://financial-data-quality-pipeline-prod-raw-ingest/');

GRANT USAGE ON INTEGRATION financial_dq_s3_integration TO ROLE PIPELINE_ROLE;
```

Get the external ID:

```sql
DESC INTEGRATION financial_dq_s3_integration;
```

Copy `STORAGE_AWS_EXTERNAL_ID`. Update the CloudFormation stack with this value (see SETUP_AWS.md Step 6).

---

## Step 8 — Create the external stage

```sql
USE ROLE PIPELINE_ROLE;
USE WAREHOUSE PIPELINE_WH;
USE SCHEMA FINANCIAL_DATA.BRONZE;

CREATE OR REPLACE STAGE financial_dq_s3_stage
  URL = 's3://financial-data-quality-pipeline-prod-raw-ingest/financial_transactions/'
  STORAGE_INTEGRATION = financial_dq_s3_integration
  FILE_FORMAT = (TYPE = PARQUET);

LIST @financial_dq_s3_stage;
```

`LIST` should return successfully (empty list is fine).

---

## Step 9 — Create the Snowpipe

Use the SQS queue ARN from CloudFormation:

```sql
CREATE OR REPLACE PIPE FINANCIAL_TRANSACTIONS_PIPE
  AUTO_INGEST = TRUE
  AWS_SNS_TOPIC = 'arn:aws:sqs:eu-north-1:707258693559:financial-dq-snowpipe-queue'
AS
  COPY INTO FINANCIAL_TRANSACTIONS (
    transaction_id, account_id, counterparty_id, transaction_type,
    amount, currency, status, channel, merchant_category,
    initiated_at, settled_at, is_flagged, flag_reason,
    source_system, ingested_at
  )
  FROM (
    SELECT
      $1:transaction_id::VARCHAR,
      $1:account_id::VARCHAR,
      $1:counterparty_id::VARCHAR,
      $1:transaction_type::VARCHAR,
      $1:amount::FLOAT,
      $1:currency::VARCHAR,
      $1:status::VARCHAR,
      $1:channel::VARCHAR,
      $1:merchant_category::VARCHAR,
      $1:initiated_at::TIMESTAMP_NTZ,
      $1:settled_at::TIMESTAMP_NTZ,
      $1:is_flagged::BOOLEAN,
      $1:flag_reason::VARCHAR,
      $1:source_system::VARCHAR,
      CURRENT_TIMESTAMP()
    FROM @financial_dq_s3_stage
  );

GRANT MONITOR, OPERATE ON PIPE FINANCIAL_TRANSACTIONS_PIPE TO ROLE PIPELINE_ROLE;
```

Verify the pipe is ready:

```sql
SELECT SYSTEM$PIPE_STATUS('FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS_PIPE');
```

Should return `executionState: RUNNING`.

---

## Step 10 — Create the QUARANTINE table

```sql
USE SCHEMA FINANCIAL_DATA.QUARANTINE;

CREATE OR REPLACE TABLE FINANCIAL_TRANSACTIONS_QUARANTINE
  LIKE FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS;

ALTER TABLE FINANCIAL_TRANSACTIONS_QUARANTINE 
  ADD COLUMN failed_check_name VARCHAR;
ALTER TABLE FINANCIAL_TRANSACTIONS_QUARANTINE 
  ADD COLUMN quarantined_at TIMESTAMP_NTZ;
ALTER TABLE FINANCIAL_TRANSACTIONS_QUARANTINE 
  ADD COLUMN inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
```

---

## Step 11 — Create the AUDIT table

```sql
USE SCHEMA FINANCIAL_DATA.AUDIT;

CREATE OR REPLACE TABLE DATA_QUALITY_LOG (
  dataset_name        VARCHAR,
  checkpoint          VARCHAR,
  scan_ts             TIMESTAMP_NTZ,
  scan_status         VARCHAR,
  failed_checks       VARIANT,
  quarantine_stats    VARIANT,
  remediation_status  VARCHAR,
  created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  dag_run_id          VARCHAR
);
```

---

## Step 12 — Seed the CI fixtures

After Snowpipe has ingested at least one batch of clean data:

```sql
CREATE OR REPLACE TABLE FINANCIAL_DATA.BRONZE_CI.FINANCIAL_TRANSACTIONS AS
SELECT * FROM FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS LIMIT 100;
```

(If GOLD has data — only after dbt runs — also seed GOLD_CI; otherwise skip and run this after the first successful DAG run.)

```sql
CREATE OR REPLACE TABLE FINANCIAL_DATA.GOLD_CI.FCT_FINANCIAL_TRANSACTIONS AS
SELECT * FROM FINANCIAL_DATA.GOLD.FCT_FINANCIAL_TRANSACTIONS LIMIT 100;
```

---

## Step 13 — Create the schema drift email integration and alert

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE NOTIFICATION INTEGRATION schema_drift_email
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('your-email@example.com');

USE DATABASE FINANCIAL_DATA;
USE SCHEMA AUDIT;

CREATE OR REPLACE ALERT schema_drift_alert
  WAREHOUSE = PIPELINE_WH
  SCHEDULE = '60 MINUTE'
  IF (EXISTS (
    SELECT column_name FROM FINANCIAL_DATA.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_schema = 'BRONZE' AND table_name = 'FINANCIAL_TRANSACTIONS'
    EXCEPT
    SELECT column_name FROM FINANCIAL_DATA.INFORMATION_SCHEMA.COLUMNS 
    WHERE table_schema = 'BRONZE_CI' AND table_name = 'FINANCIAL_TRANSACTIONS'
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'schema_drift_email',
    'your-email@example.com',
    'Schema drift detected: BRONZE vs BRONZE_CI',
    'Production BRONZE schema has changed. Refresh BRONZE_CI to match.'
  );

ALTER ALERT FINANCIAL_DATA.AUDIT.schema_drift_alert RESUME;
```

Replace `your-email@example.com` with your real email.

Verify:

```sql
SHOW ALERTS IN SCHEMA FINANCIAL_DATA.AUDIT;
```

Should show `SCHEMA_DRIFT_ALERT` with state `started`.

---

## Step 14 — Create the retention task

```sql
USE DATABASE FINANCIAL_DATA;
USE SCHEMA AUDIT;

-- Archive tables
CREATE TABLE IF NOT EXISTS QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE_ARCHIVE 
  LIKE QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE;

CREATE TABLE IF NOT EXISTS AUDIT.DATA_QUALITY_LOG_ARCHIVE 
  LIKE AUDIT.DATA_QUALITY_LOG;

-- Retention task
CREATE OR REPLACE TASK retention_cleanup
  WAREHOUSE = PIPELINE_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
AS
BEGIN
  -- Archive quarantine rows older than 90 days
  INSERT INTO FINANCIAL_DATA.QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE_ARCHIVE
  SELECT * FROM FINANCIAL_DATA.QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE
  WHERE inserted_at < DATEADD(day, -90, CURRENT_TIMESTAMP());

  DELETE FROM FINANCIAL_DATA.QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE
  WHERE inserted_at < DATEADD(day, -90, CURRENT_TIMESTAMP());

  -- Archive audit log rows older than 90 days
  INSERT INTO FINANCIAL_DATA.AUDIT.DATA_QUALITY_LOG_ARCHIVE
  SELECT * FROM FINANCIAL_DATA.AUDIT.DATA_QUALITY_LOG
  WHERE created_at < DATEADD(day, -90, CURRENT_TIMESTAMP());

  DELETE FROM FINANCIAL_DATA.AUDIT.DATA_QUALITY_LOG
  WHERE created_at < DATEADD(day, -90, CURRENT_TIMESTAMP());
END;

ALTER TASK retention_cleanup RESUME;
```

Verify:

```sql
SHOW TASKS IN SCHEMA AUDIT;
```

Should show `RETENTION_CLEANUP` with state `started`.

---

## Step 15 — Verify everything works

Generate test data:

```bash
S3_BUCKET=financial-data-quality-pipeline-prod-raw-ingest \
python3 data_generator/generate_transactions.py
```

Wait 2 minutes, then in Snowflake:

```sql
SELECT COUNT(*) FROM FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS;
```

Should be ~1000 rows (matches the generator's output).

```sql
SELECT SYSTEM$PIPE_STATUS('FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS_PIPE');
```

Should show `executionState: RUNNING` and a recent `lastIngestedTimestamp`.

---

## What you've now got

- All 7 schemas with appropriate grants
- BRONZE table receiving data via Snowpipe
- QUARANTINE and AUDIT tables for the quality pipeline
- CI fixture schemas seeded with 100 rows each
- Hourly schema drift email alert
- Daily retention task archiving rows >90 days

---

## Common gotchas

| Issue | Cause | Fix |
|---|---|---|
| `STORAGE_AWS_ROLE_ARN does not exist` | IAM role not created yet | Run CloudFormation first |
| Snowpipe doesn't ingest | SQS subscription not configured | Check S3 bucket notification points at the queue |
| `Insufficient privileges` errors | PIPELINE_ROLE missing grants | Re-run Step 5 |
| Pipe status `PAUSED_BY_SYSTEM` | Auto-suspended due to errors | `ALTER PIPE ... RESUME;` |
| Alert never fires | Notification integration not connected | `DESC INTEGRATION schema_drift_email;` and verify ALLOWED_RECIPIENTS |
| Task doesn't run | Not RESUMED after creation | `ALTER TASK ... RESUME;` |

---

## Teardown

```sql
USE ROLE ACCOUNTADMIN;

DROP DATABASE FINANCIAL_DATA CASCADE;
DROP WAREHOUSE PIPELINE_WH;
DROP ROLE PIPELINE_ROLE;
DROP USER PIPELINE_USER;
DROP NOTIFICATION INTEGRATION schema_drift_email;
DROP STORAGE INTEGRATION financial_dq_s3_integration;
```
