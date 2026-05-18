# Soda Core 4.x Setup Guide

Complete step-by-step recreation of the Soda Core 4.x data contracts and scan runners used by both the Airflow DAG and the GitHub Actions CI gate.

## What you'll build

- Two Soda Core 4.x contract files (bronze and gold) defining all the rules
- A reusable scan runner script (`ci/run_soda_scan.py`)
- Runtime-generated data source YAML for both production (in the DAG) and CI (in GitHub Actions)

## Prerequisites

- Snowflake schemas `FINANCIAL_DATA.BRONZE`, `GOLD`, `BRONZE_CI`, `GOLD_CI` exist
- `PIPELINE_USER` has SELECT access to all four schemas
- Python 3.11 environment available locally and in CI

---

## Step 1 — Install Soda Core 4.x

In your project's `astro_deploy/requirements.txt`:

```
soda-core==4.7.0
soda-snowflake==4.7.0
```

Same lines go into your CI workflow's pip install step.

---

## Step 2 — Create the bronze contract

Save this as `astro_deploy/include/contracts/bronze/financial_transactions_bronze.yml`:

```yaml
# Soda Core 4.x data contract for BRONZE.FINANCIAL_TRANSACTIONS
# Snowflake stores column names in UPPERCASE — match here

dataset: snowflake_bronze/BRONZE/FINANCIAL_TRANSACTIONS

columns:
  - name: TRANSACTION_ID
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - duplicate:
          threshold:
            must_be: 0

  - name: ACCOUNT_ID
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: COUNTERPARTY_ID
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: TRANSACTION_TYPE
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - invalid:
          valid_values: [PAYMENT, TRANSFER, REFUND]
          threshold:
            must_be: 0

  - name: AMOUNT
    data_type: float
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: CURRENCY
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: STATUS
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - invalid:
          valid_values: [COMPLETED, PENDING, FAILED, REVERSED]
          threshold:
            must_be: 0

  - name: CHANNEL
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - invalid:
          valid_values: [MOBILE, WEB, ATM, BRANCH, API]
          threshold:
            must_be: 0

  - name: MERCHANT_CATEGORY
    data_type: varchar

  - name: INITIATED_AT
    data_type: timestamp_ntz
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: SETTLED_AT
    data_type: timestamp_ntz

  - name: IS_FLAGGED
    data_type: boolean
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: FLAG_REASON
    data_type: varchar

  - name: SOURCE_SYSTEM
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - invalid:
          valid_values: [CORE_BANKING_v2]
          threshold:
            must_be: 0

  - name: INGESTED_AT
    data_type: timestamp_ntz
    checks:
      - missing:
          threshold:
            must_be: 0
```

### Critical Soda 4.x rules to follow

1. **Column names UPPERCASE** to match Snowflake storage
2. **`dataset:` uses slash format** — `data_source_name/schema/table` (NOT dots)
3. **Each column's checks list is per-column** — wrap under `checks:` key
4. **Each check has its own top-level YAML key** — `missing:`, `invalid:`, `duplicate:`
5. **Threshold uses `must_be:` not `must_be_less_than:` etc** — exact equality
6. **No top-level `checks:` at dataset level** — the schema check there returns NOT_EVALUATED in 4.x

---

## Step 3 — Create the gold contract

Save this as `astro_deploy/include/contracts/gold/fct_financial_transactions.yml`:

```yaml
# Soda Core 4.x data contract for GOLD.FCT_FINANCIAL_TRANSACTIONS

dataset: snowflake_gold/GOLD/FCT_FINANCIAL_TRANSACTIONS

columns:
  - name: TRANSACTION_ID
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - duplicate:
          threshold:
            must_be: 0

  - name: AMOUNT_GBP
    data_type: float
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: TRANSACTION_DATE
    data_type: date
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: AMOUNT_BUCKET
    data_type: varchar
    checks:
      - missing:
          threshold:
            must_be: 0
      - invalid:
          valid_values: [REFUND, MICRO, SMALL, MEDIUM, LARGE, WHALE]
          threshold:
            must_be: 0

  - name: HOUR_OF_DAY
    data_type: integer
    checks:
      - missing:
          threshold:
            must_be: 0

  - name: SETTLEMENT_HOURS
    data_type: float

  - name: LOADED_AT
    data_type: timestamp
    checks:
      - missing:
          threshold:
            must_be: 0
```

---

## Step 4 — Create the scan runner script

Save as `ci/run_soda_scan.py`:

```python
"""
ci/run_soda_scan.py
Runs a Soda Core 4.x contract verification with verbose output.
Usage: python3 ci/run_soda_scan.py <data_source_yaml> <contract_path>
"""
import sys
from soda_core.contracts import verify_contract_locally

data_source_yaml = sys.argv[1]
contract_path    = sys.argv[2]

print(f"Running Soda scan...")
print(f"Data source: {data_source_yaml}")
print(f"Contract: {contract_path}")
print("-" * 60)

result = verify_contract_locally(
    data_source_file_path=data_source_yaml,
    contract_file_path=contract_path,
    publish=False,
)

# Print every check result with its pass/fail outcome
for cr in (result.contract_verification_results or []):
    for check in (cr.check_results or []):
        outcome = check.outcome.name.lower() if hasattr(check.outcome, "name") else str(check.outcome).lower()
        check_id = getattr(check.check, "identity", "unknown")
        print(f"[{outcome.upper()}] {check_id}")
        if hasattr(check, "diagnostic_lines"):
            for line in check.diagnostic_lines:
                print(f"    {line}")

print("-" * 60)

if not result.is_ok:                  # is_ok is a property in Soda 4.x — no parens
    print(f"❌ Contract FAILED: {contract_path}")
    sys.exit(1)

print(f"✅ Contract PASSED: {contract_path}")
sys.exit(0)
```

### Critical things this script does right

- `result.is_ok` — **property** access, not a method call (`.is_ok()` raises `'bool' object is not callable`)
- Prints every check ID and outcome, so you can debug why CI is red
- Returns exit code 1 on any failure — GitHub Actions and Airflow both pick this up

---

## Step 5 — Verify locally with production data

Install Soda Core locally:

```bash
python3 -m pip install "soda-core==4.7.0" "soda-snowflake==4.7.0"
```

Create a local data source YAML at `/tmp/data_source_bronze.yml`:

```yaml
type: snowflake
name: snowflake_bronze
connection:
  account: OEB79775-XYZ12345
  user: PIPELINE_USER
  password: <pipeline_user_password>
  database: FINANCIAL_DATA
  warehouse: PIPELINE_WH
  schema: BRONZE
  role: PIPELINE_ROLE
```

Run the scan:

```bash
python3 ci/run_soda_scan.py \
  /tmp/data_source_bronze.yml \
  astro_deploy/include/contracts/bronze/financial_transactions_bronze.yml
```

Expected output (with clean BRONZE):

```
[PASSED] 4b69624e
[PASSED] ef6d78fb
... (17 checks)
✅ Contract PASSED: ...
```

---

## Step 6 — Generate data source YAML at runtime (DAG path)

The Airflow DAG reads credentials from the `snowflake_pipeline` connection and writes a temporary YAML file before each scan. The relevant code lives in `_run_soda_scan()` in the DAG:

```python
hook = SnowflakeHook(snowflake_conn_id="snowflake_pipeline")
creds = hook.get_connection("snowflake_pipeline")
extra = creds.extra_dejson

yaml_content = f"""
type: snowflake
name: {data_source}
connection:
  account:   {extra["account"]}
  user:      {creds.login}
  password:  {creds.password}
  database:  {extra["database"]}
  warehouse: {extra["warehouse"]}
  schema:    {schema}
  role:      {extra["role"]}
"""

ds_file = f"/tmp/{data_source}.yml"
with open(ds_file, "w") as f:
    f.write(yaml_content)

result = verify_contract_locally(
    data_source_file_path=ds_file,
    contract_file_path=contract_path,
    publish=False,
)
```

This pattern avoids storing static credentials in the repo and ensures the same Airflow connection drives every scan.

---

## Step 7 — Generate data source YAML at runtime (CI path)

The GitHub Actions workflow does the same thing using `cat <<` heredocs against secrets:

```yaml
- name: Generate Soda data source YAML files
  run: |
    mkdir -p .soda_runtime
    cat > .soda_runtime/data_source_bronze.yml << YAML
    type: snowflake
    name: snowflake_bronze
    connection:
      account:   ${SNOWFLAKE_ACCOUNT}
      user:      ${SNOWFLAKE_USER}
      password:  ${SNOWFLAKE_PASSWORD}
      database:  ${SNOWFLAKE_DATABASE}
      warehouse: ${SNOWFLAKE_WAREHOUSE}
      schema:    BRONZE_CI
      role:      PIPELINE_ROLE
    YAML
```

The CI YAML points at `BRONZE_CI`/`GOLD_CI` schemas; the production YAML points at `BRONZE`/`GOLD`.

---

## Step 8 — Patch contracts at runtime (CI only)

The same contract files run in production AND in CI. The only difference is the schema. The CI workflow patches the `dataset:` line before scanning:

```yaml
- name: Patch contracts to point at CI schemas
  run: |
    sed -i 's|snowflake_bronze/BRONZE/|snowflake_bronze/BRONZE_CI/|' \
      astro_deploy/include/contracts/bronze/financial_transactions_bronze.yml

    sed -i 's|snowflake_gold/GOLD/|snowflake_gold/GOLD_CI/|' \
      astro_deploy/include/contracts/gold/fct_financial_transactions.yml
```

Because the workflow runs on a fresh checkout, the patches don't persist — they only affect this CI run.

---

## Step 9 — Inject errors and verify Soda catches them

```bash
S3_BUCKET=financial-data-quality-pipeline-prod-raw-ingest \
python3 data_generator/generate_transactions.py --inject-errors
```

This adds 30 bad rows: 7 with `INVALID_TYPE`, 7 with null IDs, 7 duplicates, etc.

After Snowpipe ingests, re-run the scan. You should see:

```
[FAILED] 8e8cad25
[FAILED] 7f10e229
[FAILED] 6939dec2
| TRANSACTION_TYPE | No invalid values | level: fail | ❌ FAILED | invalid_count: 7 |
| TRANSACTION_ID   | No missing values | level: fail | ❌ FAILED | missing_count: 7 |
| TRANSACTION_ID   | No duplicate values | level: fail | ❌ FAILED | duplicate_count: 6 |
```

If Soda catches all three failure modes, your contract is working.

---

## What you've now got

- Bronze and gold contracts in Soda 4.x format
- A reusable scan runner that works in CI and DAG
- Runtime credential injection from Snowflake connection (DAG) or GitHub Secrets (CI)
- The same contracts running in both production and CI, against different schemas

---

## Common gotchas

| Issue | Cause | Fix |
|---|---|---|
| `Checks: 0` in scan summary | Column checks defined without `checks:` wrapper | Wrap each column's rules under `checks:` |
| `invalid identifier '"transaction_id"'` | Lowercase column names in contract | Use UPPERCASE matching Snowflake storage |
| `'bool' object is not callable` | `is_ok()` with parens | `is_ok` is a property — drop the parens |
| Schema check returns NOT_EVALUATED | Soda 4.x bug | Remove the schema check; use Snowflake email alert instead |
| Dataset path with dots | Soda 3.x format | Use slashes: `data_source/schema/table` |

---

## Teardown

These are repo files — just delete them if you're done. No external state to clean up.
