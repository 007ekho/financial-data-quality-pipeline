# Separation of Concerns

This document explains exactly **what each layer of the pipeline does**, **when it fires**, and **what failure modes it catches** — so you can reason about which layer is responsible when something goes wrong.

The pipeline has three concerns running in parallel: **quality enforcement**, **observability**, and **remediation**. Each has multiple layers.

---

## Quality enforcement

Five distinct layers, each catching a different class of failure. None alone is sufficient — together they form defence in depth.

### Layer 1 — CI gate (Soda 4.x against test fixtures)

| Property | Value |
|---|---|
| **Fires** | On every PR touching `astro_deploy/include/contracts/**` or `dbt/**` |
| **Where** | GitHub Actions runner |
| **What it scans** | `FINANCIAL_DATA.BRONZE_CI.FINANCIAL_TRANSACTIONS` and `GOLD_CI.FCT_FINANCIAL_TRANSACTIONS` (100-row static fixtures) |
| **What it catches** | Broken contract syntax, contract rules that fail against representative data |
| **What it can't catch** | Production data quality issues (it's against fixtures, not production) |
| **On failure** | PR merge blocked, comment added |
| **Why static fixtures** | Deterministic results; can't accidentally quarantine production rows during a CI test |

**Concrete example of what it catches:**

Someone proposes adding `transaction_type IN [PAYMENT, TRANSFER, REVERSAL]` to the contract (dropping `REFUND`). The fixture has 5 REFUND rows. CI fails on `invalid_count: 5`. PR blocked.

### Layer 2 — Bronze Soda checkpoint (Soda 4.x against production)

| Property | Value |
|---|---|
| **Fires** | After Snowpipe ingests, before dbt runs (in the Airflow DAG `soda_check_raw` task) |
| **Where** | Astronomer cloud worker |
| **What it scans** | `FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS` |
| **What it catches** | Bad data in production: invalid enums, missing IDs, duplicate IDs |
| **What it can't catch** | Issues that only surface after dbt transforms |
| **On failure** | Quarantine bad rows → Slack alert → dbt skipped |

**Concrete example of what it catches:**

The source system flips a column value to a new enum we don't yet support. Snowpipe loads it. Soda flags `invalid_count: 1` on the affected column. The 1 row goes to quarantine. dbt doesn't run, so it can't propagate the bad value into gold.

### Layer 3 — dbt tests (relationships and transformation correctness)

| Property | Value |
|---|---|
| **Fires** | After `dbt run` completes (in the dbt Cloud job) |
| **Where** | dbt Cloud worker, against Snowflake `SILVER` and `GOLD` |
| **What it catches** | Referential integrity, transformation bugs, range violations on derived columns |
| **What it can't catch** | Bronze-level data shape issues (those are Soda's job) |
| **On failure** | dbt Cloud job fails → Airflow `run_dbt_transform` task fails → Slack alert |

**Concrete example of what it catches:**

The silver model has a typo that filters out 10% of rows. Bronze data is fine; Soda passes. But `gold.transaction_id` has rows that don't exist in `silver.transaction_id`. dbt's `relationships` test catches this: "referential integrity failure".

**The key distinction from Soda:**

- Soda: "Is the data shaped right at one point in time?"
- dbt tests: "Did the transformation preserve correctness between two points in time?"

A row can pass Soda and fail dbt — and vice versa.

### Layer 4 — Gold Soda checkpoint (Soda 4.x against production)

| Property | Value |
|---|---|
| **Fires** | After dbt produces gold (in the Airflow DAG `soda_check_serving` task) |
| **Where** | Astronomer cloud worker |
| **What it scans** | `FINANCIAL_DATA.GOLD.FCT_FINANCIAL_TRANSACTIONS` |
| **What it catches** | Derived column issues (wrong bucket assignment, FX bugs), gold-specific contract violations |
| **What it can't catch** | Bronze input issues (those are Layer 2's job) |
| **On failure** | Quarantine bad gold rows → Slack alert → pipeline marked failed |

**Concrete example of what it catches:**

A bug in the FX conversion macro produces `amount_gbp` values for non-GBP currencies that are too large by 100x. Bronze data is fine. dbt tests don't catch it (the values are within range, just wrong). Gold Soda catches it via `amount_bucket: WHALE` showing up 100x more often than expected — eventually triggering a `valid_values` or volume check.

### Layer 5 — Schema drift email alert (Snowflake ALERT)

| Property | Value |
|---|---|
| **Fires** | Hourly via Snowflake's native scheduler |
| **Where** | Inside Snowflake, no external compute |
| **What it scans** | `INFORMATION_SCHEMA.COLUMNS` for `BRONZE` vs `BRONZE_CI` |
| **What it catches** | Production schema added/removed/renamed a column that BRONZE_CI doesn't have |
| **What it can't catch** | Data quality issues (it only watches structure) |
| **On detection** | Email to ops |

**Concrete example of what it catches:**

The source system adds a new `merchant_country` column to BRONZE next week. BRONZE_CI still has the old schema. The CI gate keeps passing because the fixture doesn't have the new column. Eventually the new column will need to be added to the contract — this alert tells you proactively that BRONZE_CI is stale, before a contract change goes through that doesn't account for the new column.

### Quick reference matrix

| Failure mode | Layer that catches it |
|---|---|
| Contract syntax error | 1 (CI gate) |
| Bad enum in source data | 2 (Bronze Soda) |
| Duplicate transaction IDs | 2 (Bronze Soda) |
| dbt model drops rows | 3 (dbt tests via relationships) |
| Buggy derived column | 4 (Gold Soda) |
| Source adds new column | 5 (Schema drift alert) |
| All bronze data quality | 2 |
| All transformation correctness | 3 |
| All gold output validation | 4 |

---

## Observability

The pipeline writes to four observability channels. Each has a distinct audience and purpose.

### Channel 1 — Airflow UI (Astronomer cloud)

| Audience | Engineer triaging a failure |
|---|---|
| **What it shows** | DAG task graph, retry attempts, runtime per task, branching path taken |
| **Latency** | Real-time |
| **Retention** | Astronomer default (typically 30 days task logs) |
| **Use it when** | A Slack alert fires and you need to understand which task failed and why |

### Channel 2 — Snowflake AUDIT.DATA_QUALITY_LOG

| Audience | Engineer or analyst doing historical analysis |
|---|---|
| **What it shows** | Every Soda scan (pass or fail), bronze and gold, with `dag_run_id`, failed check IDs (JSON), quarantine stats |
| **Latency** | Synchronous with DAG execution |
| **Retention** | 90 days in live table, indefinite in `_ARCHIVE` (via retention TASK) |
| **Use it when** | You want to ask "how many bronze failures last month?" or "did this run already write an audit row?" |

Sample query:

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
WHERE created_at > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY created_at DESC;
```

### Channel 3 — Snowflake QUARANTINE.*_QUARANTINE

| Audience | Engineer remediating bad data |
|---|---|
| **What it shows** | The actual rows that failed, plus metadata (`failed_check_name`, `quarantined_at`, `inserted_at`) |
| **Latency** | Synchronous with DAG failure path |
| **Retention** | 90 days live, indefinite in `_ARCHIVE` |
| **Use it when** | Slack alert fires; need to see the actual bad rows to decide fix vs delete |

Sample query:

```sql
SELECT *
FROM FINANCIAL_DATA.QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE
WHERE quarantined_at > DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY quarantined_at DESC;
```

### Channel 4 — Slack `#financial-dq-alerts`

| Audience | On-call engineer (interrupt-driven) |
|---|---|
| **What it shows** | Failure summary with the failed check IDs, quarantined row counts, dataset affected |
| **Latency** | Real-time |
| **Retention** | Slack channel history (depends on workspace plan) |
| **Use it when** | This is push-notification; you don't query it, it pages you |

### Channel 5 — Email (schema drift)

| Audience | Platform/data engineer |
|---|---|
| **What it shows** | "Production BRONZE schema has changed. Refresh BRONZE_CI to match." |
| **Latency** | Up to 1 hour (next ALERT schedule) |
| **Retention** | Inbox |
| **Use it when** | Push-notification only; tells you the CI fixture is stale |

### Observability flow on failure

```
Soda check FAILS
    ↓
Quarantine task copies bad rows to QUARANTINE      ← Channel 3
    ↓
Audit log written with FAILED status               ← Channel 2
    ↓
Alert payload built (failed check IDs)             ← (in-DAG)
    ↓
Slack webhook fired                                ← Channel 4
    ↓
DAG task graph shows red                           ← Channel 1
```

All four channels capture the same failure from different angles. You don't have to remember which to check — they reinforce each other.

---

## Remediation

When a failure fires, what should you actually DO? Remediation is **deliberately manual** because the right action depends on the data.

### Step 1 — Triage in Slack

Slack alert tells you what failed:

```
🔴 RAW contract FAILED — FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS
Failed checks: 3/17
• 6939dec2
• 8e8cad25
• 7f10e229
Rows quarantined: 7
dbt transform blocked.
```

Click into the Airflow UI to see which exact task failed.

### Step 2 — Inspect the bad rows

```sql
SELECT
    transaction_id,
    transaction_type,
    status,
    failed_check_name,
    quarantined_at
FROM FINANCIAL_DATA.QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE
WHERE quarantined_at > DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY quarantined_at DESC;
```

Cross-reference the `failed_check_name` against your contract to know what rule failed.

### Step 3 — Decide fix vs delete

| Situation | Action |
|---|---|
| Source system sent wrong value (e.g. `INVALID_TYPE` should have been `PAYMENT`) | UPDATE the BRONZE row with the correct value |
| Source sent duplicate (same transaction twice) | DELETE the extras, keep the original |
| Genuinely garbage data | DELETE |
| Data is right but the contract is wrong | Fix the contract via PR (then CI gate verifies your fix) |

**Why not auto-delete?**

Auto-delete from BRONZE is dangerous. A bug in a contract change could wipe legitimate rows. The pipeline isolates and alerts; the engineer decides the cure. This is intentional — it gives you the audit trail and the human judgement that production environments demand.

### Step 4 — Execute the fix

For a UPDATE (correcting wrong values):

```sql
UPDATE FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS
SET transaction_type = 'PAYMENT'
WHERE transaction_type = 'INVALID_TYPE'
  AND ingested_at > DATEADD(hour, -2, CURRENT_TIMESTAMP());
```

For a DELETE (removing garbage):

```sql
DELETE FROM FINANCIAL_DATA.BRONZE.FINANCIAL_TRANSACTIONS
WHERE transaction_id IS NULL
   OR TRIM(transaction_id) = '';
```

### Step 5 — Mark the audit log resolved

```sql
UPDATE FINANCIAL_DATA.AUDIT.DATA_QUALITY_LOG
SET remediation_status = 'RESOLVED'
WHERE scan_status = 'FAILED'
  AND remediation_status = 'PENDING'
  AND dag_run_id = 'manual__2026-05-18T...';
```

This is your paper trail. When someone asks "why was that batch missing rows?" you can show "row X was quarantined under check Y at timestamp Z, remediated by user W at timestamp V".

### Step 6 — Re-trigger the DAG

```
Airflow UI → financial_transactions_pipeline → ▶ Trigger DAG
```

If your fix was complete, the pipeline now runs the happy path: bronze passes → dbt runs → gold passes → audit logged.

### Step 7 — If the failure repeats, escalate

A recurring failure means the source system or upstream contract needs to change. Open a ticket against the source system owner. The pipeline keeps catching the bad data correctly — but the upstream needs fixing.

---

## Architectural rationale

You might ask why we have so many layers. The reason: each layer **detects a different failure mode at the right cost**.

- **CI gate** is cheap and runs at PR time when fixes are easy → catch broken contract changes before merge
- **Bronze Soda** is cheap and runs at ingest time → catch bad source data before it pollutes downstream
- **dbt tests** run after transformation → catch transformation bugs the contracts can't see
- **Gold Soda** runs after dbt → catch derived-column bugs the dbt tests don't enforce
- **Schema drift alert** runs hourly → tell us proactively when CI fixtures go stale

Removing any one layer creates a blind spot. The pipeline is **defence in depth, not redundancy** — every layer earns its place.

---

## Quick reference

```
WHAT FAILED        WHERE TO LOOK FIRST           THEN
--------------     -------------------------     -----------------
Soda bronze        Slack alert in #dq-alerts     QUARANTINE table → fix bronze → re-trigger DAG
Soda gold          Slack alert in #dq-alerts     QUARANTINE gold table → fix dbt → re-trigger
dbt test           dbt Cloud run page            Fix dbt model → re-trigger
dbt source freshness  Airflow UI                 Check Snowpipe is running → investigate S3 drops
Schema drift       Email                          Refresh BRONZE_CI fixture
CI gate failure    GitHub Actions tab            Fix the contract change in the PR
DAG timeout        Airflow UI                    Check Snowflake warehouse capacity
```
