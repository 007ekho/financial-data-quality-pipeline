# Build Journey & Decision Log

This document captures the actual path this project took from first commit to production. It records the dead ends, the corrections, and the trade-offs behind each piece of the final architecture. Read this if you want to understand why the codebase looks the way it does.

The headline story: the architecture in the README is the **third** orchestrator design we landed on. The first two pivots reshaped the project meaningfully.

---

## Tooling pivot 1 — Terraform → CloudFormation

**Initial choice:** Terraform for the AWS infrastructure (Snowpipe S3 stage, SQS queue, IAM role, S3 bucket notification). The first commit had a 100+ line `snowpipe.tf` that defined an `aws_iam_role`, `aws_s3_bucket_notification`, a `snowflake_stage`, a `snowflake_pipe`, and grants — all in one file using both the AWS and Snowflake Terraform providers.

**Why it was wrong:** Two problems surfaced fast.

1. **Duplicated ownership of Snowflake objects.** My Snowflake schemas, tables, and grants were already managed by Snowflake DCM. Putting the Snowpipe object and stage in Terraform meant two different IaC tools would manage the same Snowflake namespace — exactly the drift problem DCM exists to prevent.
2. **No need for Terraform's multi-cloud abstraction.** The only infrastructure I needed to provision was on AWS. CloudFormation does the same job natively, with no state file to manage and no extra tooling to install.

**Resolution:** Replaced Terraform entirely with one CloudFormation template at `cloudformation/step1_s3_sqs.yml` that provisions the S3 bucket, SQS queue, queue policy, and the IAM role Snowflake assumes to read S3. The Snowpipe object and external stage live in DCM alongside the rest of the schema.

**What I'd say in an interview:** I started with Terraform because it's the default reach for IaC in data engineering. The right question was "what does this project actually need?" and the answer was AWS-only resources sitting next to a DCM-managed Snowflake. CloudFormation is the leaner fit.

---

## Tooling pivot 2 — RAW/SERVING → BRONZE / SILVER / GOLD medallion

**Initial choice:** Two-layer architecture — `RAW` (Snowpipe lands here) and `SERVING` (dbt builds the fact tables). The contracts were named `journey_events_raw.yml` and `fct_journey_events.yml`. The first DAG iteration referenced these names throughout.

**Why I changed it:** Once the pipeline grew to include quarantine, audit, and a transformation step that did real work (FX normalisation, bucketing, derived columns), the two-layer model started to feel cramped. Specifically:

- The `RAW → SERVING` jump is too big. A failed serving check told me "something broke in dbt" but not where in the transformation chain.
- The dbt model had to do too much — type casting, deduplication, *and* business logic in one model.
- The naming didn't follow the lakehouse vocabulary most teams I'd be interviewing with would expect to see.

**Resolution:** Migrated to a three-layer medallion architecture (`BRONZE` / `SILVER` / `GOLD`) with `SILVER` as a thin staging view that handles type casting and quarantine exclusion, and `GOLD` as the materialised fact table with all business logic. This is a project-wide rename — every contract, schema, DAG variable, dbt target, and Snowflake object updated together.

The rename also forced me to write the **custom dbt schema macro**. By default dbt concatenates `{target.schema}_{custom_schema_name}`, so a model declaring `schema='GOLD'` with `target.schema='GOLD'` produced `GOLD_GOLD`. I tried a conditional version of the macro that checked `target.name` — it failed silently. The unconditional override at `dbt/macros/get_custom_schema.sql` was the only version that worked.

**What I'd say in an interview:** I made the medallion change because the original two-layer split conflated staging concerns with business logic. The custom schema macro was a discovery from doing the work — it's a known dbt sharp edge but I only hit it because of the rename.

---

## Tooling pivot 3 — MWAA → Astronomer Astro

**This is the biggest pivot in the project and worth being completely honest about.**

**Initial choice:** AWS MWAA (Managed Workflows for Apache Airflow). The original architecture had four CloudFormation stacks: VPC, S3+SQS, Secrets Manager, MWAA + IAM. The reasoning was sensible: stay all-in AWS, use the managed Airflow service, native IAM and Secrets Manager integration.

**What actually happened:** I invested significant time setting up the full MWAA stack. The deployment failed repeatedly with `CREATE_FAILED` and `NotStabilized` errors. Each failure required:

1. Reading the stack events to find the missing permission or misconfigured resource
2. Patching the IAM role (MWAA execution role) or my deploy user's permissions
3. Deleting the failed stack (MWAA leaves the stack in `ROLLBACK_COMPLETE` which can't be updated)
4. Waiting 30+ seconds and redeploying
5. Going back to step 1 with the next error

The errors were not the same error twice — each redeploy uncovered a new missing permission. Examples I worked through:

- `ec2:CreateNetworkInterface` and the rest of the network interface permissions for placing workers in private subnets
- `ec2:CreateVpcEndpoint`, `route53:AssociateVPCWithHostedZone` for VPC endpoint setup
- `s3:GetAccountPublicAccessBlock` on the execution role
- Missing `plugins/plugins.zip` and `requirements.txt` in the DAGs bucket — MWAA refuses to create without them
- Multiple `cloudformation:ListChangeSets`, `cloudformation:GetTemplateSummary` etc on my deploy user
- The 2048-byte inline IAM policy size limit forcing me to switch from user policies to group policies

After ~15 NotStabilized failures, I made the call to abandon MWAA entirely.

**Why I made that call:** MWAA is a Black Box at startup. When the stack reports `NotStabilized` there is no useful failure message in the CloudFormation event stream beyond a hint at the broken sub-resource. The provisioning is opaque and can take 20-30 minutes per attempt. Compounding that, MWAA has aggressive Airflow version and Python package compatibility requirements — a `requirements.txt` change rebuilds the workers from scratch.

**Resolution:** Deleted every MWAA-related CloudFormation stack (`step0_vpc`, `step2_secrets_manager`, `step3_mwaa`, `step4_iam_mwaa`) and replaced the entire orchestration layer with **Astronomer Astro** (cloud).

Astronomer was meaningfully simpler:

- `astro deployment create` provisioned a cloud Airflow environment in under 2 minutes
- DAGs deploy via `astro deploy -f` from any local working directory — no S3 bucket dance
- Connections and Variables are managed in the cloud Airflow UI (with a deployment-variable CLI fallback)
- The deployment exposes a real Airflow UI accessible from anywhere — no PrivateLink, no bastion host

The only CloudFormation stack that remains is `step1_s3_sqs.yml`. Everything else MWAA-related was deleted.

**What I'd say in an interview:** I started with MWAA because it was the all-AWS choice and I was already using CloudFormation. After ~15 deployment failures with no clear root cause and a 20-minute feedback loop per attempt, I switched to Astronomer. The pragmatic lesson: a managed service is only a good choice if its provisioning is reliable. MWAA's NotStabilized failures were the kind of opaque infrastructure problem that drains a project. Astronomer was running my DAG within an hour of switching.

This is also the moment that taught me to be willing to throw away work. Sunk-cost reasoning on the four CloudFormation stacks would have kept me debugging MWAA for days. The right call was to delete them.

---

## Soda Core 3.x → 4.x migration

The project's data quality layer is Soda Core. Halfway through the build, Soda Core 4.x landed with a completely different Python API and YAML format. The migration was painful because the 3.x → 4.x change is not a version bump — it's effectively a different library.

**What changed:**

| Aspect | Soda 3.x | Soda 4.x |
|---|---|---|
| Import | `from soda.scan import Scan` | `from soda_core.contracts import verify_contract_locally` |
| Invocation | `Scan()` class methods | One function call |
| Result property | `scan.has_check_fails()` method | `result.is_ok` **property** (no parens) |
| Config format | SodaCL multi-source `data_sources:` block | One data source per YAML file |
| Dataset reference | `database.schema.table` | `data_source/schema/table` (slash-separated) |
| Column checks | Flat keys like `missing: ...`, `invalid: ...` | Wrapped under per-column `checks:` list |

**What this cost me:**

- Multiple iterations to get the contract YAML format right. Early attempts had column checks as flat keys and produced `Checks: 0` in the Soda summary — Soda silently treated the file as having zero checks rather than erroring on the format.
- Column casing matters in Soda 4.x. Snowflake stores column names in UPPERCASE by default; lowercase column names in the contract produced `invalid identifier '"transaction_id"'` SQL errors.
- The `is_ok` property was being called as a method in the first version of the CI scan runner. `if not result.is_ok():` raises `'bool' object is not callable`. The fix is dropping the parens, but the original error message hid the cause.
- Soda 4.x doesn't return `failedRowsQuery` in the diagnostics object the way 3.x did. My quarantine logic originally relied on this field. I rewrote it to quarantine the latest partition (`WHERE ingested_at = MAX(...)`) for all failed checks — less precise but correct for the production failure mode (today's batch fails, today's batch goes to quarantine).
- The `schema` check (catches column drift) returns `NOT_EVALUATED` in Soda 4.x rather than passing or failing. My `run_soda_scan.py` initially counted `NOT_EVALUATED` as a failure, which made the CI gate red on a passing PR. The fix was to drop the in-contract schema check entirely — the Snowflake `ALERT` already covers schema drift at the database level.

**What I'd say in an interview:** Soda 4.x was a hard cutover, not a soft migration. The library replaced its public API and YAML format in one release. The portfolio version of this story: I was on the bleeding edge of a tool I genuinely needed and that meant absorbing the cost of the upgrade. The trade-off: 4.x produces clean per-column check results that map directly to quarantine logic — that's worth the migration.

---

## dbt-fusion 2.0 YAML syntax migration

Adding dbt tests to the project surfaced another version-related migration. dbt Cloud is on **dbt-fusion 2.0 (preview)** — the new dbt engine — which has stricter YAML validation than classic dbt:

**Sources.yml:** `loaded_at_field` and `freshness` at the table level are deprecated. The new format wraps them in a `config:` block:

```yaml
# Old (classic dbt):
- name: financial_transactions
  loaded_at_field: ingested_at
  freshness:
    warn_after: { count: 2, period: hour }

# New (dbt-fusion 2.0):
- name: financial_transactions
  config:
    loaded_at_field: ingested_at
    freshness:
      warn_after: { count: 2, period: hour }
```

**Schema.yml tests:** generic test arguments like `values`, `field`, `to`, `min_value`, `max_value` must be nested under `arguments:`:

```yaml
# Old:
tests:
  - accepted_values:
      values: ['PAYMENT', 'TRANSFER', 'REFUND']

# New (dbt-fusion 2.0):
tests:
  - accepted_values:
      arguments:
        values: ['PAYMENT', 'TRANSFER', 'REFUND']
```

The error message from dbt-fusion was helpful — it explicitly named the deprecated key and pointed at the right line:

```
ERROR dbt1159: Deprecated test arguments: ["values"] at top-level detected.
  --> models/silver/schema.yml:44:23
```

**What I'd say in an interview:** Two version-driven migrations on one project (Soda 3→4, dbt classic → fusion). Treating libraries as fixed dependencies is a luxury; in practice you'll be on a moving train and need to absorb breaking changes. Document them in the journey so the next engineer doesn't relearn.

---

## The Slack webhook URL had trailing newlines

A small bug that ate an evening. When I pasted the Slack webhook URL into the Airflow Variable, two trailing newlines came along for the ride. Soda's failure path showed the task `slack_raw_failure` going green with no errors — but no Slack message arrived in `#financial-dq-alerts`.

The failure was visible only after I added explicit `log.info` for the response status:

```
INFO - Posting to Slack webhook (length: 83)
ERROR - HTTPError: 404 Client Error: Not Found for url:
  https://hooks.slack.com/services/.../...%0A%0A
```

The `%0A%0A` are URL-encoded newlines. Slack returned 404 because the path didn't match any active webhook. I'd assumed `requests.post` would just work — but the function wasn't checking the response status code, so a 404 returned successfully and the task went green.

**Two fixes applied:**

1. Re-paste the URL in the Airflow UI without trailing whitespace
2. Add `url.strip()` in the helper function so the same mistake can't bite future me

**What I'd say in an interview:** Two lessons from this. First, always call `response.raise_for_status()` on outbound HTTP — silent 200s on actual 404s ate the evening. Second, defensive `.strip()` on string config values is cheap and would have saved me an hour. Both are now standing rules in this codebase.

---

## Airflow Variable vs `AIRFLOW_VAR_*` environment variable

While debugging the Slack issue I also discovered the Airflow Variable I'd set in the cloud UI was not visible to my running DAG. The error was `VARIABLE_NOT_FOUND: 'Variable slack_webhook_url not found'` despite the variable being visible in `Admin → Variables`.

What worked instead: setting it as a deployment variable via the Astronomer CLI with the `AIRFLOW_VAR_*` prefix:

```bash
astro deployment variable create \
  --key AIRFLOW_VAR_SLACK_WEBHOOK_URL \
  --value "https://hooks.slack.com/..." \
  --secret
```

Astronomer auto-translates `AIRFLOW_VAR_*` env vars into Airflow Variables at runtime. The helper function now checks the env var first and falls back to `Variable.get` only if it's not set:

```python
def _get_slack_webhook() -> str:
    import os
    url = os.environ.get("AIRFLOW_VAR_SLACK_WEBHOOK_URL")
    if url:
        return url.strip()
    from airflow.models import Variable
    return Variable.get("slack_webhook_url").strip()
```

**What I'd say in an interview:** This is the kind of platform-specific gotcha you only learn by running on the managed product. Pure Airflow's `Variable.get()` is the textbook answer; Astronomer's env-var pathway is the production answer. The helper function tries both.

---

## CI gate against isolated schemas (`BRONZE_CI` / `GOLD_CI`)

The CI gate runs Soda contracts on every PR. The non-obvious decision was: against what data?

**Wrong answer:** Production `BRONZE` and `GOLD`. Three reasons it's wrong:
1. Non-deterministic results — production data changes every night, so the same PR could pass at noon and fail at midnight
2. Risk — a bad contract change could quarantine real production rows during a CI test
3. Compute contention with the actual nightly pipeline

**Right answer:** A static fixture. I provisioned `BRONZE_CI` and `GOLD_CI` as separate Snowflake schemas seeded from a 100-row snapshot of production. Every PR sees the same data. The CI workflow does two things at runtime:

1. Generates Soda data source YAML from GitHub Secrets (no static credentials in the repo)
2. Patches the contract files via `sed` to point at the CI schemas instead of production

This pattern keeps one set of contracts as the source of truth for both production and CI.

**Verified end-to-end:** I tested the CI gate by deliberately adding a broken rule (`AMOUNT must be >= 999999`) to the bronze contract on a test branch. The CI workflow correctly failed and blocked the PR from merging.

**The trade-off this creates:** the CI fixture goes stale when source schema evolves. I cover that with the hourly Snowflake schema-drift alert (see README). It's not perfect — there's a window between schema drift and fixture refresh — but the in-DAG bronze Soda check catches the same issue in production, so the two layers together cover the gap.

---

## Idempotency: why `dag_run_id`, not `scan_ts`

The audit log started without idempotency. A re-run of the same Airflow DAG would write two rows for the same scan checkpoint, which polluted reporting queries.

The fix was to add a `dag_run_id` column to `AUDIT.DATA_QUALITY_LOG` and use it as a deduplication key. The choice of `dag_run_id` (vs `scan_ts`) matters:

- `scan_ts` is generated when the scan completes. A retried task produces a new `scan_ts`. Using it as a key wouldn't actually dedupe retries.
- `dag_run_id` is constant across all retries of the same DAG run. Using it correctly dedupes.

The `_write_audit_log` function now checks for an existing row with the same `(dag_run_id, checkpoint)` before inserting:

```python
if dag_run_id:
    check_sql = f"""
        SELECT COUNT(*) FROM {AUDIT_TABLE}
        WHERE dag_run_id = '{dag_run_id}'
          AND checkpoint = '{scan_result["checkpoint"]}'
    """
    if cur.fetchone()[0] > 0:
        return
```

This required propagating Airflow's run context into the task functions via `**context` — a small change but one that touches every audit-log call site. **Forgetting to add `**context` to a function signature** caused a `NameError: name 'context' is not defined` runtime error that took two redeploys to track down. The lesson: Airflow's `context` is opt-in, never magically injected.

---

## Bronze audit log gap on the happy path

Initially the audit log only got written from the failure path (the quarantine task wrote it) and from `pipeline_complete` (which wrote the gold audit row). On the happy path, the bronze check passing was not recorded.

That meant the audit log gave you "every failure" but only partial "every success". For a true "every Soda scan recorded" property, the bronze pass needed its own audit row.

The fix: add the bronze audit write at the start of `run_dbt_transform`. That task only runs when bronze has passed, so it's a natural place to record the bronze success before kicking off dbt.

```python
def run_dbt_transform(scan_result: dict, **context) -> str:
    # Record the bronze pass in the audit log (idempotent).
    hook = SnowflakeHook(snowflake_conn_id="snowflake_pipeline")
    _write_audit_log(hook, scan_result, [], BRONZE_TABLE,
                     dag_run_id=context["run_id"])
    # ... then trigger dbt Cloud job
```

**What I'd say in an interview:** The original design had an asymmetry — failures were audited but passes were partially missed. That's the kind of gap that doesn't surface until you query the audit log and notice the math doesn't add up. The audit log should answer "what did the pipeline do" not just "what went wrong."

---

## Schema drift alert: Snowflake `ALERT`, not Lambda

I considered three approaches for detecting schema drift between production and the CI fixture:

| Approach | Pros | Cons |
|---|---|---|
| Snowflake Account Events → SNS → Lambda → Slack | Real-time, professional architecture | 3 extra AWS services, IAM setup, more failure modes |
| Snowflake `ALERT` + email notification | Pure SQL, runs inside Snowflake, native email | Hourly schedule, no real-time |
| Documented runbook, no automation | Zero cost | Pure discipline; easy to forget |

The right answer for this codebase was the middle one. The schema-drift problem isn't time-critical — an hour of lag between drift and detection is fine because the bronze Soda check in the production DAG will also catch a contract / actual schema mismatch.

Three SQL statements implement the alert:

```sql
CREATE NOTIFICATION INTEGRATION schema_drift_email ...;
CREATE OR REPLACE ALERT schema_drift_alert ...;
ALTER ALERT schema_drift_alert RESUME;
```

**What I'd say in an interview:** This is a good example of choosing simple over impressive. The Lambda architecture sounds better in a portfolio but the Snowflake ALERT solves the same problem with less surface area to maintain. Senior engineers know when *not* to over-engineer.

---

## Retention: Snowflake TASK, not external cron

The `AUDIT.DATA_QUALITY_LOG` and `QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE` tables grow unbounded — every nightly run adds rows, and we never clean up. For a portfolio piece this is fine; for a real production system it's a latent storage problem.

The fix is a Snowflake `TASK` running daily at 03:00 UTC (one hour after the main DAG) that:

1. Copies rows older than 90 days into a parallel `*_ARCHIVE` table
2. Deletes those rows from the live table

```sql
CREATE OR REPLACE TASK AUDIT.retention_cleanup
  WAREHOUSE = PIPELINE_WH
  SCHEDULE = 'USING CRON 0 3 * * * UTC'
AS
BEGIN
  INSERT INTO AUDIT.DATA_QUALITY_LOG_ARCHIVE
  SELECT * FROM AUDIT.DATA_QUALITY_LOG
  WHERE created_at < DATEADD(day, -90, CURRENT_TIMESTAMP());

  DELETE FROM AUDIT.DATA_QUALITY_LOG
  WHERE created_at < DATEADD(day, -90, CURRENT_TIMESTAMP());
  -- (same pattern for QUARANTINE)
END;
```

Could this be a dbt incremental model or an Airflow task? Yes — but the data never leaves Snowflake, so the cleanup logic shouldn't either. A Snowflake TASK is the right level of abstraction.

---

## Remediation: manual cleanup, by design

The quarantine task COPIES bad rows into `QUARANTINE.*` — it does NOT delete them from `BRONZE`. Initially I assumed the DAG should auto-clean bronze too, but on reflection the deletion needs human judgement:

| Situation | Action |
|---|---|
| Source system sent wrong value (e.g. INVALID_TYPE should have been PAYMENT) | `UPDATE` with correct value |
| Source sent duplicate (same transaction twice) | `DELETE` extras, keep original |
| Row is genuinely garbage | `DELETE` |

Auto-DELETE from BRONZE would be dangerous — a contract bug could wipe legitimate data. Production data engineers prefer the human-in-the-loop pattern: the pipeline isolates and alerts, the engineer decides how to fix.

This is documented in the README's "Remediation workflow" section.

---

## Auto-deploy on merge: closing the GitOps loop

The pipeline initially required a manual `astro deploy -f` after every merge. That worked but was a discipline rather than enforcement — easy to forget after the CI gate passed.

Adding a GitHub Actions workflow that auto-runs `astro deploy` after merge to main closed the loop:

```yaml
on:
  push:
    branches: [main]
    paths: ["astro_deploy/**"]

steps:
  - uses: actions/checkout@v4
  - uses: astronomer/deploy-action@v0.9.0
    with:
      deployment-id: cmp93atnn6cez01oruyf4jv07
      root-folder: astro_deploy/
    env:
      ASTRO_API_TOKEN: ${{ secrets.ASTRO_API_TOKEN }}
```

Now a merge to main triggers: deploy → cloud Airflow picks up new bundle (~60s). The local `astro deploy` workflow still exists for emergency one-off fixes, but the path of least resistance is now the proper GitOps flow.

---

## dbt tests alongside Soda: belt and braces

Adding dbt tests felt at first like duplication of Soda. The distinction is real though:

- **Soda enforces contracts** — "does the data have the shape and values we expect?"
- **dbt tests enforce relationships** — "did the transformation preserve correctness?"

Concretely:

- Soda catches: nulls, duplicates, invalid enums, datatype drift — across both BRONZE and GOLD
- dbt tests catch: `relationships()` between models (gold's transaction_id must exist in silver), `accepted_range()` for bounded numerics (hour 0-23), `unique` on derived keys

A dbt model with a bug that drops 10% of rows still passes Soda's column checks on the surviving rows — but fails dbt's `relationships` test because gold rows reference non-existent silver rows. The two tools cover different failure modes.

The tests run as part of the dbt Cloud job after `dbt run`. A test failure marks the dbt run as failed, which the Airflow `run_dbt_transform` task propagates as a task failure → Slack alert.

---

## Things I'd do differently

1. **Start on Astronomer.** The MWAA detour cost a meaningful amount of project time. If I were building this again I would start on Astronomer for the orchestration layer.

2. **Adopt medallion naming on day one.** The RAW/SERVING → BRONZE/SILVER/GOLD rename touched every file in the project. Two-layer would have been fine for the original scope; medallion was always going to be the eventual structure.

3. **Match Soda Core to whatever version the docs ship.** The 4.x migration was triggered by Soda releasing 4.x mid-project. Building on a beta library is a tax I paid; for a portfolio piece it would have been cheaper to lock to 3.x at the start.

4. **Add response-status checks on every outbound HTTP call upfront.** The silent-Slack-failure debugging would not have happened if `raise_for_status()` had been in the helper from day one.

5. **Document the deploy-variable distinction in Astronomer earlier.** Airflow Variable vs `AIRFLOW_VAR_*` env var burned an hour. A README note would have saved it.

6. **Write `**context` into every Airflow task signature from the start.** Adding it ad-hoc when one task needs `context["run_id"]` led to the `pipeline_complete` bug. Better to make it standard kit on every `@task` declaration.

7. **Test the happy path early and often.** Most of my testing focused on the failure path (`--inject-errors`). The first end-to-end happy-path run surfaced two bugs (`pipeline_complete` missing context, bronze audit gap on happy path). Both should have been caught earlier.

8. **Audit the audit log itself.** I added bronze-on-pass to the audit log only after noticing it was missing from a SELECT query. There should be invariants on the audit log ("every dag_run_id has exactly N rows for a full happy run") that you can check periodically.

---

## What the journey teaches

The architecture in the README is clean and reads as if it was designed up front. It wasn't. It is the product of three deliberate pivots (Terraform → CloudFormation, RAW/SERVING → medallion, MWAA → Astronomer), two library version migrations (Soda 3→4, dbt classic → fusion), and a handful of smaller corrections (Slack newlines, Airflow context, audit log asymmetry).

The cleanliness comes from being willing to delete bad work rather than work around it. Every pivot was a conscious decision to throw out work that wasn't earning its place. That instinct — to delete rather than patch — is the senior engineer move.

In an interview, the most useful thing this project demonstrates is not the final architecture but the engineering judgement behind each correction. Each pivot tells a story about reading the cost of staying vs the cost of switching and choosing well.
