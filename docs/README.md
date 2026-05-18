# Documentation Index

Welcome. This `docs/` folder contains everything you need to recreate the pipeline from scratch with perfection, plus the reference docs that explain why it's built the way it is.

## Where to start

Read these in order if you're building from scratch:

1. **[SETUP_AWS.md](./SETUP_AWS.md)** — S3 bucket, SQS queue, IAM role, CloudFormation
2. **[SETUP_SNOWFLAKE.md](./SETUP_SNOWFLAKE.md)** — Schemas, RBAC, Snowpipe, alerts, retention task
3. **[SETUP_DBT_CLOUD.md](./SETUP_DBT_CLOUD.md)** — Project, environment, nightly job
4. **[SETUP_SODA_CORE.md](./SETUP_SODA_CORE.md)** — Contracts, scan runner
5. **[SETUP_ASTRONOMER.md](./SETUP_ASTRONOMER.md)** — Cloud deployment, connections, DAG
6. **[SETUP_GITHUB_ACTIONS.md](./SETUP_GITHUB_ACTIONS.md)** — CI gate and auto-deploy workflows

## Reference docs

- **[QUALITY_AND_OBSERVABILITY.md](./QUALITY_AND_OBSERVABILITY.md)** — Separation of concerns: what each quality layer catches, what each observability channel shows, how to remediate failures

## Higher-level docs (at the project root)

- **[../README.md](../README.md)** — Architecture overview, stack rationale, how to run
- **[../JOURNEY.md](../JOURNEY.md)** — Build journey: pivots, dead ends, decisions, things I'd do differently

## Order dependencies

The pieces depend on each other in this order:

```
AWS (S3 bucket exists)
  ↓
Snowflake (Snowpipe needs the bucket; storage integration needs the IAM role)
  ↓
dbt Cloud (needs Snowflake credentials)
  ↓
Soda (contracts need Snowflake schemas to scan against)
  ↓
Astronomer (DAG triggers dbt Cloud + runs Soda)
  ↓
GitHub Actions (CI gate needs Snowflake CI schemas; deploy needs Astronomer)
```

If you're recreating: don't try to do them in parallel. Each step depends on the previous step's outputs.

## Time budget

A first-time recreate with careful step-following takes roughly:

- AWS: 30 min
- Snowflake: 60 min (most laborious — many schemas/grants)
- dbt Cloud: 20 min
- Soda contracts: 15 min (mostly copy-paste from the guide)
- Astronomer: 30 min
- GitHub Actions: 20 min

Total: ~3 hours of focused work, plus another hour for testing happy path + failure path.

A second-time recreate with this guide as reference should be under an hour.
