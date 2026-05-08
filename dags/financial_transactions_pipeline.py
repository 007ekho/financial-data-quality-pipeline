"""
dags/financial_transactions_pipeline.py

End-to-end pipeline: Snowpipe → Soda RAW → dbt → Soda SERVING
==============================================================
                                                     
  S3 drop                                            
     │                                               
     ▼  (auto-ingest, event-driven)                  
  Snowpipe ──► RAW.FINANCIAL_TRANSACTIONS                    
                     │                               
                     ▼                               
              [Soda RAW check] ──FAIL──► quarantine + alert + STOP
                     │                               
                   PASS                              
                     │                               
                     ▼                               
              dbt transform                          
                     │                               
                     ▼                               
              [Soda SERVING check] ──FAIL──► quarantine + alert + STOP
                     │                               
                   PASS                              
                     │                               
                     ▼                               
              SERVING.FCT_FINANCIAL_TRANSACTIONS ✓           
                     │                               
                     ▼                               
              audit log written                      

Schedule: 02:05 daily (5 min after nightly 02:00 data drop)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.trigger_rule import TriggerRule
import boto3

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
# Secret names match Step 2 CloudFormation outputs exactly.
# MWAA execution role (Step 4) grants GetSecretValue on these paths.
ENV             = "prod"
PROJECT         = "financial-data-quality-pipeline"
SECRET_SNOWFLAKE = f"/{PROJECT}/{ENV}/snowflake/connection"
SECRET_SLACK     = f"/{PROJECT}/{ENV}/slack/webhook-url"
SECRET_DBT       = f"/{PROJECT}/{ENV}/dbt-cloud/credentials"

DATABASE        = "FINANCIAL_DATA"
RAW_TABLE       = f"{DATABASE}.RAW.FINANCIAL_TRANSACTIONS"
SERVING_TABLE   = f"{DATABASE}.SERVING.FCT_FINANCIAL_TRANSACTIONS"
QUARANTINE_DB   = f"{DATABASE}.QUARANTINE"
AUDIT_TABLE     = f"{DATABASE}.AUDIT.DATA_QUALITY_LOG"
PIPE_NAME       = f"{DATABASE}.RAW.FINANCIAL_TRANSACTIONS_PIPE"

SODA_RAW_CONTRACT     = "contracts/raw/financial_transactions_raw.yml"
SODA_SERVING_CONTRACT = "contracts/serving/fct_financial_transactions.yml"
SODA_DATA_SOURCE_RAW     = "snowflake_raw"
SODA_DATA_SOURCE_SERVING = "snowflake_serving"
SODA_CONFIG_PATH = "soda/configuration.yml"

DBT_SELECT = "fct_financial_transactions+"

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get_secret(secret_name: str) -> dict | str:
    """
    Fetch a secret from AWS Secrets Manager.
    Returns parsed dict for JSON secrets, raw string for plain secrets.
    MWAA workers inherit the execution role which has GetSecretValue
    permission on all pipeline secrets (granted in Step 4 IAM stack).
    """
    client = boto3.client("secretsmanager", region_name="eu-west-2")
    value  = client.get_secret_value(SecretId=secret_name)["SecretString"]
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _run_soda_scan(contract_path: str, data_source: str, checkpoint: str) -> dict:
    """
    Execute a SodaCL contract scan and return structured results.
    Shared by both RAW and SERVING checkpoints.
    """
    from soda.scan import Scan

    scan = Scan()
    scan.set_data_source_name(data_source)
    scan.add_configuration_yaml_file(SODA_CONFIG_PATH)
    scan.add_sodacl_yaml_file(contract_path)
    scan.set_scan_definition_name(f"financial_transactions_{checkpoint}")

    exit_code = scan.execute()

    results = []
    for check in scan.get_checks():
        results.append({
            "name":        check.name,
            "outcome":     check.outcome.value,
            "diagnostics": check.get_cloud_diagnostics_dict(),
        })

    failed = [c for c in results if c["outcome"] == "fail"]
    warned = [c for c in results if c["outcome"] == "warn"]

    return {
        "checkpoint":    checkpoint,
        "exit_code":     exit_code,
        "scan_ts":       datetime.utcnow().isoformat(),
        "total_checks":  len(results),
        "failed":        len(failed),
        "warned":        len(warned),
        "failed_checks": failed,
        "warned_checks": warned,
    }


def _quarantine_rows(hook: SnowflakeHook, scan_result: dict, source_table: str) -> list:
    """
    For each failed check, insert failing records into QUARANTINE schema
    with full failure metadata. Returns list of {check, rows_quarantined}.
    """
    target = f"{QUARANTINE_DB}.{source_table.split('.')[-1]}_QUARANTINE"
    stats = []

    for check in scan_result["failed_checks"]:
        failed_row_sql = check["diagnostics"].get("failedRowsQuery")

        if failed_row_sql:
            insert_sql = f"""
                INSERT INTO {target}
                SELECT src.*, 
                    '{check["name"]}'          AS failed_check_name,
                    '{scan_result["scan_ts"]}'::TIMESTAMP AS quarantined_at,
                    CURRENT_TIMESTAMP()        AS inserted_at
                FROM ({failed_row_sql}) src
            """
        else:
            # Volume / freshness checks: quarantine latest partition
            insert_sql = f"""
                INSERT INTO {target}
                SELECT src.*,
                    '{check["name"]}'          AS failed_check_name,
                    '{scan_result["scan_ts"]}'::TIMESTAMP AS quarantined_at,
                    CURRENT_TIMESTAMP()        AS inserted_at
                FROM {source_table} src
                WHERE ingested_at = (SELECT MAX(ingested_at) FROM {source_table})
            """

        with hook.get_conn() as conn:
            cur = conn.cursor()
            cur.execute(insert_sql)
            rows = cur.rowcount

        stats.append({"check": check["name"], "rows_quarantined": rows})
        log.info("Quarantined %d rows for check: %s", rows, check["name"])

    return stats


def _write_audit_log(hook: SnowflakeHook, scan_result: dict,
                     quarantine_stats: list, dataset: str) -> None:
    status = "FAILED" if scan_result["failed"] > 0 else "PASSED"
    remediation = "PENDING" if status == "FAILED" else "NOT_REQUIRED"

    sql = f"""
        INSERT INTO {AUDIT_TABLE}
        (dataset_name, checkpoint, scan_ts, scan_status,
         failed_checks, quarantine_stats, remediation_status, created_at)
        VALUES (
            '{dataset}',
            '{scan_result["checkpoint"]}',
            '{scan_result["scan_ts"]}'::TIMESTAMP,
            '{status}',
            PARSE_JSON('{json.dumps(scan_result["failed_checks"])}'),
            PARSE_JSON('{json.dumps(quarantine_stats)}'),
            '{remediation}',
            CURRENT_TIMESTAMP()
        )
    """
    with hook.get_conn() as conn:
        conn.cursor().execute(sql)


# ── DAG ────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="financial_transactions_pipeline",
    default_args=default_args,
    description="Snowpipe → Soda RAW → dbt → Soda SERVING with auto-quarantine",
    schedule="5 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "soda", "snowpipe", "dbt"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── 1. Wait for Snowpipe to finish loading ─────────────────────────────────
    @task(task_id="wait_for_snowpipe")
    def wait_for_snowpipe() -> str:
        """
        Poll SYSTEM$PIPE_STATUS until Snowpipe has no pending files.
        Returns the scan window timestamp once clear.
        """
        import time
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)

        max_wait_seconds = 600  # 10 min max wait
        poll_interval = 30
        elapsed = 0

        while elapsed < max_wait_seconds:
            with hook.get_conn() as conn:
                cur = conn.cursor()
                cur.execute(f"SELECT SYSTEM$PIPE_STATUS('{PIPE_NAME}')")
                status = json.loads(cur.fetchone()[0])

            pending = status.get("pendingFileCount", 0)
            executing = status.get("executingFileCount", 0)
            log.info("Snowpipe status — pending: %d, executing: %d", pending, executing)

            if pending == 0 and executing == 0:
                log.info("Snowpipe clear. Proceeding to Soda RAW check.")
                return datetime.utcnow().isoformat()

            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(f"Snowpipe did not clear within {max_wait_seconds}s")

    snowpipe_ready = wait_for_snowpipe()

    # ── 2. Soda check: RAW layer ───────────────────────────────────────────────
    @task(task_id="soda_check_raw")
    def soda_check_raw(_) -> dict:
        return _run_soda_scan(SODA_RAW_CONTRACT, SODA_DATA_SOURCE_RAW, "raw")

    raw_scan = soda_check_raw(snowpipe_ready)

    # ── 3. Branch on RAW result ────────────────────────────────────────────────
    @task.branch(task_id="raw_quality_gate")
    def raw_quality_gate(scan_result: dict) -> str:
        if scan_result["failed"] > 0:
            log.warning("RAW quality gate FAILED. Routing to quarantine.")
            return "quarantine_raw_failures"
        log.info("RAW quality gate PASSED. Proceeding to dbt.")
        return "run_dbt_transform"

    raw_gate = raw_quality_gate(raw_scan)

    # ── 4a. FAIL PATH: quarantine raw failures ─────────────────────────────────
    @task(task_id="quarantine_raw_failures")
    def quarantine_raw_failures(scan_result: dict) -> dict:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        stats = _quarantine_rows(hook, scan_result, RAW_TABLE)
        _write_audit_log(hook, scan_result, stats, RAW_TABLE)
        return {"quarantine_stats": stats, "scan_result": scan_result}

    raw_quarantine = quarantine_raw_failures(raw_scan)

    @task(task_id="alert_raw_failure")
    def alert_raw_failure(quarantine_result: dict) -> str:
        sr = quarantine_result["scan_result"]
        stats = quarantine_result["quarantine_stats"]
        total = sum(s["rows_quarantined"] for s in stats)
        checks = "\n".join([f"• `{c['name']}`" for c in sr["failed_checks"]])
        return json.dumps({
            "text": f":red_circle: *RAW contract FAILED* — `{RAW_TABLE}`\n"
                    f"*Failed checks:* {sr['failed']}/{sr['total_checks']}\n"
                    f"{checks}\n"
                    f"*Rows quarantined:* {total:,}\n"
                    f"dbt transform *blocked*."
        })

    raw_alert_payload = alert_raw_failure(raw_quarantine)

    @task(task_id="slack_raw_failure")
    def slack_raw_failure(payload: str) -> None:
        import requests
        webhook_url = _get_secret(SECRET_SLACK)
        requests.post(webhook_url, data=payload, timeout=10)

    raw_slack = slack_raw_failure(raw_alert_payload)

    raw_stop = EmptyOperator(task_id="raw_pipeline_stopped")

    # ── 4b. PASS PATH: run dbt transform ──────────────────────────────────────
    @task(task_id="run_dbt_transform")
    def run_dbt_transform() -> str:
        """
        Trigger dbt Cloud job via API, or run dbt Core CLI locally.
        Returns job run ID / completion timestamp.
        """
        import requests, time

        creds    = _get_secret(SECRET_DBT)
        token    = creds["api_token"]
        acct_id  = creds["account_id"]
        job_id   = creds["job_id"]
        headers  = {"Authorization": f"Token {token}", "Content-Type": "application/json"}

        resp = requests.post(
            f"https://cloud.getdbt.com/api/v2/accounts/{acct_id}/jobs/{job_id}/run/",
            headers=headers,
            json={"cause": "Triggered by MWAA financial_transactions_pipeline"},
            timeout=30
        )
        run_id = resp.json()["data"]["id"]
        log.info("dbt Cloud run triggered: %s", run_id)

        for _ in range(40):
            time.sleep(30)
            status = requests.get(
                f"https://cloud.getdbt.com/api/v2/accounts/{acct_id}/runs/{run_id}/",
                headers=headers, timeout=30
            ).json()["data"]
            if status["is_complete"]:
                if status["is_success"]:
                    return f"dbt run {run_id} succeeded"
                raise Exception(f"dbt run {run_id} failed: {status['status_humanized']}")

        raise Exception(f"dbt run {run_id} did not complete within 20 minutes")

    dbt_done = run_dbt_transform()

    # ── 5. Soda check: SERVING layer ──────────────────────────────────────────
    @task(task_id="soda_check_serving")
    def soda_check_serving(_) -> dict:
        return _run_soda_scan(SODA_SERVING_CONTRACT, SODA_DATA_SOURCE_SERVING, "serving")

    serving_scan = soda_check_serving(dbt_done)

    # ── 6. Branch on SERVING result ───────────────────────────────────────────
    @task.branch(task_id="serving_quality_gate")
    def serving_quality_gate(scan_result: dict) -> str:
        if scan_result["failed"] > 0:
            return "quarantine_serving_failures"
        return "pipeline_complete"

    serving_gate = serving_quality_gate(serving_scan)

    # ── 7a. Quarantine serving failures ───────────────────────────────────────
    @task(task_id="quarantine_serving_failures")
    def quarantine_serving_failures(scan_result: dict) -> dict:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        stats = _quarantine_rows(hook, scan_result, SERVING_TABLE)
        _write_audit_log(hook, scan_result, stats, SERVING_TABLE)
        return {"quarantine_stats": stats, "scan_result": scan_result}

    serving_quarantine = quarantine_serving_failures(serving_scan)

    @task(task_id="alert_serving_failure")
    def alert_serving_failure(quarantine_result: dict) -> str:
        sr = quarantine_result["scan_result"]
        stats = quarantine_result["quarantine_stats"]
        total = sum(s["rows_quarantined"] for s in stats)
        checks = "\n".join([f"• `{c['name']}`" for c in sr["failed_checks"]])
        return json.dumps({
            "text": f":large_yellow_circle: *SERVING contract FAILED* — `{SERVING_TABLE}`\n"
                    f"Raw data was clean — issue is in dbt transform logic.\n"
                    f"*Failed checks:* {sr['failed']}/{sr['total_checks']}\n"
                    f"{checks}\n"
                    f"*Rows quarantined:* {total:,}"
        })

    serving_alert_payload = alert_serving_failure(serving_quarantine)

    @task(task_id="slack_serving_failure")
    def slack_serving_failure(payload: str) -> None:
        import requests
        webhook_url = _get_secret(SECRET_SLACK)
        requests.post(webhook_url, data=payload, timeout=10)

    serving_slack = slack_serving_failure(serving_alert_payload)

    # ── 7b. All passed ────────────────────────────────────────────────────────
    @task(task_id="pipeline_complete")
    def pipeline_complete(scan_result: dict) -> None:
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN)
        _write_audit_log(hook, scan_result, [], SERVING_TABLE)
        log.info("Pipeline complete. All quality gates passed. ✓")

    complete = pipeline_complete(serving_scan)

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # ── Wire ──────────────────────────────────────────────────────────────────
    start >> snowpipe_ready >> raw_scan >> raw_gate

    # RAW fail path
    raw_gate >> raw_quarantine >> raw_alert_payload >> raw_slack >> raw_stop >> end

    # RAW pass → dbt → SERVING check → branch
    raw_gate >> dbt_done >> serving_scan >> serving_gate

    # SERVING fail path
    serving_gate >> serving_quarantine >> serving_alert_payload >> serving_slack >> end

    # SERVING pass path
    serving_gate >> complete >> end