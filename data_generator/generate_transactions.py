"""
generate_transactions.py
========================
Generates a nightly batch of financial transaction events
and uploads a dated .parquet file to S3.

Usage:
    python generate_transactions.py                      # generates for today
    python generate_transactions.py --date 2025-01-15   # specific date
    python generate_transactions.py --rows 5000         # override row count
    python generate_transactions.py --dry-run           # local only, no S3 upload

S3 output path:
    s3://{bucket}/financial_transactions/dt={YYYY-MM-DD}/transactions.parquet

Schema (matches your Snowflake DCM table + SodaCL contract):
    transaction_id      VARCHAR   unique event identifier
    account_id          VARCHAR   source account
    counterparty_id     VARCHAR   destination account
    transaction_type    VARCHAR   PAYMENT | TRANSFER | REFUND
    amount              DECIMAL   transaction amount (GBP)
    currency            VARCHAR   ISO 4217 currency code
    status              VARCHAR   COMPLETED | PENDING | FAILED | REVERSED
    channel             VARCHAR   MOBILE | WEB | ATM | BRANCH | API
    merchant_category   VARCHAR   nullable — populated for PAYMENT type
    initiated_at        TIMESTAMP when the transaction was initiated
    settled_at          TIMESTAMP nullable — null if PENDING or FAILED
    is_flagged          BOOLEAN   fraud flag
    flag_reason         VARCHAR   nullable — reason if flagged
    source_system       VARCHAR   always 'CORE_BANKING_v2'
    ingested_at         TIMESTAMP set to now() at generation time

Realism features:
    - ~2% of transactions are flagged for fraud
    - ~5% FAILED, ~8% PENDING, rest COMPLETED or REVERSED
    - amounts follow a realistic log-normal distribution
    - REFUND amounts are negative
    - settled_at is null for PENDING/FAILED
    - ~3% intentional quality issues injected when --inject-errors flag is set
      (for testing Soda quarantine behaviour)
"""

import argparse
import os
import uuid
import random
import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import pandas as pd
import numpy as np
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s'
)
log = logging.getLogger(__name__)
fake = Faker('en_GB')
rng  = np.random.default_rng(seed=42)


# ── Constants ──────────────────────────────────────────────────────────────────

S3_BUCKET  = os.environ.get('S3_BUCKET',  'tfgm-data-platform-prod-raw-ingest')
S3_PREFIX  = os.environ.get('S3_PREFIX',  'financial_transactions')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-west-2')

TRANSACTION_TYPES = ['PAYMENT', 'TRANSFER', 'REFUND']
TYPE_WEIGHTS      = [0.60,       0.30,       0.10]

STATUSES      = ['COMPLETED', 'PENDING', 'FAILED', 'REVERSED']
STATUS_WEIGHTS = [0.82,        0.08,      0.05,     0.05]

CHANNELS      = ['MOBILE', 'WEB', 'ATM', 'BRANCH', 'API']
CHANNEL_WEIGHTS = [0.40,    0.25,  0.15,  0.10,     0.10]

CURRENCIES    = ['GBP', 'USD', 'EUR', 'GBP', 'GBP']  # weighted towards GBP

MERCHANT_CATEGORIES = [
    'GROCERY', 'FUEL', 'RESTAURANT', 'RETAIL', 'TRAVEL',
    'UTILITIES', 'HEALTHCARE', 'ENTERTAINMENT', 'ONLINE', 'OTHER'
]

FLAG_REASONS = [
    'UNUSUAL_AMOUNT', 'HIGH_FREQUENCY', 'FOREIGN_CURRENCY',
    'NEW_COUNTERPARTY', 'ODD_HOURS', 'VELOCITY_BREACH'
]

SOURCE_SYSTEM = 'CORE_BANKING_v2'


# ── Generators ─────────────────────────────────────────────────────────────────

def generate_account_id() -> str:
    return f"ACC{fake.numerify('########')}"


def generate_amount(txn_type: str) -> float:
    """
    Log-normal distribution centred around £150 for payments/transfers.
    Refunds are negative and smaller.
    """
    if txn_type == 'REFUND':
        amount = round(abs(rng.lognormal(mean=3.5, sigma=0.8)), 2)
        return -amount
    amount = round(abs(rng.lognormal(mean=5.0, sigma=1.2)), 2)
    return min(amount, 50000.0)  # cap at £50k for realism


def generate_timestamps(txn_date: datetime, status: str):
    """
    initiated_at: random time during the business day on txn_date
    settled_at:   initiated_at + settlement delay, null for PENDING/FAILED
    """
    # Most transactions during business hours, some overnight
    _hw = np.array([1,1,1,1,1,2,3,5,7,8,8,7,7,7,7,6,6,5,5,4,3,2,1,1], dtype=float); _hw /= _hw.sum()
    hour = rng.choice(range(24), p=_hw)
    minute = rng.integers(0, 60)
    second = rng.integers(0, 60)

    initiated_at = txn_date.replace(
        hour=int(hour), minute=int(minute), second=int(second),
        tzinfo=timezone.utc
    )

    if status in ('PENDING', 'FAILED'):
        settled_at = None
    else:
        # Settlement: 0-3 seconds for API/MOBILE, up to 2 days for BRANCH
        delay_seconds = rng.integers(0, 172800)
        settled_at = initiated_at + timedelta(seconds=int(delay_seconds))

    return initiated_at, settled_at


def generate_row(txn_date: datetime, account_pool: list) -> dict:
    txn_type   = rng.choice(TRANSACTION_TYPES, p=TYPE_WEIGHTS)
    status     = rng.choice(STATUSES, p=STATUS_WEIGHTS)
    channel    = rng.choice(CHANNELS, p=CHANNEL_WEIGHTS)
    currency   = rng.choice(CURRENCIES)
    amount     = generate_amount(txn_type)
    initiated_at, settled_at = generate_timestamps(txn_date, status)

    # Merchant category only makes sense for PAYMENT
    merchant_category = (
        rng.choice(MERCHANT_CATEGORIES) if txn_type == 'PAYMENT' else None
    )

    # Fraud flagging: ~2% of transactions
    is_flagged  = rng.random() < 0.02
    flag_reason = rng.choice(FLAG_REASONS) if is_flagged else None

    # Counterparty: different from source account
    account_id      = rng.choice(account_pool)
    counterparty_id = rng.choice([a for a in account_pool if a != account_id])

    return {
        'transaction_id':    str(uuid.uuid4()),
        'account_id':        account_id,
        'counterparty_id':   counterparty_id,
        'transaction_type':  txn_type,
        'amount':            amount,
        'currency':          currency,
        'status':            status,
        'channel':           channel,
        'merchant_category': merchant_category,
        'initiated_at':      initiated_at,
        'settled_at':        settled_at,
        'is_flagged':        bool(is_flagged),
        'flag_reason':       flag_reason,
        'source_system':     SOURCE_SYSTEM,
        'ingested_at':       datetime.now(timezone.utc),
    }


def inject_errors(df: pd.DataFrame, error_rate: float = 0.03) -> pd.DataFrame:
    """
    Injects intentional data quality issues to test Soda quarantine behaviour.
    Only used when --inject-errors flag is passed.

    Issues injected:
      - Null transaction_id (violates NOT NULL contract)
      - Invalid transaction_type value
      - amount out of plausible range
      - Duplicate transaction_id (uniqueness violation)
    """
    n = len(df)
    error_count = max(1, int(n * error_rate))

    log.warning("Injecting %d error rows for Soda quarantine testing", error_count)

    # Null transaction_ids
    null_idx = rng.choice(df.index, size=error_count // 4, replace=False)
    df.loc[null_idx, 'transaction_id'] = None

    # Invalid transaction_type
    bad_type_idx = rng.choice(df.index, size=error_count // 4, replace=False)
    df.loc[bad_type_idx, 'transaction_type'] = 'INVALID_TYPE'

    # Out-of-range amounts
    bad_amount_idx = rng.choice(df.index, size=error_count // 4, replace=False)
    df.loc[bad_amount_idx, 'amount'] = 999999999.0

    # Duplicate transaction_ids
    dup_idx = rng.choice(df.index, size=error_count // 4, replace=False)
    df.loc[dup_idx, 'transaction_id'] = df.loc[dup_idx[0], 'transaction_id']

    return df


# ── Main ───────────────────────────────────────────────────────────────────────

def generate(
    txn_date:      datetime,
    n_rows:        int,
    inject_errors_flag: bool
) -> pd.DataFrame:

    log.info("Generating %d transaction rows for %s", n_rows, txn_date.date())

    # Pool of realistic account IDs — consistent across rows (same accounts transact)
    account_pool = [generate_account_id() for _ in range(max(50, n_rows // 20))]

    rows = [generate_row(txn_date, account_pool) for _ in range(n_rows)]
    df   = pd.DataFrame(rows)

    # Enforce dtypes
    df['amount']       = df['amount'].astype('float64')
    df['is_flagged']   = df['is_flagged'].astype('bool')
    df['initiated_at'] = pd.to_datetime(df['initiated_at'], utc=True)
    df['settled_at']   = pd.to_datetime(df['settled_at'],   utc=True)
    df['ingested_at']  = pd.to_datetime(df['ingested_at'],  utc=True)

    if inject_errors_flag:
        df = inject_errors(df)

    log.info(
        "Generated: %d rows | types: %s | statuses: %s | flagged: %d",
        len(df),
        df['transaction_type'].value_counts().to_dict(),
        df['status'].value_counts().to_dict(),
        df['is_flagged'].sum()
    )
    return df


def upload_to_s3(df: pd.DataFrame, txn_date: datetime, dry_run: bool) -> str:
    date_str = txn_date.strftime('%Y-%m-%d')
    s3_key   = f"{S3_PREFIX}/dt={date_str}/transactions.parquet"

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)

    if dry_run:
        local_path = f"/tmp/transactions_{date_str}.parquet"
        with open(local_path, 'wb') as f:
            f.write(buffer.read())
        log.info("Dry run — saved locally to %s (%d KB)",
                 local_path, os.path.getsize(local_path) // 1024)
        return local_path

    s3 = boto3.client('s3', region_name=AWS_REGION)
    buffer.seek(0)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer,
        ContentType='application/octet-stream',
        ServerSideEncryption='AES256',
        Metadata={
            'source':       SOURCE_SYSTEM,
            'generated-at': datetime.now(timezone.utc).isoformat(),
            'row-count':    str(len(df)),
        }
    )

    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    log.info("Uploaded %d rows → %s", len(df), s3_uri)
    return s3_uri


def main():
    parser = argparse.ArgumentParser(
        description='Generate nightly financial transaction data and upload to S3'
    )
    parser.add_argument(
        '--date',
        type=str,
        default=datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        help='Transaction date YYYY-MM-DD (default: today)'
    )
    parser.add_argument(
        '--rows',
        type=int,
        default=1000,
        help='Number of rows to generate (default: 1000)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Save locally only, skip S3 upload'
    )
    parser.add_argument(
        '--inject-errors',
        action='store_true',
        help='Inject data quality errors to test Soda quarantine'
    )
    args = parser.parse_args()

    txn_date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    df     = generate(txn_date, args.rows, args.inject_errors)
    output = upload_to_s3(df, txn_date, args.dry_run)

    log.info("Done. Output: %s", output)
    return output


if __name__ == '__main__':
    main()