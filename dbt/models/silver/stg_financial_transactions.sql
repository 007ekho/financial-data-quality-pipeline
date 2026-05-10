-- models/silver/stg_financial_transactions.sql
-- Staging model: light clean of RAW.FINANCIAL_TRANSACTIONS
-- Materialised as a view — no storage cost, always fresh.
-- Purpose:
--   - Cast types explicitly (Snowpipe loads everything as VARCHAR from Parquet)
--   - Rename nothing — keep column names identical to RAW for traceability
--   - Filter out quarantined records (those flagged in AUDIT layer)
--   - No business logic here — that lives in the serving model

{{
    config(
        materialized='view',
        tags=['raw', 'nightly']
    )
}}

WITH source AS (

    SELECT
        transaction_id::VARCHAR                          AS transaction_id,
        account_id::VARCHAR                              AS account_id,
        counterparty_id::VARCHAR                         AS counterparty_id,
        transaction_type::VARCHAR                        AS transaction_type,
        amount::FLOAT                                    AS amount,
        currency::VARCHAR                                AS currency,
        status::VARCHAR                                  AS status,
        channel::VARCHAR                                 AS channel,
        merchant_category::VARCHAR                       AS merchant_category,
        initiated_at::TIMESTAMP_NTZ                      AS initiated_at,
        settled_at::TIMESTAMP_NTZ                        AS settled_at,
        is_flagged::BOOLEAN                              AS is_flagged,
        flag_reason::VARCHAR                             AS flag_reason,
        source_system::VARCHAR                           AS source_system,
        ingested_at::TIMESTAMP_NTZ                       AS ingested_at

    FROM {{ var('database') }}.BRONZE.FINANCIAL_TRANSACTIONS

),

-- Exclude records currently sitting in quarantine
-- so they never reach the serving layer
not_quarantined AS (

    SELECT s.*
    FROM source s
    LEFT JOIN {{ var('database') }}.QUARANTINE.FINANCIAL_TRANSACTIONS_QUARANTINE q
        ON s.transaction_id = q.transaction_id
    WHERE q.transaction_id IS NULL

)

SELECT * FROM not_quarantined