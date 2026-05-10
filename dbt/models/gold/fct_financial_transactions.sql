-- models/gold/fct_financial_transactions.sql
-- Serving model: business-ready financial transactions fact table
-- Materialised as a table, clustered by transaction_date.
-- Adds all derived columns the SERVING Soda contract checks against:
--   - amount_gbp        (FX-normalised amount)
--   - amount_bucket     (MICRO / SMALL / MEDIUM / LARGE / WHALE / REFUND)
--   - transaction_date  (date part of initiated_at)
--   - hour_of_day       (hour of initiated_at for time-of-day analysis)
--   - settlement_hours  (hours between initiation and settlement)
--   - loaded_at         (when this dbt run inserted the row)

{{
    config(
        materialized='table',
        tags=['serving', 'nightly'],
        post_hook="ALTER TABLE {{ this }} CLUSTER BY (transaction_date)"
    )
}}

WITH staged AS (

    SELECT * FROM {{ ref('stg_financial_transactions') }}

),

-- Simple FX rates for normalisation to GBP.
-- In production replace with a live rates seed or an external FX table.
fx_rates AS (

    SELECT *
    FROM (VALUES
        ('GBP', 1.000),
        ('USD', 0.790),
        ('EUR', 0.860)
    ) AS t (currency, rate_to_gbp)

),

enriched AS (

    SELECT
        -- ── Core identifiers ───────────────────────────────────────────────
        s.transaction_id,
        s.account_id,
        s.counterparty_id,

        -- ── Transaction attributes ─────────────────────────────────────────
        s.transaction_type,
        s.status,
        s.channel,
        s.currency,
        s.merchant_category,
        s.is_flagged,
        s.flag_reason,
        s.source_system,

        -- ── Amounts ───────────────────────────────────────────────────────
        s.amount                                         AS amount_original,
        ROUND(
            s.amount * COALESCE(fx.rate_to_gbp, 1.0),
            2
        )                                                AS amount_gbp,

        -- amount_bucket — checked by SERVING Soda contract
        CASE
    WHEN s.transaction_type = 'REFUND'                           THEN 'REFUND'
    WHEN ROUND(s.amount * COALESCE(fx.rate_to_gbp, 1.0), 2) BETWEEN 0    AND 10    THEN 'MICRO'
    WHEN ROUND(s.amount * COALESCE(fx.rate_to_gbp, 1.0), 2) BETWEEN 10   AND 100   THEN 'SMALL'
    WHEN ROUND(s.amount * COALESCE(fx.rate_to_gbp, 1.0), 2) BETWEEN 100  AND 1000  THEN 'MEDIUM'
    WHEN ROUND(s.amount * COALESCE(fx.rate_to_gbp, 1.0), 2) BETWEEN 1000 AND 10000 THEN 'LARGE'
    WHEN ROUND(s.amount * COALESCE(fx.rate_to_gbp, 1.0), 2) > 10000                THEN 'WHALE'
    ELSE 'MICRO'
END                                           AS amount_bucket,

        -- ── Timestamps ────────────────────────────────────────────────────
        s.initiated_at,
        s.settled_at,
        s.ingested_at,

        -- Derived date/time columns — checked by SERVING Soda contract
        s.initiated_at::DATE                             AS transaction_date,
        EXTRACT(HOUR FROM s.initiated_at)::INTEGER       AS hour_of_day,

        -- settlement_hours: null for PENDING / FAILED
        CASE
            WHEN s.settled_at IS NOT NULL
            THEN ROUND(
                DATEDIFF('minute', s.initiated_at, s.settled_at) / 60.0,
                2
            )
            ELSE NULL
        END                                              AS settlement_hours,

        -- loaded_at — checked by freshness in SERVING Soda contract
        CURRENT_TIMESTAMP()                              AS loaded_at

    FROM staged s
    LEFT JOIN fx_rates fx
        ON s.currency = fx.currency

)

SELECT * FROM enriched