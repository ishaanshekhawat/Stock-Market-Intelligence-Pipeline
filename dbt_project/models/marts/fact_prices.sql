-- Promotes data from intermediate layer into the fact_prices physical table.
-- This is what fact_indicators reads from.

{{
    config(
        materialized='table',
        unique_key=['ticker_id', 'fetched_at']
    )
}}

SELECT
    ticker_id,
    fetched_at,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    daily_return
FROM {{ ref('int_price_with_returns') }}
