-- This test fails (returns rows) if any RSI value is outside 0–100.
-- dbt tests pass when the query returns 0 rows.

SELECT ticker_id, fetched_at, rsi_14
FROM {{ ref('fact_indicators') }}
WHERE rsi_14 IS NOT NULL
  AND (rsi_14 < 0 OR rsi_14 > 100)
