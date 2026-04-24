-- Staging model: light validation and renaming on top of the raw stg_prices table.
-- This model is a view — it adds no storage overhead.

WITH source AS (
    SELECT
        ticker_id,
        fetched_at,
        ingested_at,
        open,
        high,
        low,
        close,
        adj_close,
        volume
    FROM stg_prices
    WHERE
        adj_close IS NOT NULL    -- exclude rows where yfinance returned no price
        AND volume IS NOT NULL
        AND adj_close > 0        -- sanity check: price must be positive
)

SELECT * FROM source
