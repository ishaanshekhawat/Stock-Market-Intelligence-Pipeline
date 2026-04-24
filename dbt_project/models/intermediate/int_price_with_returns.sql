-- Computes daily_return per ticker per bar
-- daily_return = (current close - previous close) / previous close

WITH base AS (
    SELECT * FROM {{ ref('stg_prices') }}
),

with_return AS (
    SELECT
        ticker_id,
        fetched_at,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        LAG(adj_close) OVER (
            PARTITION BY ticker_id
            ORDER BY fetched_at
        ) AS prev_close
    FROM base
)

SELECT
    ticker_id,
    fetched_at,
    open,
    high,
    low,
    close,
    adj_close,
    volume,
    CASE
        WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
        ELSE ROUND((adj_close - prev_close) / prev_close, 3)
    END AS daily_return
FROM with_return
