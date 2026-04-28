{{
    config(
        materialized='table',
        unique_key=['ticker_id', 'fetched_at', 'alert_type']
    )
}}

-- Generates one row per alert condition breached.
-- This model is a pure INSERT view — it doesn't update the notified column.
-- The Airflow notify task handles that separately.

WITH indicators AS (
    SELECT
        i.*,
        -- MACD crossover: compare current vs previous MACD histogram
        (i.macd_value - i.macd_signal) AS macd_histogram,
        LAG(i.macd_value - i.macd_signal) OVER (
            PARTITION BY i.ticker_id ORDER BY i.fetched_at
        ) AS prev_macd_histogram
    FROM {{ ref('fact_indicators') }} i
),

alerts AS (

    -- RSI overbought
    SELECT ticker_id, fetched_at, 'rsi_overbought'  AS alert_type,
           rsi_14 AS metric_value, 70 AS threshold
    FROM indicators WHERE rsi_14 > 70

    UNION ALL

    -- RSI oversold
    SELECT ticker_id, fetched_at, 'rsi_oversold'    AS alert_type,
           rsi_14 AS metric_value, 30 AS threshold
    FROM indicators WHERE rsi_14 < 30

    UNION ALL

    -- Volume spike
    SELECT ticker_id, fetched_at, 'volume_spike'    AS alert_type,
           volume_zscore AS metric_value, 3 AS threshold
    FROM indicators WHERE volume_zscore > 3

    UNION ALL

    -- Bollinger upper breach
    SELECT ticker_id, fetched_at, 'bb_upper_breach' AS alert_type,
           NULL AS metric_value, NULL AS threshold
    FROM {{ ref('fact_prices') }} fp
    JOIN indicators i USING (ticker_id, fetched_at)
    WHERE fp.adj_close > i.bb_upper AND i.bb_upper IS NOT NULL

    UNION ALL

    -- Bollinger lower breach
    SELECT ticker_id, fetched_at, 'bb_lower_breach' AS alert_type,
           NULL AS metric_value, NULL AS threshold
    FROM {{ ref('fact_prices') }} fp
    JOIN indicators i USING (ticker_id, fetched_at)
    WHERE fp.adj_close < i.bb_lower AND i.bb_lower IS NOT NULL

    UNION ALL

    -- MACD bullish crossover (histogram goes from negative to positive)
    SELECT ticker_id, fetched_at, 'macd_bullish_cross' AS alert_type,
           macd_histogram AS metric_value, 0 AS threshold
    FROM indicators
    WHERE prev_macd_histogram < 0 AND macd_histogram > 0

    UNION ALL

    -- MACD bearish crossover
    SELECT ticker_id, fetched_at, 'macd_bearish_cross' AS alert_type,
           macd_histogram AS metric_value, 0 AS threshold
    FROM indicators
    WHERE prev_macd_histogram > 0 AND macd_histogram < 0

)

SELECT
    ticker_id,
    fetched_at,
    alert_type,
    ROUND(metric_value::NUMERIC, 4) AS metric_value,
    threshold::NUMERIC              AS threshold
FROM alerts
