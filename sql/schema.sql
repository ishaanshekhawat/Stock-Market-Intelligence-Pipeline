CREATE SCHEMA IF NOT EXISTS raw;

-- ─── dim_tickers ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.dim_tickers (
    ticker_id     SERIAL PRIMARY KEY,
    symbol        VARCHAR(10)  NOT NULL UNIQUE,
    company_name  VARCHAR(255) NOT NULL,
    exchange      VARCHAR(20)  NOT NULL,
    sector        VARCHAR(100),
    industry      VARCHAR(100),
    market_cap    BIGINT,
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ─── stg_prices (raw landing table) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw.stg_prices (
    id            BIGSERIAL PRIMARY KEY,
    ticker_id     INT         NOT NULL REFERENCES raw.dim_tickers(ticker_id),
    fetched_at    TIMESTAMPTZ NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    open          NUMERIC(12,4),
    high          NUMERIC(12,4),
    low           NUMERIC(12,4),
    close         NUMERIC(12,4),
    adj_close     NUMERIC(12,4),
    volume        BIGINT,
    UNIQUE (ticker_id, fetched_at)
);

-- ─── fact_prices (partitioned, promoted from staging) ──────────────────────
CREATE TABLE IF NOT EXISTS fact_prices (
    ticker_id     INT         NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL,
    open          NUMERIC(12,4),
    high          NUMERIC(12,4),
    low           NUMERIC(12,4),
    close         NUMERIC(12,4),
    adj_close     NUMERIC(12,4),
    volume        BIGINT,
    daily_return  NUMERIC(10,6),
    PRIMARY KEY (ticker_id, fetched_at)
) PARTITION BY RANGE (fetched_at);

-- Create partitions for current and next two months
DO $$
DECLARE
    month_start DATE;
    month_end   DATE;
    tbl_name    TEXT;
BEGIN
    FOR i IN 0..2 LOOP
        month_start := DATE_TRUNC('month', NOW() + (i || ' months')::INTERVAL)::DATE;
        month_end   := (month_start + INTERVAL '1 month')::DATE;
        tbl_name    := 'fact_prices_' || TO_CHAR(month_start, 'YYYY_MM');

        IF NOT EXISTS (
            SELECT 1 FROM pg_tables WHERE tablename = tbl_name
        ) THEN
            EXECUTE FORMAT(
                'CREATE TABLE %I PARTITION OF fact_prices FOR VALUES FROM (%L) TO (%L)',
                tbl_name, month_start, month_end
            );
            RAISE NOTICE 'Created partition: %', tbl_name;
        END IF;
    END LOOP;
END $$;

-- ─── fact_indicators ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_indicators (
    ticker_id       INT         NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    sma_20          NUMERIC(12,4),
    sma_50          NUMERIC(12,4),
    ema_12          NUMERIC(12,4),
    ema_26          NUMERIC(12,4),
    rsi_14          NUMERIC(6,2),
    macd_value      NUMERIC(10,4),
    macd_signal     NUMERIC(10,4),
    macd_histogram  NUMERIC(10,4),
    bb_upper        NUMERIC(12,4),
    bb_middle       NUMERIC(12,4),
    bb_lower        NUMERIC(12,4),
    volume_zscore   NUMERIC(8,4),
    volatility_20   NUMERIC(10,6),
    PRIMARY KEY (ticker_id, fetched_at)
);

-- ─── fact_alerts ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_alerts (
    alert_id      BIGSERIAL PRIMARY KEY,
    ticker_id     INT         NOT NULL REFERENCES raw.dim_tickers(ticker_id),
    alert_type    VARCHAR(50) NOT NULL,
    triggered_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metric_value  NUMERIC(12,4),
    threshold     NUMERIC(12,4),
    notified      BOOLEAN     NOT NULL DEFAULT FALSE,
    notified_at   TIMESTAMPTZ
);

-- ─── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_stg_prices_ticker_time
    ON raw.stg_prices (ticker_id, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_fact_indicators_ticker_time
    ON fact_indicators (ticker_id, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_fact_alerts_notified
    ON fact_alerts (notified, triggered_at DESC);
