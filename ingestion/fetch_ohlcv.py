"""
fetch_ohlcv.py
--------------
Fetches OHLCV bars from Twelve Data for a single ticker and upserts
them into stg_prices.
"""

import os
import time
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import psycopg2
import pandas as pd
from twelvedata import TDClient
from twelvedata.exceptions import (
    TwelveDataError,
    BadRequestError,
    InternalServerError,
)

load_dotenv(os.path.expanduser(".env"))

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

# Twelve Data free plan: 8 calls/minute hard limit.
# We sleep briefly between calls when running multiple tickers sequentially to stay safe. Airflow's max_active_tasks=5 handles the parallel cap.
RATE_LIMIT_SLEEP_SECONDS = 8

# How many bars to fetch on the very first run.
INITIAL_OUTPUTSIZE = 500

# Interval used for all intraday fetches.
INTERVAL = "1h"


# ── Database helpers ───────────────────────────────────────────────────────────

def get_db_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "market_db"),
        user=os.getenv("DB_USER", "pipeline"),
        password=os.getenv("DB_PASSWORD"),
    )


def get_ticker_id(conn: psycopg2.extensions.connection, symbol: str) -> int:
    """
    Looks up the integer ticker_id for a symbol in dim_tickers.
    Raises ValueError if the symbol is not seeded — catch this early so the error message is clear rather than a FK violation later.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker_id FROM raw.dim_tickers WHERE symbol = %s AND is_active = TRUE",
            (symbol,)
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"Ticker '{symbol}' not found in dim_tickers. "
                f"Run sql/seed_tickers.sql first."
            )
        return row[0]


def get_watermark(
    conn: psycopg2.extensions.connection,
    ticker_id: int
) -> datetime | None:
    """
    Returns the most recent fetched_at already stored for this ticker.
    On the first run (empty table) returns None.

    This is the watermark pattern: instead of re-fetching all history
    on every DAG run, we only ask Twelve Data for bars AFTER this point.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(fetched_at) FROM raw.stg_prices WHERE ticker_id = %s",
            (ticker_id,)
        )
        row = cur.fetchone()
        return row[0]  # None if no rows exist yet for this ticker


# ── Twelve Data fetch ──────────────────────────────────────────────────────────

def fetch_bars(symbol: str, watermark: datetime | None) -> pd.DataFrame:
    """
    Calls Twelve Data to retrieve OHLCV bars for `symbol`.

    Behaviour:
    - First run (watermark=None): fetches INITIAL_OUTPUTSIZE bars (maximum history).
    - Subsequent runs: fetches only bars after the watermark timestamp, using outputsize=90 (last 90 bars) which is enough to cover any reasonable gap between hourly DAG runs.

    Returns a clean DataFrame with columns:
        fetched_at (UTC-aware datetime), open, high, low, close, adj_close, volume

    Raises on Twelve Data API errors so Airflow marks the task as failed and retries according to the DAG's retry policy.
    """
    api_key = os.getenv("TWELVEDATA_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "TWELVEDATA_API_KEY is not set in .env. "
            "Get a free key at https://twelvedata.com"
        )

    td = TDClient(apikey=api_key)

    # On first run, pull maximum history. On subsequent runs, pull a small recent window — enough to cover gaps + a bit of overlap.
    outputsize = INITIAL_OUTPUTSIZE if watermark is None else 90

    logger.info(
        f"{symbol}: calling Twelve Data — interval={INTERVAL}, "
        f"outputsize={outputsize}, watermark={watermark}"
    )

    try:
        ts = (
            td.time_series(
                symbol=symbol,
                interval=INTERVAL,
                outputsize=outputsize,
                timezone="UTC",
                order="ASC",        # oldest first — matches our upsert order
            )
            .as_pandas()
        )
    except BadRequestError as e:
        # Usually means the symbol is invalid or not available on free plan
        raise ValueError(f"{symbol}: Twelve Data bad request — {e}") from e
    except InternalServerError as e:
        # Transient server error — Airflow will retry
        raise RuntimeError(f"{symbol}: Twelve Data server error — {e}") from e
    except TwelveDataError as e:
        raise RuntimeError(f"{symbol}: Twelve Data error — {e}") from e

    if ts is None or ts.empty:
        logger.warning(f"{symbol}: Twelve Data returned an empty response.")
        return pd.DataFrame()

    # ── Normalise the DataFrame ──────────────────────────────────────────────

    # Twelve Data returns the timestamp as the index, named "datetime"
    ts = ts.reset_index()

    # Rename to match our schema columns
    ts = ts.rename(columns={"datetime": "fetched_at"})

    # Ensure fetched_at is UTC-aware. Twelve Data returns UTC when timezone="UTC" is set, but defensively handle both cases.
    if ts["fetched_at"].dt.tz is None:
        ts["fetched_at"] = ts["fetched_at"].dt.tz_localize("UTC")
    else:
        ts["fetched_at"] = ts["fetched_at"].dt.tz_convert("UTC")

    # Cast all price/volume columns to correct Python types.
    # Twelve Data returns strings for numeric columns — always cast explicitly.
    for col in ["open", "high", "low", "close"]:
        ts[col] = pd.to_numeric(ts[col], errors="coerce")

    ts["volume"] = pd.to_numeric(ts["volume"], errors="coerce").fillna(0).astype(int)

    # Twelve Data does not return a separate adjusted close on the free plan.
    # We set adj_close = close.
    ts["adj_close"] = ts["close"]

    # Drop rows where the price came back null (malformed API response)
    before = len(ts)
    ts = ts.dropna(subset=["open", "high", "low", "close"])
    dropped = before - len(ts)
    if dropped > 0:
        logger.warning(f"{symbol}: dropped {dropped} rows with null price values.")

    # ── Apply watermark filter ───────────────────────────────────────────────
    # Even on incremental runs we fetch outputsize=90 bars (which overlaps with already-stored data) and filter here. The upsert handles any remaining overlap gracefully via ON CONFLICT DO UPDATE.
    if watermark is not None:
        watermark_ts = pd.Timestamp(watermark)
        if watermark_ts.tzinfo is None:
            watermark_ts = watermark_ts.tz_localize("UTC")
        else:
            watermark_ts = watermark_ts.tz_convert("UTC")
        ts = ts[ts["fetched_at"] > watermark_ts]

    logger.info(f"{symbol}: {len(ts)} new bars to upsert after watermark filter.")

    return ts[["fetched_at", "open", "high", "low", "close", "adj_close", "volume"]]


# ── Postgres upsert ────────────────────────────────────────────────────────────

def upsert_bars(
    conn: psycopg2.extensions.connection,
    ticker_id: int,
    df: pd.DataFrame
) -> int:
    """
    Upserts rows into stg_prices.

    ON CONFLICT DO UPDATE makes this idempotent:
    - Safe to retry on Airflow task failure without creating duplicates.
    - Also handles the overlap between watermark-based fetches and
      already-stored bars gracefully.

    Returns the number of rows processed.
    """
    if df.empty:
        return 0

    upsert_sql = """
        INSERT INTO raw.stg_prices
            (ticker_id, fetched_at, open, high, low, close, adj_close, volume)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker_id, fetched_at) DO UPDATE SET
            open        = EXCLUDED.open,
            high        = EXCLUDED.high,
            low         = EXCLUDED.low,
            close       = EXCLUDED.close,
            adj_close   = EXCLUDED.adj_close,
            volume      = EXCLUDED.volume,
            ingested_at = NOW()
    """

    rows = []
    for row in df.itertuples(index=False):
        rows.append((
            ticker_id,
            row.fetched_at.to_pydatetime(),
            float(row.open),
            float(row.high),
            float(row.low),
            float(row.close),
            float(row.adj_close),
            int(row.volume),
        ))

    with conn.cursor() as cur:
        cur.executemany(upsert_sql, rows)

    conn.commit()
    return len(rows)


# ── Main entry point ───────────────────────────────────────────────────────────

def ingest_ticker(symbol: str) -> dict:
    """
    Orchestrates the full ingest cycle for one ticker:
        1. Get watermark from DB
        2. Fetch bars from Twelve Data
        3. Upsert into stg_prices
        4. Return summary dict (logged by Airflow via XCom)

    Called by each PythonOperator task in the fetch_market_data DAG.
    One call per ticker — Airflow runs them in parallel up to max_active_tasks.

    On any exception: rolls back the DB transaction, re-raises so Airflow marks the task as failed and triggers the retry policy.
    """
    logger.info(f"[{symbol}] Starting ingest cycle.")
    conn = get_db_connection()

    try:
        ticker_id = get_ticker_id(conn, symbol)
        watermark = get_watermark(conn, ticker_id)

        logger.info(f"[{symbol}] ticker_id={ticker_id}, watermark={watermark}")

        df = fetch_bars(symbol, watermark)

        rows_upserted = upsert_bars(conn, ticker_id, df)

        summary = {
            "symbol":        symbol,
            "ticker_id":     ticker_id,
            "rows_upserted": rows_upserted,
            "watermark_was": str(watermark),
            "new_high_ts":   str(df["fetched_at"].max()) if not df.empty else None,
            "status":        "success",
        }

        logger.info(f"[{symbol}] Done — {rows_upserted} rows upserted.")
        return summary

    except Exception as e:
        conn.rollback()
        logger.error(f"[{symbol}] Ingest failed: {e}", exc_info=True)
        raise  # re-raise so Airflow retries

    finally:
        conn.close()


# ── CLI usage ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Manual test / one-off ingest
    """
    import sys
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    symbols = sys.argv[1:] if len(sys.argv) > 1 else ["AAPL"]

    for i, sym in enumerate(symbols):
        try:
            result = ingest_ticker(sym)
            print(json.dumps(result, indent=2))
        except Exception as exc:
            print(f"ERROR [{sym}]: {exc}")

        # Sleep between tickers when running manually to respect rate limits
        if i < len(symbols) - 1:
            logger.info(f"Waiting {RATE_LIMIT_SLEEP_SECONDS}s before next ticker...")
            time.sleep(RATE_LIMIT_SLEEP_SECONDS)
