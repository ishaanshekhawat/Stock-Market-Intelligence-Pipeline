# Stock Market Intelligence Pipeline

A production-grade data engineering pipeline that ingests hourly OHLCV price data for 7 US equities, computes technical indicators via SQL-based transformations, detects anomalies, and visualises signals on a live Metabase dashboard.

> **Stack:** Python · Apache Airflow 2.8 · PostgreSQL 15 · dbt 1.7 · Twelve Data API · Metabase

> **Infrastructure:** Oracle Cloud VM.Standard.A1.Flex (2 OCPU, 12 GB RAM)

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Why These Tools](#3-why-these-tools)
4. [Data Source & A Real Problem Encountered](#4-data-source--a-real-problem-encountered)
5. [Database Schema & Design Decisions](#5-database-schema--design-decisions)
6. [Airflow DAG Design](#6-airflow-dag-design)
7. [dbt Transformation Layer](#7-dbt-transformation-layer)
8. [Technical Indicators](#8-technical-indicators)
9. [Anomaly Detection](#9-anomaly-detection)
10. [Dashboard](#10-dashboard)
11. [Infrastructure & Cost Optimisation](#11-infrastructure--cost-optimisation)
12. [Pipeline Reliability Patterns](#12-pipeline-reliability-patterns)
13. [Design Tradeoffs](#13-design-tradeoffs)
14. [Failures & How They Were Resolved](#14-failures--how-they-were-resolved)
15. [Project Structure](#15-project-structure)

---

## 1. Project Overview

The original motivation for this project was a Reddit sentiment analysis pipeline, i.e., scraping subreddit posts hourly, running NLP on them, and storing results in Postgres. That plan failed immediately when I learned that Oracle Cloud's IP range is hard-blocked by Reddit's anti-bot systems, and no amount of header spoofing resolved it.

Therefore, I redesigned the project around a problem with more genuine business value: monitoring US equities for technical trading signals. The same core stack remained, i.e., Airflow for orchestration, Postgres for storage, hourly ingestion, dbt for transformation, but the data source became the Twelve Data API (free tier, legitimate, no IP blocks), and the analytical layer became financial signal detection instead of sentiment scoring.

The pipeline continuously ingests OHLCV bars for 7 stocks across Technology and Finance sectors, computes 8 technical indicators per ticker per hour, and populates a Metabase dashboard that auto-refreshes with the latest signals.

**Tickers monitored:** AAPL, MSFT, GOOGL, AMZN, META, NVDA, and JPM

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  SOURCE                                                         │
│  Twelve Data API                                                │
└───────────────────────────┬─────────────────────────────────────┘
                            │  hourly, market hours only
┌───────────────────────────▼─────────────────────────────────────┐
│  ORCHESTRATION: Apache Airflow             │                    │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  STORAGE: PostgreSQL                                            │
│  Schema: raw.*       → written by Airflow (ingestion layer)     │
│  Schema: public.*    → written by dbt   (transformation layer)  │
│                                                                 │
│            raw.stg_prices   +   raw.dim_tickers                 │
│                           ↓ dbt                                 │
│        public.fact_prices (range-partitioned by month)          │
│        public.fact_indicators                                   │
│        public.fact_alerts                                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  TRANSFORMATION: dbt                                            │
│  staging → intermediate → marts                                 │
│  8 indicators: SMA, EMA, RSI, MACD, Bollinger, Z-score,         │
│  Volatility, Daily Return                                       │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  VISUALISATION: Metabase (self-hosted, port 3000)              │
│  Market overview · RSI trends · Volume anomalies · Alert log    │
└─────────────────────────────────────────────────────────────────┘
```

All services run natively on an Oracle Cloud ARM VM

---

## 3. Why These Tools


**Airflow**: Airflow adds retry logic, task-level logging, dependency management between ingestion and transformation, and a UI to inspect historical runs. For a pipeline where one failing ticker shouldn't block nine others, Airflow's per-task parallelism and independent failure handling are necessary.

**dbt**: The transformation layer could have been written as Python functions in the Airflow DAGs directly. dbt was chosen because it enforces a clean separation between ingestion and transformation, produces a lineage graph showing data flow, and ships built-in test primitives (`not_null`, `unique`, `relationships`, custom range assertions). These make data quality issues visible.

**LocalExecutor**: LocalExecutor runs tasks as subprocesses and supports parallelism.

**Metabase**: Metabase requires zero configuration to build standard charts from SQL queries and doesn't require learning a new query language.

**Twelve Data over yfinance**: yfinance was the original choice. It was abandoned mid-project when Oracle Cloud's IP range was discovered to be hard-blocked by Yahoo Finance's bot detection. Twelve Data is a legitimate, documented API with a free tier of 800 calls/day that places no restrictions on cloud infrastructure IPs.

---

## 4. Data Source & A Real Problem Encountered

### Original plan: yfinance

The pipeline was initially built with yfinance, which requires no API key and wraps Yahoo Finance's internal endpoints. This worked in local testing but failed completely once deployed to Oracle Cloud.

### The failure

Oracle Cloud's IP ranges are flagged by Yahoo Finance's bot detection as sources of automated scraping traffic. Every request returned a 429.

### The fix: Twelve Data API

Twelve Data is a market data API with a legitimate free tier:

| Property | Value |
|---|---|
| Cost | Free |
| API key | Required (instant signup, no credit card) |
| Rate limit | 800 calls/day, 8 calls/minute |
| Intraday intervals (free) | 1h minimum (1min–45min are paid) |
| Historical depth | Up to 5000 bars |
| IP restrictions | None |

The move from yfinance to Twelve Data required changing only `fetch_ohlcv.py`. The Airflow DAGs, schema, and all dbt models were unchanged, because the interface contract (a DataFrame of OHLCV rows going into `raw.stg_prices`) stayed identical.

### Tradeoff introduced

The free plan's minimum intraday interval is 1 hour, not 15 minutes. The pipeline, therefore, ingests hourly bars rather than 15-minute bars. For signal detection (RSI, MACD crossovers, volume anomalies) over a multi-week horizon, this is an acceptable tradeoff. It would not be acceptable for intraday trading decisions, but this pipeline is not explicitly intended for that purpose.

### Rate limit management

With 7 tickers and an hourly schedule:

```
7 tickers × 1 call each × 6.5 market hours × 5 days = ~210 calls/week (considering 6 fetches for each ticker daily)
```

30 calls/day against an 800/day limit gives a comfortable 26× headroom. The pipeline also enforces an 8-second sleep between sequential ticker calls in manual/backfill mode to stay within the 8 calls/minute cap.

---

## 5. Database Schema & Design Decisions

### Schema separation: `raw` vs `public`

A critical design decision was separating raw ingested data from dbt-managed data using Postgres schemas.

```
raw.stg_prices      ← written by Airflow (Python ingestion)
raw.dim_tickers     ← created manually, read by ingestion
public.fact_prices  ← written by dbt, read by Metabase
public.fact_indicators
public.fact_alerts
```

**Why this matters:** If both the raw table and the dbt staging model are named `stg_prices` in the same schema, dbt attempts to create a view named `stg_prices` and immediately collides with the existing table. This was discovered as a real failure during development and corrected by moving raw tables into their own `raw` schema. This is also the correct convention in production data warehouses, and the raw landing zone is always kept separate from the transformation layer.

### Table design

**`raw.stg_prices`**: Raw landing table. Every Airflow ingest run writes here first. The unique constraint on `(ticker_id, fetched_at)` enables the idempotent upsert pattern. Contains `ingested_at` as a separate bookkeeping column distinct from `fetched_at` (the bar timestamp), allowing detection of late-arriving data.

**`public.fact_prices`**: Range-partitioned by month on `fetched_at`. Partition pruning means queries filtered by date range only scan the relevant monthly partition rather than the full table. Includes `daily_return` computed by dbt.

**`public.fact_indicators`**: One row per ticker per bar timestamp, with all 8 computed indicators as columns. Kept flat rather than normalised (one row per indicator per ticker) to make dashboard queries simple JOINs rather than pivots.

**`public.fact_alerts`**: Append-only log of every threshold breach.

### Indexes

Three targeted indexes were created:

```sql
-- Serves the watermark query run before every ingest (one per ticker per hour)
CREATE INDEX idx_stg_prices_ticker_time
    ON raw.stg_prices (ticker_id, fetched_at DESC);

-- Serves all dashboard queries (filter by ticker, order by time)
CREATE INDEX idx_fact_indicators_ticker_time
    ON public.fact_indicators (ticker_id, fetched_at DESC);
```

`fact_prices` is not separately indexed because it is range-partitioned, so it already eliminates irrelevant months before any scan. `dim_tickers.symbol` is not indexed because Postgres automatically creates an index to enforce the `UNIQUE` constraint.

---

## 6. Airflow DAG Design

### DAG: `fetch_market_data`

**Schedule:** `*/60 * * * 1-5` - top of every hour, Monday–Friday

**Task graph:**

```
check_market_open
      │
      ├── ingest_AAPL ──┐
      ├── ingest_MSFT   │
      ├── ingest_GOOGL  │ (parallel, max_active_tasks=5)
      ├── ...           │
      └── ingest_JPM  ──┘
                        │
               trigger_dbt_models
```

**`MarketOpenSensor`** - A custom `BaseSensorOperator` that checks NYSE trading hours (9:30am–4:00pm ET) and trading calendar before executing any fetch. Uses `mode="reschedule"` so it releases its worker slot while waiting rather than blocking it. Falls back to a simple weekday + time-window check if the `trading_calendars` library is unavailable.

**Per-ticker `PythonOperator`** - One task per ticker rather than one task that loops over all tickers. This means a Twelve Data timeout on NVDA doesn't block the AAPL task. Each task is independently retried up to 2 times. Failures are visible per-ticker in the Airflow UI.

**`catchup=False`** - Explicitly set to prevent Airflow from attempting to backfill every missed hour when the DAG is first enabled or restarted after downtime. Historical data is loaded via a separate manual backfill process.

**`max_active_runs=1`** - Prevents a slow run from overlapping with the next scheduled run.

### DAG: `run_dbt_models`

**Schedule:** `None` - triggered exclusively by `fetch_market_data` via `TriggerDagRunOperator`, not on its own schedule. This ensures transformations only run after new data exists, not on a fixed clock.

**Tasks:** `dbt_run` → `dbt_test`. If `dbt_test` fails (a data quality assertion is violated), the pipeline logs the failure but does not block the next ingestion cycle. Data quality issues are surfaced, not silently swallowed.

### DAG: `monthly_maintenance`

**Schedule:** `0 5 1 * *` - 5am on the first of every month. Creates the next month's `fact_prices` partition.

---

## 7. dbt Transformation Layer

### Model structure

```
models/
├── staging/
│   └── stg_prices.sql           -- view over raw.stg_prices, light validation
├── intermediate/
│   └── int_price_with_returns.sql  -- adds daily_return via LAG window function
└── marts/
    ├── fact_prices.sql           -- materialized table, promoted from intermediate
    ├── fact_indicators.sql       -- all 8 indicators computed via window functions
    └── fact_alerts.sql           -- threshold evaluation, one row per breach
```

Staging and intermediate models are materialised as **views** (no storage cost, always reflect the latest data). Mart models are materialised as **tables** (pre-computed, fast to query from Metabase).

### dbt source declarations

Raw tables are declared as dbt `sources` in `models/staging/sources.yml` rather than referenced directly. This means:
- dbt's lineage graph correctly shows `raw.stg_prices` as an external input, not a dbt-owned model
- `dbt source freshness` checks can be run to detect ingestion staleness

### Data quality tests

```yaml
# Runs after every dbt execution
- not_null on: ticker_id, fetched_at, adj_close (all mart models)
- unique on: (ticker_id, fetched_at) in fact_prices and fact_indicators
- accepted_values: alert_type must be one of the 7 defined types
- custom test: rsi_14 must be between 0 and 100 (returns rows on failure)
- relationships: fact_indicators.fetched_at must exist in fact_prices
```

---

## 8. Technical Indicators

All indicators are computed in `fact_indicators.sql` using Postgres window functions. No external libraries are called during transformation and everything runs in the database.

| Indicator | Computation | Periods | Business signal |
|---|---|---|---|
| SMA | `AVG(adj_close) OVER (... ROWS BETWEEN N-1 PRECEDING AND CURRENT ROW)` | 20, 50 | Trend direction; SMA-20/SMA-50 crossover = trend change signal |
| EMA | Weighted window approximation (see note above) | 12, 26 | Faster trend line; inputs to MACD |
| RSI | Avg gain / avg loss over 14 bars, scaled 0–100 | 14 | >70 overbought, <30 oversold |
| MACD | EMA-12 − EMA-26; signal = EMA-9 of MACD | 12, 26, 9 | Crossover direction signals trend reversals |
| Bollinger Bands | SMA-20 ± 2×stddev | 20 | Band width = volatility; price at band edge = extreme |
| Volume Z-score | `(volume − avg_vol) / stddev_vol` over 20 bars | 20 | Z > 3 = abnormal activity, likely news-driven |
| Volatility | `STDDEV(daily_return) OVER (... 20 bars)` | 20 | Rolling risk measure |
| Daily return | `(close − LAG(close)) / LAG(close)` | 1 | Percentage change per bar |

---

## 9. Anomaly Detection

`fact_alerts.sql` evaluates every row in `fact_indicators` against threshold conditions. A row is inserted for each breach, and there is no deduplication, so a stock can generate multiple consecutive `rsi_overbought` alerts if it stays above 70 across several bars. This is intentional: the alert table is a log, not a notification queue.

| Alert type | Trigger condition |
|---|---|
| `rsi_overbought` | `rsi_14 > 70` |
| `rsi_oversold` | `rsi_14 < 30` |
| `volume_spike` | `volume_zscore > 3` |
| `bb_upper_breach` | `adj_close > bb_upper` |
| `bb_lower_breach` | `adj_close < bb_lower` |
| `macd_bullish_cross` | MACD histogram goes from negative to positive |
| `macd_bearish_cross` | MACD histogram goes from positive to negative |

---

## 10. Dashboard

Metabase is self-hosted on the same VM (port 3000), connected directly to the `market_db` Postgres database. It runs as a systemd service and uses a separate `metabase_meta` Postgres database for its own internal state.

---

## 11. Infrastructure & Cost Optimisation

### Oracle Cloud Always Free tier

The entire project runs on Oracle Cloud's permanently free ARM infrastructure:

| Resource | Allocated | Used | Free limit |
|---|---|---|---|
| Compute | 2 OCPU, 12 GB RAM | ~900 MB RAM at rest | 4 OCPU / 24 GB total across all A1 instances |
| Block storage | ~15 GB (OS + data) | ~15 GB | 200 GB total |
| Network egress | ~50 MB/week | ~50 MB/week | 10 TB/month |
| Cost | **$0** | - | Permanently free |

---

## 12. Pipeline Reliability Patterns

### Idempotency

Every write uses `INSERT ... ON CONFLICT (ticker_id, fetched_at) DO UPDATE`. The composite unique key is the idempotency key. A DAG task can be retried any number of times without creating duplicate rows or corrupting the time series. This was essential given that Airflow retries failed tasks automatically.

### Watermark pattern

Before each Twelve Data API call, the pipeline queries:

```sql
SELECT MAX(fetched_at) FROM raw.stg_prices WHERE ticker_id = %s
```

Only bars after this timestamp are requested. This makes every run incremental. API calls are proportional to time elapsed since the last successful run, not total history size. On first run (no watermark), `outputsize=5000` is requested to load the maximum history.

### Late-arriving data

The upsert key is the bar's own timestamp (`fetched_at`), not the ingestion time (`ingested_at`). If a bar arrives delayed, for example, the 2:00 pm bar arriving at 2:10 pm, it upserts into the correct position in the time series without creating a duplicate or out-of-order record.

### Timezone handling bug (and fix)

During development, a `ValueError: Cannot pass a datetime or Timestamp with tzinfo with the tz parameter` was encountered in production. The root cause: Postgres `TIMESTAMPTZ` columns return timezone-aware Python datetimes. The original code passed this already-aware datetime into `pd.Timestamp(watermark, tz="UTC")`, which pandas rejects. The fix:

```python
watermark_ts = pd.Timestamp(watermark)
if watermark_ts.tzinfo is None:
    watermark_ts = watermark_ts.tz_localize("UTC")
else:
    watermark_ts = watermark_ts.tz_convert("UTC")
```

This defensively handles both aware and naive datetimes regardless of Postgres configuration.

### Backfill safety

`catchup=False` on all DAGs prevents Airflow from auto-backfilling missed runs.

---

## 13. Design Tradeoffs

### Batch (hourly) vs streaming

The pipeline is batch-based. Data is fetched, stored, and transformed in discrete hourly cycles. A streaming architecture would allow sub-minute latency but would require significantly more infrastructure, better data sources, and may not be sustainable on the free Oracle tier.

### Single VM vs distributed

All services run on one machine. A production system would separate the database server, Airflow scheduler, and webserver onto different machines for fault isolation. On a single VM, a crash of the OS affects everything simultaneously. The tradeoff was accepted for cost reasons, i.e., the Oracle free tier allows only one relevant VM configuration, and the project's scope does not require high availability. Even in a case where a VM crashes, the next time it is turned on, the next job run will automatically be able to recover data for at least the last 90 bars for that ticker.

### Staging schema (`raw`) vs dbt sources only

An alternative approach would be to skip the `raw.stg_prices` table entirely and have Airflow write directly to a format that dbt reads. The staging table was kept because it decouples ingestion from transformation, so if dbt has a bug, the raw data is still intact and can be reprocessed. The raw table also provides an audit trail: `ingested_at` records when data arrived, separate from `fetched_at` (the bar's timestamp), which helps diagnose API lag issues.

---

## 14. Failures & How They Were Resolved

### 1. Yahoo Finance IP block

**Problem:** Oracle Cloud IP ranges are flagged by Yahoo Finance's bot detection. 100% of requests failed once deployed, despite working locally.

**Resolution:** Replaced yfinance with Twelve Data API. Only `fetch_ohlcv.py` changed and the rest of the pipeline was unaffected because the interface contract (DataFrame of OHLCV rows) remained identical.

### 2. dbt staging model collision with raw table

**Problem:** Both the raw Postgres table and the dbt staging model were named `stg_prices` in the `public` schema. dbt's attempt to create a view named `stg_prices` collided with the existing table, causing:
```
Database Error: relation "stg_prices" does not exist
```

**Resolution:** Moved all raw tables to a separate `raw` schema (`raw.stg_prices`, `raw.dim_tickers`). dbt models remain in `public`. The staging model now reads from `{{ source('raw', 'stg_prices') }}`.

### 3. Pandas timezone error on watermark

**Problem:** In production, `ingest_NVDA` failed repeatedly with:
```
ValueError: Cannot pass a datetime or Timestamp with tzinfo with the tz parameter.
Use tz_convert instead.
```

**Root cause:** Postgres `TIMESTAMPTZ` returns timezone-aware Python datetimes. The code `pd.Timestamp(watermark, tz="UTC")` is invalid when `watermark` is already timezone-aware.

**Resolution:** Added a conditional check:
```python
watermark_ts = pd.Timestamp(watermark)
if watermark_ts.tzinfo is None:
    watermark_ts = watermark_ts.tz_localize("UTC")
else:
    watermark_ts = watermark_ts.tz_convert("UTC")
```

### 4. Deprecated Airflow operator import

**Problem:** `monthly_maintenance.py` used `from airflow.operators.postgres_operator import PostgresOperator`, which was removed in Airflow 2.x. This caused a DAG import error that crashed the Airflow webserver.

**Resolution:** Updated to the correct provider path:
```python
from airflow.providers.postgres.operators.postgres import PostgresOperator
```
And installed the required provider package: `pip install apache-airflow-providers-postgres`.

---

## 15. Project Structure

```
Stock-Market-Intelligence-Pipeline/
├── dags/
│   ├── fetch_market_data.py     # Main hourly ingest DAG
│   ├── run_dbt_models.py        # dbt transformation DAG (triggered, not scheduled)
│   ├── monthly_maintenance.py   # Creates next month's fact_prices partition
│   └── sensors.py               # MarketOpenSensor custom operator
├── ingestion/
│   ├── fetch_ohlcv.py           # Twelve Data fetch + Postgres upsert
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml             
│   └── models/
│       ├── staging/
│       │   └── stg_prices.sql
│       ├── intermediate/
│       │   └── int_price_with_returns.sql
│       └── marts/
│           ├── fact_prices.sql
│           ├── fact_indicators.sql
│           ├── fact_alerts.sql
│           └── schema.yml       # dbt test definitions
|   └── tests/
|       |── assert_rsi_valid_range.sql  # Custom dbt test: RSI must be 0–100
├── sql/
│   ├── schema.sql               # Full DDL: all tables, schemas, indexes
│   └── seed_tickers.sql         # Watchlist data for dim_tickers
├── airflow_home/
│   └── airflow.cfg              # Airflow configuration
├── requirements.txt
└── .gitignore                   # Includes venv/, .env, airflow_home/logs/
```

---

## Notes

This project is built for learning and portfolio demonstration. It is not intended for live trading decisions. Technical indicators computed here are lagged signals, and the data source has known limitations (see section 13).
