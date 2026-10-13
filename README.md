# pit-market-data

A personal, local, batch pipeline for daily US equity/ETF prices that can answer
**"what did I know as of time T?"** — reproducibly.

Status: design (P0). Nothing below is implemented yet.

## Why point-in-time

Vendors revise history and corporate actions arrive late. A naive "latest
adjusted close" table silently rewrites the past, so a backtest or report run
last month can't be reproduced today. This project keeps every vendor response
immutable, stamps it with `ingested_at`, and computes adjustments at query time
from only what was known by `as_of`.

```
get_prices(symbols, start, end, as_of)
  -> uses only prices and corporate actions with ingested_at <= as_of
  -> derives adjustment factors on the fly from those rows
```

## Architecture

```
 Tiingo EOD API
      │  fetch (~50 symbols, daily)
      ▼
 raw/          source=tiingo/ingest_date=YYYY-MM-DD/<symbol>.json     immutable, as received
      │  parse + type + quality rules
      ├──────────────► quarantine (DuckDB)   failing rows + rule + run_id, never dropped
      ▼
 staged/       prices/ingest_date=YYYY-MM-DD/part.parquet             Polars, typed
               corporate_actions/ingest_date=YYYY-MM-DD/part.parquet
      │  dbt-duckdb
      ▼
 curated       adjusted series, security_master (SCD2 via dbt snapshot),
               dbt tests + source freshness
      │
      ▼
 query         DuckDB over Parquet: get_prices(..., as_of)

 pipeline_runs (DuckDB): run_id, partition, timings, rows in/out/quarantined, status
 Orchestration: plain Python CLI first, Airflow DAG (ingest → dbt run → dbt test) in P3
```

### Key decisions

| Decision | Why |
|---|---|
| Staged partitioned by `ingest_date`, not `trade_date` | One run's output is a pure function of that day's raw files, so reruns are byte-identical and revisions become new rows instead of overwrites. The natural key is `(symbol, trade_date, ingested_at)`; "current" is the latest `ingested_at` per `(symbol, trade_date)` at or before `as_of`. |
| Atomic writes: temp file → `rename` | A crash mid-write leaves the old partition intact; rerunning is always safe. |
| Compute adjustments, don't store vendor `adjClose` | Vendor-adjusted prices bake in today's knowledge. Raw close + split/dividend events → factors at query time. |
| Quarantine, never drop | Bad rows are evidence. Each rule writes rule name + detail + run_id. |
| Hand-written quality rules, not Great Expectations | ~6 rules; GE's setup cost outweighs it at this size. dbt tests cover the curated layer. |
| DuckDB for metadata | No extra service; Postgres adds nothing at this scale. |
| `exchange_calendars` for trading days | Don't maintain a holiday table by hand. |

### Quality rules

1. Schema and types
2. Unique `(symbol, trade_date)` within one ingest
3. OHLC invariants: `low <= open, close <= high`, `volume >= 0`
4. Missing trading days vs the exchange calendar
5. Suspected unrecorded split: price jump near an integer ratio with no matching corporate action
6. Freshness SLA: today's bar present by a cutoff time

Each rule has a passing and a failing test case.

## Data source

**Tiingo** (free tier). Its EOD endpoint returns raw OHLCV plus `splitFactor`
and `divCash`, so corporate actions arrive in the same response. Free-tier
limits fit ~50 symbols/day. Alpha Vantage's free daily quota is too small for
50 symbols; Stooq has no explicit corporate-action feed.

Vendor data is **not** committed or redistributed. The repo holds code and a
small synthetic sample only. The ingest layer depends on one function signature,
so the vendor is swappable.

## Stack

Python 3.12 · Polars · Parquet (Hive-style partitions, local FS — same layout as
an S3 data lake) · DuckDB · dbt-duckdb · Airflow · pytest

## Milestones

| Phase | Scope | Done when |
|---|---|---|
| P0 | Source choice, repo, this doc | README has architecture, scope, limitations |
| P1 | Ingest → raw → staged, one day, idempotent | Same partition run twice → identical hash; queryable in DuckDB |
| P2 | Quality rules + quarantine, corporate actions, adjustment | Each rule has +/- tests; hand-computed adjustment golden test passes |
| P3 | dbt curated layer + SCD2 security master; Airflow DAG; backfill; `pipeline_runs` | 1-year backfill succeeds; `dbt test` green |
| P4 | `as_of` queries + simulated bad-data drill | Same query differs across `as_of` values; drill written up, labelled simulated |

Stop after P4.

## Limitations

- Daily bars only. No intraday, tick, streaming or Kafka.
- Local only. No cloud deployment.
- **Point-in-time starts at first ingest.** A backfill of past years is stamped
  with the day it was fetched; the pipeline cannot know what the vendor said
  before it started watching. `as_of` earlier than the first ingest returns
  nothing rather than pretending.
- Survivorship: the universe is a fixed list chosen today. `security_master`
  has `valid_from/valid_to` for delistings and renames, but past delisted
  names are not reconstructed.
- No strategies, backtests, risk models, option chains or UI.
- Personal project; not a production system.
