# pit-market-data

A personal, local, batch pipeline for US equity/ETF 15-minute bars and option
chain snapshots that can answer **"what did I know as of time T?"** —
reproducibly.

Status: design (P0). Nothing below is implemented yet.

## Why point-in-time

Vendors revise history and corporate actions arrive late. A naive "latest
adjusted close" table silently rewrites the past, so a backtest or report run
last month can't be reproduced today. This project keeps every vendor response
immutable, stamps it with `ingested_at`, and computes adjustments at query time
from only what was known by `as_of`.

```
get_bars(symbols, start, end, as_of)
  -> uses only bars and corporate actions with ingested_at <= as_of
  -> derives adjustment factors on the fly from those rows

get_chain(underlying, at)
  -> the latest option snapshot taken at or before `at`
     (snapshots are never revised, so snapshot time is knowledge time)
```

Option chains have no free history, so snapshots collected from day one are a
dataset that can't be bought back later.

## Data

| Dataset | Source | Grain | Universe | History |
|---|---|---|---|---|
| Equity/ETF bars | Alpaca, free plan | 15-min OHLCV, unadjusted | 50 symbols in `config/alpaca_bars.toml` | Backfillable to 2016 |
| Daily bars | Alpaca, free plan | Daily OHLCV, unadjusted | Same | Reconciliation only (rule 12) |
| Corporate actions | Alpaca | Event | Same | Backfillable |
| Option chain snapshots | IBKR, OPRA subscription | Every 15 min in market hours | Per underlying in `config/ibkr_options.toml`: trading class, nearest N expiries, strikes within ±X% of spot at each snapshot (SPX example: SPXW, 3 expiries, ±3%) | **From first run; no backfill** |

**Alpaca** free plan serves SIP (all-exchange) historical bars except the latest
15 minutes, at 200 calls/min. Its free options feed is indicative
(model-derived), so it is not used for options.

**IBKR** gives real OPRA quotes plus IBKR-computed IV/greeks. It needs IB
Gateway running and logged in — daily restarts and periodic 2FA make the
options pipeline not fully unattended — and market-data line limits force
batched snapshots. P1 must confirm a full cycle fits well inside 15 minutes;
the universe shrinks if not. The Gateway is logged into a **paper account**
with market-data sharing and **Read-Only API**, so the pipeline cannot touch
the live account.

Each dataset sits behind one fetch function, so a source can be swapped.

Considered and rejected: Yahoo Finance (unofficial scraping, short intraday
history), Tiingo (free intraday is IEX-only), Massive (bid/ask only on the
$199/mo plan; OPRA via IBKR is already paid for).

Vendor data is **not** committed or redistributed. The repo holds code and a
small synthetic sample only.

### Option snapshot fields

bid, ask, last, volume, open interest, IBKR model IV and greeks, underlying
price, contract identifiers (incl. `multiplier`, `tradingClass`), request time,
IBKR tick time, and a derived `quote_status`:

| `quote_status` | Condition | Typical case |
|---|---|---|
| `two_sided` | bid > 0 and ask > 0 | Normal contract |
| `ask_only` | bid missing or 0, ask > 0 | Deep OTM: MM offers, nobody bids |
| `bid_only` | bid > 0, ask missing or 0 | Rare one-sided bid |
| `no_quote` | neither side | No market at all |
| `crossed` | bid > ask | Quarantined (rule 4) |

Missing quote fields (IBKR's -1/NaN) are stored as null, never 0. Greeks may
arrive without a quote; they are model values, and `quote_status` tells them
apart. The list of contracts requested is stored with each snapshot.

## Architecture

```
 Alpaca REST                                  IB Gateway container (paper, read-only)
   bars, corporate actions                        option snapshots
        │                                              │
        ▼                                              ▼
 raw/        source=<alpaca|ibkr>/ingest_date=YYYY-MM-DD/<dataset>/<ingested_at>.json
             one file per fetch, never overwritten
        │  parse + type + row rules (Polars)
        ├────────► quarantine/      error rows + rule + run_id
        ▼
 staged/     <dataset>/ingest_date=YYYY-MM-DD/<ingested_at>.parquet   (1:1 with raw; warn rows kept, flagged)
        │  dbt-duckdb (eod only)
        ▼
 curated     adjusted bars, chain views, security_master (SCD2), dbt tests
        │
        ▼
 query       DuckDB: get_bars(..., as_of), get_chain(...)

 pipeline_runs/   one file per run: run_id, dataset, interval, timings,
                  rows in/out/quarantined, partition checks, status
```

Everything on disk is Parquet. Ingest only appends files; dbt in `eod` is the
only writer of the `.duckdb` file, because DuckDB allows one writing process
and both ingest DAGs fire at the same moment.

### Airflow

| DAG | Schedule | Tasks |
|---|---|---|
| `equity_bars` | Custom Timetable: every 15 min in NYSE sessions (`exchange_calendars`: holidays, half days, DST), from open+30 min to close+30 min | fetch the session's completed bars so far → raw → staged |
| `option_snapshots` | Same Timetable | Gateway health sensor → batched snapshots → raw → staged |
| `eod` | Same Timetable, once at session close | corporate actions + daily bars → refetch last 10 trading days of 15-min bars → `dbt build` (runs tests) |

Ingest DAGs declare their staged datasets as Assets for lineage.
`equity_bars` supports catchup/backfill; `option_snapshots` does not — a
missed snapshot is recorded in `pipeline_runs` as missed, never imputed.

### Key decisions

| Decision | Why |
|---|---|
| Raw: one file per fetch, named by `ingested_at` (UTC) | Truly immutable. A rerun re-parses its existing raw file; only an explicit refetch creates a new file with a new `ingested_at`. |
| Staged partitioned by `ingest_date`, not `trade_date` | Staged is a pure function of raw, so reruns give identical data and revisions become new rows, not overwrites. Bar key `(symbol, bar_start, ingested_at)`; "current" = latest `ingested_at` per `(symbol, bar_start)` at or before `as_of`. |
| Refetch to capture revisions: every `equity_bars` run refetches today so far; `eod` refetches 10 days | Without refetching, vendor revisions are never seen and point-in-time is only nominal. |
| Bars lag one run | The free plan withholds the latest 15 min, so a run fetches bars ending ≥ 15 min ago. Bar timestamps are bar start, UTC. |
| Atomic writes: temp file → `rename` | A crash mid-write leaves the old partition intact; rerunning is always safe. |
| Compute adjustments; store unadjusted bars | Vendor-adjusted prices bake in today's knowledge. |
| `security_master` SCD2 keyed on `ingested_at` | dbt snapshot's default validity time is run time, wrong on backfill/replay. Use the `check` strategy with an explicit timestamp, or a hand-written model. |

## Quality rules

**Keep everything, filter downstream.** Missing quotes and extreme moves are
real market states — a 5% index day or a +200% option is data, not an error.
No rule rejects on move size, and ingest never imputes. Curated models decide
what to use (e.g. IV surfaces exclude zero-bid quotes, as Cboe's VIX method
does).

Two severities, mirroring dbt: `error` → row moved to quarantine (kept as
evidence, never deleted); `warn` → row stays in staged with a flag. Only
logically impossible rows are errors. Rules are hand-written (~10 don't justify
Great Expectations); each has a passing and a failing test.

Row rules, `error`:

1. Schema and types
2. Unique `(symbol, bar_start)` / `(contract, snapshot_time)` within one fetch
3. Bar OHLC invariants: `low <= open, close <= high`, `volume >= 0`
4. Option quotes: not `crossed`; expiry not in the past; IV not negative

Row rules, `warn`:

5. Suspected unrecorded split (equity bars): jump near an integer ratio, no matching corporate action, no reversion
6. Suspected bad tick (equity bars): single-bar spike that reverts next bar on low volume
7. Non-standard option contract (adjusted deliverable or multiplier ≠ 100)
8. IV missing on a `two_sided` quote

Partition checks (→ `pipeline_runs`):

9. Missing 15-min bars vs the session calendar (`warn`: halts/LULD cause real gaps)
10. Freshness: latest bar/snapshot within SLA of the interval end
11. Snapshot breakdown: count per `quote_status`, IBKR error codes, and cycle time vs the 15-min budget

dbt tests (curated):

12. 15-min bars aggregated to daily match Alpaca daily bars (high, low, volume)
13. Cross-source: IBKR underlying price at snapshot time lies within the matching Alpaca bar's `[low, high]` (stocks/ETFs only; Alpaca has no index data, so SPX-style underlyings have no second source)

## Stack

| Layer | Choice |
|---|---|
| Language / env | Python 3.12, uv (Airflow installed with its official constraints file) |
| Orchestration | Airflow 3, LocalExecutor |
| Runtime | Docker Compose: Postgres (Airflow metadata), Airflow (`standalone`, image extended from `apache/airflow`), IB Gateway. Postgres lets both ingest DAGs run in parallel (SQLite would not). `src/`, `dags/`, `config/`, `data/` are bind-mounted. |
| Data dir | `./data` bind-mounted, so Parquet stays on the host and is queryable locally |
| IB Gateway | `gnzsnz/ib-gateway` image (Gateway + IBC: daily auto-restart; VNC on localhost for 2FA/UI). Gateway, not TWS: no trading UI, lighter, built for API use |
| Alpaca client | httpx, not the SDK — raw stores the response as received |
| IBKR client | ib_async (maintained ib_insync fork); raw = received fields serialized to JSON |
| Transform | Polars |
| Storage | Parquet, Hive-style partitions on local FS (same layout as an S3 data lake) |
| Query / curated | DuckDB, dbt-duckdb |
| Trading calendar | exchange_calendars (Timetable + rule 9) |
| Config | `universe.toml` via stdlib `tomllib`; API keys in env vars |
| Retries | Airflow task retries |
| Test / lint | pytest, ruff — run locally with uv, no container rebuild |
| CI | GitHub Actions: pytest + `dbt build` on the synthetic sample (P3) |

## Milestones

| Phase | Scope | Done when |
|---|---|---|
| P0 | Sources, repo, this doc | This README |
| P1 | Docker Compose up; ingest → raw → staged for both sources, idempotent; `pipeline_runs`; IBKR cycle-time check; ingest DAGs on cron + market-open check | Same run twice → identical staged data (hash of sorted rows, not file bytes); queryable in DuckDB; option snapshots collecting |
| P2 | Row rules + quarantine, partition checks, corporate actions, adjustment | Each rule has +/- tests; hand-computed adjustment golden test passes |
| P3 | Market-hours Timetable (replaces cron), `eod` DAG; dbt curated layer, SCD2, rules 12–13; backfill; CI | 1-year equity backfill succeeds; a full week unattended except Gateway re-auth; `dbt build` green |
| P4 | `as_of` queries + simulated bad-data drill | Same query differs across `as_of` values; drill written up, labelled simulated |

Option snapshots start in P1 and keep running, so history accumulates while the
rest is built. Stop after P4.

## Limitations

- **Point-in-time starts at first ingest.** A backfill of past years is stamped
  with the day it was fetched; `as_of` earlier than the first ingest returns
  nothing rather than pretending.
- **Option history starts at the first snapshot** and has permanent gaps
  wherever the Gateway was down. Gaps are recorded, not filled.
- IV and greeks are IBKR's model values, stored as received, not recomputed.
- Survivorship: universes are fixed lists chosen today. `security_master` has
  `valid_from/valid_to` for delistings and renames, but past delisted names are
  not reconstructed.
- 15-min granularity only. No tick data, streaming, Kafka or Spark.
- Local only; no cloud. No strategies, backtests, risk models or UI.
- Personal project; not a production system.
