# Runbook: a dataset stops getting new data (stale snapshots or bars)

Based on real incidents: [2026-10-23](../incidents/2026-10-23.md) (late runs), [2026-10-26](../incidents/2026-10-26.md)
(a whole session lost).

## Symptoms

- `⏸️ 数据停滞：<dataset>已 15 分钟没有新数据`: the `watchdog_check` DAG (every 5 min, 09:00–16:55 on session days)
  found no successful run of an intraday dataset that *finished* in the last 15 minutes, inside its window:

  | Dataset | Expected | Source |
  |---|---|---|
  | `option_snapshots` | open → close | IBKR |
  | `index_snapshots` | open → close | IBKR |
  | `equity_bars` | open + 20 min → close + 30 min | Alpaca |

- An hourly progress card with a red dataset, or RTH coverage below what's due (`正规时段覆盖 x/y`).
- **No cards at all** on a session day (none at 08:00, 09:02, 09:28): the host, Docker or Airflow is down.

The stale alert fires once per episode. For option or index snapshots, the watchdog also runs Gateway self-healing
once and sends `数据停滞后的 Gateway 检查` with the outcome.

**Time matters for options:** a missed option snapshot can never be fetched later. Equity bars can: the 17:30 `eod`
run refetches the last 10 sessions.

## Triage

1. Which dataset, and since when? The card says when the last run finished.
2. Is a run stuck, or are runs failing?

   ```bash
   docker compose exec airflow airflow dags list-runs option_snapshots -o plain | head -5
   docker compose exec airflow airflow dags list-runs equity_bars -o plain | head -5
   ```

   A run `running` for longer than its execution timeout (8 min option chains, 2 min indices, 3 min equity bars) is stuck.
   A run of `failed` runs means each slot fails.

3. Read the latest errors from `pipeline_runs`:

   ```bash
   docker compose exec airflow python -c "
   import polars as pl, glob, datetime as d
   day = d.datetime.now(d.timezone.utc).date()
   fs = glob.glob(f'/opt/airflow/data/pipeline_runs/dataset=*/ingest_date={day}/*.parquet')
   df = pl.concat([pl.read_parquet(f, columns=['dataset','started_at','status','error']) for f in fs], how='diagonal')
   print(df.sort('started_at').tail(12))"
   ```

## By cause

| What you see | Cause | Do |
|---|---|---|
| Options **and** indices failing, `TimeoutError()` after ~4 s | Gateway API not accepting connections | [gateway.md](gateway.md), section A |
| Options failing, `every chain failed ... TimeoutError()` after ~6 min | One request hung for the whole run budget | [gateway.md](gateway.md), sections A–B |
| `no spot price; sent RECONNECTDATA` | Underlying quote missing (farm down or not subscribed) | [gateway.md](gateway.md), sections C–D |
| Some chains in `chain_errors` with `skipped: run budget used up` | Run too slow for the 6-min budget | Look at run times in the progress card; trim chains or cadence in `config/ibkr.toml` |
| Only `equity_bars` stale | Alpaca: API down, keys, or the free plan's 15-min lag | `docker compose exec airflow python -c "from pit.health import check_alpaca; print(check_alpaca())"` |
| Everything stale, no cards | Host / Docker / Airflow down | `docker compose ps`; `docker compose up -d`; check the machine woke from sleep |

## Recover

- IBKR datasets: fix the Gateway. Collection resumes at the next 5-min slot by itself (snapshot tasks have no retries:
  the next slot is the retry).
- Equity bars: nothing to backfill by hand for the last 10 sessions; `eod` refetches them. For older gaps:
  `docker compose exec airflow python -m pit.pipeline backfill /opt/airflow/config/alpaca_bars.toml <start> <end>`.
- A stuck run: it hits its execution timeout and fails on its own. To end it sooner, mark it failed in the Airflow UI
  (`127.0.0.1:8080`).

## Verify

- The next run succeeds, and the failing task sends `✅ 已恢复`.
- The next hourly progress card shows the dataset green.

## Record

Whole sessions or long gaps of option snapshots are permanent. Write them up in `ops/incidents/<date>.md`
(impact, timeline, cause, actions), as for 2026-10-26.
