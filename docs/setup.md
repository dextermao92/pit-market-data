# Setup

Two ways in:

- [**Try it locally**](#try-it-locally): no accounts, no Docker. Builds a small synthetic dataset, runs the whole
  transform and query path on it, and replays the Copilot's recorded evals. About two minutes.
- [**Run the collector**](#run-the-collector): your own IBKR and Alpaca accounts, Docker Compose on an always-on
  machine.

Both need [uv](https://docs.astral.sh/uv/) and git. uv installs Python 3.12 and every dependency from `uv.lock`.

## Try it locally

```bash
git clone https://github.com/dextermao92/pit-market-data.git
cd pit-market-data
uv sync
```

**1. Build a synthetic dataset.** One session of 5-min bars for two symbols, daily bars, corporate actions, asset
snapshots and an option chain snapshot, written as raw vendor responses and then staged by the real staging code:

```bash
uv run python -m pit.sample sample_data
```

**2. Build the curated layer and run the data tests**, including the cross-source checks:

```bash
DATA_DIR=sample_data uv run dbt build --project-dir dbt --profiles-dir dbt
```

On Windows PowerShell, set the variable first: `$env:DATA_DIR = "sample_data"`.

**3. Ask point-in-time questions.** The sample was fetched at 2026-10-14 21:00 UTC, so a question asked before that
moment gets nothing back:

```python
# uv run python
from datetime import datetime, UTC
from pathlib import Path
from pit.query import get_bars, get_chain

d = Path("sample_data")
start, end = datetime(2026, 10, 14, 13, 30, tzinfo=UTC), datetime(2026, 10, 14, 14, 0, tzinfo=UTC)
get_bars(["SPY"], start, end, as_of=datetime(2026, 10, 15, tzinfo=UTC), data_dir=d)      # 6 bars
get_bars(["SPY"], start, end, as_of=datetime(2026, 10, 14, 20, tzinfo=UTC), data_dir=d)  # empty: not fetched yet
get_chain("SPY", datetime(2026, 10, 14, 15, tzinfo=UTC), data_dir=d)                     # the 10:30 ET snapshot, 6 contracts
```

**4. Replay the Copilot evals.** 25 diagnosis cases and 5 prompt-injection cases, scored from recorded model
responses, so no API key is needed:

```bash
uv run python -m pit.copilot.eval --model deepseek-flash --memory loo --replay
uv run python -m pit.copilot.eval --model deepseek-flash --memory loo --replay --kinds injection
```

**5. Run the tests and lint**, as CI does:

```bash
uv run pytest -q
uv run ruff check src tests dags scripts plugins
```

## Run the collector

### What you need

| | Why | Notes |
|---|---|---|
| An always-on machine with Docker | Airflow, Postgres and IB Gateway run in Docker Compose | Tested on macOS and on Windows 11 (Docker Desktop on WSL 2). The images are multi-arch |
| IBKR account with an OPRA subscription | Option quotes, IV and greeks | Either a **paper** account with market-data sharing turned on (simplest; but your own live logins then compete for the data, IBKR code 10197), or, as this deployment does, a **separate user** on the account with its own market-data subscriptions (they are per user). Either way the compose file sets the API to read-only |
| A second IBKR username for the collector (recommended) | Two sessions on one username kick each other off | Without one, logging into TWS or the mobile app on the same user disconnects the Gateway |
| Alpaca account (free) | 5-min and daily bars, corporate actions, asset attributes | Paper keys are enough: same market data, no access to money |
| Discord webhook or SMTP account | Status cards and alerts | Optional, but monitoring is silent without one |
| DeepSeek API key | The Ops Copilot | Optional; collection doesn't need it |

### Steps

1. **Configure.** Copy `.env.example` to `.env` and fill it in. `.env` is gitignored; move it between machines by
   hand, never through chat or email.
2. **Choose what to collect.** Equity symbols are in [`config/alpaca_bars.toml`](../config/alpaca_bars.toml). Option
   chains and index levels are `[[chains]]` and `[[indices]]` blocks in [`config/ibkr.toml`](../config/ibkr.toml):
   underlying, DTE window, strike range and spacing, and how often (`every_minutes`, `offset_minutes`). One Gateway
   connection does about 18 contracts/s on 100 market-data lines, and a run has a 6-minute budget, so size the
   chains to fit.
3. **Start.**

   ```bash
   docker compose up -d --build
   ```

   Approve the IBKR 2FA prompt on your phone. With `VNC_SERVER_PASSWORD` set, the Gateway UI is on
   `127.0.0.1:5900`. Airflow is on <http://127.0.0.1:8080>; the generated admin password is in
   `docker compose logs airflow`.
4. **Unpause the DAGs.** A fresh Airflow database pauses new DAGs:

   ```bash
   for d in option_snapshots equity_bars eod contract_cache watchdog_check gateway_precheck gateway_check warmup_check progress_check; do
     docker compose exec airflow airflow dags unpause $d
   done
   ```

5. **Check.** This should post a green card to your channel:

   ```bash
   docker compose exec airflow python -m pit.health gateway --send
   ```

6. **Backfill equity history** (optional; option snapshots can't be backfilled):

   ```bash
   docker compose exec airflow python -m pit.pipeline backfill config/alpaca_bars.toml 2025-01-01 2026-01-01
   ```

### Keep the host up

- Make Docker Desktop start at sign-in, and stop the machine from sleeping.
- On Windows, register the host watchdog from an Administrator PowerShell. It alerts when Docker, a container or the
  Airflow scheduler is down, or when Windows is waiting to restart for an update:

  ```powershell
  powershell -ExecutionPolicy Bypass -File ops\register_host_watch.ps1
  ```

  `uv run python -m pit.host_watch --dry-run` shows what it sees.
- IB Gateway restarts itself every night (23:45 ET) and periodically needs a 2FA approval.
  [`ops/runbooks/gateway.md`](../ops/runbooks/gateway.md) covers login, data-farm and subscription problems.

### Moving to another machine

What matters lives in two places: `.env`, and `data/`. In `data/`, `raw/` is the irreplaceable part; `staged/`,
`quarantine/` and `curated.duckdb` can be rebuilt from it. Airflow's Postgres database doesn't need to move, because
run history is in `data/pipeline_runs/`.

1. **Stop the old machine first** (`docker compose down`), after the close summary (~17:40 ET) and before the 08:00
   ET Gateway check, ideally on a weekend. Two Gateways on one username kick each other.
2. Clone the repo, copy `.env` and `data/` over, then follow [Steps](#steps) 3–5.

If only `raw/` survives:

```bash
uv run python -m pit.pipeline restage data
DATA_DIR=data uv run dbt build --project-dir dbt --profiles-dir dbt
```

### Optional: the Copilot

```bash
uv run --env-file .env python -m pit.copilot.agent "Why were there no option snapshots on 2026-10-26?"
docker compose --profile copilot up -d api   # HTTP API on 127.0.0.1:8000; needs PIT_API_TOKEN in .env
```

[`.mcp.json`](../.mcp.json) registers the same read-only tools as an MCP server for Claude Code.
