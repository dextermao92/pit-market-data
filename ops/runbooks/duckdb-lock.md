# Runbook: dbt build fails on the DuckDB lock

**Simulated:** this has not happened in production. The lock behaviour below was reproduced on a scratch file with
duckdb 1.5.6 (2026-11-10).

## How it happens

The curated layer is one DuckDB file, `data/curated.duckdb`, written by `dbt build` in the `eod` DAG at about 17:30 ET.
DuckDB allows one writing process per file. Across processes:

| Already open | Another process opens read-write (dbt) | Another process opens read-only |
|---|---|---|
| A writer | Fails | **Fails** |
| A read-only connection | **Fails** | Works |

So any open handle blocks the nightly build, **even a read-only one**: a DuckDB CLI session, a notebook, DBeaver, or a
Python REPL left open.

## Symptoms

- The **close summary** card shows `❌ dbt build` with failed nodes. If dbt never wrote results, it shows
  `本交易日没有运行结果（最近一次是 MM-DD）`.
- The `eod.dbt_build` task log contains:
  `IO Error: Could not set lock on file ".../curated.duckdb": Conflicting lock is held in <program> (PID <pid>)`
- `dbt_build` has no retries. Collection is **not** affected: raw and staged Parquet are written by the collectors,
  not through DuckDB.

## Fix

1. Find the holder. The error message names the program and PID.
   - Inside the stack: `docker compose exec airflow ps -ef | grep -iE "dbt|duckdb|python"`
   - On the host: close notebooks, DuckDB CLI or UI sessions, and SQL clients pointed at `data/curated.duckdb`.
2. Close it. If it's a stale `dbt` from an earlier run, let it finish, or stop it.
3. Re-run the build:

   ```bash
   docker compose exec airflow bash -c "cd /opt/airflow && /home/airflow/dbt/bin/dbt build --project-dir dbt --profiles-dir dbt"
   ```

   Or clear the `dbt_build` task of today's `eod` run in the Airflow UI, so `close_summary` runs again too.

## Verify

`dbt build` ends with `Completed successfully`. The next close summary shows `✅ dbt build` with today's date.

## Avoid

- For ad-hoc queries, don't open the live file. Query the staged Parquet directly (`duckdb -c "select ... from
  'data/staged/option_snapshots/*/*.parquet'"`) or work on a copy:
  `cp data/curated.duckdb /tmp/curated.duckdb`.
- Never keep a session open on `curated.duckdb` across 17:30 ET.
- Losing `curated.duckdb` is not data loss: it rebuilds from staged with `dbt build`.
