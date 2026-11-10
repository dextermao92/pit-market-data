# Runbook: disk filling up

**Simulated:** the disk has not filled up in production. Growth figures were measured on the Mac's `data/` (2026-10).

## Growth

| | Size |
|---|---|
| raw + staged + `pipeline_runs` per session day | ~150 MB (10-20 → 10-22), ~200 MB after `spx_weekly` (10-23); more with `xsp_weekly` |
| Per year (~252 sessions) at 200 MB/day | ~50 GB |
| `curated.duckdb` | ~300 MB (rebuilt nightly) |
| `cache/` (contract lists, alert markers) | ~30 MB |

Also outside `data/`: Airflow task logs (`/opt/airflow/logs` inside the `airflow` container), Postgres (volume `pgdata`),
and Docker images. On Windows, all Docker data lives in the WSL virtual disk, which grows but doesn't shrink on its own.

## Symptoms

- The **close summary** shows `⚠️ 磁盘 剩余 N GB` when free space on the data volume drops below 20 GB.
- When it is actually full: tasks fail with `OSError: [Errno 28] No space left on device`. Raw files are written to a
  `.tmp` and renamed, so a full disk loses the snapshot but doesn't leave a half-written raw file. Postgres
  (Airflow's metadata) may stop, so the scheduler stops too, and cards stop.

## Triage

```bash
df -h .
du -sh data/* | sort -h
docker system df
docker compose exec airflow du -sh /opt/airflow/logs
```

## What can be deleted, in order

| What | Safe? | How |
|---|---|---|
| Old contract caches `data/cache/ibkr_contracts/<day>/` | Yes, except the **latest day**: each morning copies the latest earlier day forward and re-lists only new expiries plus 4 of the oldest. Deleting every day forces a full re-list, the refresh that stalled IBKR's secdef service on 2026-10-26 | Delete day folders older than the latest one |
| Old alert markers `data/cache/alerts/<day>/` | Yes | Delete older day folders |
| Airflow task logs older than ~30 days | Yes | `docker compose exec airflow find /opt/airflow/logs -type f -mtime +30 -delete` |
| Unused Docker images / build cache | Yes | `docker image prune`, `docker builder prune` |
| `curated.duckdb` | Yes: rebuilt by `dbt build` | Only if desperate; the next `eod` rebuilds it |
| `staged/`, `quarantine/` | Rebuildable from raw, but rebuilding takes time | Last resort: `docker compose exec airflow python -m pit.pipeline restage /opt/airflow/data`, then `dbt build` |
| **`raw/`** | **Never.** It's the only copy of the observations; option history can't be re-fetched | Move to a bigger disk instead |
| `pipeline_runs/` | No: run history, small | Keep |

## Longer term

- Move `data/` to a bigger disk (it's a bind mount: stop the stack, move the folder, start it again).
- Back up `raw/` off the machine. Today it exists only on the collecting host and on the copy made during the
  Windows migration.
- Compress older raw JSON (not implemented): it's text and compresses well, but the stagers read it directly.

## Verify

`df -h` is back above 20 GB free. The next close summary shows `✅ 磁盘`, and any failed task sends `✅ 已恢复` on its next success.
