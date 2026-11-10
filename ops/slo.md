# SLOs

What "working" means for the collector, measured from data it already writes. One SQL file computes all three
indicators ([`slo.sql`](slo.sql)):

```bash
uv run python -m pit.slo                          # last 20 sessions up to now, on ./data
uv run python -m pit.slo --end 2026-10-26 --sessions 9 --data-dir /path/to/data
```

Window: the last **20 sessions** (about four weeks), rolling. Sessions, opens and closes come from the exchange
calendar, so half days have fewer slots, and a session with no runs at all still counts against the SLO.

## Indicators and objectives

| | SLI | Objective | Error budget per 20 sessions |
|---|---|---|---|
| **SLI 1: option coverage** | Regular-hours 5-min slots with a successful option snapshot ÷ slots due | **≥ 99 %** | ~16 slots (≈ 1 h 20 min of snapshots) |
| **SLI 2: option timeliness** | Slots whose snapshot *finished* within 5 min of the slot time ÷ slots due | **≥ 95 %** | ~78 slots |
| **SLI 3: equity bar completeness** | Usable 5-min bars (staged, any version) in regular hours ÷ (symbols × slots due) | **≥ 99.5 %** | ~390 bars |

Why these three:

- **SLI 1 is the one that matters.** Option quotes can't be re-fetched, so a missed slot is a permanent hole in the
  history the project exists to build.
- **SLI 2** catches the slow failure: runs that succeed but late, which happens when a stall or retry pushes later
  slots back (`max_active_runs=1`). A late snapshot is still valid point-in-time data, so the target is looser.
- **SLI 3** covers the second source. Equity bars can be refetched (the 17:30 `eod` run refetches 10 sessions), so it
  is measured on what is in `staged/` at query time, after any refetch.

Counting rules:

- A slot is identified by its Airflow `run_id` (`scheduled__<slot>`), not by when the run started, so a retry that
  succeeds late still counts for its own slot. Counting by start time was the 2026-10-23 miscount.
- Manual runs (`cli__…`) and backfills don't count.
- Quarantined bars count as missing (SLI 3): the SLI measures usable data, not rows received.

## Current values (Mac data, 9 sessions 2026-10-14 → 10-26)

| Session | SLI 1 coverage | SLI 2 on time | SLI 3 bars | Note |
|---|---|---|---|---|
| 10-14 | 100 % | 100 % | 100 % | |
| 10-15 | 100 % | 89.7 % | 100 % | First days: cold contract lookups inside the runs |
| 10-16 | 100 % | 60.3 % | 100 % | `1100` connectivity loss at 10:10 hung one run 26 min; the slots behind it ran late |
| 10-19 | 100 % | 97.4 % | 100 % | |
| 10-20 → 10-22 | 100 % | 100 % | 100 % | |
| 10-23 | 100 % | 94.9 % | 100 % | [Incident](incidents/2026-10-23.md): secdef stall, 4 slots late |
| 10-26 | **0 %** | 0 % | 100 % | [Incident](incidents/2026-10-26.md): Gateway frozen all session |
| **Window** | **88.9 %** ❌ | **82.5 %** ❌ | **100 %** ✅ | |

10-26 alone used 78 slots, about five times SLI 1's budget. Collection moved to the Windows host on ~2026-10-27, so
later sessions have to be computed there (`uv run python -m pit.slo` against its `data/`).

## When the budget is gone

- **SLI 1 below objective:** stop adding chains, underlyings or cadence. Reliability work comes first (the 10-26
  fixes: day cache, self-healing from the watchdog, paced `contract_cache`). Write up every lost session in
  [`incidents/`](incidents/).
- **SLI 2 below objective, SLI 1 fine:** runs are too slow or get stuck behind each other. Look at run times in the
  progress cards and trim chains or cadence in `config/ibkr.toml` before the 6-min run budget starts skipping chains.
- **SLI 3 below objective:** check whether Alpaca recovered the bars (the `eod` refetch); if not, backfill
  ([stale-snapshots.md](runbooks/stale-snapshots.md)).

## Not covered (yet)

- Index snapshots, and the EOD datasets (corporate actions, assets, daily bars, `dbt build`): the close summary card
  reports them daily, but they have no objective.
- Quote quality: an option snapshot counts as successful even if many contracts have no two-sided quote.
  `quote_status` counts are in `pipeline_runs` if this ever needs an SLI.
- Alerting on budget burn: the SLIs are computed on demand, not by a DAG.
