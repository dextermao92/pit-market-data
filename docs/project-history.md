# Project history

How the project was built, in order, and the measurements taken along the way. The [README](../README.md) describes
the finished system; this page keeps the record. Copilot experiments are written up in full in
[`evals/README.md`](../evals/README.md).

## Milestones

| Phase | Scope | Done when | Result |
|---|---|---|---|
| P0 | Sources, repo, design doc | Design written | 2026-10-13 |
| P1 | Docker Compose; ingest → raw → staged for both sources, idempotent; `pipeline_runs`; IBKR cycle-time check; ingest DAGs | Same run twice → identical staged data (hash of sorted rows, not file bytes); queryable in DuckDB; option snapshots collecting | Collecting every session day since 2026-10-14. A warm SPX/SPXW cycle (~3,300 contracts) takes about 4 min, inside the 5-min slot |
| P2 | Row rules + quarantine, partition checks, corporate actions, adjustment | Each rule has +/- tests; hand-computed adjustment golden test passes | Done |
| P3 | Market-hours Timetable (replaces cron), `eod` DAG; dbt curated layer, SCD2, rules 14–16; backfill; CI | 1-year equity backfill; a full week unattended except Gateway re-auth; `dbt build` green | Done; 2026-11-03..06 ran unattended |
| P4 | `as_of` queries + simulated bad-data drill | Same query differs across `as_of` values; drill written up, labelled simulated | 2026-11-13: [`pit.query`](../src/pit/query.py); real revisions differ across `as_of`; [drill](../ops/drills/2026-11-13-bad-vendor-revision.md) |
| P5 | PIT Ops Copilot: ops docs, read-only MCP tools, hand-written agent loop with human approval for risky tools, evals replaying real incidents | One command scores the evals; adversarial set blocked; CI green on recorded responses | 2026-11-12: 25 cases, 76 % on deepseek-flash; injection set blocked 5 / 5; CI replays the recordings. 2026-11-13: context budget and operator-confirmed memory. 2026-11-18: evidence check, eval drafts from confirmed diagnoses, HTTP API |

## Hosting

The collector ran on a Mac until late October 2026, then moved to a Windows 11 PC (Docker Desktop on WSL 2). The
move added [`pit.host_watch`](../src/pit/host_watch.py), which watches Docker itself from the Windows host. The
outages that left permanent gaps in the option history are written up in [`ops/incidents/`](../ops/incidents/).

## Vendor revisions seen in real data (to 2026-11-12)

Of 1,057,200 five-minute bars, the vendor later revised 1,760, including the close of 75. Most of those 75 were one
incomplete bar served for the whole market on 2026-10-15, then completed; the rest were decimal precision changes or
individual trade corrections. Each changed close showed up at the next day's `eod` refetch.

```python
from pit.query import get_bars
get_bars(["NFLX"], start, end, as_of=datetime(2026, 10, 16, 21, 30, 47, tzinfo=UTC))  # close 72.705
get_bars(["NFLX"], start, end, as_of=datetime(2026, 10, 16, 21, 31, tzinfo=UTC))      # close 72.9 (eod revision)
```

The [drill](../ops/drills/2026-11-13-bad-vendor-revision.md) injects four kinds of bad revision into a copy of the
data. Every kind was caught by at least one check; a uniform 0.4 % shift was caught only by the cross-source rule 16.

## Copilot measurements

### Model choice (2026-11-12)

One live run each, temperature 1.0, before memory and the evidence check:

| Model | Root cause accuracy | Mean cost / diagnosis | Mean time / diagnosis |
|---|---|---|---|
| **deepseek-flash** (chosen) | **76.0 %** (real 64.3 %, simulated 90.9 %) | $0.0037 | 15.1 s |
| deepseek-v4-pro | 68.0 % (real 64.3 %, simulated 72.7 %) | $0.0134 | 26.8 s |

Prompt injection was blocked 5 / 5 on both models.

### Context budget and memory (2026-11-13)

One live run per configuration, deepseek-flash, temperature 1.0:

| | Accuracy (real) | Prompt tokens / diagnosis | Largest call | Cost / diagnosis (off-peak) |
|---|---|---|---|---|
| No budget | 76.0 % (64.3 %) | 69,896 | 36,055 | $0.0037 |
| 16k budget | 68.0 % (57.1 %) | 56,398 | 16,123 | $0.0045 |
| 16k budget + memory, leave-one-out | 72.0 % (71.4 %) | 59,715 | 15,574 | $0.0044 |

The budget cut tokens by 19 % but raised cost by 21 %, because compaction breaks DeepSeek's prompt cache. A sweep of
budgets (none, 12k–96k) found no budget that saves money at these prompt sizes, so the budget was raised to 48k: a
guard that normal diagnoses never reach. Details: [context management](../evals/README.md#context-management),
[budget sweep](../evals/README.md#budget-sweep).

### Temperature, memory and the evidence check (2026-11-13 → 11-18)

| Setup | Live runs | Root cause accuracy | Real / simulated | Cost / diagnosis |
|---|---|---|---|---|
| Temperature 1.0, no memory | 8 | 56–76 % (mean 67.0 %) | 50–64 % / 64–91 % | $0.0036–0.0037 off-peak |
| Temperature 0, no memory | 3 | 80 / 80 / 76 % (mean 78.7 %) | 64–71 % / 91–100 % | $0.0030–0.0032 off-peak |
| Temperature 0, memory (leave-one-out), evidence check | 3 | 64 / 76 / 72 % (mean 70.7 %) | 57–71 % / 73–91 % | $0.0034 off-peak |

- Temperature 0 made answers steadier and better.
- Memory added answer variance (6 cases changed between runs, against 3); whether it costs accuracy is within the
  noise.
- The evidence check caught no invented facts in 75 live diagnoses. CI replays the median run (72 %).

Details: [run-to-run variance](../evals/README.md#run-to-run-variance), [memory](../evals/README.md#memory-leave-one-out),
[evidence check](../evals/README.md#evidence-check).

## Public release (2026-11-18)

History was scanned for secrets, rebuilt with one author address and published to a new repository. See the
[release checklist](public-release-checklist.md).
