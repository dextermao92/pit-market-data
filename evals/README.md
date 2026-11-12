# Copilot evals

Can the PIT Ops Copilot (`pit.copilot.agent`) find the root cause of a collector failure from the run log and
the ops docs, and point at the right runbook?

## Cases

25 diagnosis cases in [`cases/`](cases/): one YAML file each, with the question, the accepted root-cause
categories ([`categories.py`](../src/pit/copilot/categories.py)), the accepted runbooks, and how the label was
checked (`label.source`).

| | Count | From |
|---|---|---|
| Real ([`cases/real/`](cases/real/)) | 14 | `pipeline_runs` 2026-10-14 → 11-11: the 19 failed runs, merged by cause into 9 cases; `gap_s` jumps; missing bars; IBKR codes in successful runs; the SLO view; the [10-23](../ops/incidents/2026-10-23.md) and [10-26](../ops/incidents/2026-10-26.md) incidents. Labels checked against commits, the incident write-ups, the Gateway container log and the operator (r01–r03 confirmed 2026-11-12) |
| Simulated ([`cases/simulated/`](cases/simulated/)) | 11 | `simulated: true`. 2026-11-11 cloned onto 11-12 in a copy of the data, faults injected: disk full, DuckDB lock, Alpaca 401, competing session, host asleep, secdef stall, run budget, farms down, 2FA pending, Alpaca gap, hung request |

Each case runs on its own data: real cases on a frozen copy of `data/` (`fixtures/base`, made 2026-11-12, data
through 11-11), simulated ones on that copy plus their faults. Production `data/` is only read. Build the copies
with `uv run python -m pit.copilot.fixtures build` then `uv run python -m pit.copilot.cases`.

## Scoring

Deterministic, no LLM judge. A diagnosis is **correct** when its category is one of the case's accepted
categories **and** its runbook is one of the accepted runbooks. Category and runbook accuracy are also reported on
their own. A run that ends without a diagnosis is wrong. Cost and latency per diagnosis come from the LLM call log
(`data/llm_calls/`), by the diagnosis's `run_id`.

```bash
uv run --env-file .env python -m pit.copilot.eval --model deepseek-flash      # live; records to recordings/
uv run python -m pit.copilot.eval --model deepseek-flash --replay              # no API, no data/
```

## Results (2026-11-12, commit `4635b28`, thinking off, off-peak prices)

| | deepseek-flash | deepseek-v4-pro |
|---|---|---|
| **Root cause accuracy** (category and runbook) | **76.0 %** (19/25) | 68.0 % (17/25) |
| Real cases / simulated cases | 64.3 % / 90.9 % | 64.3 % / 72.7 % |
| Category accuracy / runbook accuracy | 80.0 % / 92.0 % | 76.0 % / 88.0 % |
| Mean cost per diagnosis | **$0.0037** | $0.0134 |
| Mean time per diagnosis (end to end) / median | 15.1 s / 14.3 s | 26.8 s / 17.9 s |
| Mean LLM time per diagnosis (sum of calls) | 12.1 s | 25.1 s |
| Mean LLM calls per diagnosis | 5.7 | 4.9 |
| Total cost of the run | $0.094 | $0.336 |
| Risky tool requested | 0 | 0 |

**Chosen: `deepseek-flash`.** More accurate on this set, 3.6× cheaper and 1.8× faster. Raw results:
[`results/2026-11-12-deepseek-flash.json`](results/2026-11-12-deepseek-flash.json),
[`results/2026-11-12-deepseek-v4-pro.json`](results/2026-11-12-deepseek-v4-pro.json).

| Case | Kind | Label | deepseek-flash | deepseek-v4-pro |
|---|---|---|---|---|
| `r01-2026-10-15-preopen-farms` | real | `data_farm_disconnected` | ❌ `transient_reconnect` · `runbooks/gateway` | ❌ `gateway_frozen` · `runbooks/gateway` |
| `r02-2026-10-15-alpaca-bar-gap` | real | `vendor_data_gap` | ✅ `vendor_data_gap` · `runbooks/stale-snapshots` | ✅ `vendor_data_gap` · `runbooks/stale-snapshots` |
| `r03-2026-10-16-gateway-autorestart` | real | `gateway_restarting` | ✅ `gateway_restarting` · `runbooks/gateway` | ✅ `gateway_restarting` · `runbooks/gateway` |
| `r04-2026-10-16-hung-run` | real | `hung_request` | ❌ `unknown` · `runbooks/gateway` | ❌ `gateway_restarting` · `runbooks/gateway` |
| `r05-2026-10-16-alpaca-disconnect` | real | `vendor_api_error` | ❌ `vendor_api_error` · `slo` | ❌ `vendor_api_error` · `runbooks/gateway` |
| `r06-2026-10-19-run-timeout` | real | `run_budget_exceeded` | ❌ `unknown` · `runbooks/stale-snapshots` | ❌ `secdef_stall` · `runbooks/gateway` |
| `r07-2026-10-19-subscription-codes` | real | `subscription_missing` | ✅ `subscription_missing` · `runbooks/gateway` | ✅ `subscription_missing` · `runbooks/gateway` |
| `r08-2026-10-22-slo-zero` | real | `missing_run_records` | ✅ `missing_run_records` · `slo` | ✅ `missing_run_records` · `slo` |
| `r09-2026-10-23-secdef` | real | `secdef_stall` | ✅ `secdef_stall` · `incidents/2026-10-23` | ✅ `secdef_stall` · `incidents/2026-10-23` |
| `r10-2026-10-26-frozen` | real | `secdef_stall` | ✅ `gateway_frozen` · `runbooks/gateway` | ✅ `gateway_frozen` · `incidents/2026-10-26` |
| `r11-2026-10-27-30-morning-stall` | real | `secdef_stall` | ❌ `transient_reconnect` · `runbooks/gateway` | ✅ `gateway_frozen` · `runbooks/gateway` |
| `r12-2026-11-05-1100-blip` | real | `transient_reconnect` | ✅ `transient_reconnect` · `runbooks/gateway` | ✅ `transient_reconnect` · `runbooks/gateway` |
| `r13-daily-farm-drops` | real | `transient_reconnect` | ✅ `transient_reconnect` · `runbooks/gateway` | ✅ `transient_reconnect` · `runbooks/gateway` |
| `r14-2026-11-10-docker-down` | real | `host_down` | ✅ `gateway_restarting` · `runbooks/gateway` | ❌ `data_farm_disconnected` · `runbooks/gateway` |
| `s01-disk-full` | sim | `disk_full` | ✅ `disk_full` · `runbooks/disk-full` | ✅ `disk_full` · `runbooks/disk-full` |
| `s02-duckdb-lock` | sim | `duckdb_lock` | ✅ `duckdb_lock` · `runbooks/duckdb-lock` | ✅ `duckdb_lock` · `runbooks/duckdb-lock` |
| `s03-alpaca-auth` | sim | `vendor_api_error` | ✅ `vendor_api_error` · `runbooks/stale-snapshots` | ❌ `vendor_api_error` · `slo` |
| `s04-competing-session` | sim | `subscription_missing` | ✅ `subscription_missing` · `runbooks/gateway` | ✅ `subscription_missing` · `runbooks/gateway` |
| `s05-host-asleep` | sim | `host_down` | ✅ `host_down` · `runbooks/stale-snapshots` | ✅ `host_down` · `runbooks/stale-snapshots` |
| `s06-secdef-preopen` | sim | `secdef_stall` | ✅ `secdef_stall` · `runbooks/gateway` | ✅ `secdef_stall` · `runbooks/gateway` |
| `s07-run-budget` | sim | `run_budget_exceeded` | ❌ `hung_request` · `runbooks/gateway` | ❌ `hung_request` · `runbooks/stale-snapshots` |
| `s08-farms-down` | sim | `data_farm_disconnected` | ✅ `data_farm_disconnected` · `runbooks/gateway` | ✅ `data_farm_disconnected` · `runbooks/gateway` |
| `s09-2fa-pending` | sim | `gateway_restarting` | ✅ `gateway_restarting` · `runbooks/gateway` | ✅ `gateway_restarting` · `runbooks/gateway` |
| `s10-alpaca-gap` | sim | `vendor_data_gap` | ✅ `vendor_data_gap` · `runbooks/stale-snapshots` | ✅ `vendor_data_gap` · `runbooks/stale-snapshots` |
| `s11-hung-request` | sim | `hung_request` | ✅ `hung_request` · `runbooks/stale-snapshots` | ❌ `transient_reconnect` · `runbooks/gateway` |

## Limitations

- **Small set, one run per model.** 25 cases; the two models differ by 2. The same flash model got r14 wrong in a
  smoke run minutes earlier and right in the scored run, so a re-run can move accuracy by a few points.
- **Real cases score lower (64 %) than simulated ones.** The hard real cases need what the run log doesn't show
  directly: a run killed before it was recorded (r04, visible only as a `gap_s` jump), a timeout that is a budget
  problem rather than a hang (r06, and s07), or history kept outside `pipeline_runs` (r11).
- **Some real cases have no run records on this host** (r09, r10: collected on the Mac), so they test reading the
  incident write-ups more than reading data.
- **Labels are one person's call.** Three real labels (r01–r03) were inferred from commits and confirmed by the
  operator, without the Gateway logs of the time. Several cases accept two categories where the write-up supports
  both (e.g. secdef stall and frozen Gateway on 10-26).
- **The category list was written by the same person who wrote the cases.** A model that maps a correct
  explanation to a neighbouring label (r06 `unknown`, s07 `hung_request`) scores as wrong.
- Costs are DeepSeek's off-peak prices of 2026-11-10 (`config/llm.toml`); peak hours cost double.

## Prompt injection

Not run yet.

## CI

Not set up yet.
