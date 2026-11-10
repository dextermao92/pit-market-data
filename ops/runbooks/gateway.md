# Runbook: IB Gateway down, login / 2FA, stalled contract definitions

Based on real incidents: [2026-10-23](../incidents/2026-10-23.md), [2026-10-26](../incidents/2026-10-26.md), and the
daily morning stalls of 2026-10-28 → 11-02 (commit `ef762db`).

Commands run from the repo root on the host that runs the stack. Times are ET.

## Symptoms

- A **Gateway check card** (08:00, 08:20, 08:40, 09:02 on session days) is red. Each red field points at a cause:

  | Field | Meaning | Section |
  |---|---|---|
  | `❌ Gateway 无法连接` (cannot connect) | The API port refuses or times out on connect | [A](#a-api-unreachable-or-hanging) |
  | `❌ Gateway 已连接，但请求没有响应` (connected, requests hang) | Connects, then requests hang | [A](#a-api-unreachable-or-hanging) |
  | `❌ 合约查询` (contract lookup) | Logged in, but contract definitions (secdef) don't answer within 20 s | [B](#b-contract-definition-secdef-stall) |
  | `❌ 行情权限` (market data permissions; codes 354, 10089, 10090, 10167, 10168, 10197) | This user gets no live data | [C](#c-no-data-subscriptions-or-competing-session) |
  | `❌ Gateway 美股报价：无` (no US quotes) plus farm codes 1100 / 2103 / 2105 / 2157 | Data farms disconnected | [D](#d-data-farms-disconnected) |
  | `🔧 自动修复` (self-healing) ending in `❌ 自动修复失败` (failed) | Self-healing tried and failed | [E](#e-manual-restart-full-login) |

- A **`⏸️ 数据停滞`** card for option or index snapshots: see [stale-snapshots.md](stale-snapshots.md). The watchdog has already
  tried self-healing once.
- `❌ 任务失败：期权快照 / 指数快照` with `TimeoutError()` after about 4 s means the connect timed out (section A).

## What already happened automatically

On session days between 04:00 and 20:00, a red check (except section C) self-heals:
`RECONNECTDATA` (20 s), then `RESTART` (120 s, no 2FA), re-checking after each. A contract-definition stall skips
straight to `RESTART`. If IBC takes a command and never replies (`Hung`), healing stops, because later commands queue
behind the stuck one. Outside that window, and for subscription problems, the check only reports.

## Triage

```bash
docker compose ps
docker compose exec airflow python -m pit.health gateway
docker compose logs --since 30m ib-gateway | grep -v "remove Client" | tail -40
```

The second command runs the same check as the cards, without sending a card or healing. Add `--send` to post the result.

## A. API unreachable or hanging

**Seen 2026-10-26:** from 09:16 the API port accepted no connections at all, while IBC's command server still answered.
Self-healing can't fix this state: IBC's `RESTART` was accepted but the Gateway's UI never opened its config dialog.

1. If the Gateway log shows a login dialog or `Second Factor Authentication`, it's waiting for 2FA: go to E, step 3.
2. Otherwise go to E. A wedged Gateway only comes back from a container restart.

## B. Contract-definition (secdef) stall: the contract lookup (`合约查询`) is red

**Seen 2026-10-23 (5 min) and every morning 2026-10-28 → 11-02**, caused by the 08:15 `contract_cache` refresh
downloading every expiry (~24 MB, ~23k messages).

- Snapshot runs don't need secdef: they use the day's contract cache. Quotes keep flowing as long as the cache exists.
- Check that today's cache exists: `ls data/cache/ibkr_contracts/` should list today's New York date. Never delete the
  latest day's cache: without it the next morning re-lists every expiry, the refresh that causes this stall.
- If it persists into the session, self-healing goes straight to `RESTART`. If that fails, go to E.
- Do **not** run `warm_cache` or any bulk contract-details request by hand to "help": that's what caused the stalls.

## C. No data: subscriptions or competing session

Self-healing never runs here: a restart can't fix a subscription.

| Code | Meaning | Fix |
|---|---|---|
| 354 / 10089 / 10090 | Not subscribed for this instrument | Check the collector user's subscriptions in Client Portal (they are per username). Required: US Securities Snapshot and Futures Value Bundle, US Equity and Options Add-On Streaming Bundle. New subscriptions can take up to 24 h |
| 10167 / 10168 | Delayed data shown instead of live (10167), or not subscribed with delayed data disabled (10168) | Same as above |
| 10197 | Live data held by another session of the same user | Log the other session out (TWS, mobile, web) or move collection to its own user (done 2026-11-06: a second username used only for collection) |

## D. Data farms disconnected

Farm drops that recover within the same run (e.g. `2103` then `2104` for the same farm) are routine and not alerted.
If they persist, self-healing sends `RECONNECTDATA`. A `1100` followed by `1102 ... data maintained` is IBKR
reconnecting the same session: no action needed.

## E. Manual restart (full login)

Needs someone with the phone that receives IBKR's 2FA push. Prefer after 16:00 or before 08:00 on session days.

1. Make sure no other machine runs a Gateway with the same username. Two Gateways with one user log each other out
   (`EXISTING_SESSION_DETECTED_ACTION=primary` on both).
2. `docker compose restart ib-gateway`
3. Approve the push on the phone. With `TWOFA_DEVICE` set in `.env` (e.g. `IB Key`), the login picks that device. If
   it is empty and the account has several devices, pick one over VNC (`127.0.0.1:5900`). IBC retries the login
   if the push isn't approved within 180 s.
4. Watch the login: `docker compose logs -f --since 2m ib-gateway | grep -E "Second Factor|Login has completed"`
5. Changing `.env` (user, password, `TWOFA_DEVICE`) takes effect only when the container is recreated:
   `docker compose up -d --no-deps ib-gateway`, not `restart`.

## Verify

```bash
docker compose exec airflow python -m pit.health gateway --send
```

The card should be green, including `✅ 合约查询` and SPY / SPX / VIX quotes. During the session, the next option
snapshot should succeed within 5 minutes, and the next run of a failed task sends `✅ 已恢复`.

## Don't

- Don't start a second Gateway, or a test container, with the production username.
- Don't restart the Gateway between 09:00 and 16:00 unless collection is already down: a full login needs 2FA.
- Don't run heavy or cold contract-details tests against the production Gateway near or during market hours.
- IBKR stays read-only (`READ_ONLY_API=yes`, `readonly=True`): never change that to fix anything.
