# Drill 2026-11-13: the vendor serves bad revisions of a session we hold (SIMULATED)

> **Simulated.** Nothing in this drill happened in production. The faults were injected into a hard-linked copy of
> `data/staged`; `data/` was only read. The dates are real because the drill reuses the real 2026-11-10 session.

| | |
|---|---|
| Run | 2026-11-13, `uv run python scripts/drill_bad_revision.py` (about 30 s, two `dbt build`s included) |
| Scenario | At 21:00 ET on 2026-11-10 a re-fetch of the day's 5-min bars returns bad data for 4 symbols; the next `eod` (17:30 ET 11-11) re-fetches the last 10 sessions and gets correct data again |
| Copy | 3,400 staged files fetched up to 21:00 ET 11-10 (bars, daily bars, corporate actions, option snapshots, assets), hard-linked; the drill adds files only |
| Question asked | `get_bars(["AAPL", "MSFT", "NVDA", "SPY"], 11-10 session, as_of)` as of 20:00 ET 11-10 (before), 22:00 ET 11-10 (bad) and 18:00 ET 11-11 (fixed) |

## Steps

1. Hard-link `data/staged` as it stood at 21:00 ET 11-10 into a scratch directory.
2. Take the real `eod` raw file of 11-10 (`20261110T223108474260Z.json`, every symbol, the whole session plus the
   previous close) and corrupt it:

   | Symbol | Fault | What it imitates |
   |---|---|---|
   | AAPL | Every price halved, volume doubled, all 78 bars | A 2:1 split nobody announced |
   | MSFT | 13:00 ET bar: high below low | A malformed record |
   | NVDA | Every price 0.4% too high, all 78 bars | A plausible shift: no single bar looks wrong |
   | SPY | 15:00 ET bar closes 3% low on 1% of normal volume | A bad tick |

3. Write it as a new raw file fetched at 21:00 ET and stage it with the real `pit.alpaca_bars.stage_file` (the
   staging rules run as in production).
4. Run `dbt build --store-failures` on the copy (the same build `eod` runs) and read back the rule failures.
5. Write the uncorrupted payload as the next eod fetch (17:30 ET 11-11), stage it, run `dbt build` again.
6. Ask `get_bars` the same question at the three `as_of` times, and once more as of the bad time after the fix.

## What happened

**Staging rules:**

| Symbol | Result |
|---|---|
| AAPL | All 78 bars staged with the warning `overnight_split_ratio_jump` |
| MSFT | The bad bar was **quarantined** (`ohlc_invariant`); the other 77 bars were staged |
| NVDA | All 78 bars staged with **no flag** |
| SPY | The bad bar was staged with the warning `bad_tick` |

**dbt on the bad state:** `PASS=15 ERROR=3`, so that night's `eod` task would have failed and posted a card.

| Rule | Failing rows on 11-10 |
|---|---|
| 14 daily reconciliation (5-min bars vs the vendor's daily bar) | AAPL 1, SPY 1 |
| 15 split-ratio jump with no split in corporate actions | AAPL 1 |
| 16 IBKR option-snapshot spot outside the Alpaca bar | AAPL 26, NVDA 19 |

**dbt after the fix:** `PASS=18 ERROR=0`.

**`get_bars` across `as_of`:**

| `as_of` | Bars returned | Closes different from "before" | Flagged rows | AAPL mean close | MSFT 13:00 ET bar, version used |
|---|---|---|---|---|---|
| 20:00 ET 11-10 (before) | 312 | none | none | 348.19 | 530.57, fetched 17:31 ET 11-10 |
| 22:00 ET 11-10 (bad) | 312 | AAPL 78, NVDA 78, SPY 1 | AAPL 78, SPY 1 | **174.10** | 530.57, fetched 17:31 ET 11-10 |
| 18:00 ET 11-11 (fixed) | 312 | none | none | 348.19 | 530.57, fetched 17:30 ET 11-11 |

Asked again after the fix, the query as of 22:00 ET 11-10 returned exactly the same bad frame as before the fix.

## Conclusions

1. **Every fault was caught by at least one check, and the build went red that night.** The staging rules caught
   three of the four, and dbt rules 14–16 failed the `eod` build.
2. **The 0.4% NVDA shift was caught only by rule 16, the cross-source check**: no staging rule flagged it, and it is
   inside rule 14's 0.5% slack. Rule 16 exists only for the five Alpaca symbols that also have IBKR option snapshots
   (AAPL, NVDA, QQQ, SPY, TSM). This is an inference from the rules, not something the drill tested: a similar
   shift on any of the other 45 symbols would pass every check.
3. **`get_bars` returns flagged rows; it does not drop them.** As of the bad night it served AAPL at half price,
   marked in `flags`. Whoever consumes the query has to read `flags`, or use the dbt build status, before trusting
   a session.
4. **A quarantined revision never replaces a good version.** The broken MSFT bar never reached staging, so every
   `as_of` kept the earlier valid fetch.
5. **Recovery needed no action, and the bad night stays reproducible.** The next `eod` re-fetch overwrote all four
   faults as the latest version, and dbt went green again. The point-in-time query still answers "what did we
   believe at 22:00 ET?" with the bad data, which is how a decision made that night can be explained later.

## Follow-ups (not done)

- A uniform shift under 0.5% on a symbol without option snapshots passes every check. A second daily-bar vendor,
  or a tighter rule 14 on the close, would close that gap.
- An option such as `get_bars(..., drop_flagged=True)` would help callers that can't handle flags.

## Real revisions, for comparison (not simulated)

The real data has the same mechanism at a much smaller scale. Of 1,057,200 staged 5-min bars to 2026-11-12, the
vendor later revised 1,760 in at least one field, including the close of 75 and the volume of 1,670. Every changed close showed up at the next
day's `eod` re-fetch, 26–32 hours after the bar. The median change in close was 0.023% and the largest 0.27%;
55 of the 75 were on 2026-10-15.

Example: NFLX, 12:55 ET bar on 2026-10-15, revised by the eod re-fetch at 17:30 ET on 10-16:

| `as_of` | close | volume |
|---|---|---|
| 1 s before the re-fetch | 72.705 | 63,650 |
| at the re-fetch | 72.9 | 373,339 |
