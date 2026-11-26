# Stage 2 · P1: whole-market universe and daily bars (design)

Status: **reviewed**, 2026-11-25; the four open questions are decided (see Decisions). P1a is done: steps 1–3
are built and live (`pit.universe`, the whole-market `assets` / `corporate_actions` / `daily_bars` fetches,
`eod`; first full run 2026-11-25: 24,498 symbols, 29.3M daily bars, 63 min), and the survey (step 4) is in
[p1-data-survey.md](p1-data-survey.md). P1b is not built; sections 3–6 below predate the survey and are
replaced by [p1b-curate-design.md](p1b-curate-design.md).

**Scheduling.** Past daily bars never expire, unlike option snapshots, so discovery and backfill have no deadline;
they only have to stay out of the way. The `universe_refresh` DAG runs them on the first Saturday of January,
April, July and October at 20:00 ET: no US session for a day and a half, and Sunday 10:00–12:00 in Sydney through
both DST changes, away from the Australian-night hours when the market-hours collection runs. A manual trigger
always runs; the first, full backfill is one. The backfill is incremental (new symbols in full, known ones from
their last backfill's end) and resumable, so each quarter is short: discovery (~25 min) plus a top-up.

**The one hard rule: stay out of the collection window**, from 08:00 ET (the first pre-open check, 90 minutes
before the open) to 30 minutes after the close (the last 5-minute bar run), on session days only
(`pit.market_hours.collection_window`). The Alpaca fetches never touch the IB Gateway, but they share the machine
with the option snapshots, which can't be backfilled and have a 6-minute budget per run, and share Alpaca's rate
limit with the 5-minute bars. So any heavy job stops on its own before the window: the backfill starts no batch
within 5 minutes of it and reports `stopped` (the next run resumes from its markers); discovery, which can't
resume, refuses to start with less than 45 minutes left and fails rather than run into the window. Any other
time is fine, weekday evenings included.

By hand, outside market hours:

```bash
uv run --env-file .env python -m pit.universe refresh config/alpaca_bars.toml   # discover + backfill
uv run --env-file .env python -m pit.universe backfill config/alpaca_bars.toml 2016-01-01 2026-11-24
```

## Goal

Give cross-sectional, long-only, intraday research on US small and mid caps a survivorship-free base:

- every exchange-listed US security that traded on any session since 2016, **including delisted ones**;
- unadjusted daily bars for all of them, plus every corporate action;
- a **stable security identity** across renames and ticker reuse;
- a daily eligible universe, computed only from what was known before that session's open.

P2 (1-minute bars with extended hours) fetches exactly the securities that P1's universe ever marks eligible, so
P1 also decides the size of P2.

Out of scope: intraday data (P2), EDGAR filings, earnings dates, float, macro calendar, VIX (P3), quotes (P4),
anything for short selling.

## Approach: acquire first, curate after looking

P1 runs in two steps, and only the first is designed in detail here:

- **P1a · Acquire.** Get every vendor response for the whole market into raw, as received, with the existing
  raw → staged path and no new processing. Raw is the source of truth and staged can be rebuilt
  (`python -m pit.pipeline restage`), so nothing decided later needs a refetch.
- **P1b · Curate.** Look at the data first, then clean, deduplicate, link identities and build the universe.
  Sections 3–6 below are the current thinking, kept as a starting point; they will be revised from what the data
  shows, not built as written.

What P1a must get right, because only a refetch can fix it: complete history (2016 on), unadjusted prices,
the SIP feed, every symbol tried recorded in the request (so coverage can be proven later, including the symbols
that returned nothing), and every corporate action type kept in raw.

## What the vendor actually does (probed 2026-11-25)

Read-only probes against Alpaca's free plan (`/v2/assets`, `/v2/stocks/bars`, `/v1/corporate-actions`):

| Question | Finding |
|---|---|
| Asset list | 14,445 active `us_equity` assets (1,186 OTC), 19,146 inactive (16,255 OTC) |
| Does the inactive list cover delisted stocks? | **No.** SIVB, BBBY (Bed Bath), TWTR, FRC, ATVI, FB are all missing; XLNX is there |
| Do delisted stocks still have bars? | **Yes**, daily and 1-minute: SIVB to 2023-03-09, TWTR to 2022-10-27, FRC, ATVI, XLNX, FB to their last day |
| Renames | A security's **current** symbol returns its whole history, back-mapped: `META` returns FB's bars from 2016, and `FB` also returns them up to 2022-06-08. The same bars appear under two symbols |
| Ticker reuse | An **old** symbol returns the periods it was actually traded under that name, which can be two companies: `BBBY` returns Bed Bath & Beyond 2016–2023-05 **and** Beyond Inc. 2025-08 to 2026-08 (renamed from BYON, later NXH) |
| Corporate actions | Whole-market queries work without a symbol filter (2023-03: ~1,000 actions, several pages). Types include splits, reverse and unit splits, cash and stock dividends, cash / stock / stock-and-cash mergers, spin-offs, name changes (with old and new CUSIP), redemptions. History starts in 2016, like the bars |
| Multi-symbol requests | 1,000 symbols per daily-bar request works; 200 requests/min |
| Several symbols of one security in one request | The security is returned **once**, under one of them: SIVB + SIVBQ → only SIVBQ, FRC + FRCB → only FRCB, FB + META → only META. Usually harmless (the same bars), but FB was later reused by an ETF, and that ETF's bars are lost when FB is requested next to META. Discovery and backfill therefore re-request, in shuffled batches, the symbols that returned nothing (see the trial below) |
| Brute-force symbol discovery | 1,000 random 4-letter symbols, 2016 to today: 33 had bars, 4 pages, 4 s. The first 1,000 of the 1–3 letter symbols: 571 had bars, 107 pages, 106 s. Cost scales with real rows, not with symbols tried |

### Trial run (2026-11-25, scratch data directory, 02:00–03:00 ET)

| Step | Result |
|---|---|
| Assets | 33,600 assets, 2 requests, 4 s; raw 15 MB |
| Corporate actions, full history | 383,591 splits and dividends staged, 0 quarantined; 166 s; raw **89 MB** per full fetch |
| Discovery | 490,597 symbols tried in 491 batches, **24,327 with bars**, 5 rejected (`AT1`, `AT1P`, `ENLV1`, `OPP-C`, `XZW0`); 23 min; raw 167 MB |
| Discovery sources | brute force 19,682; corporate actions 19,102; assets 15,392 (overlapping). **1,290 symbols that stopped trading before 2026-10 were found only by brute force** (KORS, VAXX, ...) |
| Recheck of listed / action symbols with no bars | 12,109 rechecked (mostly OTC names and mutual funds with no SIP bars): round 1 found 167 more (ABB, AMRS, ...), round 2 none |
| Symbols with bars per year | 2016: 9,275 · 2018: 9,892 · 2020: 11,214 · 2021: 13,367 · 2023: 13,238 · 2025: 13,890 · 2026: 14,632 (all types, before any filter) |
| Daily bars, 2026-10 | 293,388 bars, 13,474 symbols, 22 sessions; 69 batches, 100 s; raw 34 MB, staged 8 MB |
| Full backfill, estimated | ~29.5M daily bars, ~3,000 requests (about 25 min at 150/min plus staging), raw ~3.4 GB, staged ~0.8 GB |

Known cases after discovery: TWTR, ATVI, XLNX, OSTK, BYON, BBBY, METV, NXH, META and FB have bars; SIVB and FRC
come back under SIVBQ and FRCB (the collision above), with the same history.

Two consequences drive the design:

1. **No vendor list of delisted securities exists**, so the universe has to be discovered.
2. **A symbol is not an identity.** It can hold a back-mapped copy of another symbol's history, or two unrelated
   companies. Joining on symbol would double-count FB/META and splice Bed Bath onto Beyond.

## Design

### 1. Symbol discovery (`pit.universe.discover`, once in full, then quarterly)

Once the first backfill is in, a security delisted later was active until then, so `eod` has been fetching it
every day. The quarterly rerun is an audit: it catches what listed and delisted between two asset snapshots, and
shows whether the vendor's history coverage changed.

The candidate symbols are the union of:

- **every 1–4 letter symbol** (475,254 strings), tried in batches of 1,000 against monthly bars 2016 to today. This
  covers NYSE (1–3 letters) and Nasdaq (4 letters), including delisted symbols no list remembers. The cost is
  bounded by the real data: about 500 batches plus the pages of real bars, 23 minutes in the trial;
- every symbol in `/v2/assets`, active and inactive, excluding OTC: this adds 5-letter symbols and class shares
  (`BRK.B`, `GOOGL`);
- every symbol in the corporate actions history (`symbol`, `old_symbol`, `new_symbol`, `acquiree_symbol`,
  `acquirer_symbol`, `source_symbol`).

Discovery probes with **monthly** bars (cheap: a symbol's whole history is ~130 bars). Each batch is a raw file
in its own dataset, `universe_probe`, never staged, listing every symbol tried, including those that returned
nothing. The output is a derived file `cache/universe/candidates.parquet`:
`(symbol, n_months, first_month, last_month, sources)`, which sizes the backfill batches.

Known gap: **delisted 5-letter symbols with no corporate action** are missed. Most 5-letter Nasdaq symbols are
warrants, units, rights or preferreds (suffixes W, U, R, P), which the universe excludes anyway. Coverage is
measured, not assumed (see Validation).

### 2. Vendor datasets: extend the existing ones to the whole market

The existing datasets stay; they get whole-market fetches. Nothing new is added beside them, so rules, dbt
sources and `get_bars` keep working.

| Dataset | Today | P1 |
|---|---|---|
| `assets` | 50 symbols, one request each | `GET /v2/assets?status=active` and `?status=inactive`: 2 requests per day. Raw keeps every field; staged columns unchanged in P1a |
| `corporate_actions` | Full history of 50 symbols every day; only splits and dividends are staged | Whole market; raw keeps **all types**, staging unchanged in P1a (splits and dividends). Daily: process dates in the last 30 days and the next 365. Weekly (Saturday): full history from 2016, to catch late revisions without storing ~130 pages every day |
| `daily_bars` | 50 symbols, last 10 sessions at `eod` | Backfill: every candidate, 2016 to the last session, batches of ~300k bars (≤ 1,000 symbols), incremental and resumable. `eod`: last 10 sessions for every active non-OTC asset plus the configured symbols, 1,000 symbols per request |

Volume (measured in the trial): ~29.5M daily bars, raw JSON ~3.4 GB, staged Parquet ~0.8 GB. The backfill is
about 3,000 requests; it is paced to 150/min so the live 5-minute bar DAG keeps headroom, and it runs outside
market hours. Ongoing raw growth, roughly: assets 15 MB per day, the weekly full corporate-action fetch 89 MB,
the quarterly discovery 167 MB: about 9 GB per year.

**P1b note: `adjust()` must filter by type** before any other action type is staged. Today it treats any action
with a null `cash` as a split, so a staged merger or name change would be applied as a split. Rule 15 already
filters by type.

**Side effects of P1a on what already runs**: staged `daily_bars` will hold back-mapped duplicates (FB and META
both carry FB's history). Nothing reads daily bars by symbol across the market yet: rule 14 joins them to the
50 symbols' 5-minute bars only. `daily_bars_current` grows to ~25M rows, which adds time to the `eod` dbt
build; measured on the first run.

**Point in time.** A backfilled bar's `ingested_at` is the fetch day, as with the existing equity backfill, so an
`as_of` before 2026-11 sees none of it. Research over the past uses the latest known data, and lookahead is
prevented by **trade-date discipline** (a feature for session d uses only sessions before d). From the first
fetch on, revisions are versioned as usual. Both modes are stated in the query docstrings.

*Sections 3–6 are P1b: current thinking, to be revised after looking at the acquired data.*

### 3. Security identity (dbt, curated)

**`security_days`**: one row per (session, security), deduplicated:

1. Take `daily_bars_current` for every symbol.
2. **Remove back-mapped copies.** Bars that are identical (open, high, low, close, volume) on the same session
   under two or more symbols are one security-day. Keep the row under the **as-traded** symbol: the one the
   name-change chain says was in effect that day; if no name change explains it, the symbol whose own series
   starts earliest.
3. **Split reused tickers.** A symbol's series is cut into segments at gaps longer than 20 sessions and at
   `name_change` events where the symbol is the `new_symbol` with a different CUSIP. Each segment is a separate
   identity unless a name-change chain links it to another.

**`security_identity`**: `security_id` (a stable hash of the first symbol and first session of the chain),
`symbol`, `valid_from`, `valid_to`, `cusip` where known, `asset_id` where the security is still in
`/v2/assets`, and how it ended (`delisted`, `cash_merger`, `stock_merger`, `active`). Chains come from
`name_change` (old → new symbol, same CUSIP or a CUSIP change recorded in the action) and from the dedupe in
step 2.

Corporate actions are attached to `security_id` through the symbol in effect on the action's date, so
`adjust()` works per security, not per symbol. This is the riskiest part of P1b; it is worked out on the
acquired data, starting from the known cases below.

### 4. Security type

Needed to keep common stocks and drop warrants, units, rights, preferreds and funds:

- from `assets`, when the security is still there: `name` keywords (`Common Stock`, `Ordinary Shares`, `ADS`/
  `American Depositary`, `Warrant`, `Unit`, `Right`, `Preferred`, `ETF`/`Fund`/`Trust`) and `exchange`;
- otherwise from symbol patterns (5th letter W/U/R/P on 5-letter symbols, `.WS`, `.U`, `.RT`, `-P`);
- otherwise `unknown`.

`unknown` is kept in the universe with a flag, and its share per year is reported. If it is large, P3's EDGAR
data (SIC codes, filer type) can refine it.

### 5. Daily universe (`universe_daily`, dbt)

One row per (session d, security), using only sessions **before d**:

| Column | Definition (defaults to review) |
|---|---|
| `listed` | The security has a bar on d−1. Whether it trades again later is not used: that would be lookahead |
| `prev_close` | Close of d−1, unadjusted |
| `adv20_usd` | Median of close × volume over the 20 sessions before d |
| `age_sessions` | Sessions since the first bar |
| `type` | From section 4 |
| `is_adr` | `type = 'adr'`; ADRs are eligible, and the flag lets a study drop them |
| `eligible` | `type in ('common', 'adr', 'unknown')` and `prev_close >= 1` and `adv20_usd >= 1,000,000` and `age_sessions >= 20` |
| `rank_adv` | Rank of `adv20_usd` among eligible securities on d |

The thresholds live in a dbt var (defaults decided below), so the research can rerun with stricter ones. ETFs are kept in a separate flag
(`is_fund`) for benchmarks and sector context, not in `eligible`.

### 6. Queries

In `pit.query`:

- `get_universe(session, *, eligible_only=True)` → `security_id`, `symbol`, flags, `rank_adv`;
- `get_daily_bars(security_ids, start, end, as_of, adjust="all"|"split"|"none")` → by identity, adjusted with
  the actions known at `as_of`.

`get_bars` (5-minute) is unchanged.

## Validation

**Known cases**, each a pytest over a frozen sample and a dbt test on the full data:

| Case | Expected |
|---|---|
| FB → META (2022-06-09) | One `security_id`; no session appears twice; symbol FB before, META from 2022-06-09 |
| Roundhill META ETF → METV | A separate `security_id`, `is_fund`, never merged with Meta Platforms |
| OSTK → BYON → BBBY → NXH | One chain; no duplicate days |
| Bed Bath & Beyond (BBBY, 2016–2023) | A separate `security_id` from Beyond's later BBBY period; ends 2023-05 as delisted |
| SIVB, FRC | Present to their last session, end as delisted |
| TWTR, ATVI, XLNX | End on the merger date as `cash_merger` / `stock_merger` |
| GE 1:8 reverse split (2021-08-02) | Split-adjusted series continuous; rule 15 does not fire |

**Coverage**: securities per session and eligible securities per session from 2016, by type and exchange,
plotted. Expected: no step changes except real ones (for example, the 2020–2021 SPAC wave). A sudden drop in
older years means the discovery missed delisted symbols.

**Rules**: rule 15 (split-like jump without an action) runs on the whole market as a `warn`. On small caps it
will fire more, and each firing is either a missing action or a real move; the first run is calibrated by hand,
as rule 14 was.

**No duplicates**: a dbt test that no (session, OHLCV) tuple belongs to two `security_id`s.

## Build order

**P1a · Acquire**

1. `assets` and `corporate_actions` fetch the whole market; `eod` gets the weekly full corporate-action
   history. Staging unchanged.
2. `pit.universe.discover`: brute-force 1–4 letter symbols plus asset and corporate-action symbols, probed
   with monthly bars (raw dataset `universe_probe`), with rechecks; writes the candidate list.
3. `daily_bars` backfill for every candidate, 2016 on, paced to 150 requests/min, incremental and resumable;
   `eod` refetches the last 10 sessions for the whole market; the `universe_refresh` DAG runs discovery and the
   backfill quarterly (Saturday 20:00 ET).
4. A short **data survey**, written up in `docs/stage2/`: securities per session by year and exchange, how many
   symbols carry back-mapped duplicates, how many reused tickers, the corporate action types and counts,
   asset names by type keyword, rule 15 firings. This is what P1b is designed from.

**P1b · Curate** (designed after the survey)

5. `adjust()` filters by type; stage the other corporate action types.
6. dbt: `security_days`, `security_identity`, `universe_daily`, with the known-case tests.
7. `get_universe`, `get_daily_bars`; coverage report; docs (`README` Data table, `docs/setup.md`).

## Decisions (2026-11-25)

1. **Eligibility defaults**: previous close ≥ $1 and 20-day median dollar volume ≥ $1M, the widest option. The
   research can tighten them through the dbt var; P2 fetches what these defaults ever mark eligible.
2. **ADRs are eligible**, flagged with `is_adr` so a study can exclude them.
3. **Brute-force discovery** of 1–4 letter symbols: once in full, then quarterly as an audit, Saturday 20:00 ET
   (Sunday daytime in Australia, away from the Australian-night collection hours). About 500 extra requests per
   run, within the rate limit.
4. **Delisting without a merger price** (SIVB): an overnight position exits at the last close. This flatters long
   positions in failing companies; studies that hold overnight report how many trades hit this case. Positions
   flat by the close are unaffected.
