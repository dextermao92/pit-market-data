# Stage 2 · P1b: curate the whole-market daily data (design)

Status: **reviewed**, 2026-11-26; decisions at the end. Build step 1 (`pit.identity`, staged `corporate_events`) is built and run on production data (scratch output); not wired into `eod` yet. Built on the P1a survey ([p1-data-survey.md](p1-data-survey.md)) and a
prototype run of the identity rules on the production data; replaces sections 3–6 of
[p1-universe-design.md](p1-universe-design.md). Nothing here is built yet.

## Goal

Turn the as-received whole-market daily bars (24,498 symbols, 29.3M bars, 2016 on) into:

1. **securities, not symbols**: one `security_id` per listed security, with the symbol it traded under on each
   session, so a renamed company is one series and a reused ticker is two;
2. **a security type** good enough to keep common stocks and ADRs and drop funds, warrants, units, rights and
   preferreds;
3. **a daily universe** (`universe_daily`): for each session, which securities a strategy could have traded,
   using only information available before that session;
4. **queries** by security: `get_universe`, `get_daily_bars` (split/dividend adjusted as of a date).

P2 (1-minute bars) is sized and fetched from this: securities ever eligible, by the symbol that carries each
period.

## What the data says (recap)

- Bars are clean (no OHLC failures); 6.4% are zero-volume **filler bars** (OHLC = previous close).
- **Back-mapped copies**: 2,080 symbol pairs share identical bars; the vendor serves a renamed security's whole
  history under the new symbol and the as-traded history under the old one.
- **Reused tickers**: 684 symbols restart after a gap of more than 250 sessions, usually at a different price.
- **Actions** explain renames and endings from 2019–2020 on, not before; splits are covered throughout.
- **No security type** from Alpaca; funds are about half of the liquid listings.

## Prototype evidence (2026-11-26, production data)

**Twin runs.** For each pair of symbols sharing ≥ 5 identical traded bars, what each symbol does after the last
shared bar (next traded bar within 60 sessions, and within ×1.5 of the last shared close = `smooth`):

| After the shared run | Pairs | Reading |
|---|---|---|
| one stops, one continues smoothly | 1,722 | Rename: the one that stops is the old (as-traded) symbol, the other its successor |
| one stops, one continues with a jump | 286 | Rename with a reverse split or a price move on the day; same reading |
| both stop | 61 | Two complete copies (SIVB/SIVBQ, FRC/FRCB): a tie, broken by rule |
| one jumps, one continues smoothly | 4 | The jumping symbol is reused at once by another security (ADEA/XPER) |
| both continue smoothly | 7 | Units and warrant classes, mostly 5–6 shared bars: left unmerged, flagged |

So in 2,069 of 2,080 pairs **the as-traded symbol during a shared run is the one that stops (or jumps) at its
end**, and the other is the successor. Every known case resolves this way:

| Case | Shared run | Result |
|---|---|---|
| FB / META | 2016-01-04 – 2022-06-08 | FB stops, META continues: FB until 2022-06-08, then META. FB's later segment (from 2025-06-26, $39.91) is a different security |
| OSTK / BYON / BBBY / NXH | OSTK–NXH to 2023-11-03; BYON–NXH 2023-11-06 – 2025-08-28; BBBY–NXH 2025-08-29 – 2026-08-14 | One chain: OSTK → BYON → BBBY → NXH |
| Bed Bath & Beyond | BBBY segment 2016-01-04 – 2023-05-02 | No twin: its own security, separated from BBBY's 2025 segment by a gap of more than two years |
| DWAC / DJT | 2021-09-30 – 2024-03-25 | DWAC then DJT; DWAC's 2016–2018 segment is a different security |
| SIVB / SIVBQ, FRC / FRCB | both stop | Tie: kept as one security, symbol by the tie rule |
| BKH / HZN | 2016-01-04 – 2026-11-13 | BKH until 2026-11-13, then HZN |

**Gaps between traded bars** and the price across them:

| Gap (sessions) | Gaps | Close moved ≥ 2× across the gap |
|---|---|---|
| 21–60 | 3,631 | 52 (1%) |
| 61–120 | 362 | 33 (9%) |
| 121–250 | 207 | 71 (34%) |
| 251–500 | 165 | 72 (44%) |
| > 500 | 557 | 304 (55%) |

Short gaps are halts and thin trading, long ones mostly new securities. Landing near $10 or $25 is not a usable
signal (SPACs and preferreds sit there anyway).

**Splits by symbol.** Of 6,762 past splits, 4,421 have a traded bar under the action's symbol within 3 days of
the ex-date, 2,140 name a symbol with no SIP bars at all (OTC foreign names such as `…F`), and 201 name a symbol
that trades but not near the date. Attaching actions by (symbol, date) works.

## Design

### 0. Principles

- Raw and staged stay as they are; everything here is a pure function of staged data (and of the type sources
  below), rebuilt in full on each run.
- **Identity uses hindsight; the universe does not.** Knowing that FB became META is a historical fact, not a
  signal, so identity is built from the whole history. `universe_daily` uses only sessions before d: no
  "delisted later", no future type changes beyond what section 2 states.
- PIT: queries with `as_of` read bars and actions ingested by `as_of`; identity and type are rebuilt, not
  versioned. A rebuild can move history between securities (a newly discovered twin); the build report says
  how many security-days changed.

### 1. Identity (`pit.identity`, Python, curated Parquet)

Python rather than dbt: the run logic is easier to write and test in polars, and it runs in about a minute.
Output under `data/curated/identity/`, read by dbt as sources.

1. **Traded bars**: `volume > 0`. Filler bars are kept aside as `no_trade` sessions.
2. **Twins**: group traded bars by (session, open, high, low, close, volume); a key held by more than one symbol
   is a shared bar. A **twin pair** is two symbols sharing ≥ 5 bars; its **run** is first to last shared session.
3. **Segments**: cut each symbol's traded series
   - at a gap of more than 250 sessions;
   - at a gap of 61–250 sessions when the close moves ≥ 2× across it;
   - at the edges of a twin run, for the **old name** of the pair (the one that stops or jumps when the shared
     run ends while the other carries on), when both symbols also trade with different bars on the same
     sessions before the run (cut at its first shared bar) or after it (cut at its first bar after). Two
     different bars on one day cannot be one security: the old name carried another one then, a ticker handed
     to the acquirer in a merger (Primo's PRMW taken by Cott the next day) or reused after a short pause
     (EAGL). This replaced a price-jump rule after the first production run, which left 20 securities mixing
     two companies.
4. **Links** between segments:
   - twin runs (≥ 5 shared bars);
   - `name_changes` without a twin run (renames newer than the last discovery, before the quarterly refresh
     fetches the back-mapped copy): old symbol's last traded bar within 3 sessions before the process date, new
     symbol's first traded bar on or after it, close within ×1.5 or matching a split on that date;
   - `unit_splits` are **not** links: the unit, the share and the warrant are different securities.
5. **Securities** are the connected components of linked segments. On each session the security has one bar
   (twins are identical) and one **as-traded symbol**: among the segments carrying that session, the one whose
   twin run ends first; on a tie, the one that is the `old_symbol` of a later name change; then the symbol
   without a bankruptcy suffix (`Q` fifth letter); then the shorter, then alphabetical. Ties are flagged.
6. **`security_id`**: a stable hash of the first as-traded symbol and the first session. It changes only if the
   start of the security changes (for example a newly discovered older twin).
7. **End**: the last traded session, and how it ended, from an action within 10 days of it: `cash_merger`,
   `stock_merger` (with the acquirer's `security_id` when it is in the data), `worthless`, `active` (traded in
   the last 5 sessions), else `delisted_unknown`. The survey says this is mostly unknown before 2020.

Tables:

| Table | Grain | Columns |
|---|---|---|
| `security_days` | (session, security) | `security_id`, `session`, `symbol` (as-traded), OHLCV, `vwap`, `trade_count`, `no_trade`, `fetch_symbol` |
| `security_symbols` | (security, symbol period) | `security_id`, `symbol`, `valid_from`, `valid_to`, `fetch_symbol` |
| `securities` | security | `security_id`, `first_session`, `last_session`, `end_reason`, `successor_id`, type columns (section 2), `flags` |
| `identity_report` | build | counts: securities, twins merged, ties, cuts by rule, unlinked name changes, changed security-days |

**First production run** (2026-11-26, scratch output, 90 s on the host):

| | |
|---|---|
| Symbols / traded bars in | 24,498 / 27,401,431 |
| Segments / cuts | 25,347; long gap 722, gap with a 2× move 102, twin edges 25 (3 at the start, 22 at the end) |
| Links | 2,084 twin pairs (7 left apart, both carrying on smoothly), 556 by name change only |
| **Securities** | **22,819**; 1,538,381 back-mapped bars merged away |
| Ends | active 13,157; delisted, reason unknown 6,811; cash merger 1,421; stock merger 729; renamed to a symbol outside the data 668; worthless 33 |
| Flags | tie 64 securities (61,477 days, SIVB/SIVBQ-style full copies); twin cut 47; smooth twin 14; conflict 3 (1,490 days, mostly one NTN/BTX/ERNA tangle) |
| Bars in two securities | 2,127: two smooth-twin pairs (AEON/PMGM 392, NUVB/PANA 113) and 1,508 pairs of ≤ 4 identical bars (when-issued `…V` symbols, units, coincidences on illiquid days), under the 5-bar threshold |

Every known case in the validation table resolves as expected. Of the 675 renames left unlinked, 495 point to a
symbol with no SIP bars (OTC, foreign); most of the rest are share-class consolidations (CUK → CCL, CWEN.A →
CWEN) and rights or notes, which are different securities, so not linking them is right.

`fetch_symbol` is the symbol whose vendor series carries that period (for back-mapped chains, the current
symbol carries all of it; for a reused ticker, the old symbol with a date range). P2 fetches 1-minute bars by
(`fetch_symbol`, period).

### 2. Security type

Needed per security: `common`, `adr`, `fund` (ETF, ETN, closed-end fund), `preferred`, `warrant`, `unit`,
`right`, `spac` (blank-check common before its merger), `unknown`. A cascade, first answer wins, each answer
recording its source:

1. **Symbol shape** (deterministic for most non-common equity): `.WS`/5th-letter `W` → warrant; `.U`/`U` →
   unit; `.RT`/`R` → right; `.PR*`/`-P` → preferred.
2. **Fund lists**:
   - SEC *Investment Company Series and Class* annual files, 2010–2026: every fund class ticker that existed in
     each year (open-end funds and ETFs; closed-end funds only partly). A ticker counts as a fund for segments
     that overlap that year, which keeps a reused ticker from inheriting it.
   - Nasdaq Trader `otherlisted.txt` `ETF` flag and `nasdaqlisted.txt` (current), fetched daily from now on so
     the history builds up.
3. **Names**: the Alpaca asset name (current and inactive assets, 76–92% of eligible names depending on the
   year): `ETF`/`ETN`/`Fund`/`Trust` (not `Realty`/`Bancorp`), `American Depositary`/`ADS`/`ADR`, `Acquisition
   Corp`.
4. **SEC filer data** for names still on EDGAR: `company_tickers_exchange.json` (current ticker → CIK), then the
   CIK's SIC code: 6770 blank checks → `spac`, 6726 investment offices → `fund`, a 20-F/40-F filer → `adr`
   candidate.
5. **SEC fail-to-deliver files** (semi-monthly, symbol + CUSIP + description): the period name and CUSIP of
   most securities, delisted ones included (probe below), which settles type and reuse (a CUSIP change across a
   gap) for the years where actions are missing.
6. Else `unknown`.

Every source is a new raw dataset (fetched once, then on a schedule), staged, and joined by symbol and date.
The type report gives, per year, the eligible securities by type and source, and the `unknown` share.

*Built 2026-11-27* (`pit.security_types`, build step 3; in `eod` after `identity`, before `dbt_build`). It writes
`curated/identity/security_types.parquet`, one row per (security, symbol period) with `type`, `source`,
`evidence` and `flags`, and `type_report.json`. Changes from the plan above, found on production data:

- Order: SEC fund list, symbol shape, Nasdaq ETF flag, then text from fail-to-deliver descriptions (dated) and
  Alpaca/Nasdaq names (the symbol's current holder only). A specific text answer (fund, ADR, SPAC …) beats
  `common`, because fail-to-deliver text is cut at 30 characters and often loses "ETF"/"ADR".
- Two more types: `debt` (exchange-traded notes) and `when_issued` (`.WI`, fifth letter V), both not eligible.
- An ADS of a preferred share (ITUB, BBD, PBR.A) is `adr`; Nasdaq's "Closed End Fund" label on BDCs is ignored
  (BDCs are common); closed-end "… Trust" names without "Fund" (RVT, PPR, GBAB) are funds by a name rule.
- SPACs: by name, or by structure (a unit of the same root up to 200 days earlier, at the start of a security,
  and either opening near $10 or ending with its unit as the security carries on under the target's symbol:
  IPOE → SOFI, CCIV → LCID). A SPAC merging under its own symbol is split where its description changes (7).
  SIC codes were not needed.

First run (3 s, 2.75 GB peak on the host): 22,819 securities, 25,382 periods; liquid securities (≥ 20 days
with close ≥ $1 and ≥ $1M traded) per year in eligible types 3,890–5,253, of which `unknown` 0–0.35% (mostly
bankrupt `Q` symbols). Known misses: PSTH (a SPAC with no units, typed common), ADRs whose names never say
ADR (TSM, typed common, still eligible), registered CLO funds (OXLC, ECC, typed common). Inside the Airflow
container `pit.identity` plus `pit.security_types` took 77 s with the container at 5.2 GiB at most (1.9 GiB idle,
15.6 GiB limit), on 2026-11-27 after `eod`.

#### 2a. SEC probe (2026-11-26)

| Source | What came back | Coverage |
|---|---|---|
| Fail-to-deliver (`cnsfailsYYYYMM{a,b}.zip`) | 414 half-month files, 2009-07 to 2026-10 (SEC lists none for 2019-10a and 2023-08b), 0.9–1.7 MB zipped each, under four different paths; pipe-delimited `SETTLEMENT DATE\|CUSIP\|SYMBOL\|QUANTITY (FAILS)\|DESCRIPTION\|PRICE`, Latin-1, a trailer line | Q1 2016 alone: 4,311 of 4,738 eligible names (91%); 1,204 of the 1,312 eligible names missing from today's Alpaca assets (92%), with names such as `RUBY TUESDAY INC (GA)`, `ORBITAL ATK INC` |
| Fund series and classes, one CSV a year | 2010–2026, 7–8 MB and ~40k classes a year; the 2016 file has snake-case headers and writes nulls as `[NULL]`, the others have title-case headers; the ticker is optional | 704 of Q1 2016's eligible names are fund tickers (ETFs); ETFs without a ticker in the file still say `ETF` in their fail-to-deliver description |
| `company_tickers_exchange.json` | `{"fields": ["cik", "name", "ticker", "exchange"], "data": [...]}`, 10,400 rows, current only | Today's listings only |

#### 2b. SEC fetch plan (build step 2b)

*Built 2026-11-26* (`pit.sec`, `collect_sec`, the `sec_tickers` task in `eod` and `sec_reference` in
`universe_refresh`). A dry run against sec.gov (2 fund files, 4 half-months, the tickers) took 6 s; a second run
fetched nothing new. Deploying needs `SEC_USER_AGENT` passed to the Airflow container (`docker-compose.yml`),
which means recreating that container outside the collection window.

**One client** (`pit.sec`): the User-Agent comes from `SEC_USER_AGENT` in `.env` and nothing is sent without
it; at most 5 requests a second (SEC allows 10); 429/503 retried with backoff; redirects followed. The
User-Agent is never written to raw, staged, logs or notifications.

**Raw is the file as received.** These are files (zip, CSV, JSON), not API pages, so raw keeps the bytes under
the usual layout, named by fetch time plus the SEC file name:
`raw/source=sec/ingest_date=D/<dataset>/<ingested_at>__<sec file name>`. Wrapping a 1.2 MB zip as JSON text
would make it about 5 MB and no longer what SEC served. Staging reads these files; `restage` learns the
extension.

| Dataset | What | First fetch | Then | Staged (one row per) |
|---|---|---|---|---|
| `sec_ftd` | Fail-to-deliver half-months, from 2016-01a | 258 files, ~310 MB, ~1 min of requests at 5/s | New periods as SEC posts them (about a month late), checked by `universe_refresh` and by a weekly check | (period, symbol, CUSIP, description): first and last settlement date, days listed, last price; about 4M rows, ~50 MB |
| `sec_fund_classes` | Fund series/class CSV, 2016–2026 | 11 files, ~90 MB | The new year's file (published around June), checked quarterly | (year, class id): CIK, entity, org type, series, class name, ticker |
| `sec_tickers` | `company_tickers_exchange.json` | 1 request | Weekly, Fridays in `eod` with the full corporate actions | (cik, ticker): name, exchange |

- The file list for fail-to-deliver and fund files comes from SEC's pages, not from a URL pattern (four
  paths; the 2016 fund file has no year in its name, so the year comes from the link text, checked against the
  header format). A period is fetched once; one already in raw is skipped, so a run resumes after a failure.
- Staging handles both fund header formats and `[NULL]`, Latin-1 fail-to-deliver text, the trailer line, blank
  symbols (quarantined), and keeps the description exactly as given (trailing spaces trimmed).
- Rules: `error` on a missing symbol or CUSIP; `warn` on a CUSIP whose check digit is wrong.
- **When**: the first fetch by hand after the US close (`python -m pit.sec backfill`), like the first universe
  refresh; afterwards inside `universe_refresh` (quarterly) and `eod` (weekly tickers), all outside the
  collection window. These requests do not touch Alpaca's rate limit or the IB Gateway.
- **Not now**: per-company filings (`data.sec.gov/submissions`, SIC codes). They cover only companies still on
  EDGAR under today's ticker, which Nasdaq Trader and the Alpaca names already describe; revisited if the type
  report shows a gap SIC would close. Fail-to-deliver before 2016 (156 more files) is not needed for bars that
  start in 2016.

Tests: header variants and `[NULL]`, the fail-to-deliver trailer and Latin-1, link parsing (four paths, the
2016 name), refusing to run without `SEC_USER_AGENT`, skipping periods already fetched, and a 429 retry.

### 3. Corporate actions per security

- Stage every action type (today only splits and dividends are staged), keeping all symbol and CUSIP fields.
- Attach an action to a security by its symbol and date: the security whose as-traded symbol is that symbol
  within 3 sessions of the date (merger acquirees by `effective_date`, name changes by `process_date`). Actions
  on CUSIP-like or OTC symbols stay unattached and are counted.
- `adjust()` works per `security_id` (splits, then dividends for `adjust="all"`). Dividends on funds are kept
  (benchmarks); `adjust()` no longer needs a type filter once it works per security.
- Rule 15 (split-like jump without a split) runs per security on traded bars, `warn`, calibrated first on the
  liquid set. A jump that coincides with a cut or a link is reported as identity, not price.

### 4. Daily universe (`universe_daily`, dbt)

One row per (session d, security with a bar on d−1), from sessions before d only:

| Column | Definition |
|---|---|
| `symbol` | As-traded symbol on d−1 |
| `prev_close` | Last traded close before d (filler bars skipped) |
| `prev_close_age` | Sessions since that close |
| `adv20_usd` | Median over the 20 sessions before d of close × volume, a no-trade session counting as 0 |
| `age_sessions` | Traded sessions since the security's first bar |
| `type`, `is_adr`, `is_fund` | Section 2 |
| `eligible` | `type in ('common', 'adr', 'unknown')`, `prev_close >= 1`, `adv20_usd >= 1,000,000`, `age_sessions >= 20`, `prev_close_age <= 5` |
| `rank_adv` | Rank of `adv20_usd` among eligible securities on d |

Thresholds in dbt vars, as decided in P1. The exit price for a position whose security stops trading is its last
traded close (P1 decision 4).

### 5. Queries (`pit.query`)

- `get_universe(session, *, eligible_only=True)` → `security_id`, `symbol`, `type`, flags, `adv20_usd`, `rank_adv`.
- `get_daily_bars(security_ids, start, end, *, as_of=None, adjust="all" | "split" | "none", traded_only=True)`
  → by security, as-traded symbol on each row.
- `get_security(symbol, session)` → the security that traded under a symbol on a date (for reading old notes and
  for P2).

### 6. Schedule

- `pit.identity` and `pit.security_types` run in `eod` after the fetches, before `dbt_build` (about 80 s; built
  2026-11-27).
- Type sources: the Nasdaq Trader directories daily in `eod`; the SEC files weekly (Friday, with the full
  corporate actions); the SEC fail-to-deliver and fund files once by hand, then new periods in `universe_refresh`
  (section 2b).
- A new symbol seen by `eod` (a rename) gets its full history at the next quarterly refresh; until then the
  name-change link (section 1.4) joins it to its predecessor.

## Validation

**Known cases** (pytest on a frozen sample, dbt tests on the full build):

| Case | Expected |
|---|---|
| FB → META | One security; FB to 2022-06-08, META from 2022-06-09; no session twice |
| FB from 2025-06-26 | A separate security (a fund) |
| OSTK → BYON → BBBY → NXH | One security, four symbol periods, no duplicate sessions |
| Bed Bath & Beyond (BBBY 2016 – 2023-05-02) | Its own security, `delisted_unknown` or worthless, not linked to NXH |
| DWAC 2016–2018 vs DWAC → DJT | Two securities |
| SIVB, FRC | One security each (twin tie), ending 2023-03-09 / 2023-04-28 |
| TWTR, ATVI, XLNX | End as `cash_merger` / `stock_merger` |
| GE 1:8 reverse split (2021-08-02) | One security; split-adjusted series continuous; rule 15 silent |
| BKH → HZN (2026-11-16) | One security |

**Invariants**: no (session, OHLCV) tuple in two securities unless flagged; one row per (session, security);
every traded bar belongs to exactly one security; every symbol ever eligible maps to a security.

**Reports**: securities and eligible securities per session by type and year; `unknown` share; actions attached
vs not; ties and cuts by rule; and the P2 sizing (securities ever eligible, small/mid subset).

## Build order

1. **Stage all action types**; `pit.identity` (sections 1 and 3) with the known-case tests; identity report on
   production data, reviewed before going further.
2. **Type sources** (acquire): (a) Nasdaq Trader directories, daily in `eod` (built); (b) SEC fail-to-deliver,
   fund class files and tickers (section 2b), first fetch by hand after the close.
3. **Type cascade** and its report.
4. **`adjust()` per security**, rule 15 per security.
5. **`universe_daily`** in dbt, `get_universe` / `get_daily_bars` / `get_security`, coverage report.
6. Docs: README data table, `docs/setup.md`, and P2's fetch list.

## Decisions (2026-11-26)

1. **`unknown` type is eligible, flagged** (`type_unknown`). With funds at half the listings, an `unknown` that
   is really a fund would leak into the stock universe; excluding it would lose delisted stocks the type sources
   miss. Revisited once the type report sizes it.
2. **SPAC common shares are excluded** before their merger (`type = 'spac'`); after the merger the security is
   the operating company and eligible.
3. **BDCs and REITs are common stock; closed-end funds are funds** and excluded.
4. **SEC requests** send a contact address in the User-Agent, as SEC asks of automated clients. The address is
   the user's to give and lives in `.env` (`SEC_USER_AGENT`), never in the repository; the SEC sources (build
   step 2) wait for it.
