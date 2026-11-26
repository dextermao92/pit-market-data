# Stage 2 · P1a data survey (2026-11-25)

The first full `universe_refresh` (manual, 2026-11-25 23:16 ET, 63 min) acquired the whole market as received.
This survey looks at that data before anything is cleaned, as the design asks
([p1-universe-design.md](p1-universe-design.md), build step 4). P1b is designed from it.

Inputs: the production data directory after the run: staged `daily_bars` deduplicated to the latest fetch per
(symbol, session), the latest `assets` snapshot, the full `corporate_actions` fetch of 2026-11-25 and
`cache/universe/candidates.parquet`. Sessions are the XNYS calendar, 2016-01-04 to 2026-11-25 (2,741 sessions).
Prices are as traded (unadjusted).

## Summary

| | |
|---|---|
| Symbols with bars | 24,498 |
| Daily bars | 29,268,157 (all on XNYS sessions; none on holidays or weekends) |
| Bars that fail OHLC sanity (high < low, open/close outside the range, price ≤ 0) | 0 |
| Zero-volume filler bars | 1,866,726 (6.4%) |
| Symbol pairs with identical bars (back-mapped copies) | 2,080 pairs sharing ≥ 5 bars; 1,539 sharing ≥ 250 |
| Symbols with a gap > 250 sessions between traded bars (ticker reuse) | 684 |
| Eligible per session (≥ $1, ≥ $1M median dollar volume), before type filter and dedupe | 4,300 (2016) → 6,000 (2026) |

What it means for P1b, in short: the bars themselves are clean; the work is **identity** (one security under
several symbols, several securities under one symbol) and **type** (half of what passes the liquidity filter is
a fund). Both are bigger than the design assumed, and the corporate actions explain them only from 2019–2020 on.

## 1. Coverage by year

| Year | Bars | Symbols | Symbols per session | Eligible per session | Eligible names in the year | Small/mid proxy per session¹ |
|---|---|---|---|---|---|---|
| 2016 | 2,158,339 | 9,327 | 8,565 | 4,313 | 5,647 | 2,302 |
| 2017 | 2,204,551 | 9,599 | 8,783 | 4,443 | 5,886 | 2,270 |
| 2018 | 2,290,007 | 9,913 | 9,124 | 4,607 | 6,115 | 2,277 |
| 2019 | 2,372,121 | 10,261 | 9,413 | 4,505 | 6,033 | 2,246 |
| 2020 | 2,472,488 | 11,206 | 9,773 | 4,871 | 7,161 | 2,478 |
| 2021 | 2,840,693 | 13,333 | 11,273 | 5,796 | 8,478 | 2,875 |
| 2022 | 3,083,069 | 13,328 | 12,283 | 5,294 | 7,358 | 2,628 |
| 2023 | 2,971,517 | 13,135 | 11,886 | 4,850 | 6,657 | 2,416 |
| 2024 | 2,928,786 | 12,925 | 11,622 | 5,018 | 6,852 | 2,384 |
| 2025 | 2,999,224 | 13,765 | 11,997 | 5,534 | 7,821 | 2,552 |
| 2026 | 2,947,362 | 14,664 | 12,984 | 6,046 | 8,130 | 2,612 |

¹ Eligible, previous close < $50 and 20-session median dollar volume < $20M: a rough stand-in for the small/mid
names the strategies target, before dedupe and type filtering.

The curve has no step down in the early years; the only step is the 2020–2021 SPAC and listing wave, which is
real. Survivorship is handled by the discovery, not the assets list: of the names eligible on 2016-12-30, only
3,514 of 4,641 (76%) are in today's `/v2/assets`; on 2024-12-31 it is 4,983 of 5,427 (92%). Every symbol whose
last bar is in 2016 or 2017 (1,296) is missing from the assets list and was found only by brute force or the
corporate actions.

## 2. Where the symbols came from

| Sources | Symbols |
|---|---|
| brute force + assets + corporate actions | 10,137 |
| brute force + corporate actions | 5,161 |
| brute force + assets | 3,149 |
| corporate actions only | 2,629 |
| brute force only | 1,304 |
| assets + corporate actions | 1,298 |
| assets only | 770 |
| (config symbols, all also found elsewhere) | 50 |

Symbol shapes: 19,799 plain 1–4 letters; 2,107 five letters ending W/U/R/Z (warrants, units, rights);
610 other five-letter symbols; 1,051 `.PR*` preferreds; 495 `.WS` warrants; 245 `.U` units; 121 `.RT`
rights; 43 `.X` share classes (BRK.A and the like). The non-plain shapes come almost entirely from the corporate
actions, since brute force covers only 1–4 letters.

Life spans are short: the median symbol has 44 months of bars, a quarter have 18 or fewer, and 337 have a single
month.

## 3. Assets list

33,589 assets: 13,261 active and 2,891 inactive on the listing exchanges (NASDAQ, NYSE, ARCA, BATS, AMEX,
TXSE), the rest OTC (17,437, almost all inactive and without SIP bars). Only 502 listed assets (all but one
inactive) have no bars since 2016: they ended before the history starts.

**There is no security-type field.** `attributes` carries trading flags (`has_options`, `overnight_tradable`,
`fractional_eh_enabled`, `ptp_*`), not a type. Name keywords on the listed assets:

| Name keyword | Active | Inactive |
|---|---|---|
| ETF / ETN / Fund / Trust / Shares / Index / Portfolio | 7,518 | 868 |
| none of the below (mostly common stock) | 4,500 | 1,340 |
| Warrant | 447 | 30 |
| Unit | 297 | 365 |
| Preferred / notes / depositary shares representing | 282 | 175 |
| Right | 121 | 74 |
| Acquisition Corp (SPAC common) | 61 | 30 |
| ADR / ADS / American Depositary | 35 | 9 |

The keyword match is rough (`Trust` also hits REITs and banks, ADRs are often just "Ordinary Shares"), but the
scale is clear: **funds outnumber common stocks among active listings**. Of the 6,120 names eligible on
2026-11-25, 2,643 have a fund-like name, 220 an ADR name, and 3,257 neither.

## 4. Corporate actions

395,444 actions in the full fetch (2016-01-04 to 2027-11-23, 3,800 dated in the future: announced dividends and
splits). Counts by process year, dividends aside:

| Type | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| forward_splits | 58 | 86 | 60 | 32 | 82 | 137 | 93 | 93 | 99 | 90 | 159 |
| reverse_splits | 221 | 162 | 163 | 193 | 737 | 428 | 552 | 760 | 674 | 801 | 1,127 |
| name_changes | 0 | 2 | 2 | 53 | 304 | 576 | 399 | 500 | 700 | 670 | 547 |
| stock_mergers | 0 | 2 | 0 | 36 | 741 | 625 | 474 | 568 | 410 | 187 | 199 |
| cash_mergers | 0 | 0 | 0 | 0 | 86 | 187 | 171 | 370 | 357 | 401 | 530 |
| stock_and_cash_mergers | 0 | 0 | 0 | 0 | 8 | 28 | 19 | 34 | 34 | 71 | 63 |
| unit_splits | 0 | 0 | 0 | 0 | 13 | 69 | 110 | 104 | 69 | 81 | 67 |
| worthless_removals | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 10 | 131 | 147 | 237 |
| spin_offs | 0 | 1 | 0 | 1 | 5 | 11 | 14 | 40 | 48 | 72 | 59 |
| stock_dividends | 0 | 0 | 15 | 12 | 37 | 50 | 45 | 49 | 33 | 35 | 43 |
| rights_distributions | 0 | 0 | 0 | 0 | 4 | 14 | 12 | 6 | 20 | 36 | 31 |

Also: 376,631 cash dividends (24k in 2016 rising to 49k in 2026), and four types that start only in 2026
(capital_gains_distributions 695, reorganizations 121, partial_calls 79) or are tiny (redemptions 16).

**Splits and dividends cover the whole history; identity events do not.** Name changes and mergers are
essentially absent before 2019 and sparse in 2019; cash mergers start in 2020, worthless removals in 2023. Some
action symbols are CUSIP-like codes (`007CVR020`), mostly CVRs and escrow shares; they never match a bar.

Splits check out where they can be seen: of 6,807 splits, 4,209 have a bar on the ex-date under the same symbol,
and in 3,935 of those (93%) the close moved by about the inverse of the split ratio.

## 5. Bar quality

- **OHLC sanity: no failures.** No high < low, no open or close outside the range, no price ≤ 0, no null VWAP,
  no bars on non-sessions.
- **Zero-volume filler bars.** 1,866,726 bars (6.4%; 4–8% a year) have volume 0, trade count 0, VWAP 0 and
  open = high = low = close = the previous close. Alpaca fills sessions without trades, so every symbol has a
  bar on every session between its first and last bar (coverage ratio 1.0 in every liquidity bucket). 1,266
  symbols end with a run of filler bars after their last trade (median 5 sessions), and 61 start with one.
  These bars are not trades: medians, returns and "listed on d−1" must ignore them.
- **Prices.** 1.3% of bars close at or below $0.10, 5.4% between $0.10 and $1, 11.8% between $1 and $5.
- **Extreme moves.** 10,250 close-to-close moves of ≥ 2.5× or ≤ 0.4× (previous trade within 5 sessions); 3,590
  fall on a split ex-date (± 1 session). Of the 6,660 without a split, 2,179 happened on names that were above $1
  with ≥ $1M dollar volume that day. The largest are real squeezes and news (AMC and KODK in 2020–2021, GME's
  2021-02-02 drop); others are likely missing reverse splits or ticker changes. Unexplained moves peak in
  2022–2024 (1,000+ a year), the years of most reverse splits. This is what rule 15 will see on the whole market.

## 6. Identity

### Back-mapped copies (one security, several symbols)

1,538,561 (session, OHLCV) tuples belong to more than one symbol, up to nine symbols on one tuple (chains of
renames). 2,080 symbol pairs share ≥ 5 identical traded bars, 1,539 of them ≥ 250.

- **1,522 pairs are a recorded `name_change`** (old and new symbol). Alpaca serves the full history under both
  symbols: BKH/HZN share 2,733 bars, BK/BNY 2,610. Of the 3,753 name changes, 2,164 have a new symbol whose bars
  start more than 10 days before the change (back-mapped), and 2,885 have bars under both symbols.
- **19 pairs are other recorded actions** (unit splits, stock mergers).
- **~540 pairs have no linking action**: renames from before the actions start (OSTK/NXH, ELY/CALY), bankruptcy
  suffixes (SIVB/SIVBQ, FRC/FRCB, QVCAQ), and the vendor collision seen in the trial (DWAC/DJT, ORBS/OCTO).
  Identical bars are the only signal for these, and a reliable one.

The double counting is real but bounded: on 2016-12-30, 281 of 4,641 eligible names were a twin of another
eligible name the same day; on 2024-12-31, 101 of 5,427.

### Reused tickers (several securities, one symbol)

722 gaps longer than 250 sessions between traded bars, in 684 symbols (1,043 symbols with a gap over 60
sessions). The price across the gap usually jumps by an order of magnitude (ZINC 0.077 → 25.21; ASTI 0.06 →
9.10; BBLU 0.041 → 10.06), or lands near $10 or $25, the SPAC and preferred issue prices: a new security under an
old symbol. Gaps must be measured on traded bars; with filler bars counted, only 314 symbols show a gap.

### How ended symbols end

Symbols whose last traded bar is before 2026-11-18, and whether an action within 10 days explains it (merger
acquiree, worthless removal, old symbol of a name change or unit split):

| Last bar | Ended | With an action | Merger | Rename | Unit split | Worthless | In today's assets |
|---|---|---|---|---|---|---|---|
| 2016 | 645 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2017 | 651 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2018 | 625 | 0 | 0 | 0 | 0 | 0 | 132 |
| 2019 | 684 | 48 | 0 | 48 | 0 | 0 | 335 |
| 2020 | 862 | 318 | 103 | 207 | 8 | 0 | 494 |
| 2021 | 1,275 | 637 | 258 | 320 | 59 | 0 | 702 |
| 2022 | 1,173 | 571 | 252 | 234 | 85 | 0 | 394 |
| 2023 | 1,487 | 823 | 422 | 316 | 77 | 1 | 19 |
| 2024 | 1,323 | 841 | 356 | 432 | 37 | 16 | 16 |
| 2025 | 1,206 | 811 | 383 | 410 | 15 | 3 | 38 |
| 2026 | 1,397 | 933 | 500 | 385 | 11 | 13 | 161 |

3,039 symbols ended within 30 days of being eligible; 1,697 of them have an action. Since 2023 the share is high
(960 of 1,095, 88%); before 2020 it is near zero.

## What changes for P1b

1. **Filler bars are not prices.** `security_days` marks them `no_trade`. Closes, returns, the last close of a
   delisted name and gap detection use traded bars only; a no-trade session still counts as zero dollar volume
   in `adv20_usd`, which is what it was.
2. **Dedupe by identical bars first, actions second.** The design's step 2 holds, but identical OHLCV is the
   primary signal (it catches the ~540 pairs no action links); name changes then decide which symbol was
   as-traded on each day. Where no action decides, the twin that keeps trading alone after the overlap is the
   current symbol, and the overlap belongs to the symbol that traded alone before it. The known cases are the
   tests.
3. **Split reused tickers at gaps on traded bars**, with the threshold tuned between 60 and 250 sessions and a
   large price jump across the gap (or a landing near $10/$25) as the supporting signal. The design's 20
   sessions would also cut long halts; there are 1,043 symbols with a traded gap over 60 sessions and 684 over
   250.
4. **Security type needs a second source.** Alpaca has no type, and name keywords are rough and only cover
   symbols still in `/v2/assets` (76% of 2016's eligible names). Candidates: SEC EDGAR company tickers and
   SIC codes (P3 brings EDGAR anyway), Nasdaq Trader's symbol directories (current only, with an ETF flag),
   then symbol shape. `unknown` will be large for 2016–2019 delistings and must be reported per year.
5. **Ending reasons are reliable only from 2020.** Before that, almost every end is "delisted, reason unknown";
   decision 4 of the design (exit at the last close) applies to those by default. Studies should report how many
   trades exit this way per year.
6. **Rule 15 on the whole market will fire about 6,700 times over the history**, 2,200 of them on liquid names.
   Calibrate it on the liquid set first and treat a jump that coincides with a twin or a reuse gap as an identity
   problem, not a price problem.
7. **The universe is about half funds.** Until the type is known, the eligible count per session (6,000 now)
   overstates the stock universe by roughly 2×; the small/mid proxy (about 2,500 a session) is the relevant
   order of magnitude for P2's 1-minute fetch.
