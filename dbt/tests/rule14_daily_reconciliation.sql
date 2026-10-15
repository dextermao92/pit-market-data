-- Rule 14: regular-session 5-min bars aggregated to a day must fit inside the vendor daily bar.
-- Calibrated on a year of data (2025-10 .. 2026-10), so the checks are one-sided and loose where the vendor's
-- methodology differs, and still catch a wrong price or an unadjusted split:
--   high/low: 0.5% slack. Some trade conditions (odd lots, out-of-sequence prints) don't update the official
--             daily high/low but do enter Alpaca's intraday bars; seen up to 0.32%.
--   volume:   5-min volume can't exceed the daily bar's. No lower bound: the daily bar includes extended hours
--             and the 16:00 closing auction, which on index-rebalance and quad-witching days is most of the
--             day's volume (5-min share down to 13%). Missing bars are counted directly (rule 11).
with agg as (
    select symbol, (bar_start at time zone 'America/New_York')::date as day,
           max(high) as high, min(low) as low, sum(volume) as volume
    from {{ ref('bars_current') }}
    group by all
),

daily as (
    select symbol, (bar_start at time zone 'America/New_York')::date as day, high, low, volume
    from {{ ref('daily_bars_current') }}
)

select agg.*, daily.high as daily_high, daily.low as daily_low, daily.volume as daily_volume
from agg join daily using (symbol, day)
where agg.high > daily.high * 1.005
   or agg.low < daily.low * 0.995
   or agg.volume > daily.volume * 1.001
