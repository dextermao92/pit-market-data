-- Rule 14: regular-session 5-min bars aggregated to a day must fit inside the vendor daily bar. The daily bar
-- includes extended hours (5-min volume is ~80% of it) and official auction prices, so the checks are one-sided
-- with 0.1% slack, plus a 30% volume floor that catches a session with whole chunks of bars missing.
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
where agg.high > daily.high * 1.001
   or agg.low < daily.low * 0.999
   or agg.volume > daily.volume * 1.001
   or agg.volume < daily.volume * 0.3
