-- Rule 16: the underlying price IBKR reported with a snapshot lies inside the Alpaca 5-min bar containing it
-- (0.1% slack for timing). Only underlyings Alpaca carries; index underlyings such as SPX have no second source.
-- Violations looked into are listed with the reason in seeds/rule16_acknowledged.csv and skipped.
with spots as (
    select distinct underlying, spot, spot_time from {{ source('staged', 'option_snapshots') }}
    anti join {{ ref('rule16_acknowledged') }} as a using (underlying, spot_time)
)

select s.*, b.low, b.high
from spots as s
join {{ ref('bars_current') }} as b
  on b.symbol = s.underlying
 and s.spot_time >= b.bar_start and s.spot_time < b.bar_start + interval 5 minute
where s.spot < b.low * 0.999 or s.spot > b.high * 1.001
