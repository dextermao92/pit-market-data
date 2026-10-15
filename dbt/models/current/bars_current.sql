-- Latest known version of every 5-min bar, plus how often the vendor has served it and whether it ever changed.
select
    * exclude (flags),
    flags,
    count(*) over w as fetches,
    count(distinct (open, high, low, close, volume)) over w > 1 as revised,
    min(ingested_at) over w as first_seen_at
from {{ source('staged', 'equity_bars') }}
window w as (partition by symbol, bar_start)
qualify row_number() over (partition by symbol, bar_start order by ingested_at desc) = 1
