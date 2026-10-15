select *
from {{ source('staged', 'daily_bars') }}
qualify row_number() over (partition by symbol, bar_start order by ingested_at desc) = 1
