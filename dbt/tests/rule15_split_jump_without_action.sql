-- Rule 15: an overnight split-ratio jump (staging rule 6) with no split on that ex_date in corporate actions.
select distinct b.symbol, (b.bar_start at time zone 'America/New_York')::date as day
from {{ ref('bars_current') }} as b
where list_contains(b.flags, 'overnight_split_ratio_jump')
  and not exists (
      select 1 from {{ ref('corporate_actions_current') }} as a
      where a.symbol = b.symbol
        and a.type in ('forward_split', 'reverse_split')
        and a.ex_date = (b.bar_start at time zone 'America/New_York')::date
  )
