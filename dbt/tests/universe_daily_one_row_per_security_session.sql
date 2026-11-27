-- universe_daily: one row per (session, security); a duplicate means identity put a security-day in twice.
select session, security_id, count(*) as n
from {{ ref('universe_daily') }}
group by 1, 2
having count(*) > 1
