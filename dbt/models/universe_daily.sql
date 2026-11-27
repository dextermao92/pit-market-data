-- One row per (session d, security with a row on d-1 in identity's security_days), from sessions before d only:
-- who a strategy could have traded at d's open. Unadjusted prices on purpose: the $1 floor and dollar volume are
-- what the market showed then. Type comes from pit.security_types (hindsight about what the security is, not
-- about its prices). docs/stage2/p1b-curate-design.md section 4.
{% set data = env_var("DATA_DIR", "data") %}
-- Also written as Parquet so research reads it with polars (pit.query.get_universe) without opening
-- curated.duckdb, whose lock would stop the next eod dbt_build.
-- Sorted by session so a one-day read skips most row groups.
{{ config(post_hook="copy (select * from {{ this }} order by session, security_id) to '" ~ data
                    ~ "/curated/universe_daily.parquet' (format parquet, compression zstd)") }}
with cal as (
    select day, row_number() over (order by day) as sid
    from read_parquet('{{ data }}/curated/market_calendar.parquet')
    where is_session
),

days as (
    select d.security_id, d.session, d.symbol, d.close, d.volume, d.no_trade, c.sid
    from read_parquet('{{ data }}/curated/identity/security_days/*.parquet') as d
    join cal as c on c.day = d.session
),

-- features as of the end of each security-day (d-1)
feat as (
    select
        security_id, session as last_session, sid, symbol,
        last_value(case when not no_trade then close end ignore nulls) over w_all as prev_close,
        last_value(case when not no_trade then sid end ignore nulls) over w_all as close_sid,
        median(case when no_trade then 0 else close * volume end) over w_20 as adv20_usd,  -- no trade counts as 0
        count(*) filter (where not no_trade) over w_all as age_sessions
    from days
    window w_all as (partition by security_id order by sid rows between unbounded preceding and current row),
           w_20 as (partition by security_id order by sid rows between 19 preceding and current row)
),

typed as (
    select f.*, coalesce(t.type, 'unknown') as type
    from feat as f
    left join read_parquet('{{ data }}/curated/identity/security_types.parquet') as t
        on t.security_id = f.security_id and f.last_session between t.valid_from and t.valid_to
),

universe as (
    select
        n.day as session, t.security_id, t.symbol, t.type,
        t.type = 'adr' as is_adr, t.type = 'fund' as is_fund, t.type = 'unknown' as type_unknown,
        t.prev_close, n.sid - t.close_sid as prev_close_age, t.adv20_usd, t.age_sessions,
        t.type in ('common', 'adr', 'unknown')
            and t.prev_close >= {{ var('universe_min_price') }}
            and t.adv20_usd >= {{ var('universe_min_adv_usd') }}
            and t.age_sessions >= {{ var('universe_min_age_sessions') }}
            and n.sid - t.close_sid <= {{ var('universe_max_close_age') }} as eligible
    from typed as t
    join cal as n on n.sid = t.sid + 1  -- the next session: d
)

select
    *,
    case when eligible then rank() over (partition by session, eligible order by adv20_usd desc) end as rank_adv
from universe
