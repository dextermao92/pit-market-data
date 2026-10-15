-- SCD2 keyed on Alpaca asset_id (stable across renames). Validity comes from ingested_at, not dbt run time,
-- so rebuilding or backfilling gives the same history.
with snapshots as (
    select
        asset_id, symbol, name, exchange, asset_class, status, tradable, ingested_at,
        md5(concat_ws('|', symbol, name, exchange, asset_class, status, tradable::varchar)) as attrs
    from {{ source('staged', 'assets') }}
),

changes as (
    select *, lag(attrs) over (partition by asset_id order by ingested_at) as prev_attrs
    from snapshots
)

select
    asset_id, symbol, name, exchange, asset_class, status, tradable,
    ingested_at as valid_from,
    lead(ingested_at) over (partition by asset_id order by ingested_at) as valid_to,
    valid_to is null as is_current
from changes
where prev_attrs is distinct from attrs
