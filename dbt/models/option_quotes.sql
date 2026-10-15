-- Two-sided option quotes with the usual derived fields. Other quote states stay in staged.
select
    ingested_at, underlying, trading_class, spot, con_id, local_symbol, expiry, strike, "right",
    bid, ask, (bid + ask) / 2 as mid, ask - bid as spread, (ask - bid) / ((bid + ask) / 2) as rel_spread,
    strike / spot - 1 as moneyness,
    expiry - (ingested_at at time zone 'America/New_York')::date as dte,
    last, volume, open_interest, iv, delta, gamma, vega, theta, flags
from {{ source('staged', 'option_snapshots') }}
where quote_status = 'two_sided'
