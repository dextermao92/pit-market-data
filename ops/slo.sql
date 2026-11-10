-- The three SLIs of ops/slo.md, per session and over the window (last row). Run it with `python -m pit.slo`, which sets
-- the inputs: table sessions(session DATE, open TIMESTAMPTZ, close TIMESTAMPTZ) from the exchange calendar (so
-- half days count right and a session with no runs at all still counts), variables data_dir and n_symbols.
-- A slot is the 5-min run time in the run_id (scheduled__<slot>), not the start time: a late retry still counts
-- toward its own slot (the 2026-10-23 coverage miscount).
with slots as (
    select session, unnest(generate_series(open, close - interval 5 minute, interval 5 minute)) as slot
    from sessions
),
runs as (
    select regexp_extract(run_id, 'scheduled__(.+)$', 1)::timestamptz as slot, status, finished_at
    from read_parquet(getvariable('data_dir') || '/pipeline_runs/dataset=option_snapshots/*/*.parquet', union_by_name = true)
    where starts_with(run_id, 'scheduled__')
),
option_slots as (
    select s.session, s.slot,
           coalesce(bool_or(r.status = 'success'), false) as ok,
           coalesce(bool_or(r.status = 'success' and r.finished_at <= s.slot + interval 5 minute), false) as on_time
    from slots s left join runs r on r.slot = s.slot
    group by s.session, s.slot
),
bars as (  -- usable 5-min bars (staged, so quarantined bars count as missing), any version
    select s.session, count(distinct (b.symbol, b.bar_start)) as present
    from sessions s
    join read_parquet(getvariable('data_dir') || '/staged/equity_bars/*/*.parquet', union_by_name = true) b
      on b.bar_start >= s.open and b.bar_start < s.close
    group by s.session
),
per_session as (
    select o.session,
           count(*) as option_slots,
           count(*) filter (where o.ok) as option_ok,
           count(*) filter (where o.on_time) as option_on_time,
           count(*) * getvariable('n_symbols') as bars_expected,
           coalesce(any_value(b.present), 0) as bars_present
    from option_slots o left join bars b on b.session = o.session
    group by o.session
)
select coalesce(session::varchar, 'window') as session,
       sum(option_slots)::int as option_slots,
       sum(option_ok)::int as option_ok,
       round(100 * sum(option_ok) / sum(option_slots), 2) as sli1_coverage_pct,
       round(100 * sum(option_on_time) / sum(option_slots), 2) as sli2_on_time_pct,
       sum(bars_present)::int as bars_present,
       sum(bars_expected)::int as bars_expected,
       round(100 * sum(bars_present) / sum(bars_expected), 2) as sli3_bars_pct
from per_session
group by rollup (session)
order by session is null, session;
