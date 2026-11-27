-- NYSE calendar, one row per calendar day: sessions with their hours, half days, and holidays by name.
-- Written by pit.market_calendar (eod's first task) from exchange_calendars; reaches about a year ahead.
select *
from read_parquet('{{ env_var("DATA_DIR", "data") }}/curated/market_calendar.parquet')
