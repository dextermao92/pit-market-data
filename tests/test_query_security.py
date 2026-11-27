"""pit.query by security: get_security, get_daily_bars (point-in-time actions, adjusted by security across a
rename) and get_universe, on a tiny curated/ and staged/ tree."""

from datetime import UTC, date, datetime

import polars as pl
import pytest

from pit import query
from pit.storage import TS

SYMBOLS = pl.DataFrame({
    "security_id": ["meta", "meta", "fund"], "symbol": ["FB", "META", "FB"], "fetch_symbol": ["META", "META", "FB"],
    "valid_from": [date(2021, 3, 1), date(2022, 6, 9), date(2025, 6, 26)],
    "valid_to": [date(2022, 6, 8), date(2026, 11, 20), date(2026, 11, 20)],
})


def day(sec, d, sym, close, volume=1000):
    return {"security_id": sec, "session": d, "symbol": sym, "fetch_symbol": sym, "open": close, "high": close,
            "low": close, "close": close, "vwap": close, "volume": volume, "trade_count": 10, "no_trade": volume == 0}


DAYS = pl.DataFrame([
    day("meta", date(2021, 3, 1), "FB", 100.0), day("meta", date(2021, 3, 2), "FB", 100.0),
    day("meta", date(2022, 6, 9), "META", 80.0), day("meta", date(2022, 6, 10), "META", 0.0, volume=0),
    day("meta", date(2023, 3, 2), "META", 40.0), day("fund", date(2025, 7, 1), "FB", 20.0),
])


def actions(ingested: datetime, rows: list[tuple]) -> pl.DataFrame:
    return pl.DataFrame([{"ingested_at": ingested, "id": i, "symbol": s, "type": t, "ex_date": e, "process_date": e,
                          "price_factor": f, "cash": c, "special": False, "flags": []} for i, s, t, e, f, c in rows],
                        schema_overrides={"ingested_at": TS, "flags": pl.List(pl.String)})


@pytest.fixture
def root(tmp_path):
    ident = tmp_path / "curated" / "identity"
    (ident / "security_days").mkdir(parents=True)
    DAYS.write_parquet(ident / "security_days" / "part-0.parquet")
    SYMBOLS.write_parquet(ident / "security_symbols.parquet")
    acts = tmp_path / "staged" / "corporate_actions"
    first = datetime(2023, 1, 5, tzinfo=UTC)
    (acts / "ingest_date=2023-01-05").mkdir(parents=True)
    actions(first, [("div", "FB", "cash_dividend", date(2021, 3, 2), None, 1.0)]).write_parquet(
        acts / "ingest_date=2023-01-05" / f"{first:%Y%m%dT%H%M%S%fZ}.parquet")
    later = datetime(2023, 3, 3, tzinfo=UTC)
    (acts / "ingest_date=2023-03-03").mkdir(parents=True)
    actions(later, [("split", "META", "forward_split", date(2023, 3, 2), 0.5, None)]).write_parquet(
        acts / "ingest_date=2023-03-03" / f"{later:%Y%m%dT%H%M%S%fZ}.parquet")
    pl.DataFrame({"session": [date(2026, 11, 30)] * 3, "security_id": ["meta", "fund", "x"],
                  "symbol": ["META", "FB", "X"], "eligible": [True, False, True], "rank_adv": [1, None, 2],
                  "type": ["common", "fund", "unknown"]}).write_parquet(tmp_path / "curated" / "universe_daily.parquet")
    return tmp_path


def test_get_security_by_symbol_and_date(root):
    assert query.get_security("FB", date(2021, 6, 1), data_dir=root) == "meta"
    assert query.get_security("FB", date(2025, 7, 1), data_dir=root) == "fund"
    assert query.get_security("FB", date(2024, 1, 2), data_dir=root) is None


def test_get_daily_bars_adjusts_by_security_as_known_then(root):
    def closes(as_of, how="all", traded_only=True):
        out = query.get_daily_bars(["meta"], date(2021, 1, 1), date(2026, 1, 1), as_of=as_of, adjust=how,
                                   traded_only=traded_only, data_dir=root)
        return out["symbol"].to_list(), out["close"].to_list()

    # before the split was known: only the FB-era dividend (1/100) adjusts the FB-era day before it
    syms, c = closes(datetime(2023, 2, 1, tzinfo=UTC))
    assert syms == ["FB", "FB", "META"] and c == pytest.approx([99.0, 100.0, 80.0])
    # once known, META's split reaches back across the rename; sessions after as_of's date are cut
    assert closes(datetime(2023, 3, 4, tzinfo=UTC))[1] == pytest.approx([49.5, 50.0, 40.0, 40.0])
    assert closes(datetime(2023, 3, 4, tzinfo=UTC), how="split")[1] == pytest.approx([50.0, 50.0, 40.0, 40.0])
    assert closes(datetime(2023, 3, 4, tzinfo=UTC), how="none")[1] == pytest.approx([100.0, 100.0, 80.0, 40.0])
    assert len(closes(datetime(2023, 3, 4, tzinfo=UTC), traded_only=False)[1]) == 5   # with the filler day
    # as of 2022-06-09 the dividend (ingested 2023-01-05) was not known yet
    assert closes(datetime(2022, 6, 9, 12, tzinfo=UTC))[1] == pytest.approx([100.0, 100.0, 80.0])


def test_get_daily_bars_keeps_a_reused_tickers_holders_apart(root):
    out = query.get_daily_bars(["fund"], date(2025, 1, 1), date(2026, 1, 1), data_dir=root)
    assert out.select("symbol", "close").rows() == [("FB", 20.0)]   # Meta's FB-era dividend is not the fund's


def test_get_universe(root):
    assert query.get_universe(date(2026, 11, 30), data_dir=root)["security_id"].to_list() == ["meta", "x"]
    assert query.get_universe(date(2026, 11, 30), eligible_only=False, data_dir=root).height == 3
    assert query.get_universe(date(2026, 12, 1), data_dir=root).height == 0
