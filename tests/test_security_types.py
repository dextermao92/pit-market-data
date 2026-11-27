"""pit.security_types: the text and symbol rules on real descriptions, and the cascade on small frames shaped
like the cases met on production data (docs/stage2/p1b-curate-design.md section 2)."""

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from pit import security_types as st


@pytest.mark.parametrize("text, kind", [
    # funds: words, sponsors, closed-end trusts, ETNs
    ("PROSHARES TR ULTRASHORT INDLS", "fund"),
    ("ISHARES TR MSCI EAFE SMALL CAP", "fund"),
    ("SPDR GOLD TR", "fund"),
    ("PIMCO DYNAMIC INCOME FD", "fund"),
    ("BARCLAYS BK PLC IPATH ETN LKD", "fund"),
    ("ROYCE SMALL-CAP TRUST INC COM", "fund"),
    ("VOYA PRIME RATE TR COM SHS BEN", "fund"),
    ("BLACKROCK CALIFORNIA MUNICIPAL", "fund"),
    ("INVESCO ETF TR II PFD ETF", "fund"),          # a preferred-stock ETF is a fund
    # not funds: sponsors' own stock, REITs, BDCs, royalty trusts
    ("BLACKROCK INC", "common"),
    ("WISDOMTREE INVTS INC", "common"),
    ("REALTY INCOME CORP", "common"),
    ("REDWOOD TST INC", "common"),
    ("CORENERGY INFRASTRUCTURE TR CO", "common"),
    ("FRANKLIN BSP RLTY TR INC COM (", "common"),
    ("GOLDMAN SACHS BDC INC SHS", "common"),
    ("Ares Capital Corporation", "common"),
    ("SABINE ROYALTY TR UT BI", "common"),
    ("CROSSAMERICA PARTNERS LP UNIT", "common"),     # a partnership's common unit
    ("FACEBOOK INC CL A COM STK (DE)", "common"),
    # ADRs, including an ADS of a preferred share and "the right to receive"
    ("UNILEVER PLC SPONSORED ADR NEW", "adr"),
    ("BANCO BRADESCO ADS REP ONE PFD", "adr"),
    ("RLX Technology Inc. American Depositary Shares, each representing the right to receive one share", "adr"),
    # other issue types
    ("STAG INDL INC CUM RED PFD SER", "preferred"),
    ("JPMorgan Chase & Co Depositary Shares, each representing a 1/400th interest in a share of Preferred", "preferred"),
    ("B RILEY FINL INC SR NT", "debt"),
    ("GOLDMAN SACHS GROUP INC 6.5% N", "debt"),
    ("CLOVER HEALTH INVTS CORP WT EX", "warrant"),
    ("Pulse Bioscience Rights (underlying PLSE; expiration 06/26/24)", "right"),
    ("BANNER ACQ CORP UNIT 1 CL A &", "unit"),
    ("DIGITAL WORLD ACQUISITION CORP", "spac"),
    ("MONTEREY INNOVATION ACQUISITIO", "spac"),
    ("", None),
    (None, None),
])
def test_classify_text(text, kind):
    assert st.classify_text(text) == kind


@pytest.mark.parametrize("symbol, kind", [
    ("AA.PRA", "preferred"), ("XYZ.WS", "warrant"), ("XYZ.U", "unit"), ("XYZ.RT", "right"),
    ("AIV.WI", "when_issued"), ("XYZ.RTWI", "when_issued"), ("BRK.B", None), ("AAPL", None),
    ("ABCDW", "warrant"), ("ABCDU", "unit"), ("ABCDR", "right"), ("ABCDP", "preferred"), ("SNDKV", "when_issued"),
    ("ABCDY", "adr"), ("GOOGL", None), ("ABCDQ", None),
])
def test_symbol_shape(symbol, kind):
    assert st.symbol_shape(symbol) == kind


@pytest.mark.parametrize("symbol, cqs, alpaca", [
    ("MET$E", "METpE", "MET.PRE"), ("WPAC.R", "WPACr", "WPAC.RT"), ("KCAC.W", "KCAC.WS", "KCAC.WS"),
    ("BRK.A", "BRK.A", "BRK.A"), ("AAPL", None, "AAPL"),
])
def test_directory_symbol(symbol, cqs, alpaca):
    assert st.directory_symbol(symbol, cqs) == alpaca


LAST = date(2026, 11, 20)
NOW = datetime(2026, 11, 21, tzinfo=UTC)


def symbols(*rows) -> pl.DataFrame:
    return pl.DataFrame([{"security_id": s, "symbol": y, "fetch_symbol": y, "valid_from": a, "valid_to": b}
                         for s, y, a, b in rows],
                        schema={"security_id": pl.String, "symbol": pl.String, "fetch_symbol": pl.String,
                                "valid_from": pl.Date, "valid_to": pl.Date})


def ftd(*rows) -> pl.DataFrame:
    return pl.DataFrame([{"symbol": y, "cusip": c, "description": d, "first_date": a, "last_date": b}
                         for y, c, d, a, b in rows],
                        schema={"symbol": pl.String, "cusip": pl.String, "description": pl.String,
                                "first_date": pl.Date, "last_date": pl.Date})


def funds(*rows) -> pl.DataFrame:
    return pl.DataFrame([{"year": y, "ticker": t, "entity_name": e} for y, t, e in rows],
                        schema={"year": pl.Int64, "ticker": pl.String, "entity_name": pl.String})


def assets(*rows) -> pl.DataFrame:
    return pl.DataFrame([{"ingested_at": NOW, "symbol": y, "name": n, "status": s} for y, n, s in rows],
                        schema={"ingested_at": pl.Datetime("us", "UTC"), "symbol": pl.String, "name": pl.String,
                                "status": pl.String})


def directory(*rows) -> pl.DataFrame:
    return pl.DataFrame([{"ingested_at": NOW, "symbol": y, "cqs_symbol": None, "name": n, "etf": e,
                          "test_issue": False} for y, n, e in rows],
                        schema={"ingested_at": pl.Datetime("us", "UTC"), "symbol": pl.String,
                                "cqs_symbol": pl.String, "name": pl.String, "etf": pl.Boolean,
                                "test_issue": pl.Boolean})


def types(sym, f=None, fc=None, a=None, d=None, openings=None) -> dict:
    out = st.build(sym, f if f is not None else ftd(), fc if fc is not None else funds(),
                   a if a is not None else assets(), d if d is not None else directory(), openings)
    return {(r["symbol"], r["valid_from"]): r for r in out.iter_rows(named=True)}


def test_reused_ticker_gets_its_own_type():
    t = types(symbols(("meta", "FB", date(2016, 1, 4), date(2022, 6, 8)),
                      ("meta", "META", date(2022, 6, 9), LAST),
                      ("fbfund", "FB", date(2025, 6, 26), LAST)),
              ftd(("FB", "30303M102", "FACEBOOK INC CL A COM STK (DE)", date(2016, 1, 4), date(2016, 1, 14)),
                  ("META", "30303M102", "META PLATFORMS, INC. CLASS A C", date(2023, 1, 4), date(2023, 1, 9)),
                  ("FB", "74349Y613", "PROSHARES TR S&P 500 DYNAMIC B", date(2025, 6, 27), date(2025, 6, 30))),
              funds((2025, "FB", "PROSHARES TRUST"), (2026, "FB", "PROSHARES TRUST")))
    assert (t[("FB", date(2016, 1, 4))]["type"], t[("FB", date(2016, 1, 4))]["source"]) == ("common", "sec_ftd")
    assert t[("META", date(2022, 6, 9))]["type"] == "common"
    assert (t[("FB", date(2025, 6, 26))]["type"], t[("FB", date(2025, 6, 26))]["source"]) == ("fund", "sec_fund_classes")


def test_fund_list_needs_a_whole_year_when_the_period_spans_years():
    t = types(symbols(("s", "XYZ", date(2016, 1, 4), date(2020, 3, 2)), ("e", "ETFQ", date(2019, 6, 3), date(2020, 2, 3))),
              ftd(("XYZ", "123456109", "XYZ CORP", date(2017, 1, 3), date(2017, 1, 5))),
              funds((2020, "XYZ", "SOME ETF TRUST"), (2020, "ETFQ", "SOME ETF TRUST")))
    assert t[("XYZ", date(2016, 1, 4))]["type"] == "common"   # the fund took the ticker in the stock's last year
    assert t[("ETFQ", date(2019, 6, 3))]["type"] == "fund"     # a short period: any covered year counts


def test_a_specific_name_beats_truncated_common_text():
    t = types(symbols(("t", "TSMX", date(2016, 1, 4), LAST)),
              ftd(("TSMX", "874039100", "TAIWAN SEMICON MFG LTD", date(2016, 1, 4), date(2016, 1, 8))),
              a=assets(("TSMX", "Taiwan Semiconductor American Depositary Shares", "active")))
    assert (t[("TSMX", date(2016, 1, 4))]["type"], t[("TSMX", date(2016, 1, 4))]["source"]) == ("adr", "alpaca_name")


def test_names_only_describe_the_symbols_current_holder():
    t = types(symbols(("old", "OLDX", date(2016, 1, 4), date(2019, 5, 1)), ("x", "AAPL", date(2016, 1, 4), LAST)),
              a=assets(("OLDX", "Some New Fund ETF", "active")))   # active: the ticker belongs to someone new
    assert t[("OLDX", date(2016, 1, 4))]["type"] == "unknown"
    assert t[("OLDX", date(2016, 1, 4))]["flags"] == ["type_unknown"]


def test_nasdaq_etf_flag_and_bdc_label():
    t = types(symbols(("q", "QQQ", date(2016, 1, 4), LAST), ("a", "ARCC", date(2016, 1, 4), LAST)),
              ftd(("ARCC", "04010L103", "ARES CAPITAL CORP", date(2016, 1, 4), date(2016, 1, 8))),
              d=directory(("QQQ", "Invesco QQQ Trust, Series 1", True),
                          ("ARCC", "Ares Capital Corporation - Closed End Fund", False)))
    assert (t[("QQQ", date(2016, 1, 4))]["type"], t[("QQQ", date(2016, 1, 4))]["source"]) == ("fund", "nasdaqtrader")
    assert t[("ARCC", date(2016, 1, 4))]["type"] == "common"


def test_spac_merging_under_its_own_symbol_is_split():
    t = types(symbols(("g", "AGBA", date(2019, 7, 31), date(2024, 10, 15))),
              ftd(("AGBA", "G0120M109", "AGBA ACQUISITION LTD SHS", date(2019, 8, 1), date(2022, 11, 14)),
                  ("AGBA", "G01212102", "AGBA GROUP HLDG LTD SHS (VGB)", date(2022, 11, 16), date(2024, 10, 15))))
    pre, post = t[("AGBA", date(2019, 7, 31))], t[("AGBA", date(2022, 11, 15))]
    assert (pre["type"], pre["valid_to"]) == ("spac", date(2022, 11, 14))
    assert (post["type"], post["source"], post["valid_to"], post["flags"]) == ("common", "sec_ftd", date(2024, 10, 15), ["after_spac"])


def test_spac_by_structure():
    sym = symbols(
        ("u1", "CCIV.U", date(2020, 7, 30), date(2021, 7, 22)),
        ("c", "CCIV", date(2020, 9, 18), date(2021, 7, 23)), ("c", "LCID", date(2021, 7, 26), LAST),  # merged
        ("u2", "SPAQU", date(2018, 8, 10), date(2019, 6, 1)),
        ("s", "SPAQ", date(2018, 10, 2), date(2020, 1, 30)),                                         # opens at $10
        ("u3", "TBLTU", date(2018, 11, 9), date(2019, 1, 7)),
        ("b", "TBLT", date(2018, 12, 3), date(2024, 8, 8)),                                          # opens at $1.40
        ("u4", "BROGU", date(2019, 4, 15), date(2019, 12, 17)),
        ("w", "TWLV", date(2018, 7, 13), date(2019, 4, 12)), ("w", "BROG", date(2019, 4, 15), date(2025, 6, 18)))
    openings = pl.DataFrame({"security_id": ["s", "b", "w"], "symbol": ["SPAQ", "TBLT", "BROG"],
                             "valid_from": [date(2018, 10, 2), date(2018, 12, 3), date(2019, 4, 15)],
                             "opening": [10.01, 1.40, 10.2]})
    t = types(sym, ftd(("TBLT", "89157G102", "TOUGHBUILT INDS INC COM", date(2019, 1, 2), date(2019, 1, 4))),
              openings=openings)
    assert (t[("CCIV", date(2020, 9, 18))]["type"], t[("CCIV", date(2020, 9, 18))]["evidence"]) == ("spac", "CCIV.U")
    assert t[("LCID", date(2021, 7, 26))]["type"] == "unknown"
    assert t[("SPAQ", date(2018, 10, 2))]["type"] == "spac"
    assert t[("TBLT", date(2018, 12, 3))]["type"] == "common"
    assert t[("BROG", date(2019, 4, 15))]["type"] == "unknown"   # renamed units after a merger are not a SPAC
    assert t[("CCIV.U", date(2020, 7, 30))]["type"] == "unit"


def test_report_counts_liquid_securities_by_year():
    sym = symbols(("a", "AAA", date(2020, 1, 2), date(2021, 12, 31)), ("f", "FFF", date(2020, 1, 2), date(2021, 12, 31)),
                  ("u", "UUU", date(2020, 1, 2), date(2021, 12, 31)))
    t = st.build(sym, ftd(("AAA", "000000000", "AAA CORP", date(2020, 1, 2), date(2020, 1, 3))),
                 funds((2020, "FFF", "X ETF TRUST"), (2021, "FFF", "X ETF TRUST")), assets(), directory())
    days = [date(2020, 1, 2) + timedelta(days=i) for i in range(30)]
    liquid = pl.DataFrame({"security_id": ["a"] * 30 + ["f"] * 30 + ["u"] * 30, "session": days * 3})
    rep = st.report(t, liquid)
    assert rep["by_type"] == {"common": 1, "fund": 1, "unknown": 1}
    assert rep["liquid_by_year"]["2020"] == {"by_type": {"common": 1, "fund": 1, "unknown": 1}, "eligible_types": 2,
                                             "unknown_share": 0.5}


def test_run_reads_identity_and_staged_sources(tmp_path):
    ident = tmp_path / "curated" / "identity"
    (ident / "security_days").mkdir(parents=True)
    sym = symbols(("a", "AAA", date(2020, 1, 2), date(2020, 3, 31)))
    sym.write_parquet(ident / "security_symbols.parquet")
    sessions = [date(2020, 1, 2) + timedelta(days=i) for i in range(25)]
    pl.DataFrame({"security_id": ["a"] * 25, "session": sessions, "symbol": ["AAA"] * 25, "close": [20.0] * 25,
                  "volume": [100_000] * 25}).write_parquet(ident / "security_days" / "part-0.parquet")
    staged = tmp_path / "staged"
    for name, df in (("sec_ftd", ftd(("AAA", "000000000", "AAA HOLDINGS INC SPONSORED ADR", date(2020, 1, 2), date(2020, 1, 3)))),
                     ("sec_fund_classes", funds()), ("assets", assets()), ("symbol_directory", directory())):
        (staged / name / "ingest_date=2026-11-21").mkdir(parents=True)
        df.write_parquet(staged / name / "ingest_date=2026-11-21" / "x.parquet")
    rep = st.run(tmp_path)
    out = pl.read_parquet(ident / "security_types.parquet")
    assert out.select("symbol", "type", "source").rows() == [("AAA", "adr", "sec_ftd")]
    assert rep["liquid_by_year"]["2020"]["by_type"] == {"adr": 1}
    assert (ident / "type_report.json").exists()
