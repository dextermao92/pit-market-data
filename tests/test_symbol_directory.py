from datetime import UTC, datetime

import polars as pl

from pit import symbol_directory
from pit.storage import write_raw

NASDAQ = """Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares
AAAP|Pacer Barings CLO Market Flex ETF|G|N|N|100|Y|N
AACG|ATA Creativity Global - American Depositary Shares, each representing two common shares|S|N|D|100|N|N
ZXZZT|NASDAQ TEST STOCK|G|Y|N|100|N|N
File Creation Time: 1125202621:32|||||||
"""
OTHER = """ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol
AAC.W|Ares Acquisition Corporation III Redeemable warrants|N|AAC.WS|N|100|N|AAC+
ABR$D|Arbor Realty Trust 6.375% Series D Preferred Stock|N|ABRpD|N|100|N|ABR-D
GLD|SPDR Gold Shares|P|GLD|Y|40|N|GLD
|Missing Symbol Inc|N||N|100|N|
File Creation Time: 1125202621:32||||||
"""
PAYLOAD = {"ingested_at": "2026-11-26T02:00:00+00:00", "duration_s": 0.4,
           "files": {"nasdaqlisted": NASDAQ, "otherlisted": OTHER}}


def test_both_files_parse_with_exchange_etf_and_symbol_conventions():
    f = symbol_directory.to_frame(PAYLOAD)
    assert f.height == 7
    rows = {r["symbol"]: r for r in f.iter_rows(named=True)}
    assert rows["AAAP"]["etf"] and rows["AAAP"]["exchange"] == "Nasdaq"
    assert not rows["AACG"]["etf"] and rows["AACG"]["financial_status"] == "D"
    assert rows["GLD"]["etf"] and rows["GLD"]["exchange"] == "NYSE Arca"
    assert (rows["AAC.W"]["cqs_symbol"], rows["AAC.W"]["nasdaq_symbol"]) == ("AAC.WS", "AAC+")
    assert rows["ABR$D"]["cqs_symbol"] == "ABRpD"
    assert set(f["file_created"]) == {"1125202621:32"}


def test_staging_quarantines_rows_without_a_symbol_and_flags_test_issues(tmp_path):
    raw = write_raw(tmp_path, "nasdaqtrader", "symbol_directory", PAYLOAD, datetime(2026, 11, 26, 2, tzinfo=UTC))
    path, staged, quarantined = symbol_directory.stage_file(raw)
    assert path == tmp_path / "staged" / "symbol_directory" / "ingest_date=2026-11-26" / f"{raw.stem}.parquet"
    assert staged.height == 6 and quarantined.height == 1
    assert staged.filter(pl.col("symbol") == "ZXZZT")["flags"].item().to_list() == ["test_issue"]
