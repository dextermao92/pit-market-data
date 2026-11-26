import io
import json
import zipfile
from datetime import UTC, datetime, timedelta

import httpx
import polars as pl
import pytest

from pit import pipeline, sec
from pit.storage import write_raw_bytes

NOW = datetime(2026, 11, 26, 3, tzinfo=UTC)
FTD_TEXT = """SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE
20160104|781182100|RT|1200|RUBY TUESDAY INC (GA)          |6.01
20160105|781182100|RT|900|RUBY TUESDAY INC (GA)          |6.10
20160108|781182100|RT|300|RUBY TUESDAY INC (GA)          |.
20160105|68557N103|OA|50|ORBITAL ATK INC|79.90
20160106|99999999X||10|NO SYMBOL CORP|1.00
20160106|037833101|BAD|10|BAD CHECK DIGIT CORP|1.00
Trailer total quantity of shares 2460
"""
FUND_TITLE = ("﻿Reporting File Number,CIK Number,Entity Name,Entity Org Type,Series ID,Series Name,Class ID,"
              "Class Name,Class Ticker,Address_1,Address_2,City,State,Zip Code\n"
              "811-01829,0000002110,COLUMBIA ACORN TRUST,30,S000009184,Columbia Acorn Fund,C000024954,Class A,lacax ,"
              "227 W MONROE,,CHICAGO,IL,60606\n"
              "002-33165,0000048200,Nassau Life,32,S000007561,Nassau Life B,C000020626,PHOENIX B,,ONE AMERICAN ROW,,"
              "HARTFORD,CT,06102\n")
FUND_SNAKE = ("rep_file_num,CIK,entity_name,entity_org_type,series_id,series_name,class_id,class_name,class_ticker_symbol,"
              "street1,street2,city,state_code,zip\n"
              "811-22959,0001605941,1290 Funds,30,S000049990,1290 Convertible,C000157852,Class A,TNFAX,1290 AVE,[NULL],"
              "NEW YORK,NY,10104\n"
              "811-00001,0000000001,No Class,30,S000000001,Odd,[NULL],x,[NULL],a,[NULL],b,NY,1\n")
FTD_PAGE = """<a href="/files/data/frequently-requested-foia-document-fails-deliver-data/cnsfails201512b.zip">Dec</a>
<a href="/files/data/frequently-requested-foia-document-fails-deliver-data/cnsfails201601a.zip">Jan a</a>
<a href="/files/node/add/data_distribution/cnsfails202003b.zip">Mar b</a>
<a href="/files/data/fails-deliver-data/cnsfails202610a.zip">Oct a</a>
<a href="/files/data/fails-deliver-data/cnsfails202610a.zip">Oct a again</a>"""
FUND_PAGE = """<a href="/files/x/investment_company_series_class.csv"><span>2016</span></a>
<a href="/files/x/investment_company_series_class_2015.csv">2015</a>
<a href="/files/y/investment-company-series-class-2026.csv">CSV</a>"""


def ftd_zip(name: str = "cnsfails201601a.txt", text: str = FTD_TEXT) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(name, text.encode("latin-1"))
    return buf.getvalue()


@pytest.fixture
def mock_sec(monkeypatch):
    """Serves the pages and files above; returns the list of requested paths. One 429 before the first zip."""
    monkeypatch.setenv("SEC_USER_AGENT", "pit-market-data test@example.com")
    monkeypatch.setattr(sec, "MIN_INTERVAL", 0)
    monkeypatch.setattr(sec.time, "sleep", lambda s: None)
    monkeypatch.setattr(sec, "_stop_by", lambda now: NOW + timedelta(days=1000))
    seen, throttled = [], []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.url.path)
        assert req.headers["user-agent"] == "pit-market-data test@example.com"
        if req.url.path == sec.FTD_PAGE:
            return httpx.Response(200, text=FTD_PAGE)
        if req.url.path == sec.FUND_PAGE:
            return httpx.Response(200, text=FUND_PAGE)
        if req.url.path.endswith(".zip"):
            if not throttled:
                throttled.append(1)
                return httpx.Response(429, headers={"retry-after": "1"})
            period = req.url.path.rsplit("cnsfails", 1)[1][:7]
            return httpx.Response(200, content=ftd_zip(f"cnsfails{period}.txt"))
        if req.url.path.endswith(".csv"):
            return httpx.Response(200, content=(FUND_SNAKE if "class.csv" in req.url.path else FUND_TITLE).encode())
        if req.url.path == sec.TICKERS:
            return httpx.Response(200, json={"fields": ["cik", "name", "ticker", "exchange"],
                                             "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]]})
        return httpx.Response(404)

    real = sec.client
    monkeypatch.setattr(sec, "client", lambda: httpx.Client(
        base_url=sec.BASE, transport=httpx.MockTransport(handler), headers=real().headers))
    return seen


def test_no_request_without_a_contact_user_agent(monkeypatch):
    monkeypatch.delenv("SEC_USER_AGENT", raising=False)
    with pytest.raises(RuntimeError, match="SEC_USER_AGENT"):
        sec.client()


def test_links_come_from_the_pages_whatever_the_path():
    links = sec.ftd_links(FTD_PAGE)
    assert sorted(links) == ["201512b", "201601a", "202003b", "202610a"]
    assert links["202003b"] == "/files/node/add/data_distribution/cnsfails202003b.zip"
    assert sec.fund_links(FUND_PAGE) == {2016: "/files/x/investment_company_series_class.csv",
                                         2015: "/files/x/investment_company_series_class_2015.csv",
                                         2026: "/files/y/investment-company-series-class-2026.csv"}


@pytest.mark.parametrize("cusip, ok", [("037833100", True), ("037833101", False), ("30303M102", True),
                                       ("G1190F107", True), ("68557N103", True), ("12345", False), (None, False)])
def test_cusip_check_digit(cusip, ok):
    assert sec.cusip_ok(cusip) is ok


def test_ftd_staging_groups_by_security_and_skips_the_trailer(tmp_path):
    raw = write_raw_bytes(tmp_path, "sec", "sec_ftd", "cnsfails201601a.zip", ftd_zip(), NOW)
    path, staged, quarantined = sec.stage_ftd(raw)
    assert path.parent == tmp_path / "staged" / "sec_ftd" / "ingest_date=2026-11-26"
    assert quarantined["description"].to_list() == ["NO SYMBOL CORP"]
    rt = staged.filter(pl.col("symbol") == "RT").row(0, named=True)
    assert (rt["period"], rt["cusip"], rt["description"], rt["days"]) == ("201601a", "781182100", "RUBY TUESDAY INC (GA)", 3)
    assert (str(rt["first_date"]), str(rt["last_date"]), rt["last_price"]) == ("2016-01-04", "2016-01-08", None)
    assert staged.filter(pl.col("symbol") == "BAD")["flags"].item().to_list() == ["cusip_check_digit"]


@pytest.mark.parametrize("text, year, ticker", [(FUND_TITLE, 2026, "LACAX"), (FUND_SNAKE, 2016, "TNFAX")])
def test_fund_staging_reads_both_header_styles(tmp_path, text, year, ticker):
    raw = write_raw_bytes(tmp_path, "sec", "sec_fund_classes", f"{year}__f.csv", text.encode(), NOW)
    _, staged, quarantined = sec.stage_fund_classes(raw)
    first = staged.row(0, named=True)
    assert (first["year"], first["ticker"], first["org_type"]) == (year, ticker, "30")
    assert first["file_number"] and first["cik"]
    assert staged.height + quarantined.height == 2
    if year == 2016:
        assert quarantined.height == 1  # [NULL] class id
    else:
        assert staged.filter(pl.col("ticker").is_null())["flags"].item().to_list() == ["no_ticker"]


def test_fetch_ftd_resumes_skips_fetched_files_and_retries_429(tmp_path, mock_sec):
    write_raw_bytes(tmp_path, "sec", "sec_ftd", "cnsfails202003b.zip", ftd_zip(), NOW)  # fetched before
    stats = sec.fetch_ftd(tmp_path, since="201601a")
    zips = [p for p in mock_sec if p.endswith(".zip")]
    assert zips[0] == zips[1]  # the 429 was retried
    assert sorted({p.rsplit("cnsfails", 1)[1] for p in zips}) == ["201601a.zip", "202610a.zip"]  # not 2015, not 2020
    assert stats["files"] == 2 and stats["rows"] == 6 and not stats["stopped"]
    assert len(list(tmp_path.glob("staged/sec_ftd/*/*.parquet"))) == 2
    mock_sec.clear()
    assert sec.fetch_ftd(tmp_path)["files"] == 0
    assert mock_sec == [sec.FTD_PAGE]


def test_fetch_ftd_stops_before_the_collection_window(tmp_path, mock_sec, monkeypatch):
    monkeypatch.setattr(sec, "_stop_by", lambda now: NOW - timedelta(minutes=1))
    stats = sec.fetch_ftd(tmp_path)
    assert stats["stopped"] and stats["files"] == 0


def test_fetch_fund_classes_names_raw_by_year_and_tickers(tmp_path, mock_sec):
    assert sec.fetch_fund_classes(tmp_path, since=2016)["files"] == 2
    names = sorted(f.name.split("__", 1)[1] for f in tmp_path.glob("raw/source=sec/*/sec_fund_classes/*"))
    assert names == ["2016__investment_company_series_class.csv", "2026__investment-company-series-class-2026.csv"]
    t = sec.fetch_tickers(tmp_path)
    assert t["rows"] == 1
    staged = pl.read_parquet(t["staged_path"])
    assert staged.row(0, named=True)["cik"] == "0000320193"
    assert "test@example.com" not in json.dumps(t)


def test_restage_rebuilds_sec_files(tmp_path):
    raw = write_raw_bytes(tmp_path, "sec", "sec_ftd", "cnsfails201601a.zip", ftd_zip(), NOW)
    pipeline.restage(tmp_path, ["sec_ftd"])
    assert (tmp_path / "staged" / "sec_ftd" / "ingest_date=2026-11-26" / f"{raw.name.split('.')[0]}.parquet").exists()
