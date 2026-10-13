import hashlib
import json

import duckdb
import polars as pl
import pytest

from pit import pipeline
from pit.stage_options import stage_file, to_frame


def contract(strike, right, bid, ask, call_oi=0.0, put_oi=0.0, greeks=True):
    return {
        "conId": int(strike * 10) + (right == "P"), "localSymbol": f"SPXW 261014{right}{int(strike)}",
        "tradingClass": "SPXW", "expiry": "20261014", "strike": strike, "right": right, "multiplier": "100",
        "bid": bid, "bidSize": 1.0, "ask": ask, "askSize": 1.0, "last": None, "lastSize": None,
        "volume": 0.0, "close": 1.0, "callOpenInterest": call_oi, "putOpenInterest": put_oi,
        "marketDataType": 1, "tick_time": "2026-10-14T14:30:01.5+00:00",
        "modelGreeks": {"impliedVol": 0.2, "delta": 0.5, "gamma": 0.01, "vega": 1.0, "theta": -1.0,
                        "optPrice": 10.0, "pvDividend": 0.0, "undPrice": 7800.0} if greeks else None,
    }


PAYLOAD = {
    "ingested_at": "2026-10-14T14:30:00+00:00", "duration_s": 12.3, "config": {}, "errors": [
        {"reqId": -1, "code": 2104, "msg": "farm ok", "conId": None},
        {"reqId": 7, "code": 10197, "msg": "competing live session", "conId": 1},
    ],
    "underlyings": [{"symbol": "SPX", "trading_class": "SPXW", "spot": 7800.0,
                     "spot_time": "2026-10-14T14:30:00.2+00:00", "contracts": [
        contract(7800, "C", 10.0, 10.5, call_oi=500.0),            # two_sided
        contract(7800, "P", 9.0, 9.5, put_oi=700.0),               # two_sided
        contract(9000, "C", -1, 0.05, greeks=False),               # ask_only (-1 = no bid)
        contract(6000, "P", 0.05, None),                           # bid_only
        contract(9500, "C", None, None, greeks=False),             # no_quote
        contract(7810, "C", 11.0, 10.0),                           # crossed
    ]}],
}


def write_raw(tmp_path, payload=PAYLOAD):
    d = tmp_path / "raw/source=ibkr/ingest_date=2026-10-14/option_snapshots"
    d.mkdir(parents=True)
    p = d / "20261014T143000000000Z.json"
    p.write_text(json.dumps(payload))
    return p


def test_quote_status_nulls_and_open_interest():
    df = to_frame(PAYLOAD)
    assert df["quote_status"].to_list() == ["two_sided", "two_sided", "ask_only", "bid_only", "no_quote", "crossed"]
    assert df["bid"][2] is None  # -1 -> null, never 0
    assert df["open_interest"].to_list()[:2] == [500.0, 700.0]  # picked by right
    assert df["iv"][2] is None and df["iv"][0] == 0.2


def test_staging_is_idempotent_and_queryable(tmp_path):
    raw = write_raw(tmp_path)
    path, _ = stage_file(raw)
    assert path == tmp_path / "staged/option_snapshots/ingest_date=2026-10-14/20261014T143000000000Z.parquet"

    def digest():
        df = pl.read_parquet(path).sort("con_id")
        return hashlib.sha256(df.write_csv().encode()).hexdigest()

    first = digest()
    stage_file(raw)
    assert digest() == first
    n = duckdb.sql(f"select count(*) from '{tmp_path}/staged/option_snapshots/*/*.parquet' "
                   "where quote_status = 'two_sided'").fetchone()[0]
    assert n == 2


def test_pipeline_runs_records_success_and_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(pipeline, "run", lambda cfg: write_raw(tmp_path))
    rec = pipeline.collect_option_snapshots("cfg.toml", run_id="r1")
    assert rec["status"] == "success" and rec["contracts"] == 6 and rec["crossed"] == 1
    assert rec["ibkr_error_codes"] == [10197]  # farm status 2104 filtered out

    def boom(cfg):
        raise ConnectionError("gateway down")

    monkeypatch.setattr(pipeline, "run", boom)
    with pytest.raises(ConnectionError):
        pipeline.collect_option_snapshots("cfg.toml", run_id="r2")

    runs = pl.read_parquet(tmp_path / "pipeline_runs/**/*.parquet").sort("run_id")
    assert runs["status"].to_list() == ["success", "failed"]
    assert "gateway down" in runs["error"][1]
