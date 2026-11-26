"""pit.identity on synthetic bars shaped like the known cases in docs/stage2/p1b-curate-design.md."""

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from pit import identity
from pit.corporate_actions import EVENT_SCHEMA, events_frame

START = date(2020, 1, 1)
SESSIONS = [d for d in (START + timedelta(days=i) for i in range(1200)) if d.weekday() < 5][:700]


def bars(symbol: str, sids, base: float, step: float = 0.01, volume: int = 1000, jump_at: int | None = None,
         factor: float = 1.0) -> list[dict]:
    """One traded bar per session index; close = base + step * sid (x factor from jump_at on)."""
    out = []
    for s in sids:
        c = round((base + step * s) * (factor if jump_at is not None and s >= jump_at else 1.0), 4)
        out.append({"symbol": symbol, "day": SESSIONS[s], "open": c, "high": c, "low": c, "close": c, "vwap": c,
                    "volume": volume, "trade_count": 10})
    return out


def frame(*rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame([r for rs in rows for r in rs], schema={
        "symbol": pl.String, "day": pl.Date, "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
        "close": pl.Float64, "vwap": pl.Float64, "volume": pl.Int64, "trade_count": pl.Int64})


def event(type_: str, symbol: str, sid: int, new_symbol: str | None = None) -> dict:
    return {"ingested_at": datetime(2026, 1, 1, tzinfo=UTC), "id": f"{type_}-{symbol}-{sid}", "type": type_,
            "symbol": symbol, "new_symbol": new_symbol, "alt_symbol": None, "cusip": None, "new_cusip": None,
            "date": SESSIONS[sid], "process_date": SESSIONS[sid], "rate": None, "cash": None}


def events(*rows: dict) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=EVENT_SCHEMA)


def run(b: pl.DataFrame, e: pl.DataFrame | None = None) -> dict:
    return identity.build(b, e if e is not None else events(), SESSIONS)


def periods(out: dict, security_id: str) -> list[tuple[str, date, date]]:
    s = out["security_symbols"].filter(pl.col("security_id") == security_id).sort("valid_from")
    return [(r["symbol"], r["valid_from"], r["valid_to"]) for r in s.iter_rows(named=True)]


def security_of(out: dict, symbol: str, sid: int) -> str:
    return out["security_days"].filter((pl.col("symbol") == symbol) & (pl.col("session") == SESSIONS[sid]))["security_id"].item()


def test_back_mapped_rename_is_one_security_and_a_later_reuse_another():
    # META carries FB's whole history; FB stops at the rename; 300 sessions later an unrelated fund reuses FB.
    b = frame(bars("META", range(0, 400), 100), bars("FB", range(0, 200), 100), bars("FB", range(500, 560), 40))
    out = run(b, events(event("name_change", "FB", 200, "META")))
    meta = security_of(out, "META", 300)
    assert security_of(out, "FB", 100) == meta
    assert periods(out, meta) == [("FB", SESSIONS[0], SESSIONS[199]), ("META", SESSIONS[200], SESSIONS[399])]
    days = out["security_days"].filter(pl.col("security_id") == meta)
    assert days.height == 400 and days["session"].is_unique().all()
    assert set(days["fetch_symbol"]) == {"META"}  # the back-mapped symbol serves the whole history
    fund = security_of(out, "FB", 520)
    assert fund != meta
    assert out["securities"].filter(pl.col("security_id") == meta)["end_reason"].item() == "delisted_unknown"
    assert out["report"]["bars_in_two_securities"] == 0
    assert {"cut": "long_gap", "len": 1} in out["report"]["cuts"]


def test_rename_chain_with_partial_back_mapping():
    # OSTK -> BYON (sid 100) -> NXH (sid 200): NXH carries everything, BYON only its own period.
    b = frame(bars("NXH", range(0, 300), 10), bars("OSTK", range(0, 100), 10), bars("BYON", range(100, 200), 10))
    out = run(b, events(event("name_change", "OSTK", 100, "BYON"), event("name_change", "BYON", 200, "NXH")))
    sec = security_of(out, "NXH", 250)
    assert periods(out, sec) == [("OSTK", SESSIONS[0], SESSIONS[99]), ("BYON", SESSIONS[100], SESSIONS[199]),
                                 ("NXH", SESSIONS[200], SESSIONS[299])]
    assert out["securities"].height == 1


def test_two_complete_copies_tie_to_the_symbol_without_q():
    b = frame(bars("SIVB", range(0, 100), 100), bars("SIVBQ", range(0, 100), 100))
    out = run(b)
    assert out["securities"].height == 1
    sec = out["securities"].row(0, named=True)
    assert sec["first_symbol"] == sec["last_symbol"] == "SIVB"
    assert "tie" in sec["flags"]


def test_rename_without_twins_is_linked_by_the_event_only():
    b = frame(bars("OLD", range(0, 100), 20), bars("NEW", range(100, 200), 20))
    linked = run(b, events(event("name_change", "OLD", 100, "NEW")))
    assert linked["securities"].height == 1
    assert periods(linked, linked["securities"]["security_id"].item()) == [
        ("OLD", SESSIONS[0], SESSIONS[99]), ("NEW", SESSIONS[100], SESSIONS[199])]
    assert run(b)["securities"].height == 2


def test_rename_with_reverse_split_on_the_day_is_linked():
    b = frame(bars("OLD", range(0, 100), 1), bars("NEW", range(100, 200), 1, jump_at=100, factor=10))
    e = events(event("name_change", "OLD", 100, "NEW"), event("reverse_split", "OLD", 100))
    assert run(b, e)["securities"].height == 1


@pytest.mark.parametrize("gap_end, factor, securities, cut", [
    (130, 3.0, 1, None),         # 30-session halt with a big move: still one security
    (180, 1.1, 1, None),         # 80 sessions, similar price
    (180, 3.0, 2, "gap_jump"),   # 80 sessions and a 3x move: a new security
    (400, 1.0, 2, "long_gap"),   # more than 250 sessions: always a new security
])
def test_gap_rules(gap_end, factor, securities, cut):
    b = frame(bars("X", range(0, 100), 30), bars("X", range(gap_end, gap_end + 50), 30, jump_at=gap_end, factor=factor))
    out = run(b)
    assert out["securities"].height == securities
    assert out["report"]["cuts"] == ([{"cut": cut, "len": 1}] if cut else [])


def test_symbol_reused_right_after_a_shared_run_is_cut():
    # A and B are one security until sid 100; B carries on, A becomes an unrelated security at 5x the price.
    b = frame(bars("B", range(0, 200), 50), bars("A", range(0, 100), 50), bars("A", range(100, 200), 250))
    out = run(b)
    first = security_of(out, "A", 50)
    assert security_of(out, "B", 150) == first
    assert security_of(out, "A", 150) != first
    assert periods(out, first)[0] == ("A", SESSIONS[0], SESSIONS[99])
    assert "twin_cut" in out["securities"].filter(pl.col("security_id") == first)["flags"].item()


def test_ticker_handed_over_in_a_merger_is_cut_where_the_twin_run_starts():
    # COT buys the old PRMW and takes its ticker the next session (no gap, similar price); PRMB later carries
    # COT's whole history. PRMW before the takeover is a different security.
    b = frame(bars("PRMB", range(0, 300), 14.5), bars("COT", range(0, 100), 14.5),
              bars("PRMW", range(0, 100), 14.0, step=0.013), bars("PRMW", range(100, 200), 14.5))
    out = run(b)
    cott = security_of(out, "PRMB", 250)
    assert periods(out, cott) == [("COT", SESSIONS[0], SESSIONS[99]), ("PRMW", SESSIONS[100], SESSIONS[199]),
                                  ("PRMB", SESSIONS[200], SESSIONS[299])]
    assert security_of(out, "PRMW", 50) != cott
    assert out["report"]["conflict_days"] == 0


def test_old_name_reused_after_a_short_pause_while_its_twin_trades_is_cut():
    # EAGL and TH are one security until TH carries on alone; 70 sessions later EAGL trades again at a similar price.
    b = frame(bars("TH", range(0, 300), 10), bars("EAGL", range(0, 100), 10), bars("EAGL", range(170, 300), 11, step=0.02))
    out = run(b)
    assert security_of(out, "EAGL", 50) == security_of(out, "TH", 250)
    assert security_of(out, "EAGL", 200) != security_of(out, "TH", 250)
    assert out["report"]["conflict_days"] == 0


def test_twins_that_both_carry_on_smoothly_stay_apart_and_are_flagged():
    b = frame(bars("U1", range(0, 6), 10), bars("U2", range(0, 6), 10),
              bars("U1", range(6, 50), 10, step=0.02), bars("U2", range(6, 50), 10, step=0.03))
    out = run(b)
    assert out["securities"].height == 2
    assert all("smooth_twin" in f for f in out["securities"]["flags"])
    assert out["report"]["bars_in_two_securities"] == 6


def test_filler_bars_are_no_trade_days_and_do_not_extend_the_life():
    filler = [{**r, "volume": 0, "trade_count": 0} for r in bars("F", range(50, 60), 5, step=0)]
    tail = [{**r, "volume": 0, "trade_count": 0} for r in bars("F", range(100, 105), 5, step=0)]
    out = run(frame(bars("F", range(0, 50), 5), filler, bars("F", range(60, 100), 5), tail))
    days = out["security_days"].sort("session")
    assert days.height == 100 and days["session"].max() == SESSIONS[99]
    gap = days.filter(pl.col("no_trade"))
    assert gap.height == 10 and gap["close"].is_null().all() and (gap["volume"] == 0).all()
    assert gap["symbol"].unique().to_list() == ["F"]


def test_how_securities_end():
    b = frame(bars("TWTR", range(0, 100), 40), bars("XLNX", range(0, 120), 140), bars("AMD", range(0, 699), 90),
              bars("SAVA", range(0, 80), 3), bars("GONE", range(0, 60), 7))
    e = events(event("cash_merger", "TWTR", 102), event("stock_merger", "XLNX", 120, "AMD"),
               event("worthless_removal", "SAVA", 85))
    out = run(b, e)
    ends = dict(zip(out["securities"]["last_symbol"], out["securities"]["end_reason"], strict=True))
    assert ends == {"TWTR": "cash_merger", "XLNX": "stock_merger", "AMD": "active", "SAVA": "worthless",
                    "GONE": "delisted_unknown"}
    xlnx = out["securities"].filter(pl.col("last_symbol") == "XLNX").row(0, named=True)
    assert xlnx["successor_id"] == security_of(out, "AMD", 200)


def test_security_id_is_a_hash_of_the_first_symbol_and_session():
    out = run(frame(bars("ABC", range(10, 20), 3)))
    assert out["securities"]["security_id"].item() == identity._security_id("ABC", SESSIONS[10])


def test_events_frame_maps_every_type_but_cash_dividends():
    payload = {"ingested_at": "2026-11-25T23:13:29+00:00", "pages": [{"corporate_actions": {
        "name_changes": [{"id": "n1", "old_symbol": "FB", "new_symbol": "META", "old_cusip": "30303M102",
                          "new_cusip": "30303M102", "process_date": "2022-06-09"}],
        "stock_mergers": [{"id": "m1", "acquiree_symbol": "XLNX", "acquiree_rate": 1, "acquirer_symbol": "AMD",
                           "acquirer_rate": 1.7234, "effective_date": "2022-02-14", "process_date": "2022-02-14"}],
        "cash_mergers": [{"id": "c1", "acquiree_symbol": "TWTR", "rate": 54.2, "effective_date": "2022-10-28",
                          "process_date": "2022-10-28"}],
        "unit_splits": [{"id": "u1", "old_symbol": "AACIU", "old_rate": 1, "new_symbol": "AACI", "new_rate": 1,
                         "alternate_symbol": "AACIW", "alternate_rate": 0.5, "effective_date": "2024-08-16",
                         "process_date": "2024-08-16"}],
        "cash_dividends": [{"id": "d1", "symbol": "AAPL", "rate": 0.25, "ex_date": "2026-11-10",
                            "process_date": "2026-11-13"}],
    }}]}
    ev = events_frame(payload).sort("id")
    assert ev["id"].to_list() == ["c1", "m1", "n1", "u1"]
    rows = {r["id"]: r for r in ev.iter_rows(named=True)}
    assert (rows["n1"]["type"], rows["n1"]["symbol"], rows["n1"]["new_symbol"]) == ("name_change", "FB", "META")
    assert rows["m1"]["rate"] == pytest.approx(1.7234) and rows["m1"]["new_symbol"] == "AMD"
    assert rows["c1"]["cash"] == 54.2 and rows["c1"]["rate"] is None
    assert rows["u1"]["alt_symbol"] == "AACIW" and rows["u1"]["date"] == date(2024, 8, 16)
