from datetime import date, datetime, timezone

import polars as pl

from pit import corporate_actions
from pit.rules import check_bars, check_corporate_actions, check_options

UTC = timezone.utc


def by_key(staged, quarantined, key):
    """{key: rules or flags}: quarantined rows -> ('error', rules), staged rows -> ('warn', flags)."""
    out = {r[key]: ("error", r["rules"]) for r in quarantined.iter_rows(named=True)}
    out |= {r[key]: ("warn", r["flags"]) for r in staged.iter_rows(named=True)}
    return out


def test_option_rules_each_pass_and_fail():
    good = dict(con_id=0, expiry=date(2026, 10, 14), strike=7800.0, right="C", quote_status="two_sided",
                iv=0.2, multiplier=100, ingested_at=datetime(2026, 10, 14, 15, 0, tzinfo=UTC))
    cases = {
        1: {}, 2: {"strike": None}, 3: {"quote_status": "crossed"}, 4: {"expiry": date(2026, 10, 13)},
        5: {"iv": -0.1}, 6: {"multiplier": 10}, 7: {"iv": None},
        # 01:00 UTC on the 14th is still the 13th in New York: a 13 Oct expiry is not yet expired
        8: {"expiry": date(2026, 10, 13), "ingested_at": datetime(2026, 10, 14, 1, 0, tzinfo=UTC)},
        9: {"con_id": 99}, 10: {"con_id": 99},
    }
    rows = [good | c | {"key": k} | ({} if "con_id" in c else {"con_id": k}) for k, c in cases.items()]
    got = by_key(*check_options(pl.DataFrame(rows)), "key")
    assert got == {
        1: ("warn", []), 2: ("error", ["required_fields"]), 3: ("error", ["crossed_quote"]),
        4: ("error", ["expired_contract"]), 5: ("error", ["negative_iv"]),
        6: ("warn", ["non_standard_contract"]), 7: ("warn", ["missing_iv"]), 8: ("warn", []),
        9: ("error", ["duplicate_contract"]), 10: ("error", ["duplicate_contract"]),
    }


def bars(symbol, closes, volumes=None, opens=None):
    return [dict(symbol=symbol, bar_start=datetime(2026, 10, 14, 14, 5 * i, tzinfo=UTC),
                 open=(opens or closes)[i], high=c + 1, low=(opens or closes)[i] - 1 if opens else c - 1, close=c,
                 volume=(volumes or [1000] * len(closes))[i]) for i, c in enumerate(closes)]


def test_bar_rules_each_pass_and_fail():
    rows = (bars("OK", [100, 100.2, 100.1, 100.3])
            + bars("SPLIT", [100, 100.5, 101], opens=[100.2, 100, 100.5])       # prev close 200 -> 2:1 split
            + bars("CRASH", [190, 189, 191])                                   # -5% overnight: no flag
            + bars("TICK", [100, 103, 100.1, 100.2], volumes=[1000, 10, 1000, 1000])
            + bars("BIGVOL", [100, 103, 100.1, 100.2]))                        # same move, real volume
    rows += [dict(rows[0], symbol="DUP"), dict(rows[0], symbol="DUP"),
             dict(rows[0], symbol="BADOHLC", low=101.0), dict(rows[0], symbol="NULL", close=None)]
    prev = pl.DataFrame({"symbol": ["SPLIT", "CRASH"], "prev_close": [200.0, 200.0]})
    staged, quarantined = check_bars(pl.DataFrame(rows), prev)

    def flags(sym):
        return staged.filter(pl.col("symbol") == sym)["flags"].to_list()

    assert flags("OK") == [[]] * 4 and flags("CRASH") == [[]] * 3
    assert flags("SPLIT") == [["overnight_split_ratio_jump"]] * 3
    assert flags("TICK") == [[], ["bad_tick"], [], []] and flags("BIGVOL") == [[]] * 4
    q = {r["symbol"]: r["rules"] for r in quarantined.iter_rows(named=True)}
    assert q == {"DUP": ["duplicate_bar"], "BADOHLC": ["ohlc_invariant"], "NULL": ["required_fields"]}
    assert quarantined.height == 4  # both duplicates are kept as evidence


PAYLOAD = {"ingested_at": "2026-10-15T21:30:00+00:00", "pages": [{"corporate_actions": {
    "forward_splits": [{"id": "f", "symbol": "NVDA", "old_rate": 1, "new_rate": 10, "ex_date": "2024-06-10", "process_date": "2024-06-10"}],
    "reverse_splits": [{"id": "r", "symbol": "XYZ", "old_rate": 10, "new_rate": 1, "ex_date": "2025-01-02", "process_date": "2025-01-02"}],
    "stock_dividends": [{"id": "s", "symbol": "ABC", "rate": 0.05, "ex_date": "2025-02-03", "process_date": "2025-02-03"}],
    "cash_dividends": [
        {"id": "c", "symbol": "KO", "rate": 0.51, "special": False, "ex_date": "2026-09-15", "process_date": "2026-10-01"},
        {"id": "sp", "symbol": "COST", "rate": 15.0, "special": True, "ex_date": "2024-01-02", "process_date": "2024-01-12"},
        {"id": "neg", "symbol": "BAD", "rate": -1.0, "special": False, "ex_date": "2024-01-02", "process_date": "2024-01-12"},
    ],
    "name_changes": [{"id": "n", "old_symbol": "FB", "new_symbol": "META"}],   # not staged
}}]}


def test_corporate_actions_factors_and_rules():
    staged, quarantined = check_corporate_actions(corporate_actions.to_frame(PAYLOAD))
    f = {r["id"]: (r["type"], r["price_factor"], r["cash"], r["flags"]) for r in staged.iter_rows(named=True)}
    assert f == {
        "f": ("forward_split", 0.1, None, []), "r": ("reverse_split", 10.0, None, []),
        "s": ("stock_dividend", 1 / 1.05, None, []), "c": ("cash_dividend", None, 0.51, []),
        "sp": ("cash_dividend", None, 15.0, ["special_dividend"]),
    }
    assert quarantined["id"].to_list() == ["neg"] and quarantined["rules"].to_list() == [["negative_cash"]]
