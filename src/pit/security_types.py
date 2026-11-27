"""Security type per security and symbol period (Stage 2 P1b step 3, docs/stage2/p1b-curate-design.md section 2).

The strategy universe keeps common stocks and ADRs and drops funds, preferreds, notes, warrants, units, rights and
blank-check (SPAC) shares before their merger. Alpaca gives no type, so it comes from a cascade of sources, each
answer recording where it came from:

1. SEC fund series/class files: the ticker was a fund class in a year the period covers -> `fund`;
2. the symbol's shape: `.WS` / `.RT` / `.U` / `.PRx` / `.WI` suffixes and Nasdaq's fifth letter -> warrant,
   right, unit, preferred, when-issued;
3. Nasdaq Trader's ETF flag (current listings) -> `fund`;
4. text: the SEC fail-to-deliver descriptions of the period (dated, so a reused ticker gets its own), Alpaca's
   asset name and Nasdaq Trader's name (the symbol's latest holder only). A specific answer from any of them
   (fund, preferred, ADR, SPAC ...) beats `common`: the fail-to-deliver text is cut at 30 characters and often
   loses the "ETF" or "ADR" at its end;
5. structure: a share that starts trading after a unit of the same root and opens near $10 is a SPAC even when
   its name does not say "Acquisition";
6. else `unknown` (eligible, flagged).

A SPAC's type changes at its merger: the period is split where the fail-to-deliver description stops naming a
blank check, or where its unit stops trading.

Like identity, this uses hindsight on purpose (a description from 2018 types the security in 2017); the
universe built on top must still only use prices before each date.
"""

import json
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from pit.storage import write_parquet

TYPES = ("common", "adr", "fund", "preferred", "debt", "warrant", "right", "unit", "spac", "when_issued", "unknown")
ELIGIBLE = ("common", "adr", "unknown")
PRIORITY = ("fund", "preferred", "debt", "warrant", "right", "unit", "spac", "adr")  # specific text answers
CURRENT_WITHIN = 10        # days before the last session: the symbol's holder is still listed
UNIT_LEAD = 200            # days a SPAC's unit may trade before its shares do
SPAC_PRICE = (9.0, 11.0)   # median close of a SPAC share's first traded sessions
OPENING_SESSIONS = 20
LIQUID_CLOSE, LIQUID_DOLLARS = 1.0, 1_000_000  # the report's "liquid" day (the universe's price and ADV floors)

SPONSOR = (r"ISHARES|SPDR|POWERSHARES|PROSHARES|DIREXION|VANECK|MARKET VECTORS|VANGUARD|IPATH|ETRACS|GRAYSCALE|"
           r"FLEXSHARES|KRANESHARES|GRANITESHS?|GRANITESHARES|ROUNDHILL|YIELDMAX|DEFIANCE|INNOVATOR|TIDAL|ALLIANZIM|"
           r"IBONDS|XTRACKERS|GLOBAL X|FIRST TR|FIRST TRUST|SCHWAB STRATEGIC|SELECT SECTOR|PIMCO|NUVEEN|EATON VANCE|"
           r"CALAMOS|GABELLI|COHEN & STEERS|ABRDN|ABERDEEN|TEMPLETON|BLACKROCK|INVESCO|WISDOMTREE|FRANKLIN|"
           r"GOLDMAN SACHS|FIDELITY|AMPLIFY|PACER|AMERICAN CENTY|DIMENSIONAL|TEUCRIUM|UNITED STATES|MICROSECTORS|"
           r"VELOCITYSHARES|CURRENCYSHARES|GUGGENHEIM|GAMCO|VOYA|ROYCE|WESTERN ASSET|ALLSPRING|NEUBERGER|VIRTUS|"
           r"JOHN HANCOCK|MFS|DWS|KAYNE ANDERSON|TORTOISE|CLEARBRIDGE")
FUND_HINT = (r"\bTR\b|\bTRUST\b|\bINDEX\b|\bSHS\b|\bMUNI?\b|MUNICIPAL|\bINCOME\b|\bINCM\b|\bNOTES?\b|\bSTRATEGY\b|"
             r"\bSTRATEGIC\b|\bBITCOIN\b|\bETHER(EUM)?\b|\bGOLD\b|\bSILVER\b|\bPLATINUM\b|\bOIL\b|\bDAILY\b|\bULTRA|"
             r"\b[0-9]X\b|\bSER\b|\bSHORT\b|\bBUFFER\b")
NOT_FUND = r"\b(BDC|RLTY|REALTY|REIT|BANCORP|FINL|INVTS INC|LTD|PLC)\b"  # a sponsor's own stock, a REIT, a BDC
# Closed-end funds named "... Trust" without "Fund" (Royce Small-Cap Trust, Voya Prime Rate Trust): a trust whose
# name is about what it invests in, and not a REIT, a royalty trust or a bank.
CEF_TRUST = r"\bTR\b|\bTRUST\b|\bTST\b"
CEF_WORDS = (r"\bINCOME\b|\bINCM\b|\bMUNI|\bMUN\b|\bBOND\b|\bBD\b|\bCREDIT\b|PRIME RATE|\bDIVIDEND|\bDIVID?\b|"
             r"OPPORTUNIT|TOTAL RETURN|SMALL[- ]CAP|MICRO[- ]CAP|HIGH YIELD|\bHI YLD\b|CONVERTIBLE|\bCONV\b|\bUTIL|"
             r"TAX[- ]|\bPREMIUM\b|ALLOCATION|FLOATING RATE|\bLOAN\b|INVESTMENT GRADE|\bDURATION\b|"
             r"\bNAT RES\b|NATURAL RESOURCES|\bVALUE TRUST\b|\bSTRATEGIC\b")
NOT_CEF = (r"REALTY|\bRLTY\b|PROPERT|\bPPTYS?\b|\bPROPS\b|\bMTG\b|MORTGA|HOSPITALITY|LODGING|HEALTH|ROYALTY|TIMBER|"
           r"BANCORP|\bBANK|STORAGE|OFFICE|\bINDL\b|INDUSTRIAL|RESIDENTIAL|\bREIT\b|REAL ESTATE|HOTEL|APARTMENT|"
           r"NET LEASE|LOGISTICS|\bRE FIN\b|\bFIN TR\b|ASSETS|CAPITAL TRUST")
AMERICAN_DEPOSITARY = r"AMERICAN DEPOS[IO]TARY|\bAMERN? DEP|\bADRS?\b|\bADS\b|\bSPONSORED\b|\bSPONS?\b|\bGDRS?\b|GLOBAL DEPOSITARY"
RULES = (
    ("warrant", r"\bWARRANTS?\b|\bWTS?\b|\bWRT\b"),
    ("right", r"\bRIGHTS?\b(?! TO\b)|\bRTS\b"),  # not an ADR's "the right to receive"
    ("fund", r"\bETFS?\b|\bETNS?\b|EXCHANGE[- ]TRADED|\bFUNDS?\b|\bFDS?\b|\bCLOSED[- ]END\b"),
    ("adr", AMERICAN_DEPOSITARY),  # before preferred: an ADS of a Brazilian preferred share is the company's equity
    ("preferred", r"\bPFD\b|\bPREF(ERRED)?\b|\bCUM\b.*\bSER\b|\bDEP(OSITARY)? SH.*\d/\d|"
                  r"\b\d+(\.\d+)?% (SER|SERIES|FIXED|CUM|NON-CUM)"),
    ("debt", r"\bNOTES? DUE\b|\bSR NT\b|\bSUB NT\b|\bDEBENTURES?\b|\b\d+(\.\d+)?% (SR )?NOTES?\b|\d% N$"),
    ("sponsor", None),
    ("cef", None),
    ("unit", r"\bUNITS?\b"),
    ("spac", r"\bACQ(U[A-Z]*)?\b|\bACQ\.|ACQUISITION|\bBLANK CHECK\b|\bMERGER CORP\b"),  # cut: ACQUISITIO
    ("adr", r"DEPOSITARY|DEPOSITORY"),
)
PARTNERSHIP = r"\bL\.? ?P\.?\b|\bPARTNERS\b|PARTNERSHIP|\bLLC\b"  # "LP UNIT" is a partnership's common unit
NASDAQ_FIFTH = {"W": "warrant", "U": "unit", "R": "right", "P": "preferred", "O": "preferred", "N": "preferred",
                "M": "preferred", "V": "when_issued", "Y": "adr"}


def classify_text(text: str | None) -> str | None:
    """Type from a security description or name; `common` when it names none of the others, None when empty."""
    if text is None or not text.strip():
        return None
    t = text.upper()
    for kind, rx in RULES:
        if kind == "sponsor":
            if (re.search(rf"\b({SPONSOR})\b", t) and re.search(FUND_HINT, t) and not re.search(NOT_FUND, t)
                    and not re.search(r"\b(INC|CORP|CO|GROUP|HLDGS|HOLDINGS|CORPORATION)\.?$", t.strip())):
                return "fund"
        elif kind == "cef":
            if re.search(CEF_TRUST, t) and re.search(CEF_WORDS, t) and not re.search(NOT_CEF, t):
                return "fund"
        elif not (kind == "unit" and re.search(PARTNERSHIP, t)) and re.search(rx, t):
            return kind
    return "common"


def symbol_shape(symbol: str) -> str | None:
    """Type from the symbol alone (Alpaca's format: BRK.B, AA.PRA, XYZ.WS, XYZ.U, XYZ.RT, XYZ.WI; Nasdaq's
    five-letter symbols carry the issue type in the fifth letter)."""
    if "." in symbol:
        suffix = symbol.split(".", 1)[1]
        if suffix.endswith("WI"):
            return "when_issued"
        for prefix, kind in (("WS", "warrant"), ("RT", "right"), ("PR", "preferred")):
            if suffix.startswith(prefix):
                return kind
        return "unit" if suffix == "U" else None
    return NASDAQ_FIFTH.get(symbol[4]) if len(symbol) == 5 else None


def directory_symbol(symbol: str, cqs_symbol: str | None) -> str:
    """Nasdaq Trader's symbol in Alpaca's format: CQS `METpE` -> MET.PRE, `WPACr` -> WPAC.RT, `XYZw` -> XYZ.WI."""
    s = cqs_symbol or symbol
    return s.replace("p", ".PR").replace("r", ".RT").replace("w", ".WI").replace(".RT.WI", ".RTWI")


def _periods(symbols: pl.DataFrame) -> pl.DataFrame:
    last = symbols["valid_to"].max()
    latest = symbols.sort("valid_to", "valid_from").group_by("symbol").agg(pl.col("security_id", "valid_from").last())
    return (symbols.select("security_id", "symbol", "valid_from", "valid_to")
            .join(latest.with_columns(latest_holder=pl.lit(True)), on=["symbol", "security_id", "valid_from"], how="left")
            .with_columns(latest_holder=pl.col("latest_holder").fill_null(False),
                          current=pl.col("valid_to") >= last - timedelta(days=CURRENT_WITHIN),
                          continues=pl.col("valid_to") < pl.col("valid_to").max().over("security_id"),
                          first=pl.col("valid_from") == pl.col("valid_from").min().over("security_id"),
                          ftd_symbol=pl.col("symbol").str.replace_all(".", "", literal=True)))


def _fund_list(periods: pl.DataFrame, fund_classes: pl.DataFrame) -> pl.DataFrame:
    """Periods whose symbol was a fund class ticker in a year they cover. When the period spans whole years, one
    of those must match: a fund that took over the ticker in the year the stock stopped does not count."""
    f = fund_classes.filter(pl.col("ticker").is_not_null()).group_by("ticker", "year").agg(pl.col("entity_name").first())
    m = (periods.join(f, left_on="symbol", right_on="ticker")
         .filter(pl.col("year").is_between(pl.col("valid_from").dt.year(), pl.col("valid_to").dt.year()))
         .with_columns(inner=pl.col("year").is_between(pl.col("valid_from").dt.year() + 1,
                                                       pl.col("valid_to").dt.year() - 1)))
    return (m.group_by("security_id", "symbol", "valid_from")
            .agg(pl.col("inner").any(), pl.col("entity_name").sort_by("year").last(),
                 spans=(pl.col("valid_to").first().dt.year() - pl.col("valid_from").first().dt.year()) >= 2)
            .filter(pl.col("inner") | ~pl.col("spans")).select("security_id", "symbol", "valid_from", "entity_name"))


def _ftd_evidence(periods: pl.DataFrame, ftd: pl.DataFrame) -> pl.DataFrame:
    """Fail-to-deliver descriptions dated inside each period (symbols compared without the dot: STAGPRC)."""
    rows = (ftd.group_by("symbol", "cusip", "description")
            .agg(pl.col("first_date").min(), pl.col("last_date").max()))
    descs = rows.select("description").unique().with_columns(
        cls=pl.col("description").map_elements(classify_text, return_dtype=pl.String))
    return (periods.select("security_id", "symbol", "valid_from", "valid_to", "ftd_symbol")
            .join(rows.join(descs, on="description"), left_on="ftd_symbol", right_on="symbol")
            .filter((pl.col("first_date") <= pl.col("valid_to")) & (pl.col("last_date") >= pl.col("valid_from")))
            .select("security_id", "symbol", "valid_from", "first_date", "last_date", "cusip", "description", "cls"))


def _name_evidence(periods: pl.DataFrame, assets: pl.DataFrame, directory: pl.DataFrame) -> pl.DataFrame:
    """Names describe a symbol's latest holder today: Alpaca's asset (an active one only while that holder is
    still listed, else the symbol now belongs to someone else) and Nasdaq Trader's current listing."""
    holders = periods.filter("latest_holder").select("security_id", "symbol", "valid_from", "current")
    alp = (assets.sort("ingested_at").group_by("symbol").agg(pl.col("name", "status").last())
           .join(holders, on="symbol")
           .filter((pl.col("status") != "active") | pl.col("current"))
           .select("security_id", "symbol", "valid_from", source=pl.lit("alpaca_name"), name="name", etf=pl.lit(None, pl.Boolean)))
    latest = directory.filter((pl.col("ingested_at").dt.date() == pl.col("ingested_at").dt.date().max())
                              & ~pl.col("test_issue").fill_null(False))
    nd = (latest.with_columns(symbol=pl.struct("symbol", "cqs_symbol").map_elements(
              lambda r: directory_symbol(r["symbol"], r["cqs_symbol"]), return_dtype=pl.String))
          .unique("symbol", keep="last")
          .join(holders.filter("current"), on="symbol")
          # Nasdaq calls BDCs "Closed End Fund" too: leave the type to the name itself (BDCs are common stock)
          .select("security_id", "symbol", "valid_from", source=pl.lit("nasdaqtrader"),
                  name=pl.col("name").str.replace(r"\s*-\s*Closed End Fund\s*$", ""), etf="etf"))
    names = pl.concat([alp, nd]).filter(pl.col("name").is_not_null() | pl.col("etf").is_not_null())
    return names.with_columns(cls=pl.col("name").map_elements(classify_text, return_dtype=pl.String))


def unit_siblings(periods: pl.DataFrame) -> pl.DataFrame:
    """Share periods with a unit of the same root (XYZU, XYZ.U) that started trading up to UNIT_LEAD days
    earlier: the SPAC pattern (units at the IPO, shares and warrants split off weeks later). unit_last is the
    unit's last session, which is about when the merger closes."""
    units = (periods.filter(pl.col("symbol").str.ends_with(".U")
                            | ((pl.col("symbol").str.len_chars() == 5) & pl.col("symbol").str.ends_with("U")))
             .with_columns(root=pl.col("symbol").str.replace(r"\.?U$", ""))
             .group_by("root").agg(unit_symbol=pl.col("symbol").first(), unit_first=pl.col("valid_from").min(),
                                   unit_last=pl.col("valid_to").max()))
    return (periods.join(units, left_on="symbol", right_on="root")
            .filter((pl.col("valid_from") - pl.col("unit_first")).dt.total_days().is_between(0, UNIT_LEAD))
            .select("security_id", "symbol", "valid_from", "valid_to", "unit_symbol", "unit_last"))


def _pick(evidence: list[tuple[str, str, str | None]]) -> tuple[str | None, str | None, str | None, bool]:
    """(type, source, text, conflict) from (source, text, class) items: the highest-priority specific class,
    else common, preferring the fail-to-deliver text (dated) over today's names."""
    specific = [e for e in evidence if e[2] not in (None, "common")]
    if specific:
        best = min(specific, key=lambda e: PRIORITY.index(e[2]))
        others = {e[2] for e in specific} - {best[2]} - ({"unit"} if best[2] == "spac" else set())
        return best[2], best[0], best[1], bool(others)
    common = [e for e in evidence if e[2] == "common"]
    if common:
        best = min(common, key=lambda e: ("sec_ftd", "alpaca_name", "nasdaqtrader").index(e[0]))
        return "common", best[0], best[1], False
    return None, None, None, False


def _row(p: dict, start: date, end: date, kind: str, source: str | None, text: str | None, flags: list[str]) -> dict:
    return {"security_id": p["security_id"], "symbol": p["symbol"], "valid_from": start, "valid_to": end, "type": kind,
            "source": source, "evidence": text, "flags": flags}


def _decide(p: dict, ftd: list[dict], names: list[dict], fund: str | None, unit: dict | None,
            opening: float | None) -> list[dict]:
    start, end = p["valid_from"], p["valid_to"]
    if fund is not None:
        return [_row(p, start, end, "fund", "sec_fund_classes", fund, [])]
    shape = symbol_shape(p["symbol"])
    if shape is not None:
        return [_row(p, start, end, shape, "symbol", p["symbol"], [])]
    etf = [n for n in names if n["etf"]]
    if etf:
        return [_row(p, start, end, "fund", "nasdaqtrader", etf[0]["name"], [])]
    ftd = sorted(ftd, key=lambda r: r["first_date"])
    evidence = [("sec_ftd", r["description"], r["cls"]) for r in reversed(ftd)] + \
               [(n["source"], n["name"], n["cls"]) for n in names]
    kind, source, text, conflict = _pick(evidence)
    flags = ["type_conflict"] if conflict else []
    if kind in (None, "common") and unit is not None and p["first"]:
        # opens near the $10 trust value, or stops with its unit and the security carries on under the target's
        # symbol (a SPAC that traded well above $10 before its merger: IPOE -> SOFI, CCIV -> LCID). Only at the
        # start of a security: a merged company's renamed units (TWLVU -> BROGU) do not make it a SPAC.
        spac_price = opening is not None and SPAC_PRICE[0] <= opening <= SPAC_PRICE[1]
        merged = p["continues"] and abs((end - unit["unit_last"]).days) <= 7
        if spac_price or merged:
            kind, source, text = "spac", "structure", unit["unit_symbol"]
    if kind is None:
        return [_row(p, start, end, "unknown", None, None, ["type_unknown"])]
    if kind != "spac":
        return [_row(p, start, end, kind, source, text, flags)]

    # A SPAC that merges under its own symbol becomes its target there: split where the fail-to-deliver
    # description stops naming a blank check (at the unit's last trade if that falls in between). The unit alone
    # is not enough: units trade thinly, so their last trade can be weeks before the merger.
    unit_last = unit["unit_last"] if unit else None
    spac_end = max((r["last_date"] for r in ftd if r["cls"] == "spac"), default=None)
    after = [r for r in ftd if spac_end and r["first_date"] > spac_end and r["cls"] not in ("spac", "unit", None)]
    cut = end
    if after:
        cut = unit_last if unit_last and spac_end <= unit_last < after[0]["first_date"] else spac_end
    if not start <= cut < end:
        return [_row(p, start, end, "spac", source, text, flags)]
    later = [("sec_ftd", r["description"], r["cls"]) for r in reversed(after)] + \
            [(n["source"], n["name"], n["cls"]) for n in names]
    k2, s2, t2, c2 = _pick([e for e in later if e[2] not in ("spac", "unit")])
    return [_row(p, start, cut, "spac", source, text, flags),
            _row(p, cut + timedelta(days=1), end, k2 or "common", s2 or "after_spac", t2,
                 ["after_spac"] + (["type_conflict"] if c2 else []))]


TYPE_SCHEMA = {"security_id": pl.String, "symbol": pl.String, "valid_from": pl.Date, "valid_to": pl.Date,
               "type": pl.String, "source": pl.String, "evidence": pl.String, "flags": pl.List(pl.String)}


def build(symbols: pl.DataFrame, ftd: pl.DataFrame, fund_classes: pl.DataFrame, assets: pl.DataFrame,
          directory: pl.DataFrame, openings: pl.DataFrame | None = None) -> pl.DataFrame:
    """security_types: one row per (security, symbol period), two for a SPAC that merged under the same symbol.
    symbols: identity's security_symbols. openings: (security_id, symbol, valid_from, opening) median close of
    the first traded sessions, needed only for the unit_siblings periods."""
    periods = _periods(symbols)
    key = ("security_id", "symbol", "valid_from")

    def by_key(df: pl.DataFrame) -> dict:
        out: dict = {}
        for r in df.iter_rows(named=True):
            out.setdefault(tuple(r[k] for k in key), []).append(r)
        return out

    fund = {tuple(r[k] for k in key): r["entity_name"] for r in _fund_list(periods, fund_classes).iter_rows(named=True)}
    ftd_ev, names = by_key(_ftd_evidence(periods, ftd)), by_key(_name_evidence(periods, assets, directory))
    units = {tuple(r[k] for k in key): r for r in unit_siblings(periods).iter_rows(named=True)}
    opens = {} if openings is None else {tuple(r[k] for k in key): r["opening"] for r in openings.iter_rows(named=True)}
    rows = []
    for p in periods.iter_rows(named=True):
        k = tuple(p[c] for c in key)
        rows += _decide(p, ftd_ev.get(k, []), names.get(k, []), fund.get(k), units.get(k), opens.get(k))
    return pl.DataFrame(rows, schema=TYPE_SCHEMA).sort("security_id", "valid_from")


def report(types: pl.DataFrame, liquid: pl.DataFrame | None = None) -> dict:
    """Counts by type and source, flags, and per year the liquid securities (>= 20 liquid days) by type."""
    out = {
        "periods": types.height, "securities": types["security_id"].n_unique(),
        "by_type": dict(types.group_by("type").len().sort("len", descending=True).iter_rows()),
        "by_source": dict(types.group_by(pl.col("source").fill_null("none")).len().sort("len", descending=True).iter_rows()),
        "flags": dict(types.explode("flags", empty_as_null=True).drop_nulls("flags").group_by("flags").len().sort("len", descending=True).iter_rows()),
    }
    if liquid is not None:
        typed = (liquid.sort("session").join_asof(types.select("security_id", "valid_from", "type").sort("valid_from"),
                                                  left_on="session", right_on="valid_from", by="security_id",
                                                  check_sortedness=False)
                 .with_columns(year=pl.col("session").dt.year(), type=pl.col("type").fill_null("untyped")))
        per = (typed.group_by("year", "security_id", "type").len().filter(pl.col("len") >= 20)
               .group_by("year", "type").agg(pl.len().alias("n")).sort("year", "type"))
        years = {}
        for (year,), g in per.group_by("year", maintain_order=True):
            counts = dict(g.select("type", "n").iter_rows())
            eligible = sum(v for t, v in counts.items() if t in ELIGIBLE)
            years[str(year)] = {"by_type": counts, "eligible_types": eligible,
                                "unknown_share": round(counts.get("unknown", 0) / eligible, 4) if eligible else None}
        out["liquid_by_year"] = years
    return out


def _openings(days: pl.LazyFrame, wanted: pl.DataFrame) -> pl.DataFrame:
    """Median close of each wanted period's first OPENING_SESSIONS traded sessions."""
    w = wanted.select("security_id", "symbol", "valid_from", "valid_to").lazy()
    return (days.filter((pl.col("volume") > 0) & pl.col("symbol").is_in(wanted["symbol"].unique().to_list()))
            .select("security_id", "symbol", "session", "close")
            .join(w, on=["security_id", "symbol"])
            .filter(pl.col("session").is_between(pl.col("valid_from"), pl.col("valid_to")))
            .sort("session").group_by("security_id", "symbol", "valid_from")
            .agg(opening=pl.col("close").head(OPENING_SESSIONS).median()).collect())


TS = pl.Datetime("us", "UTC")
SOURCES = {  # staged dataset -> the columns used, typed for when a source is not there yet (a new install, CI)
    "sec_ftd": {"symbol": pl.String, "cusip": pl.String, "description": pl.String, "first_date": pl.Date,
                "last_date": pl.Date},
    "sec_fund_classes": {"year": pl.Int64, "ticker": pl.String, "entity_name": pl.String},
    "assets": {"ingested_at": TS, "symbol": pl.String, "name": pl.String, "status": pl.String},
    "symbol_directory": {"ingested_at": TS, "symbol": pl.String, "cqs_symbol": pl.String, "name": pl.String,
                         "etf": pl.Boolean, "test_issue": pl.Boolean},
}


def _source(staged: Path, dataset: str) -> pl.DataFrame:
    schema = SOURCES[dataset]
    files = sorted((staged / dataset).rglob("*.parquet"))
    return pl.read_parquet(files, columns=list(schema)) if files else pl.DataFrame(schema=schema)


def run(data_dir: Path, out_dir: Path | None = None) -> dict:
    """Reads identity's output and the staged type sources; writes curated/identity/security_types.parquet and
    type_report.json (or under out_dir)."""
    ident = data_dir / "curated" / "identity"
    staged = data_dir / "staged"
    symbols = pl.read_parquet(ident / "security_symbols.parquet")
    days_files = sorted((ident / "security_days").glob("part-*.parquet"))
    days = pl.scan_parquet(days_files)
    openings = _openings(days, unit_siblings(_periods(symbols)))
    types = build(
        symbols,
        *(_source(staged, d) for d in SOURCES), openings)
    liquid = pl.concat([
        pl.scan_parquet(f).filter((pl.col("volume") > 0) & (pl.col("close") >= LIQUID_CLOSE)
                                  & (pl.col("close") * pl.col("volume") >= LIQUID_DOLLARS))
        .select("security_id", "session").collect() for f in days_files])
    rep = report(types, liquid)
    root = out_dir or ident
    write_parquet(types, root / "security_types.parquet")
    (root / "type_report.json").write_text(json.dumps(rep, indent=2, default=str), encoding="utf-8")
    return rep


if __name__ == "__main__":
    print(json.dumps(run(Path(sys.argv[1] if len(sys.argv) > 1 else os.environ.get("DATA_DIR", "data"))), indent=2,
                     default=str))
