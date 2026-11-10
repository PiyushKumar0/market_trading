"""scripts/macro_mood_study.py: the six registered backtests. Synthetic DuckDB, Yahoo and OI files only."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import sys
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("mt_macro_mood_study", _ROOT / "scripts" / "macro_mood_study.py")
msm = importlib.util.module_from_spec(_spec)
sys.modules["mt_macro_mood_study"] = msm
_spec.loader.exec_module(msm)
mm, rc = msm.mm, msm.rc

from engine.strategy.cost_model import CostModel  # noqa: E402

KOLKATA, NY = ZoneInfo("Asia/Kolkata"), ZoneInfo("America/New_York")
OVERLAP = [date(2022, 7, 12), date(2022, 7, 13), date(2022, 7, 14)]


# ----------------------------------------------------------------------------- synthetic world
def weekdays(n: int, start: date) -> list[date]:
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def ohlc(days: list[date], closes: list[float], opens: list[float] | None = None) -> pd.DataFrame:
    """Bars whose open is never the previous close and always inside [low, high]."""
    if opens is None:
        opens = [closes[0] - 0.5] + [c + 0.5 for c in closes[:-1]]
    return pd.DataFrame({"open": opens, "high": [max(o, c) + 1.0 for o, c in zip(opens, closes, strict=True)],
                         "low": [min(o, c) - 1.0 for o, c in zip(opens, closes, strict=True)], "close": closes},
                        index=pd.Index(days, name="date"))


def _chart_doc(df: pd.DataFrame, tz: ZoneInfo, at: time) -> dict:
    ts = [int(datetime.combine(d, at, tz).timestamp()) for d in df.index]
    quote = {k: [None if (v is None or (isinstance(v, float) and math.isnan(v))) else float(v) for v in df[k]]
             for k in ("open", "high", "low", "close")}
    quote["volume"] = [0] * len(ts)
    return {"chart": {"result": [{"meta": {"exchangeTimezoneName": tz.key, "gmtoffset": 0},
                                  "timestamp": ts, "indicators": {"quote": [quote]}}], "error": None}}


def _closes_frame(s: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({"open": s.values, "high": s.values, "low": s.values, "close": s.values},
                        index=pd.Index(list(s.index), name="date"))


def oi_csv(share: float) -> bytes:
    long = round(share * 1_000_000)
    return ('"Participant wise Open Interest (no. of contracts) in Equity Derivatives",,\n'
            "Client Type,Future Index Long,Future Index Short\n"
            f"Client,10,20\nFII,{long},{1_000_000 - long}\n").encode()


def build_world(root: Path, *, pre: pd.DataFrame | None = None, post: pd.DataFrame | None = None,
                yahoo_post: pd.DataFrame | None = None, spx: pd.Series | None = None,
                brent: pd.Series | None = None, ixic: pd.Series | None = None,
                fii: dict[date, float] | None = None, oi_raw: dict[str, bytes] | None = None,
                stocks: list[tuple] = (), ex: list[tuple] = ()) -> tuple[Path, Path]:
    """External files as the fetch script stores them (with a manifest) and a research DuckDB.

    ``pre``: Yahoo-only NIFTY sessions; ``post``: ``bars_1d`` NIFTY 50 sessions, also in Yahoo (as
    ``yahoo_post`` when given, so the overlap can be made to differ)."""
    post = ohlc(OVERLAP, [100.0, 101.0, 102.0]) if post is None else post
    pre = post.iloc[:0] if pre is None else pre
    days = list(pre.index) + list(post.index)
    flat = pd.Series(100.0, index=days)
    ext = root / "external"
    (ext / "yahoo").mkdir(parents=True)
    (ext / "nse_oi").mkdir()
    yahoo = {
        "^NSEI": (pd.concat([pre, post if yahoo_post is None else yahoo_post]), KOLKATA, time(9, 15)),
        "^GSPC": (_closes_frame(flat if spx is None else spx), NY, time(0, 0)),
        "^IXIC": (_closes_frame(flat if ixic is None else ixic), NY, time(0, 0)),
        "BZ=F": (_closes_frame(flat if brent is None else brent), NY, time(0, 0)),
    }
    files: dict[str, dict] = {}
    for sym, (df, tz, at) in yahoo.items():
        body = json.dumps(_chart_doc(df, tz, at)).encode()
        (ext / "yahoo" / f"{sym}.json").write_bytes(body)
        files[f"yahoo/{sym}.json"] = {"sha256": hashlib.sha256(body).hexdigest()}
    raw = {f"fao_participant_oi_{d:%d%m%Y}.csv": oi_csv(v) for d, v in (fii or {}).items()} | (oi_raw or {})
    for name, body in raw.items():
        (ext / "nse_oi" / name).write_bytes(body)
        files[f"nse_oi/{name}"] = {"sha256": hashlib.sha256(body).hexdigest()}
    (ext / "manifest.json").write_text(json.dumps({"created_at": "2026-11-10T00:00:00Z", "files": files,
                                                   "missing": [], "errors": {}}), encoding="utf-8")
    db = root / "research" / "full_market_test.duckdb"
    db.parent.mkdir(parents=True)
    conn = duckdb.connect(str(db))
    conn.execute('CREATE TABLE bars_1d (symbol TEXT NOT NULL, d DATE NOT NULL, "open" DECIMAL(12,2) NOT NULL, '
                 'high DECIMAL(12,2) NOT NULL, low DECIMAL(12,2) NOT NULL, "close" DECIMAL(12,2) NOT NULL, '
                 "volume BIGINT NOT NULL, src TEXT NOT NULL DEFAULT 'kite_official', PRIMARY KEY (symbol, d))")
    conn.execute("CREATE TABLE corp_actions (symbol TEXT NOT NULL, ex_date DATE NOT NULL, kind TEXT NOT NULL, "
                 "ratio TEXT, amount DECIMAL(12,2), source TEXT, recorded_at TIMESTAMPTZ, "
                 "PRIMARY KEY (symbol, ex_date, kind))")
    rows = [("NIFTY 50", d, r.open, r.high, r.low, r.close, 0) for d, r in post.iterrows()] + list(stocks)
    conn.executemany('INSERT INTO bars_1d (symbol, d, "open", high, low, "close", volume) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     rows)
    if ex:
        conn.executemany("INSERT INTO corp_actions (symbol, ex_date, kind) VALUES (?, ?, ?)", list(ex))
    conn.close()
    return db, ext


def context(db: Path, ext: Path, etfs: frozenset[str] = frozenset()):
    return msm.load_context(rc.open_snapshot(db), ext, etfs)


def stock(sym: str, days: list[date], closes: list[float], volumes: list[int]) -> list[tuple]:
    return [(sym, d, c, c, c, c, v) for d, c, v in zip(days, closes, volumes, strict=True)]


# ----------------------------------------------------------------------------- breadth
def test_breadth_counts_only_liquid_non_etf_names_and_ignores_split_on_d_minus_1(tmp_path):
    s = weekdays(26, date(2023, 1, 2))
    d, d1 = s[25], s[24]
    up = [100.0] * 24 + [101.0, 101.0]
    down = [100.0] * 24 + [99.0, 99.0]
    big = [10_000_000] * 26                                     # Rs 100 cr a session
    fwd_vol = [10_000] * 6 + [10_000_000] * 9 + [10_000] * 10 + [10_000_000]
    rows = (stock("L1", s, up, big) + stock("L2", s, up, big) + stock("L3", s, up, big)
            + stock("L4", s, down, big)
            + stock("NIFTYBEES", s, up, big)                    # ETF by suffix
            + stock("LISTED", s, up, big)                       # ETF by the NSE list
            + stock("THIN", s, up, [100] * 26)                  # Rs 10,000 a session
            + stock("SPLT", s, [100.0] * 24 + [50.0, 50.0], big)   # 1:2 split ex-dated D-1: not a fall
            + stock("FWD", s, up, fwd_vol))
    db, ext = build_world(tmp_path, post=ohlc(s, [100.0 + i for i in range(26)]), stocks=rows,
                          ex=[("SPLT", d1, "split")])
    ctx = context(db, ext, frozenset({"LISTED"}))
    # FWD's 20 sessions to D-1 hold 9 crore-plus values against 11 thin ones (median thin); counting
    # session D instead of its first would make it 10 / 10 and liquid.
    assert mm.liquid_universe(ctx.conn, d1, ctx.etf_symbols) == {"L1", "L2", "L3", "L4"}
    assert "FWD" in mm.liquid_universe(ctx.conn, d, ctx.etf_symbols)       # the fixture discriminates
    sig = msm.signals("breadth_thrust", ctx)
    assert sig.loc[d, "value"] == 75.0                          # 3 of 4; SPLT 60, FWD/ETF/THIN 80
    assert bool(sig.loc[d, "signal"]) is True
    assert sig.loc[d1, "value"] == 0.0 and not sig.loc[d1, "signal"]      # s[23] vs s[22]: flat
    assert math.isnan(sig.loc[s[1], "value"])                  # no liquid name before 20 sessions


# ----------------------------------------------------------------------------- us rebound
def test_us_rebound_trade_is_same_day_open_to_close_with_mis_cost(tmp_path):
    s = weekdays(10, date(2010, 1, 4))
    pre = ohlc(s, [100.0, 101.0, 99.0, 100.0, 98.0, 99.5, 101.0, 100.0, 102.0, 101.0])
    spx = pd.Series([100.0] * 4 + [97.0] * 6, index=s)          # the US session dated s[4] falls 3%
    db, ext = build_world(tmp_path, pre=pre, spx=spx)
    ctx = context(db, ext)
    sig = msm.signals("us_rebound", ctx)
    assert sig.loc[s[5], "value"] == pytest.approx(-3.0)
    assert [x for x in sig.index if sig.loc[x, "signal"]] == [s[5]]
    rows, _ = msm.trades_for(ctx, "us_rebound", mm.SAMPLES["us_rebound"]["confirm"])
    assert len(rows) == 1
    t = rows[0]
    gross = (pre.loc[s[5], "close"] / pre.loc[s[5], "open"] - 1.0) * 100.0
    assert (t.signal_date, t.entry_date, t.exit_date) == (s[5], s[5], s[5])
    assert t.gross == pytest.approx(gross)
    assert t.net == pytest.approx(gross - mm.cost_pct("MIS"))
    assert mm.cost_pct("MIS") != pytest.approx(mm.cost_pct("CNC"))


# ----------------------------------------------------------------------------- excluded window
def test_excluded_window_never_sampled(tmp_path):
    s = [d for d in weekdays(70, date(2026, 8, 3)) if d <= date(2026, 10, 30)]
    brent = pd.Series([100.0 if d < date(2026, 8, 28) else 94.0 if d < date(2026, 9, 15) else 94.0 * 0.94
                       for d in s], index=s)
    db, ext = build_world(tmp_path, post=ohlc(s, [100.0 + (i % 7) for i in range(len(s))]), brent=brent)
    ctx = context(db, ext)
    r = msm.hypothesis("oil_drop", ctx)
    # 08-31 is the last confirm session; its 5-session exit (09-04) may fall in the excluded window
    assert [(t["entry_date"], t["exit_date"]) for t in r["trades"]] == [(date(2026, 8, 31), date(2026, 9, 4))]
    assert r["verdict"]["n"] == 1
    assert r["population_n"] == len([d for d in s if d <= date(2026, 8, 31)])
    assert all(not mm.is_excluded(t["entry_date"]) for t in r["trades"])
    assert [t["entry_date"] for t in r["excluded_window"]["trades"]] == [date(2026, 9, 1), date(2026, 9, 16)]
    # even a window spanning it never samples the excluded sessions
    rows, _ = msm.trades_for(ctx, "oil_drop", (None, None))
    assert [t.entry_date for t in rows] == [date(2026, 8, 31)]
    assert len(msm.population(ctx, (None, None), 5)) == len([d for d in s[:-4] if not mm.is_excluded(d)])


# ----------------------------------------------------------------------------- oil flag
def test_oil_drop_flag_disagreement_reports_null(tmp_path):
    s = weekdays(220, date(2023, 1, 2))
    db, ext = build_world(tmp_path, post=ohlc(s, [99.5] * 220, opens=[100.0] * 220))
    ctx = context(db, ext)
    sig_days = s[0:210:6]                                       # 35 signals, none inside another's hold
    frame = pd.DataFrame({"signal": False, "flagged": False, "value": np.nan}, index=pd.Index(s, name="date"))
    frame.loc[sig_days, "signal"] = True
    frame.loc[sig_days[:31], "flagged"] = True
    ctx.cache["oil_drop"] = frame
    r = msm.hypothesis("oil_drop", ctx)
    assert r["labels"] == {"all": "refuted", "unflagged": "insufficient"}
    assert r["verdict_unflagged"]["n"] == 4 and r["verdict"]["n"] == 35
    assert r["label"] == "null"


# ----------------------------------------------------------------------------- combined
def _fii_change(ctx) -> pd.Series:
    sh = ctx.fii.reindex(ctx.sessions).to_numpy(dtype="float64")
    out = np.full(len(sh), np.nan)
    for k in range(4, len(sh)):
        w = sh[k - 4:k]
        if np.isfinite(w).all():
            out[k] = w[-1] - w[0]
    return pd.Series(out, index=pd.Index(ctx.sessions, name="date"))


def test_combined_requires_all_three_inputs(tmp_path):
    s = weekdays(560, date(2012, 1, 2))
    rng = np.random.default_rng(7)
    spx_r = rng.normal(0.0, 1.0, len(s))
    spx_r[539] = 10.0                                           # the US session before D* rallies 10%
    brent_r = rng.normal(0.0, 1.5, len(s))
    brent_r[539] = -30.0
    spx = pd.Series(100.0 * np.cumprod(1 + spx_r / 100.0), index=s)
    brent = pd.Series(80.0 * np.cumprod(1 + brent_r / 100.0), index=s)
    fii = {d: 0.5 + 0.05 * rng.standard_normal() for i, d in enumerate(s) if i != 537}
    db, ext = build_world(tmp_path, pre=ohlc(s, [100.0 + (i % 9) for i in range(len(s))]), spx=spx, brent=brent,
                          fii=fii)
    ctx = context(db, ext)
    d_star = s[540]                                             # FII D-4..D-1 = s[536..539] spans the gap
    comb = msm.signals("combined", ctx)
    assert math.isnan(comb.loc[d_star, "value"]) and not comb.loc[d_star, "signal"]
    assert msm.signals("combined_arm_us", ctx).loc[d_star, "signal"]    # either input alone would fire
    assert msm.signals("combined_arm_oil", ctx).loc[d_star, "signal"]
    assert math.isnan(msm.signals("combined_arm_fii", ctx).loc[d_star, "value"])
    # independently: the mean of the three trailing z-scores where all exist
    z_us = mm.zscore_trailing(mm.us_overnight(ctx.sessions, spx), mm.Z_WINDOW)
    z_oil = mm.zscore_trailing(-mm.brent_change(ctx.sessions, brent)["pct"], mm.Z_WINDOW)
    z_fii = mm.zscore_trailing(_fii_change(ctx), mm.Z_WINDOW)
    want = pd.concat([z_us, z_oil, z_fii], axis=1).mean(axis=1, skipna=False)
    got = comb["value"]
    assert got.notna().sum() > 250
    assert (got.isna() == want.isna()).all()
    assert np.allclose(got.dropna(), want.dropna())
    assert not comb.loc[got.isna(), "signal"].any()


# ----------------------------------------------------------------------------- fii
def test_fii_signal_voided_across_missing_oi_day(tmp_path):
    s = weekdays(275, date(2012, 1, 2))
    share = [0.5 + 0.001 * (i % 5) + (0.05 if i >= 255 else 0.0) + (0.15 if i >= 263 else 0.0) for i in range(275)]
    fii = {d: v for i, (d, v) in enumerate(zip(s, share, strict=True)) if i != 262}
    db, ext = build_world(tmp_path, pre=ohlc(s, [100.0 + (i % 9) for i in range(275)]), fii=fii)
    ctx = context(db, ext)
    sig = msm.signals("fii_short_cover", ctx)
    assert sig.loc[s[256], "signal"]                            # control: a jump with no gap signals
    assert sig.loc[s[256], "value"] == pytest.approx(ctx.fii[s[255]] - ctx.fii[s[252]])
    for d in s[263:267]:                                        # D-4..D-1 includes the missing s[262]
        assert math.isnan(sig.loc[d, "value"]) and not sig.loc[d, "signal"]
    assert sig.loc[s[262], "value"] == pytest.approx(ctx.fii[s[261]] - ctx.fii[s[258]])
    assert sig.loc[s[267], "value"] == pytest.approx(ctx.fii[s[266]] - ctx.fii[s[263]])
    assert not sig.loc[s[267], "signal"]                        # the +0.15 jump never reads as a change


# ----------------------------------------------------------------------------- report
def _seen_world(tmp_path):
    pre_days, post_days = weekdays(12, date(2010, 1, 4)), weekdays(12, date(2023, 1, 2))
    spx = pd.Series([100.0] * 4 + [97.0] * 8 + [97.0] * 4 + [94.09] * 8, index=pre_days + post_days)
    return build_world(tmp_path, pre=ohlc(pre_days, [100.0 + (i % 5) for i in range(12)]),
                       post=ohlc(post_days, [200.0 + (i % 5) for i in range(12)]), spx=spx), pre_days, post_days


def test_report_marks_seen_windows_not_evidence(tmp_path):
    (db, ext), pre_days, post_days = _seen_world(tmp_path)
    ctx = context(db, ext)
    doc = msm.run(ctx)
    h = doc["hypotheses"]["us_rebound"]
    assert [t["entry_date"] for t in h["trades"]] == [pre_days[5]]
    assert h["verdict"]["n"] == 1
    seen = h["seen (not evidence)"]
    assert seen["summary"]["n"] == 1 and [t["entry_date"] for t in seen["trades"]] == [post_days[5]]
    assert "label" not in seen and "verdict" not in seen
    assert doc["hypotheses"]["oil_drop"]["seen (not evidence)"] is None
    assert doc["hypotheses"]["combined"]["seen (not evidence)"] is not None
    assert "decide nothing" in doc["diagnostics"]["note"]
    text = msm.render(doc)
    assert text.isascii()
    head, _, tail = text.partition("seen (not evidence)")
    assert tail and str(post_days[5]) in tail


def test_cli_writes_reports_and_refuses_a_missing_manifest(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda *a: None)
    (db, ext), _, _ = _seen_world(tmp_path)
    etf = tmp_path / "etf.csv"
    etf.write_text("Symbol\nNIFTYBEES\n", encoding="utf-8")
    out = tmp_path / "reports" / "mm.json"
    assert msm.main(["--db", str(db), "--external", str(ext), "--etf-list", str(etf), "--out", str(out)]) == 0
    doc = json.loads(out.read_text(encoding="utf-8"))
    assert set(doc["hypotheses"]) == {"us_rebound", "us_follow", "oil_drop", "fii_short_cover",
                                      "breadth_thrust", "combined"}
    md = out.with_suffix(".md").read_text(encoding="utf-8")
    assert "unlisted OI on disk 0, 404 missing 0, fetch errors 0" in md
    assert doc["meta"]["manifest"]["oi_unlisted_on_disk"] == 0
    (ext / "manifest.json").unlink()
    assert msm.main(["--db", str(db), "--external", str(ext), "--etf-list", str(etf), "--out", str(out)]) == 2
    assert "manifest" in capsys.readouterr().err


def test_cli_refuses_a_file_that_differs_from_its_manifest(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda *a: None)
    (db, ext), _, _ = _seen_world(tmp_path)
    etf = tmp_path / "etf.csv"
    etf.write_text("Symbol\nNIFTYBEES\n", encoding="utf-8")
    (ext / "yahoo" / "BZ=F.json").write_bytes(b"{}")
    assert msm.main(["--db", str(db), "--external", str(ext), "--etf-list", str(etf)]) == 2
    assert "sha256" in capsys.readouterr().err


def test_cli_refuses_an_oi_file_the_manifest_does_not_list(tmp_path, monkeypatch, capsys):
    # a fetch killed between manifest saves leaves files on disk that fii_long_share would read unverified
    monkeypatch.setattr(rc, "market_hours_refusal", lambda *a: None)
    (db, ext), _, _ = _seen_world(tmp_path)
    etf = tmp_path / "etf.csv"
    etf.write_text("Symbol\nNIFTYBEES\n", encoding="utf-8")
    (ext / "nse_oi" / "fao_participant_oi_04012010.csv").write_bytes(oi_csv(0.4))
    assert msm.main(["--db", str(db), "--external", str(ext), "--etf-list", str(etf)]) == 2
    err = capsys.readouterr().err
    assert "not in the manifest" in err and "--resume" in err


def test_refuses_live_store_path(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda *a: None)
    live = tmp_path / "market.duckdb"
    duckdb.connect(str(live)).close()
    assert msm.main(["--db", str(live), "--external", str(tmp_path)]) == 2
    assert "live store" in capsys.readouterr().err


def test_refuses_in_market_hours(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rc, "market_hours_refusal", lambda *a: "market hours (test)")
    assert msm.main(["--db", str(tmp_path / "x.duckdb"), "--external", str(tmp_path)]) == 2
    assert "market hours" in capsys.readouterr().err


# ----------------------------------------------------------------------------- nsei gate
def test_nsei_gate_drops_bad_opens_and_excludes_a_year_over_10pct():
    y10, y11 = weekdays(20, date(2010, 1, 4)), weekdays(20, date(2011, 1, 3))
    a = ohlc(y10, [100.0 + i for i in range(20)])
    a.loc[y10[5], "open"] = a.loc[y10[4], "close"]                     # open = previous close
    a.loc[y10[9], "open"] = a.loc[y10[9], "high"] + 0.5                 # above the high
    b = ohlc(y11, [200.0 + i for i in range(20)])
    b.loc[y11[3], "open"] = b.loc[y11[2], "close"]
    b.loc[y11[7], "open"] = b.loc[y11[7], "low"] - 0.5                  # below the low
    b.loc[y11[11], "open"] = np.nan
    db_nifty = ohlc(OVERLAP, [300.0, 301.0, 302.0])
    yahoo = pd.concat([a, b, db_nifty + 0.001])                         # float noise under half a paisa
    g = msm.nsei_gate(yahoo, db_nifty)
    assert g["ok"] is True
    assert g["dropped_by_year"][2010]["sessions"] == 20 and g["dropped_by_year"][2010]["dropped"] == 2
    assert g["dropped_by_year"][2011]["dropped"] == 3
    assert g["failed_years"] == [2011]                                  # 2/20 = 10% is not "more than 10%"
    clean = g["clean_days"]
    assert y10[5] not in clean and y10[9] not in clean and y10[6] in clean and y10[0] in clean
    assert not any(d in clean for d in y11)
    assert all(d in clean for d in OVERLAP)


def test_nsei_gate_overlap_mismatch_makes_us_hypotheses_insufficient(tmp_path):
    s = weekdays(200, date(2010, 1, 4))
    r = np.where(np.arange(len(s)) % 6 == 4, -3.0, 0.0)               # signals on s[5], s[11], ...: 33
    spx = pd.Series(100.0 * np.cumprod(1 + r / 100.0), index=s)
    pre = ohlc(s, [100.0 + (i * 37 % 11) - 5 for i in range(len(s))])
    post = ohlc(OVERLAP, [5000.0, 5010.0, 5020.0])
    bad = post.copy()
    bad.loc[OVERLAP[1], "open"] += 1.0
    results = {}
    for tag, yahoo_post in (("match", None), ("mismatch", bad)):
        db, ext = build_world(tmp_path / tag, pre=pre, post=post, yahoo_post=yahoo_post, spx=spx)
        ctx = context(db, ext)
        results[tag] = {n: msm.hypothesis(n, ctx) for n in msm.HYPOTHESES}
    ok, bad_r = results["match"], results["mismatch"]
    assert ok["us_rebound"]["verdict"]["n"] == 33
    assert ok["us_rebound"]["label"] != "insufficient"
    assert not any(r["gate_failed"] for r in ok.values())
    assert bad_r["us_rebound"]["verdict"]["n"] == 33                    # the metrics are still shown
    # every hypothesis with confirm sessions priced from Yahoo ^NSEI is gated; breadth (bars_1d only) is not
    for n in ("us_rebound", "us_follow", "oil_drop", "fii_short_cover", "combined"):
        assert bad_r[n]["label"] == "insufficient" and bad_r[n]["gate_failed"] is True
    assert all(a["gate_failed"] for a in bad_r["combined"]["arms"].values())
    assert bad_r["combined"]["combining_helps"] is None
    assert bad_r["breadth_thrust"]["gate_failed"] is False


# ----------------------------------------------------------------------------- data coverage and diagnostics
def test_oi_coverage_reports_unparsed_files_per_year(tmp_path):
    s = weekdays(6, date(2012, 12, 26))                                  # 2012-12-26 .. 2013-01-02
    fii = {d: 0.4 for d in s[:4]}
    db, ext = build_world(tmp_path, pre=ohlc(s, [100.0 + i for i in range(6)]), fii=fii,
                          oi_raw={f"fao_participant_oi_{s[5]:%d%m%Y}.csv": b"garbage"})
    cov = context(db, ext).oi_coverage
    assert cov[2012] == {"nifty_sessions": 4, "oi_files": 4, "oi_parsed": 4, "unparsed": 0,
                         "sessions_without_oi": 0}
    assert cov[2013] == {"nifty_sessions": 2, "oi_files": 1, "oi_parsed": 0, "unparsed": 1,
                         "sessions_without_oi": 2}


def test_etf_cost_replaces_the_equity_stt_share_on_cnc_only():
    cm = CostModel.from_config()
    bd = cm.round_trip(Decimal("20000"), "CNC")
    stt_pct = float(bd.components["stt"] / Decimal("20000") * 100)
    assert msm.etf_cost_pct("CNC") == pytest.approx(mm.cost_pct("CNC") - stt_pct + 0.001)
    assert msm.etf_cost_pct("MIS") == pytest.approx(mm.cost_pct("MIS"))


def test_atr_stop_exits_at_the_stop_when_a_low_touches_it(tmp_path):
    s = weekdays(30, date(2023, 1, 2))
    post = ohlc(s, [100.0] * 30, opens=[100.0] * 30)                    # TR 2.0 every session: ATR14 = 2.0
    post.loc[s[22], "low"] = 94.0                                       # entry s[20] at 100, stop 100 - 5 = 95
    db, ext = build_world(tmp_path, post=post)
    ctx = context(db, ext)
    t = mm.TradeRow(s[20], s[20], s[24], 0.0, 0.0)
    got = msm.atr_stop_trade(ctx, t, cost=0.1)
    assert got.exit_date == s[22]
    assert got.gross == pytest.approx(-5.0) and got.net == pytest.approx(-5.1)
    post.loc[s[22], "low"] = 95.5
    db2, ext2 = build_world(tmp_path / "b", post=post)
    assert msm.atr_stop_trade(context(db2, ext2), t, cost=0.1).exit_date == s[24]


def test_high_beta_basket_takes_the_top_quintile_and_drops_ex_date_legs(tmp_path):
    n = 262
    s = weekdays(n, date(2023, 1, 2))
    rng = np.random.default_rng(3)
    nr = rng.normal(0.0, 1.0, n)
    nifty = 1000.0 * np.cumprod(1 + nr / 100.0)
    rows = []
    betas = {f"B{i}": 0.2 * (i + 1) for i in range(10)}                 # B9 and B8 are the top quintile
    for sym, b in betas.items():
        px = 100.0 * np.cumprod(1 + b * nr / 100.0)
        rows += stock(sym, s, [round(float(x), 2) for x in px], [10_000_000] * n)
    rows += stock("HIGHBEES", s, [round(float(x), 2) for x in 100.0 * np.cumprod(1 + 5 * nr / 100.0)],
                  [10_000_000] * n)                                     # an ETF never enters the basket
    db, ext = build_world(tmp_path, post=ohlc(s, [round(float(x), 2) for x in nifty]), stocks=rows,
                          ex=[("B8", s[258], "split")])
    ctx = context(db, ext)
    d = s[257]
    assert msm.high_beta_legs(ctx, s[256]) == ["B9", "B8"]
    got = msm.basket_trade(ctx, d, hold=3)                              # window s[257..259] holds B8's ex-date
    assert got is not None
    exit_d, gross, legs, dropped = got
    assert exit_d == s[259] and legs == 1 and dropped == 1
    b9 = {r[1]: r[5] for r in rows if r[0] == "B9"}
    assert gross == pytest.approx((b9[s[259]] / b9[d] - 1.0) * 100.0)
