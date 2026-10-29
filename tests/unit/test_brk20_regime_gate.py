"""C2 applied to the brk20 sweep (plan D-A): crossings originate only in an up NIFTY 50 regime, and the
blocked ones are journalled in brk20_regime_blocked for the rule_blocked arm (D-B)."""

from __future__ import annotations

import threading
from datetime import date, datetime

import pandas as pd
import pytest

from engine.core.clock import IST
from engine.marketdata.store import MarketStore
from engine.ops import main as opsmain
from engine.strategy import regime
from engine.strategy.scanners import brk20
from tests.conftest import FIXED_NOW
from tests.unit.test_ops_main_wiring import _CORE, _WATCH, _WIDE, _seed_index, _sweep_world

Y = date(2026, 6, 16)                                   # the session before FIXED_NOW's
RISING = [1000.0 + i for i in range(70)]
FALLING = RISING[::-1]


def _frame(closes: list[float], last: date) -> pd.DataFrame:
    return pd.DataFrame({"close": closes}, index=pd.bdate_range(end=last, periods=len(closes)))


def _blocked(conn) -> list[dict]:
    return [dict(r) for r in conn.execute("SELECT * FROM brk20_regime_blocked ORDER BY symbol")]


@pytest.mark.parametrize("closes, last, keep, reason", [
    (RISING, Y, True, None),
    (FALLING, Y, False, "down"),
    (RISING[:69], Y, False, "undefined"),
    (RISING, date(2026, 6, 15), False, "index_bar_missing"),
    ([], Y, False, "index_bar_missing"),
])
def test_gate_blocks_when_down_undefined_or_index_bar_stale(closes, last, keep, reason):
    kept, idx, why = opsmain._brk20_regime_gate(_frame(closes, last), Y)
    assert (kept, why) == (keep, reason)
    assert (idx is None) == (reason in ("undefined", "index_bar_missing"))


async def test_gate_disabled_keeps_everything(tmp_path, monkeypatch, migrated_db_template):
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template,
                            configure=lambda s: setattr(s.brk20, "regime_filter", False)) as (c, w):
        _seed_index(c.engine.store, FALLING)
        await c.engine.scan_sweep("window_open")
        assert _blocked(c.engine.conn) == []
    assert [s for sid, s in w.published if sid == brk20.STRATEGY_ID] == [_CORE, _WATCH, _WIDE]


async def test_gate_read_failure_blocks_brk20_only_and_never_raises(tmp_path, monkeypatch, migrated_db_template):
    real = MarketStore.get_bars_1d_frame

    def frame(self, symbol, start, end):
        if symbol == regime.INDEX_SYMBOL:
            raise RuntimeError("duckdb stall")
        return real(self, symbol, start, end)

    monkeypatch.setattr(MarketStore, "get_bars_1d_frame", frame)
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template) as (c, w):
        await c.engine.scan_sweep("window_open")
        assert {r["reason"] for r in _blocked(c.engine.conn)} == {"undefined"}
    published = {sid for sid, _ in w.published}
    assert brk20.STRATEGY_ID not in published and {"ins", "cat", "cat_reversal", "hi52"} <= published


async def test_blocked_crossings_written_on_loop_thread_in_window_only(tmp_path, monkeypatch, migrated_db_template):
    loop_thread, threads = threading.get_ident(), []
    real = opsmain._journal_regime_blocked

    def journal(*args, **kwargs):
        threads.append(threading.get_ident())
        return real(*args, **kwargs)

    monkeypatch.setattr(opsmain, "_journal_regime_blocked", journal)
    async with _sweep_world(tmp_path, monkeypatch, migrated_db_template,
                            now=datetime(2026, 6, 17, 9, 50, tzinfo=IST)) as (c, w):
        e = c.engine
        _seed_index(e.store, FALLING)
        await e.scan_sweep("scan_now")                       # pre-window: journals nothing
        assert _blocked(e.conn) == []
        c.time.set(FIXED_NOW)
        await e.scan_sweep("window_open")
        await e.scan_sweep("scan_now")                       # a second sweep the same day: no duplicates
        rows = _blocked(e.conn)
        resting = e.conn.execute("SELECT count(*) FROM brk20_resting_levels").fetchone()[0]
    assert threads == [loop_thread, loop_thread]
    assert [r["symbol"] for r in rows] == [_CORE, _WATCH, _WIDE]
    assert {k: rows[0][k] for k in ("signal_d", "entry", "stop", "target", "score", "reason")} == {
        "signal_d": "2026-06-17", "entry": "100.00", "stop": "94.00", "target": None, "score": 0.5,
        "reason": "down",
    }
    idx = regime.index_regime(FALLING, Y)
    assert (rows[0]["index_close"], rows[0]["sma50"], rows[0]["sma50_then"]) == (idx.close, idx.sma, idx.sma_then)
    assert resting == 0 and brk20.STRATEGY_ID not in {sid for sid, _ in w.published}
